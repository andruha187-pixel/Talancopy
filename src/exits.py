"""
Фоновые циклы управления открытыми позициями:

* exit_monitor_loop — каждые 0.5 с: стоп-лосс, тейк-профит, трейлинг-стоп и
  максимальное время удержания по настройкам кошелька. Цена — bid из живого
  стакана (WebSocket), без запросов в сеть.
* mark_loop — раз в 15 с подтягивает bid через REST для позиций, по которым
  WebSocket молчит (тонкие рынки).
* resolution_loop — раз в минуту (и сразу по событию market_resolved из
  WebSocket) проверяет, разрешён ли рынок; закрывает позицию по выплате
  1/0 и, для реальных, забирает выигрыш (auto-claim).
* reconcile_loop — раз в 5 минут сверяет наши позиции с позициями
  копируемого: если он вышел, а мы пропустили его продажу (бот был выключен,
  обрыв связи) — выходим тоже.
"""
from __future__ import annotations

import asyncio
import logging
import time

from src import book_stream, copier, db, http, markets, notifier, positions, state, trader
from src.util import cut, esc, fnum, money

log = logging.getLogger("exits")

_resolve_now = asyncio.Event()
_claimed: set[str] = set()


def _trigger_resolution(_msg: dict) -> None:
    _resolve_now.set()


book_stream.on_market_resolved(_trigger_resolution)


def exit_reason(pos: positions.Position, w, bid: float, now: float) -> str | None:
    avg = pos.avg_price
    if avg <= 0:
        return None
    if w.sl_pct > 0 and bid <= avg * (1 - w.sl_pct / 100.0):
        return "sl"
    if w.tp_pct > 0 and bid >= avg * (1 + w.tp_pct / 100.0):
        return "tp"
    if w.trail_pct > 0 and pos.peak_bid > avg and bid <= pos.peak_bid * (1 - w.trail_pct / 100.0):
        return "trail"
    if w.max_hold_h > 0 and now - pos.opened_ts >= w.max_hold_h * 3600:
        return "maxhold"
    return None


async def exit_monitor_loop() -> None:
    while True:
        await asyncio.sleep(0.5)
        now = time.time()
        for pos in positions.open_positions():
            if pos.exiting or now - pos.exit_fail_ts < 30:
                continue
            w = state.wallet(pos.wallet_id)
            if w is None:
                continue
            if not (w.sl_pct or w.tp_pct or w.trail_pct or w.max_hold_h):
                continue
            bid = pos.bid()
            if bid is None:
                if w.max_hold_h > 0 and now - pos.opened_ts >= w.max_hold_h * 3600:
                    bid = pos.last_bid or 0.0
                else:
                    continue
            if bid > pos.peak_bid:
                pos.peak_bid = bid
            reason = exit_reason(pos, w, bid, now)
            if reason:
                log.info("Выход %s по позиции %s (bid %.3f, вход %.3f)", reason, pos.id, bid, pos.avg_price)
                asyncio.ensure_future(copier.sell_position(pos, 1.0, reason))


async def mark_loop() -> None:
    while True:
        await asyncio.sleep(15)
        stale = [p for p in positions.open_positions() if not book_stream.is_fresh(p.token_id, 15000)]
        tokens = list(dict.fromkeys(p.token_id for p in stale))
        for i in range(0, len(tokens), 20):
            chunk = tokens[i:i + 20]
            try:
                books = await http.clob_post("/books", [{"token_id": t} for t in chunk], timeout=6.0)
            except Exception as exc:  # noqa: BLE001
                log.debug("POST /books: %s", exc)
                continue
            now = time.time()
            for b in books if isinstance(books, list) else []:
                tok = str(b.get("asset_id") or "")
                bids = [fnum(x.get("price")) for x in (b.get("bids") or []) if fnum(x.get("size")) > 0]
                best = max(bids) if bids else None
                for p in stale:
                    if p.token_id == tok and best is not None:
                        p.last_bid = best
                        p.last_bid_ts = now
                        p.peak_bid = max(p.peak_bid, best)


async def _backfill_meta(open_pos: list) -> None:
    """Позиции, открытые без данных рынка (сделку увидели в блокчейне, а
    Gamma ещё не ответила), — дозаполняем condition_id/название/конец."""
    for p in open_pos:
        if p.condition_id and p.title:
            continue
        meta = await markets.ensure(p.token_id, timeout=5.0)
        if meta.condition_id:
            p.condition_id = p.condition_id or meta.condition_id
            p.slug = p.slug or meta.slug
            p.title = p.title or meta.title
            p.outcome = p.outcome or meta.outcome
            p.end_ts = p.end_ts or meta.end_ts
            positions.persist(p)


async def check_resolutions() -> int:
    open_pos = positions.open_positions()
    await _backfill_meta(open_pos)
    cids = [p.condition_id for p in open_pos if p.condition_id]
    if not cids:
        return 0
    fresh = await markets.refresh_conditions(cids)
    settled = 0
    for p in open_pos:
        m = fresh.get(p.condition_id or "")
        if not m:
            continue
        payouts = markets.resolution_payouts(m)
        if not payouts or p.token_id not in payouts:
            continue
        payout = payouts[p.token_id]
        shares = p.shares
        pnl = positions.settle(p, payout, "resolution")
        db.insert_row("copies", {
            "ts": int(time.time()), "wallet_id": p.wallet_id, "mode": p.mode, "action": "SETTLE",
            "reason": "resolution", "status": "filled", "token_id": p.token_id, "condition_id": p.condition_id,
            "slug": p.slug, "title": p.title, "outcome": p.outcome, "fill_shares": round(shares, 6),
            "fill_usdc": round(shares * payout, 6), "fill_price": payout, "fee_usdc": 0.0,
            "position_id": p.id, "pnl_usdc": round(pnl, 6)})
        settled += 1
        w = state.wallet(p.wallet_id)
        if w and (w.notify and (p.mode == "live" or state.get("notify_paper"))):
            icon = "🏆" if payout >= 1 else ("🤝" if payout > 0 else "💀")
            notifier.notify(f"{icon} {'🔴' if p.mode == 'live' else '🧪'} <b>{esc(w.short())}</b> рынок разрешён\n"
                            f"{esc(cut(p.title or p.slug, 70))} — {esc(p.outcome or '?')}: "
                            f"выплата {payout:g} за акцию · PnL {money(pnl, sign=True)}")
        if p.mode == "live" and payout > 0 and state.get("auto_claim") and p.condition_id not in _claimed:
            _claimed.add(p.condition_id)
            asyncio.ensure_future(_claim(p.condition_id, p.title or p.slug or ""))
    return settled


async def _claim(condition_id: str, title: str) -> None:
    try:
        status = await trader.redeem(condition_id)
        log.info("Выигрыш забран (%s): %s", condition_id[:12], status)
        db.write("UPDATE positions SET redeemed = 1 WHERE condition_id = ? AND mode = 'live'", (condition_id,))
    except Exception as exc:  # noqa: BLE001
        notifier.notify(f"⚠️ Не удалось автоматически забрать выигрыш по «{esc(cut(title, 60))}»: "
                        f"{esc(str(exc)[:200])}\nЗабери вручную на сайте (Portfolio → Claim).")


async def resolution_loop() -> None:
    while True:
        try:
            await asyncio.wait_for(_resolve_now.wait(), timeout=60)
            await asyncio.sleep(3)   # даём Gamma обновиться после события из WebSocket
        except asyncio.TimeoutError:
            pass
        _resolve_now.clear()
        try:
            await check_resolutions()
        except Exception as exc:  # noqa: BLE001
            log.warning("Проверка резолюций: %s", exc)


async def reconcile_once() -> int:
    exits = 0
    now = time.time()
    for w in state.wallets():
        if w.mode not in ("paper", "live"):
            continue
        snap = await copier.sync_holdings(w.address)
        if snap is None or not w.copy_sells:
            continue
        for p in positions.open_positions(w.id):
            if p.exiting or now - p.opened_ts < 180:
                continue
            meta = markets.get(p.token_id)
            if (p.end_ts and now >= p.end_ts) or (meta and meta.closed):
                continue  # рынок закончился — это забота резолюции, не продажи
            if snap.get(p.token_id, 0.0) > 0:
                p.target_gone_seen = 0
                continue
            p.target_gone_seen += 1
            if p.target_gone_seen >= 2:
                log.info("Копируемый %s больше не держит токен позиции %s — выходим", w.short(), p.id)
                asyncio.ensure_future(copier.sell_position(p, 1.0, "sync"))
                exits += 1
    return exits


async def reconcile_loop() -> None:
    await asyncio.sleep(20)
    while True:
        try:
            await reconcile_once()
        except Exception as exc:  # noqa: BLE001
            log.warning("Сверка позиций: %s", exc)
        await asyncio.sleep(300)
