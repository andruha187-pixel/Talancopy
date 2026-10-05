"""
Копировщик: решение по каждой сделке копируемого кошелька.

ПОКУПКА копируемого → фильтры кошелька → размер (% от их входа или
фиксированная сумма) → потолок цены (их цена + допустимое проскальзывание)
→ проверка живого стакана → ордер (реальный FAK или виртуальный по стакану).

ПРОДАЖА копируемого → продаём ту же ДОЛЮ своей позиции (продали 30% —
продаём 30%). Долю считаем по их позиции до продажи (снимок из Data API +
все сделки, увиденные после).

Каждая попытка (исполнена / пропущена / не исполнилась) пишется в таблицу
copies с причиной, задержкой и проскальзыванием — это и есть материал для
отчётов, по которым видно, что копировать выгодно, а что нет.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque

from config import settings
from src import book_stream, db, http, markets, notifier, positions, state, trader
from src.detector import TargetTrade
from src.util import clamp_price, cut, esc, floor_tick, fmt_age, fnum, money, now_ms

log = logging.getLogger("copier")

MIN_ORDER_USDC = 1.0
SMALL_ACC_TTL = 900
# Их мелкие исполнения одного токена (лимитку часто «съедают» десятками
# кусков по $1-20) копятся, пока не наберут порог «их сделка меньше — не
# копирую». Пауза между исполнениями дольше этого — новая серия.
TARGET_ACC_GAP = 600

REASON_RU = {
    "copy": "копия входа", "mirror": "зеркальная продажа", "sl": "стоп-лосс", "tp": "тейк-профит",
    "trail": "трейлинг-стоп", "maxhold": "время удержания", "manual": "закрыто вручную",
    "sync": "копируемый вышел (сверка)", "resolution": "резолюция рынка",
}

# Позиции копируемых: wallet -> token -> акции (для расчёта доли продажи).
holdings: dict[str, dict[str, float]] = {}
_holdings_ts: dict[str, float] = {}
_recent_fills: dict[str, deque] = {}
_small_acc: dict[tuple, list] = {}
_target_acc: dict[tuple, list] = {}     # (wallet_id, mode, token) -> [их $, время последнего исполнения]
_locks: dict[tuple, asyncio.Lock] = {}
skip_counts: dict[tuple, Counter] = {}
_last_fail_notify: dict[tuple, float] = {}


def _lock(key: tuple) -> asyncio.Lock:
    lk = _locks.get(key)
    if lk is None:
        lk = _locks[key] = asyncio.Lock()
    return lk


# ------------------------------------------------------------- позиции их ----

def _apply_holdings(tt: TargetTrade) -> float:
    """Обновляем их позицию; возвращаем размер ДО этой сделки."""
    book = holdings.setdefault(tt.wallet, {})
    before = book.get(tt.token_id, 0.0)
    delta = tt.shares if tt.side == "BUY" else -tt.shares
    book[tt.token_id] = max(0.0, before + delta)
    _recent_fills.setdefault(tt.wallet, deque(maxlen=200)).append((time.time(), tt.token_id, delta))
    return before


async def sync_holdings(wallet: str) -> dict[str, float] | None:
    """Снимок их открытых позиций из Data API v2 (+ сделки последних 90 с,
    которые индекс мог ещё не учесть)."""
    snap: dict[str, float] = {}
    cursor = None
    for _ in range(5):
        params = {"user": wallet, "status": "OPEN", "limit": 500}
        if cursor:
            params["cursor"] = cursor
        try:
            payload = await http.data_get("/v2/positions", params, limiter=http.poll_limiter, retries=1)
        except Exception as exc:  # noqa: BLE001
            log.debug("Снимок позиций %s: %s", wallet[:10], exc)
            return None
        for r in http.data_items(payload):
            tok = str(r.get("token_id") or r.get("asset") or "")
            size = fnum(r.get("current_size") if r.get("current_size") is not None else r.get("size"))
            if tok and size > 0:
                snap[tok] = snap.get(tok, 0.0) + size
        cursor = http.next_cursor(payload)
        if not cursor:
            break
    now = time.time()
    for ts, tok, delta in _recent_fills.get(wallet, ()):
        if now - ts < 90:
            snap[tok] = max(0.0, snap.get(tok, 0.0) + delta)
    holdings[wallet] = snap
    _holdings_ts[wallet] = now
    return snap


# ------------------------------------------------------------- запись ----

def _record(row: dict) -> None:
    row.setdefault("ts", int(time.time()))
    db.insert_row("copies", row)


def _skip(w, mode: str, tt: TargetTrade | None, reason: str, base: dict, notify_text: bool = True,
          status: str = "skipped") -> None:
    row = dict(base)
    row.update({"status": status, "skip_reason": reason})
    _record(row)
    skip_counts.setdefault((w.id, mode), Counter())[reason.split(" (")[0].split(":")[0][:40]] += 1
    if notify_text and state.get("notify_skips") and w.notify:
        what = f"{tt.side} {esc(cut(tt.title or tt.slug, 40))}" if tt else ""
        notifier.notify(f"⏭ <b>{esc(w.short())}</b> пропуск: {esc(reason)}\n{what}")


def _mode_icon(mode: str) -> str:
    return "🔴" if mode == "live" else "🧪"


def _should_notify(w, mode: str) -> bool:
    if not w.notify:
        return False
    return mode == "live" or bool(state.get("notify_paper"))


# ------------------------------------------------------------- вход ----

async def on_target_trade(tt: TargetTrade) -> None:
    w = state.by_address(tt.wallet)
    before = _apply_holdings(tt)
    if not w or w.mode not in ("paper", "live"):
        return
    # Параллельно: метаданные рынка (для фильтров) и прогрев SDK (для ордера).
    meta_task = asyncio.ensure_future(markets.ensure(tt.token_id, tt.hint(), timeout=2.0))
    if w.mode == "live" and trader.live_possible():
        asyncio.ensure_future(trader.warm(tt.token_id))
    if tt.side == "BUY":
        await _copy_buy(w, tt, meta_task)
    else:
        await _copy_sell(w, tt, before, meta_task)


def _base_row(w, tt: TargetTrade, action: str, reason: str) -> dict:
    latency = int(tt.first_ms - tt.trade_ts * 1000) if tt.trade_ts else None
    return {
        "wallet_id": w.id, "mode": w.mode, "action": action, "reason": reason,
        "token_id": tt.token_id, "condition_id": tt.condition_id, "slug": tt.slug, "title": tt.title,
        "outcome": tt.outcome, "target_price": round(tt.price, 6), "target_usdc": round(tt.usdc, 4),
        "target_tx": tt.tx_hash, "detect_source": tt.first_source, "detect_latency_ms": latency,
    }


def _fill_meta_into(base: dict, meta) -> None:
    base["condition_id"] = base.get("condition_id") or meta.condition_id
    base["slug"] = base.get("slug") or meta.slug
    base["title"] = base.get("title") or meta.title
    base["outcome"] = base.get("outcome") or meta.outcome


def _their_amount(w, mode: str, tt: TargetTrade) -> float | None:
    """Сколько они вложили «этим решением». Исполнения меньше порога
    копятся по токену; как только сумма серии (с паузами не дольше
    TARGET_ACC_GAP) дошла до порога — копируем всю серию разом. None —
    порог пока не набран."""
    key = (w.id, mode, tt.token_id)
    now = time.time()
    acc = _target_acc.get(key)
    if acc and now - acc[1] > TARGET_ACC_GAP:
        acc = None
    total = (acc[0] if acc else 0.0) + tt.usdc
    if total < w.min_target_usdc:
        _target_acc[key] = [total, now]
        return None
    _target_acc.pop(key, None)
    return total


async def _copy_buy(w, tt: TargetTrade, meta_task) -> None:
    mode = w.mode
    base = _base_row(w, tt, "BUY", "copy")
    price = tt.price

    if state.get("paused"):
        return _skip(w, mode, tt, "бот на паузе", base, notify_text=False)
    if mode == "live" and not state.get("live_enabled"):
        return _skip(w, mode, tt, "реальная торговля выключена общим выключателем", base)
    if mode == "live" and not trader.live_possible():
        return _skip(w, mode, tt, "нет POLY_PRIVATE_KEY — реальные сделки невозможны", base)
    age = tt.age_sec()
    if age is not None and age > settings.MAX_SIGNAL_AGE_SEC:
        return _skip(w, mode, tt, f"сигнал устарел ({fmt_age(age)})", base)
    if not (w.min_price <= price <= w.max_price):
        return _skip(w, mode, tt, f"их цена {price:.3f} вне диапазона {w.min_price:g}–{w.max_price:g}", base)
    their_usdc = _their_amount(w, mode, tt)
    if their_usdc is None:
        acc = _target_acc.get((w.id, mode, tt.token_id)) or [0.0]
        return _skip(w, mode, tt, f"их сделка меньше порога — копится (эта {money(tt.usdc)}, "
                                  f"всего {money(acc[0])} из {money(w.min_target_usdc)})",
                     base, notify_text=False, status="accumulating")
    if their_usdc != tt.usdc:
        base["target_usdc"] = round(their_usdc, 4)
    if tt.role == "maker" and not w.copy_maker_fills:
        return _skip(w, mode, tt, "исполнилась их лимитка (мейкер) — такие не копирую", base, notify_text=False)
    existing = positions.get_open(w.id, mode, tt.token_id)
    if existing and not w.copy_adds:
        return _skip(w, mode, tt, "докупки выключены", base)
    if not existing and len(positions.open_positions(w.id, mode)) >= w.max_open:
        return _skip(w, mode, tt, f"лимит открытых позиций ({w.max_open})", base)

    # Данные рынка нужны фильтрам «быстрый рынок» / «лайв». Если фильтры
    # выключены и рынка нет в кэше — не ждём Gamma (минус 100-300 мс),
    # данные подтянутся в фоне.
    need_meta = w.skip_fast or w.skip_live_sports
    cached = markets.get(tt.token_id)
    if need_meta or mode == "paper" or (cached is not None and cached.complete):
        meta = await meta_task
    else:
        meta = markets.put_hint(tt.token_id, tt.hint())
    _fill_meta_into(base, meta)
    if meta.closed or not meta.accepting:
        return _skip(w, mode, tt, "рынок закрыт для торговли", base)
    if need_meta and not meta.complete and not (tt.slug and markets.is_fast_slug(tt.slug)):
        return _skip(w, mode, tt, "нет данных о рынке (Gamma не ответила) — фильтры не проверить", base)
    if w.skip_fast:
        reason = markets.fast_reason(meta, w.min_minutes_left)
        if reason:
            return _skip(w, mode, tt, f"быстрый рынок: {reason}", base, notify_text=False)
    if w.skip_live_sports and markets.in_play(meta):
        return _skip(w, mode, tt, "матч уже идёт (лайв)", base)

    # --- размер ---
    if w.size_mode == "fixed":
        size = w.size_fixed
    else:
        size = their_usdc * w.size_pct / 100.0
    size = min(size, w.max_usdc)
    room = w.max_position_usdc - (existing.cost if existing else 0.0)
    if room < MIN_ORDER_USDC:
        return _skip(w, mode, tt, f"лимит вложений в рынок {money(w.max_position_usdc)}", base)
    size = min(size, room)
    acc_key = (w.id, mode, tt.token_id)
    acc = _small_acc.get(acc_key)
    if acc and time.time() - acc[1] > SMALL_ACC_TTL:
        _small_acc.pop(acc_key, None)
        acc = None
    if size < MIN_ORDER_USDC:
        if not w.accumulate_small:
            return _skip(w, mode, tt, f"наша доля {money(size)} меньше минимального ордера $1", base,
                         notify_text=False)
        total = (acc[0] if acc else 0.0) + size
        if total < MIN_ORDER_USDC:
            _small_acc[acc_key] = [total, acc[1] if acc else time.time()]
            return _skip(w, mode, tt, f"доля {money(size)} < $1 — копится ({money(total)})", base,
                         notify_text=False, status="accumulating")
        size = min(total, w.max_usdc, room)
        _small_acc.pop(acc_key, None)
    elif acc:
        size = min(size + acc[0], w.max_usdc, room)
        _small_acc.pop(acc_key, None)

    # --- реальные: дневные стопы, потолок вложений, баланс ---
    if mode == "live":
        limit_w = w.daily_loss_limit
        if limit_w > 0 and positions.realized_today(w.id) + positions.unrealized_total(w.id) <= -limit_w:
            return _skip(w, mode, tt, f"дневной стоп кошелька {money(limit_w)}", base)
        limit_all = fnum(state.get("daily_loss_limit_live"))
        if limit_all > 0 and positions.realized_today(None) + positions.unrealized_total(None) <= -limit_all:
            return _skip(w, mode, tt, f"общий дневной стоп {money(limit_all)}", base)
        cap_total = fnum(state.get("max_live_exposure"))
        if cap_total > 0:
            free = cap_total - positions.open_cost(None, "live")
            if free < MIN_ORDER_USDC:
                return _skip(w, mode, tt, f"общий потолок вложений {money(cap_total)}", base)
            size = min(size, free)
        bal = trader.cached_balance(max_age=60)
        if bal is not None and bal < size * 1.03:
            if bal * 0.97 >= MIN_ORDER_USDC:
                size = bal * 0.97
            else:
                return _skip(w, mode, tt, f"мало USDC на балансе ({money(bal)})", base)

    # --- потолок цены ---
    tick = meta.tick or book_stream.tick_size(tt.token_id) or 0.01
    slip = max(min(w.max_slip_cents / 100.0, price * w.max_slip_pct / 100.0), tick)
    cap = clamp_price(floor_tick(price + slip, tick), tick)
    base.update({"req_usdc": round(size, 4), "price_cap": cap})
    if book_stream.is_fresh(tt.token_id):
        ask = book_stream.best_ask(tt.token_id)
        if ask is not None and ask > cap + 1e-9:
            base["best_quote"] = ask
            return _skip(w, mode, tt, f"цена ушла: ask {ask:g} > потолок {cap:g}", base)

    # --- исполнение ---
    async with _lock((w.id, mode, tt.token_id)):
        if mode == "paper":
            book = await trader.get_book(tt.token_id)
            if not book:
                return _skip(w, mode, tt, "не удалось получить стакан", base)
            res = trader.simulate_buy(book["asks"], size, cap, meta.fee_rate, meta.fee_exp)
            res.book_source = book.get("source", "")
        else:
            res = await trader.live_buy(tt.token_id, size, cap, tick, meta.fee_rate, meta.fee_exp)
        exec_ms = now_ms() - tt.first_ms
        base.update({"exec_ms": exec_ms, "order_id": res.order_id, "best_quote": res.best_quote})
        if res.status == "delayed":
            base.update({"status": "delayed", "err": "ордер задержан биржей (спорт) — проверю исполнение"})
            _record(dict(base))
            asyncio.ensure_future(_finish_delayed_buy(w, tt, meta, res.order_id, cap, dict(base)))
            return
        if not res.ok or res.shares <= 0:
            base.update({"status": res.status or "failed", "err": res.err})
            _record(base)
            skip_counts.setdefault((w.id, mode), Counter())[("не исполнилось: " + (res.err or ""))[:40]] += 1
            _notify_fail(w, mode, tt, res.err or res.status)
            return
        pos = positions.add_buy(w.id, mode, meta, res.shares, res.usdc, res.fee, price)
    slip_c = (res.avg_price - price) * 100.0
    base.update({"status": res.status, "fill_shares": round(res.shares, 6), "fill_usdc": round(res.usdc, 6),
                 "fill_price": round(res.avg_price, 6), "fee_usdc": round(res.fee, 6),
                 "slippage_cents": round(slip_c, 3), "position_id": pos.id})
    _record(base)
    if _should_notify(w, mode):
        lat = f" · задержка {fmt_age((tt.first_ms - tt.trade_ts * 1000) / 1000)}" if tt.trade_ts else ""
        add = " (докупка)" if pos.entries > 1 else ""
        part = " — частично" if res.status == "partial" else ""
        notifier.notify(
            f"{_mode_icon(mode)} <b>{esc(w.short())}</b> КУПИЛ{add}{part}\n"
            f"{esc(cut(meta.title or tt.title or tt.slug, 70))} — <b>{esc(meta.outcome or tt.outcome or '?')}</b>\n"
            f"Мы: {money(res.usdc)} по {res.avg_price:.3f} (комиссия {money(res.fee)})\n"
            f"Они: {money(tt.usdc)} по {price:.3f} · проскальзывание {slip_c:+.1f}¢{lat}")


async def _finish_delayed_buy(w, tt: TargetTrade, meta, order_id: str, cap: float, base: dict) -> None:
    mode = base.get("mode") or w.mode
    for delay in (5, 15, 45):
        await asyncio.sleep(delay)
        info = await trader.order_fill(order_id)
        if info and info[0] > 0:
            shares, _ = info
            usdc = shares * cap   # точная средняя цена неизвестна — берём потолок (консервативно)
            from src.util import taker_fee
            fee = taker_fee(shares, cap, meta.fee_rate, meta.fee_exp)
            async with _lock((w.id, mode, tt.token_id)):
                pos = positions.add_buy(w.id, mode, meta, shares, usdc, fee, tt.price)
            base.update({"status": "filled", "fill_shares": shares, "fill_usdc": usdc, "fill_price": cap,
                         "fee_usdc": fee, "slippage_cents": (cap - tt.price) * 100, "position_id": pos.id,
                         "err": "исполнен после задержки"})
            _record(base)
            if _should_notify(w, mode):
                notifier.notify(f"{_mode_icon(mode)} <b>{esc(w.short())}</b> КУПИЛ (после задержки биржи)\n"
                                f"{esc(cut(meta.title, 70))} — {esc(meta.outcome or '?')}: "
                                f"{shares:.2f} акций ≤ {cap:.3f}")
            return
    base.update({"status": "unfilled", "err": "задержанный ордер не исполнился"})
    _record(base)


def _notify_fail(w, mode: str, tt: TargetTrade | None, err: str) -> None:
    key = (w.id, mode, (err or "")[:30])
    if time.time() - _last_fail_notify.get(key, 0) < 600:
        return
    _last_fail_notify[key] = time.time()
    if mode == "live" or state.get("notify_skips"):
        what = esc(cut(tt.title or tt.slug, 50)) if tt else ""
        notifier.notify(f"⚠️ <b>{esc(w.short())}</b> {_mode_icon(mode)} не исполнилось: {esc(err)}\n{what}")


# ------------------------------------------------------------- выход ----

async def _copy_sell(w, tt: TargetTrade, before: float, meta_task) -> None:
    """Продают они — продаём ту же долю. Обе книги (виртуальную и реальную):
    если кошелёк переключили из одного режима в другой, позиции, открытые
    в прежнем режиме, тоже должны выйти вслед за копируемым."""
    held = max(before, tt.shares)
    frac = min(1.0, tt.shares / held) if held > 0 else 1.0
    found = False
    for mode in ("live", "paper"):
        pos = positions.get_open(w.id, mode, tt.token_id)
        if pos is None:
            _small_acc.pop((w.id, mode, tt.token_id), None)  # продают — копить докупку незачем
            continue
        found = True
        if not w.copy_sells:
            base = _base_row(w, tt, "SELL", "mirror")
            base["mode"] = mode
            _skip(w, mode, tt, "зеркальные продажи выключены", base, notify_text=False)
            continue
        await sell_position(pos, frac, "mirror", ref_price=tt.price, tt=tt)
    if not found:
        _skip(w, w.mode, tt, "у нас нет позиции", _base_row(w, tt, "SELL", "mirror"), notify_text=False)


async def sell_position(pos: positions.Position, frac: float, reason: str, ref_price: float | None = None,
                        tt: TargetTrade | None = None, retries: int = 3):
    """Продать долю frac позиции. Используется зеркальными продажами,
    стоп-лоссом/тейком/трейлингом, сверкой и кнопкой «Закрыть»."""
    if pos.exiting or pos.status != "open":
        return None
    pos.exiting = True
    w = state.wallet(pos.wallet_id)
    label = w.short() if w else f"кошелёк #{pos.wallet_id}"
    try:
        meta = markets.get(pos.token_id)
        if meta is None or not meta.complete:
            meta = await markets.ensure(pos.token_id, {"slug": pos.slug, "title": pos.title,
                                                       "outcome": pos.outcome}, timeout=2.0)
        tick = meta.tick or 0.01
        shares = pos.shares if frac >= 0.95 else pos.shares * frac
        bid_now = pos.bid()
        if bid_now is not None and shares < pos.shares and (pos.shares - shares) * bid_now < MIN_ORDER_USDC:
            shares = pos.shares  # остаток был бы меньше $1 — продаём всё
        if bid_now is not None and shares * bid_now < MIN_ORDER_USDC and shares < pos.shares:
            if pos.shares * bid_now < 2 * MIN_ORDER_USDC:
                shares = pos.shares
            else:
                row = {"wallet_id": pos.wallet_id, "mode": pos.mode, "action": "SELL", "reason": reason,
                       "status": "skipped", "skip_reason": f"доля продажи {money(shares * bid_now)} < $1",
                       "token_id": pos.token_id, "condition_id": pos.condition_id, "slug": pos.slug,
                       "title": pos.title, "outcome": pos.outcome, "position_id": pos.id,
                       "target_price": ref_price, "target_tx": tt.tx_hash if tt else None}
                _record(row)
                return None
        slip = fnum(state.get("exit_slip_cents"), 5.0) / 100.0
        res = None
        t0 = now_ms()
        for attempt in range(max(1, retries)):
            book = None
            ref = ref_price if (attempt == 0 and ref_price) else None
            if ref is None:
                ref = pos.bid()
            if ref is None or pos.mode == "paper":
                book = await trader.get_book(pos.token_id)
                if ref is None and book and book["bids"]:
                    ref = book["bids"][0][0]
            if ref is None:
                res = trader.ExecResult(ok=False, status="unfilled", err="в стакане нет покупателей")
            else:
                min_price = max(tick, ref - slip * (attempt + 1))
                if pos.mode == "paper":
                    res = trader.simulate_sell(book["bids"] if book else [], shares, min_price,
                                               meta.fee_rate, meta.fee_exp)
                else:
                    res = await trader.live_sell(pos.token_id, shares, min_price, tick, meta.fee_rate, meta.fee_exp)
            if res.ok and res.shares > 0:
                break
            if pos.mode == "live" and res.err and "баланс" in res.err:
                # Акций на кошельке меньше, чем мы думаем (продал вручную на сайте?)
                bal = await trader.token_balance(pos.token_id)
                if bal is not None and 0 < bal < shares:
                    shares = bal
                    continue
            if attempt + 1 < retries:
                await asyncio.sleep(1.5)
        row = {"wallet_id": pos.wallet_id, "mode": pos.mode, "action": "SELL", "reason": reason,
               "token_id": pos.token_id, "condition_id": pos.condition_id, "slug": pos.slug,
               "title": pos.title, "outcome": pos.outcome, "position_id": pos.id,
               "target_price": round(ref_price, 6) if ref_price else None,
               "target_usdc": round(tt.usdc, 4) if tt else None, "target_tx": tt.tx_hash if tt else None,
               "detect_source": tt.first_source if tt else None,
               "detect_latency_ms": int(tt.first_ms - tt.trade_ts * 1000) if tt and tt.trade_ts else None,
               "req_shares": round(shares, 6), "order_id": res.order_id if res else None,
               "exec_ms": (now_ms() - tt.first_ms) if tt else (now_ms() - t0)}
        if not res or not res.ok or res.shares <= 0:
            pos.exit_fail_ts = time.time()
            row.update({"status": (res.status if res else "failed"), "err": res.err if res else "нет ответа"})
            _record(row)
            if w:
                _notify_fail(w, pos.mode, tt, f"{REASON_RU.get(reason, reason)}: {res.err if res else ''}")
            return res
        pnl = positions.add_sell(pos, res.shares, res.usdc, res.fee, reason)
        row.update({"status": res.status, "fill_shares": round(res.shares, 6), "fill_usdc": round(res.usdc, 6),
                    "fill_price": round(res.avg_price, 6), "fee_usdc": round(res.fee, 6),
                    "slippage_cents": round((ref_price - res.avg_price) * 100, 3) if ref_price else None,
                    "pnl_usdc": round(pnl, 6)})
        _record(row)
        if w and _should_notify(w, pos.mode):
            left = "позиция закрыта" if pos.status == "closed" else f"осталось {pos.shares:.2f} акций"
            notifier.notify(
                f"{_mode_icon(pos.mode)} <b>{esc(label)}</b> ПРОДАЛ ({REASON_RU.get(reason, reason)})\n"
                f"{esc(cut(pos.title or pos.slug, 70))} — <b>{esc(pos.outcome or '?')}</b>\n"
                f"{res.shares:.2f} акций по {res.avg_price:.3f} · PnL {money(pnl, sign=True)} · {left}")
        return res
    finally:
        pos.exiting = False


def needs_live() -> bool:
    return bool(state.get("live_enabled")) and any(w.mode == "live" for w in state.wallets())
