"""
Источник №3 — опрос Data API v2 (/v2/activity?user=…&type=TRADE).

Самый медленный (Data API индексирует блокчейн с задержкой), но самый
надёжный: если потоки оборвались или провайдер Polygon прилёг, сделка всё
равно будет поймана, просто позже. Плюс отсюда приходят название рынка и
исход — для уведомлений.

Первый опрос кошелька только ставит «отметку времени» — старую историю не
копируем, только новые сделки после старта/добавления.

ВНИМАНИЕ: Data API v1 (/activity без /v2) Polymarket отключает 24.10.2026,
поэтому здесь сразу v2: ответ в {"data": [...]}, поля snake_case.
"""
from __future__ import annotations

import asyncio
import logging
import time

from config import settings
from src import http, state
from src.detector import TargetFill, detector
from src.util import fnum, now_ms

log = logging.getLogger("poller")

PAGE = 50
_watermark: dict[str, float] = {}
_seen: dict[str, set] = {}
stats = {"polls": 0, "errors": 0, "rows": 0, "last_ok_ms": 0, "last_error": ""}


def _row_uid(r: dict) -> str:
    return "|".join(str(r.get(k, "")) for k in ("transaction_hash", "token_id", "side", "size", "price", "timestamp"))


def rows_to_fills(wallet: str, rows: list[dict]) -> list[TargetFill]:
    """Строки одной транзакции по одному токену и стороне складываем —
    это один ордер копируемого, исполненный о несколько встречных."""
    groups: dict[tuple, TargetFill] = {}
    detected = now_ms()
    for r in rows:
        if str(r.get("type") or "TRADE").upper() != "TRADE":
            continue
        side = str(r.get("side") or "").upper()
        token = str(r.get("token_id") or r.get("asset") or "")
        shares = fnum(r.get("size"))
        usdc = fnum(r.get("usdc_size")) or shares * fnum(r.get("price"))
        if side not in ("BUY", "SELL") or not token or shares <= 0 or usdc <= 0:
            continue
        tx = str(r.get("transaction_hash") or r.get("transactionHash") or "").lower()
        ts = fnum(r.get("timestamp")) or None
        if ts and ts > 1e12:
            ts /= 1000.0
        key = (tx or f"poll-{ts}", token, side)
        f = groups.get(key)
        if f is None:
            groups[key] = TargetFill(
                wallet=wallet, token_id=token, side=side, shares=shares, usdc=usdc, tx_hash=key[0],
                source="poll", detected_ms=detected, uid=_row_uid(r), trade_ts=ts, complete=True,
                condition_id=r.get("condition_id"), slug=r.get("slug"), title=r.get("title"),
                outcome=r.get("outcome"), event_slug=r.get("event_slug"))
        else:
            f.shares += shares
            f.usdc += usdc
            f.uid += "+" + _row_uid(r)
    return list(groups.values())


async def fetch_activity(wallet: str, limit: int = PAGE, cursor: str | None = None,
                         limiter=None, timeout: float = 6.0):
    params = {"user": wallet, "type": "TRADE", "limit": limit}
    if cursor:
        params["cursor"] = cursor
    return await http.data_get("/v2/activity", params, limiter=limiter or http.poll_limiter,
                               retries=0, timeout=timeout)


async def poll_wallet(wallet: str) -> int:
    payload = await fetch_activity(wallet)
    rows = http.data_items(payload)
    stats["polls"] += 1
    stats["last_ok_ms"] = now_ms()

    def ts_of(r):
        t = fnum(r.get("timestamp"))
        return t / 1000.0 if t > 1e12 else t

    if wallet not in _watermark:
        newest = max((ts_of(r) for r in rows), default=time.time())
        _watermark[wallet] = max(newest, time.time() - 5)
        _seen[wallet] = {_row_uid(r) for r in rows}
        return 0

    mark = _watermark[wallet]
    seen = _seen.setdefault(wallet, set())
    # Запас 60 с: Data API иногда индексирует сделки не по порядку времени.
    fresh = [r for r in rows if ts_of(r) >= mark - 60 and _row_uid(r) not in seen]
    # Все 50 строк новые — копируемый наторговал больше страницы за интервал.
    if len(fresh) == len(rows) == PAGE:
        cursor = http.next_cursor(payload)
        if cursor:
            try:
                more = http.data_items(await fetch_activity(wallet, cursor=cursor))
                fresh += [r for r in more if ts_of(r) >= mark - 60 and _row_uid(r) not in seen]
            except Exception:  # noqa: BLE001
                pass
    if not fresh:
        return 0
    stats["rows"] += len(fresh)
    for r in fresh:
        seen.add(_row_uid(r))
    new_mark = max(ts_of(r) for r in fresh)
    _watermark[wallet] = max(mark, new_mark)
    if len(seen) > 2000:
        _seen[wallet] = {_row_uid(r) for r in rows}
    for fill in sorted(rows_to_fills(wallet, fresh), key=lambda f: f.trade_ts or 0):
        detector.submit(fill)
    return len(fresh)


def forget(wallet: str) -> None:
    _watermark.pop(wallet, None)
    _seen.pop(wallet, None)


async def run_forever() -> None:
    if not settings.POLL_ENABLED:
        return
    while True:
        wallets = sorted(state.active_addresses())
        if not wallets:
            await asyncio.sleep(1.0)
            continue
        cycle_start = time.monotonic()
        gap = settings.POLL_INTERVAL_SEC / len(wallets)
        for w in wallets:
            t0 = time.monotonic()
            try:
                await poll_wallet(w)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                stats["last_error"] = f"{type(exc).__name__}: {exc}"[:200]
                log.debug("Опрос %s: %s", w[:10], stats["last_error"])
            spent = time.monotonic() - t0
            if spent < gap:
                await asyncio.sleep(gap - spent)
        left = settings.POLL_INTERVAL_SEC - (time.monotonic() - cycle_start)
        if left > 0:
            await asyncio.sleep(left)
