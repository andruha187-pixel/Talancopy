"""
Источник №2 — поток активности Polymarket (RTDS, wss://ws-live-data.polymarket.com,
topic "activity"). Это все сделки площадки в реальном времени без ключей.
По документации Polymarket (миграция RTDS → PolyBolt) у topic "activity"
замены нет — его и надо использовать.

Типы: "orders_matched" — тейкер-ордер целиком (одно сообщение на ордер),
"trades" — отдельные исполнения (там видно и мейкер-сделки). Подписываемся
на оба; это разные «источники» для склейки, чтобы одна сделка не
посчиталась дважды.

Попутно ведём «перепись» активных кошельков (кто и на сколько торгует) —
это один из источников кандидатов для поиска кошельков.
"""
from __future__ import annotations

import asyncio
import logging
import time

import websockets

from config import settings
from src import db, state
from src.detector import TargetFill, detector
from src.util import dumps, fnum, loads, now_ms

log = logging.getLogger("rtds")

SUBSCRIBE = {"action": "subscribe", "subscriptions": [
    {"topic": "activity", "type": "orders_matched"},
    {"topic": "activity", "type": "trades"},
]}
SILENCE_RECONNECT_SEC = 180

stats = {"connected": False, "messages": 0, "activity": 0, "matched": 0, "trades": 0,
         "last_msg_ms": 0, "reconnects": 0, "error": ""}

# Перепись: wallet -> [name, first_ts, last_ts, n_trades, usdc, big_trades]
_census: dict[str, list] = {}


def _census_observe(wallet: str, name: str, usdc: float) -> None:
    if usdc < settings.CENSUS_MIN_TRADE_USDC:
        return
    now = int(time.time())
    rec = _census.get(wallet)
    if rec is None:
        _census[wallet] = [name, now, now, 1, usdc, 1 if usdc >= 1000 else 0]
    else:
        rec[2] = now
        rec[3] += 1
        rec[4] += usdc
        if usdc >= 1000:
            rec[5] += 1


async def census_flush_loop() -> None:
    while True:
        await asyncio.sleep(300)
        if not _census:
            continue
        snapshot = dict(_census)
        _census.clear()
        for wallet, (name, first_ts, last_ts, n, usdc, big) in snapshot.items():
            db.write(
                "INSERT INTO census (wallet, name, first_ts, last_ts, n_trades, usdc, big_trades) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(wallet) DO UPDATE SET "
                "name = COALESCE(excluded.name, census.name), last_ts = excluded.last_ts, "
                "n_trades = census.n_trades + excluded.n_trades, usdc = census.usdc + excluded.usdc, "
                "big_trades = census.big_trades + excluded.big_trades",
                (wallet, name, first_ts, last_ts, n, usdc, big))
        # Не даём таблице расти бесконечно: держим тех, кто торговал за 14 дней.
        db.write("DELETE FROM census WHERE last_ts < ?", (int(time.time()) - 14 * 86400,))


def parse_payload(p: dict, msg_type: str) -> TargetFill | None:
    wallet = str(p.get("proxyWallet") or "").lower()
    token = str(p.get("asset") or "")
    side = str(p.get("side") or "").upper()
    shares = fnum(p.get("size"))
    price = fnum(p.get("price"))
    if not wallet or not token or side not in ("BUY", "SELL") or shares <= 0 or price <= 0:
        return None
    tx = str(p.get("transactionHash") or "").lower()
    ts = fnum(p.get("timestamp")) or None
    if ts and ts > 1e12:  # миллисекунды
        ts = ts / 1000.0
    source = "rtds" if msg_type == "orders_matched" else "rtds_fills"
    uid = f"{msg_type}:{tx}:{token}:{side}:{shares}:{price}"
    return TargetFill(wallet=wallet, token_id=token, side=side, shares=shares, usdc=shares * price,
                      tx_hash=tx or f"rtds-{token}-{ts}", source=source, detected_ms=now_ms(), uid=uid,
                      trade_ts=ts, role="taker" if msg_type == "orders_matched" else None,
                      complete=(msg_type == "orders_matched"),
                      condition_id=p.get("conditionId"), slug=p.get("slug"), title=p.get("title"),
                      outcome=p.get("outcome"), event_slug=p.get("eventSlug"))


def handle_raw(raw) -> None:
    if not raw or (isinstance(raw, str) and raw[0] != "{"):
        return
    try:
        msg = loads(raw)
    except ValueError:
        return
    stats["messages"] += 1
    stats["last_msg_ms"] = now_ms()
    if msg.get("topic") != "activity":
        return
    p = msg.get("payload")
    if not isinstance(p, dict):
        return
    msg_type = str(msg.get("type") or "")
    stats["activity"] += 1
    if msg_type == "orders_matched":
        stats["matched"] += 1
    else:
        stats["trades"] += 1
    wallet = str(p.get("proxyWallet") or "").lower()
    if settings.CENSUS_ENABLED and wallet:
        _census_observe(wallet, str(p.get("name") or p.get("pseudonym") or ""),
                        fnum(p.get("size")) * fnum(p.get("price")))
    if wallet in state.active_addresses():
        fill = parse_payload(p, msg_type)
        if fill:
            detector.submit(fill)


async def _pinger(ws) -> None:
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")


async def _silence_guard(ws) -> None:
    while True:
        await asyncio.sleep(30)
        if stats["last_msg_ms"] and now_ms() - stats["last_msg_ms"] > SILENCE_RECONNECT_SEC * 1000:
            log.warning("RTDS молчит %ds — переподключаюсь", SILENCE_RECONNECT_SEC)
            await ws.close()
            return


async def run_forever() -> None:
    if not settings.RTDS_ENABLED:
        return
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(settings.RTDS_WS_URL, ping_interval=None,
                                          max_size=2 ** 22, open_timeout=15) as ws:
                await ws.send(dumps(SUBSCRIBE))
                stats["connected"] = True
                stats["error"] = ""
                stats["last_msg_ms"] = now_ms()
                tasks = [asyncio.create_task(_pinger(ws)), asyncio.create_task(_silence_guard(ws))]
                backoff = 1.0
                try:
                    async for raw in ws:
                        handle_raw(raw)
                finally:
                    for t in tasks:
                        t.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            stats["error"] = f"{type(exc).__name__}: {exc}"[:200]
            log.warning("RTDS отключился: %s", stats["error"])
        stats["connected"] = False
        stats["reconnects"] += 1
        await asyncio.sleep(backoff)
        backoff = min(30.0, backoff * 2)
