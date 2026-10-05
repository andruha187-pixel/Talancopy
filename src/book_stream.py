"""
Живой стакан по WebSocket (wss://ws-subscriptions-clob.polymarket.com/ws/market).

Зачем копи-боту: (1) проверить цену ДО отправки ордера — если ask уже выше
нашего потолка, ордер не шлём (экономим раунд-трип и не ловим «пустые»
FAK); (2) мгновенно видеть bid по открытым позициям для стоп-лосса,
тейк-профита и трейлинга; (3) событие market_resolved — сразу закрываем
позицию по итогу рынка.

Протокол (проверен на основном боте):
  первое сообщение: {"type":"market","assets_ids":[...],"custom_feature_enabled":true}
  на живом сокете:  {"operation":"subscribe"|"unsubscribe","assets_ids":[...]}
"""
from __future__ import annotations

import asyncio
import logging
import time

import websockets

from config import settings
from src.util import dumps, loads

log = logging.getLogger("book_stream")

MAX_BOOK_AGE_MS = 5000
RECONNECT_BACKOFF_SEC = 2
WATCHDOG_NO_DATA_MS = 8000
WATCHDOG_MIN_INTERVAL_MS = 30000

_books: dict[str, dict] = {}
_subscribed: set[str] = set()
_subscribed_at: dict[str, int] = {}
_send_queue: asyncio.Queue | None = None
_ws_ready = asyncio.Event()
_last_forced_reconnect_ms = 0
_resolved_callbacks: list = []
_last_trade: dict[str, tuple[float, int]] = {}
stats = {"connected": False, "messages": 0, "reconnects": 0, "last_msg_ms": 0}


def now_ms() -> int:
    return int(time.time() * 1000)


def _queue() -> asyncio.Queue:
    global _send_queue
    if _send_queue is None:
        _send_queue = asyncio.Queue()
    return _send_queue


def on_market_resolved(cb) -> None:
    _resolved_callbacks.append(cb)


def _level_map(levels) -> dict[float, float]:
    out = {}
    for lvl in levels or []:
        try:
            price = float(lvl.get("price"))
            size = float(lvl.get("size"))
        except (TypeError, ValueError, AttributeError):
            continue
        if size > 0:
            out[price] = size
    return out


def _apply_snapshot(asset: str, msg: dict) -> None:
    tick = msg.get("tick_size") or msg.get("tickSize") or _books.get(asset, {}).get("tick_size") or 0.01
    _books[asset] = {
        "bids": _level_map(msg.get("bids")),
        "asks": _level_map(msg.get("asks")),
        "received_ms": now_ms(),
        "tick_size": float(tick),
    }


def _apply_delta(msg: dict) -> None:
    for ch in msg.get("price_changes", []) or []:
        asset = str(ch.get("asset_id") or "")
        if not asset or asset not in _books:
            continue
        book = _books[asset]
        try:
            price = float(ch.get("price"))
            size = float(ch.get("size"))
        except (TypeError, ValueError):
            continue
        side = str(ch.get("side", "")).upper()
        target = book["bids"] if side == "BUY" else book["asks"]
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
        book["received_ms"] = now_ms()


def subscribe(asset_ids) -> None:
    new_ids = [str(a) for a in asset_ids if a and str(a) not in _subscribed]
    if not new_ids:
        return
    _subscribed.update(new_ids)
    ts = now_ms()
    for a in new_ids:
        _subscribed_at[a] = ts
    _queue().put_nowait({"operation": "subscribe", "assets_ids": new_ids, "custom_feature_enabled": True})


def unsubscribe(asset_ids) -> None:
    removed = [str(a) for a in asset_ids if str(a) in _subscribed]
    if not removed:
        return
    for a in removed:
        _subscribed.discard(a)
        _subscribed_at.pop(a, None)
        _books.pop(a, None)
    _queue().put_nowait({"operation": "unsubscribe", "assets_ids": removed})


def is_fresh(asset: str, max_age_ms: int = MAX_BOOK_AGE_MS) -> bool:
    b = _books.get(asset)
    return bool(b and b.get("received_ms") and now_ms() - b["received_ms"] <= max_age_ms)


def best_ask(asset: str) -> float | None:
    b = _books.get(asset)
    return min(b["asks"]) if b and b.get("asks") else None


def best_bid(asset: str) -> float | None:
    b = _books.get(asset)
    return max(b["bids"]) if b and b.get("bids") else None


def asks(asset: str) -> list[tuple[float, float]]:
    b = _books.get(asset)
    return sorted(b["asks"].items()) if b else []


def bids(asset: str) -> list[tuple[float, float]]:
    b = _books.get(asset)
    return sorted(b["bids"].items(), reverse=True) if b else []


def tick_size(asset: str) -> float | None:
    b = _books.get(asset)
    return float(b["tick_size"]) if b and b.get("tick_size") else None


def last_trade(asset: str) -> tuple[float, int] | None:
    return _last_trade.get(asset)


def _handle_message(msg: dict) -> None:
    event_type = msg.get("event_type")
    if event_type == "book":
        asset = str(msg.get("asset_id") or "")
        if asset:
            _apply_snapshot(asset, msg)
    elif event_type == "price_change":
        _apply_delta(msg)
    elif event_type == "tick_size_change":
        asset = str(msg.get("asset_id") or "")
        new_tick = msg.get("new_tick_size")
        if asset in _books and new_tick:
            _books[asset]["tick_size"] = float(new_tick)
    elif event_type == "last_trade_price":
        asset = str(msg.get("asset_id") or "")
        try:
            _last_trade[asset] = (float(msg.get("price")), now_ms())
        except (TypeError, ValueError):
            pass
    elif event_type == "market_resolved":
        for cb in list(_resolved_callbacks):
            try:
                cb(msg)
            except Exception as exc:  # noqa: BLE001
                log.warning("market_resolved callback: %s", exc)


async def _sender(ws) -> None:
    q = _queue()
    while True:
        payload = await q.get()
        await ws.send(dumps(payload))


async def _watchdog(ws) -> None:
    global _last_forced_reconnect_ms
    while True:
        await asyncio.sleep(3)
        ts = now_ms()
        missing = [a for a in _subscribed
                   if a not in _books and ts - _subscribed_at.get(a, ts) > WATCHDOG_NO_DATA_MS]
        if missing and ts - _last_forced_reconnect_ms > WATCHDOG_MIN_INTERVAL_MS:
            _last_forced_reconnect_ms = ts
            log.warning("Нет стакана по %d токенам после подписки — переподключаю WS", len(missing))
            await ws.close()
            return


async def run_forever() -> None:
    while True:
        try:
            async with websockets.connect(settings.CLOB_WS_URL, ping_interval=20, ping_timeout=20,
                                          max_size=2 ** 23) as ws:
                stats["connected"] = True
                _ws_ready.set()
                # Всё, что накопилось в очереди до коннекта, уже входит в полный список ниже.
                q = _queue()
                while not q.empty():
                    q.get_nowait()
                if _subscribed:
                    await ws.send(dumps({"type": "market", "assets_ids": list(_subscribed),
                                         "custom_feature_enabled": True}))
                ts0 = now_ms()
                for a in list(_subscribed):
                    _subscribed_at[a] = ts0
                sender_task = asyncio.create_task(_sender(ws))
                watchdog_task = asyncio.create_task(_watchdog(ws))
                try:
                    async for raw in ws:
                        if not raw or raw in ("PONG", "pong"):
                            continue
                        try:
                            parsed = loads(raw)
                        except ValueError:
                            continue
                        stats["messages"] += 1
                        stats["last_msg_ms"] = now_ms()
                        for msg in (parsed if isinstance(parsed, list) else [parsed]):
                            if isinstance(msg, dict):
                                _handle_message(msg)
                finally:
                    sender_task.cancel()
                    watchdog_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("Стакан WS отключился (%s), переподключаюсь", exc)
        stats["connected"] = False
        stats["reconnects"] += 1
        _ws_ready.clear()
        await asyncio.sleep(RECONNECT_BACKOFF_SEC)
