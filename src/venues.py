"""
Цены других бирж потоком — для ⚡ лаборатории скорости (src/speed_lab.py).

Зачем. Polymarket ставит цены 5m-рынков по крупным биржам. Чтобы понять, кто
двигается первым — и не реагирует ли стакан Polymarket раньше какой-нибудь
биржи, — лаборатория сравнивает моменты рывка на нескольких биржах. Здесь
только чтение публичных потоков сделок: ключи не нужны, ничего не торгуем.

  bnf — фьючерсы Binance (USDT-M), aggTrade
  cb  — Coinbase (пары к USD), канал ticker — сообщение на каждую сделку
  okx — OKX спот, канал trades
  byb — Bybit спот, publicTrade
Binance спот уже идёт через price_stream, Chainlink — через chainlink_feed.

Биржа недоступна (блок по стране, нет пары) — у неё просто нет данных, на
работу бота это не влияет. Список — SPEED_LAB_VENUES.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

import websockets

from config import settings

log = logging.getLogger("venues")

NAMES = {"bnf": "фьючерсы Binance", "cb": "Coinbase", "okx": "OKX", "byb": "Bybit"}
URLS = {
    "cb": "wss://ws-feed.exchange.coinbase.com",
    "okx": "wss://ws.okx.com/ws/v5/public",
    "byb": "wss://stream.bybit.com/v5/public/spot",
}
COINBASE_PAIRS = {"btc": "BTC-USD", "eth": "ETH-USD", "sol": "SOL-USD", "xrp": "XRP-USD", "doge": "DOGE-USD"}
PING_SEC = 20                  # OKX закрывает соединение после 30 с тишины, Bybit просит пинг раз в 20 с
STALL_SEC = 60                 # ни одного сообщения минуту — переподключаемся
MAX_BACKOFF_SEC = 120
FRESH_SEC = 30                 # последняя сделка не старше — биржа «✅»


@dataclass
class Status:
    venue: str
    connected: bool = False
    since: float = 0.0
    last_msg: float = 0.0
    last_trade: float = 0.0
    trades: int = 0
    reconnects: int = 0
    error: str = ""
    assets: set = field(default_factory=set)


_status: dict[str, Status] = {}
_listeners: list = []


def add_listener(fn) -> None:
    """fn(venue, asset, price, trade_ms, recv_ms) на каждую сделку."""
    if fn not in _listeners:
        _listeners.append(fn)


def remove_listener(fn) -> None:
    if fn in _listeners:
        _listeners.remove(fn)


def reset() -> None:
    _status.clear()


def now_ms() -> int:
    return int(time.time() * 1000)


def enabled_venues() -> list[str]:
    raw = str(getattr(settings, "SPEED_LAB_VENUES", "") or "")
    out = []
    for v in raw.split(","):
        v = v.strip().lower()
        if v in NAMES and v not in out:
            out.append(v)
    return out


def symbols(venue: str, assets) -> dict[str, str]:
    """{пара на бирже: монета}; монеты, которой на бирже нет, в словаре нет."""
    out = {}
    for a in assets:
        a = a.lower()
        if venue in ("bnf", "byb"):
            out[f"{a.upper()}USDT"] = a
        elif venue == "okx":
            out[f"{a.upper()}-USDT"] = a
        elif venue == "cb" and a in COINBASE_PAIRS:
            out[COINBASE_PAIRS[a]] = a
    return out


def url(venue: str, syms) -> str:
    if venue == "bnf":
        from src import price_stream
        return price_stream.stream_url("futures", syms)
    return URLS[venue]


def subscribe_msgs(venue: str, syms) -> list:
    if venue == "cb":
        return [{"type": "subscribe", "product_ids": sorted(syms), "channels": ["ticker"]}]
    if venue == "okx":
        # По паре на сообщение: несуществующая пара не ломает подписку остальных
        return [{"op": "subscribe", "args": [{"channel": "trades", "instId": s}]} for s in sorted(syms)]
    if venue == "byb":
        return [{"op": "subscribe", "args": [f"publicTrade.{s}"]} for s in sorted(syms)]
    return []                  # фьючерсы Binance — потоки в адресе


def ping_payload(venue: str) -> str | None:
    if venue == "okx":
        return "ping"
    if venue == "byb":
        return json.dumps({"op": "ping"})
    return None


def _f(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _i(x) -> int | None:
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def iso_ms(s) -> int | None:
    """«2026-10-16T09:23:45.123456Z» → мс."""
    if not isinstance(s, str) or not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        if "." in s:
            head, rest = s.split(".", 1)
            cut = max(rest.find("+"), rest.find("-"))
            frac, tz = (rest[:cut], rest[cut:]) if cut > 0 else (rest, "")
            s = f"{head}.{(frac + '000000')[:6]}{tz}"
        return int(datetime.fromisoformat(s).timestamp() * 1000)
    except ValueError:
        return None


def parse(venue: str, raw, sym_asset: dict) -> list[tuple[str, float, int]]:
    """Сделки из сообщения: [(монета, цена, время сделки мс)]."""
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return []              # «pong» и прочее
    if not isinstance(msg, dict):
        return []
    out = []
    if venue == "bnf":
        data = msg.get("data", msg)
        if isinstance(data, dict) and data.get("e") == "aggTrade":
            a = sym_asset.get(str(data.get("s") or "").upper())
            p, t = _f(data.get("p")), _i(data.get("T") or data.get("E"))
            if a and p and t:
                out.append((a, p, t))
    elif venue == "cb":
        if msg.get("type") == "ticker":
            a = sym_asset.get(msg.get("product_id"))
            p, t = _f(msg.get("price")), iso_ms(msg.get("time"))
            if a and p and t:
                out.append((a, p, t))
    elif venue == "okx":
        arg = msg.get("arg")
        if isinstance(arg, dict) and arg.get("channel") == "trades":
            for d in msg.get("data") or []:
                if isinstance(d, dict):
                    a = sym_asset.get(d.get("instId"))
                    p, t = _f(d.get("px")), _i(d.get("ts"))
                    if a and p and t:
                        out.append((a, p, t))
    elif venue == "byb":
        if str(msg.get("topic") or "").startswith("publicTrade."):
            for d in msg.get("data") or []:
                if isinstance(d, dict):
                    a = sym_asset.get(d.get("s"))
                    p, t = _f(d.get("p")), _i(d.get("T"))
                    if a and p and t:
                        out.append((a, p, t))
    return out


def parse_error(venue: str, raw) -> str | None:
    """Ошибка подписки от биржи (неизвестная пара и т.п.) — для статуса."""
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(msg, dict):
        return None
    if venue == "cb" and msg.get("type") == "error":
        return f"{msg.get('message')} {msg.get('reason') or ''}".strip()
    if venue == "okx" and msg.get("event") == "error":
        return str(msg.get("msg") or msg.get("code"))
    if venue == "byb" and msg.get("success") is False:
        return str(msg.get("ret_msg") or "ошибка подписки")
    return None


def dispatch(venue: str, trades, recv_ms: int) -> None:
    st = _status.setdefault(venue, Status(venue))
    if not trades:
        return
    st.last_trade = recv_ms / 1000
    st.trades += len(trades)
    for a, p, t in trades:
        st.assets.add(a)
        for fn in _listeners:
            try:
                fn(venue, a, p, t, recv_ms)
            except Exception as exc:  # noqa: BLE001 — слушатель не должен ронять поток
                log.debug("Слушатель %s упал: %s", venue, exc)


async def _pinger(ws, payload: str) -> None:
    while True:
        await asyncio.sleep(PING_SEC)
        await ws.send(payload)


async def _watchdog(ws, st: Status) -> None:
    while True:
        await asyncio.sleep(5)
        if time.time() - st.last_msg > STALL_SEC:
            st.error = f"нет данных {STALL_SEC} с"
            await ws.close()
            return


async def run_venue(venue: str, active=lambda: True) -> None:
    st = _status.setdefault(venue, Status(venue))
    backoff = 1.0
    while True:
        if not active():
            st.connected = False
            await asyncio.sleep(5)
            continue
        syms = symbols(venue, settings.ASSETS)
        if not syms:
            st.error = "нет пар"
            await asyncio.sleep(300)
            continue
        helpers = []
        try:
            async with websockets.connect(url(venue, syms), ping_interval=20, ping_timeout=20,
                                          close_timeout=5) as ws:
                st.connected, st.since, st.last_msg = True, time.time(), time.time()
                for m in subscribe_msgs(venue, syms):
                    await ws.send(json.dumps(m))
                payload = ping_payload(venue)
                if payload:
                    helpers.append(asyncio.create_task(_pinger(ws, payload)))
                helpers.append(asyncio.create_task(_watchdog(ws, st)))
                async for raw in ws:
                    recv = now_ms()
                    st.last_msg = recv / 1000
                    trades = parse(venue, raw, syms)
                    if trades:
                        dispatch(venue, trades, recv)
                    else:
                        err = parse_error(venue, raw)
                        if err:
                            st.error = err[:200]
                    if not active():
                        break
            backoff = 1.0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — биржа недоступна: ждём и пробуем снова
            st.error = (f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:200]
            backoff = min(MAX_BACKOFF_SEC, backoff * 2)
        finally:
            for t in helpers:
                t.cancel()
            st.connected = False
        st.reconnects += 1
        await asyncio.sleep(backoff)


async def run_forever(active=lambda: True) -> None:
    vs = enabled_venues()
    if vs:
        await asyncio.gather(*(run_venue(v, active) for v in vs))


def status_line(now: float | None = None) -> str:
    vs = enabled_venues()
    if not vs:
        return ""
    now = time.time() if now is None else now
    parts = []
    for v in vs:
        st = _status.get(v)
        if st is None:
            parts.append(f"{NAMES[v]} —")
        elif st.last_trade and now - st.last_trade <= FRESH_SEC:
            parts.append(f"{NAMES[v]} ✅")
        elif st.connected:
            parts.append(f"{NAMES[v]} ⏳ сделок нет" + (f" ({st.error})" if st.error else ""))
        else:
            parts.append(f"{NAMES[v]} ❌ " + (st.error or "нет связи"))
    return "Потоки бирж: " + " · ".join(parts)
