"""
Цена Binance потоком (WebSocket, сделки aggTrade) — для быстрой проверки
сигнала (src/fast_signal.py).

Обычная проверка (main.py, _instance_tick) берёт цену из свечей REST-запросом
раз в 3 с. По отчётам 25.09–08.10 у 32% рынков с сигналом условия входа
держались не дольше одной такой проверки: цена заходила в диапазон и уходила
из него между тиками. Сделки из потока приходят сразу, как только проходят на
Binance, — быстрый цикл видит цену без запроса.

Кроме последней цены по сделкам строится текущая минутная свеча (максимум,
минимум, закрытие): быстрый цикл досчитывает ею последнюю свечу из REST, чтобы
ATR/EMA/MACD считались так же, как в обычной проверке, а не по устаревшим
на несколько секунд свечам.

Монеты со спотовой парой — спотовый поток; монеты, у которых свечи пришлось
брать с фьючерсов (HYPE), — фьючерсный: цена и свечи с одного рынка.

Если поток оборвался или по монете давно ничего не приходило, latest()
возвращает None: быстрый цикл по этой монете молчит, а обычная проверка раз
в 3 с по REST работает как раньше.
"""
from __future__ import annotations
import asyncio
import itertools
import json
import logging
import time
from dataclasses import dataclass, field

import websockets

from config import settings

log = logging.getLogger("price_stream")

VENUES = ("spot", "futures")
PRICE_FRESH_MS = 3000          # цена моложе этого — свежая
QUIET_OK_MS = 15000            # монета может молчать до 15 с, если по соединению идут другие монеты
CONN_ALIVE_MS = 2000           # соединение «живое», если последнее сообщение не старше этого
NO_DATA_RECONNECT_MS = 30000   # ни одного сообщения 30 с — переподключаемся
RECONNECT_MIN_SEC = 1.0
RECONNECT_MAX_SEC = 30.0
STABLE_CONN_SEC = 60           # после минуты нормальной работы паузу переподключения сбрасываем


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Ticker:
    symbol: str
    price: float | None = None
    trade_ms: int = 0       # время последней сделки на Binance (поле T)
    recv_ms: int = 0        # когда получили (наши часы)
    gen: int = 0            # номер соединения, по которому пришла цена
    # Текущая минутная свеча по сделкам из потока (минута — по времени Binance)
    minute_ms: int = 0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    # Предыдущая минута со сделками: (minute_ms, open, high, low, close)
    prev: tuple | None = None


@dataclass
class Connection:
    venue: str
    wanted: set = field(default_factory=set)       # символы (BTCUSDT), на которые нужна подписка
    subscribed: set = field(default_factory=set)   # на что подписаны на текущем соединении
    is_open: bool = False
    gen: int = 0                 # растёт с каждым новым соединением
    open_since_ms: int = 0
    last_msg_ms: int = 0
    reconnects: int = 0
    last_error: str = ""


_tickers: dict[str, Ticker] = {}
_venue_of: dict[str, str] = {}
_conns: dict[str, Connection] = {v: Connection(v) for v in VENUES}
_wake: dict[str, asyncio.Event] = {}
_ids = itertools.count(1)


def reset() -> None:
    """Сброс состояния (для тестов)."""
    _tickers.clear()
    _venue_of.clear()
    _wake.clear()
    for v in VENUES:
        _conns[v] = Connection(v)


def _wake_event(venue: str) -> asyncio.Event:
    ev = _wake.get(venue)
    if ev is None:
        ev = _wake[venue] = asyncio.Event()
    return ev


def watch(symbol: str, venue: str = "spot") -> None:
    """Подписаться на сделки символа. Идемпотентно — main.py вызывает на
    каждом тике. Если символ переехал на другую площадку (свечи стали
    приходить с фьючерсов), сделки со старой площадки дальше не учитываются."""
    symbol = symbol.upper()
    venue = venue if venue in VENUES else "spot"
    old = _venue_of.get(symbol)
    if old == venue:
        return
    if old is not None:
        _conns[old].wanted.discard(symbol)
        _tickers.pop(symbol, None)
    _venue_of[symbol] = venue
    _conns[venue].wanted.add(symbol)
    _wake_event(venue).set()


def connection_for(symbol: str) -> Connection | None:
    venue = _venue_of.get(symbol.upper())
    return _conns.get(venue) if venue else None


def on_trade(symbol: str, price: float, trade_ms: int, recv_ms: int | None = None, gen: int = 0) -> None:
    """Учесть одну сделку: последняя цена и минутная свеча."""
    t = _tickers.get(symbol)
    if t is None:
        t = _tickers[symbol] = Ticker(symbol)
    recv_ms = now_ms() if recv_ms is None else recv_ms
    newest = trade_ms >= t.trade_ms
    if newest:
        t.price = price
        t.trade_ms = trade_ms
    t.recv_ms = recv_ms
    t.gen = gen
    minute = trade_ms // 60000 * 60000
    if minute > t.minute_ms:
        if t.minute_ms:
            t.prev = (t.minute_ms, t.open, t.high, t.low, t.close)
        t.minute_ms = minute
        t.open = t.high = t.low = t.close = price
    elif minute == t.minute_ms:
        t.high = max(t.high, price)
        t.low = min(t.low, price)
        if newest:
            t.close = price
    elif t.prev is not None and t.prev[0] == minute:
        # Запоздавшая сделка прошлой минуты — только максимум/минимум
        m, o, h, low, c = t.prev
        t.prev = (m, o, max(h, price), min(low, price), c)


def latest(symbol: str, now: int | None = None) -> Ticker | None:
    """Последняя цена символа, если ей можно верить, иначе None: потока нет,
    соединение переподключилось и по монете ещё не было сделок, или монета
    молчит дольше QUIET_OK_MS."""
    symbol = symbol.upper()
    t = _tickers.get(symbol)
    if t is None or t.price is None:
        return None
    conn = connection_for(symbol)
    if conn is None or not conn.is_open or t.gen != conn.gen:
        return None
    now = now_ms() if now is None else now
    age = now - t.recv_ms
    if age <= PRICE_FRESH_MS:
        return t
    if age <= QUIET_OK_MS and now - conn.last_msg_ms <= CONN_ALIVE_MS:
        return t
    return None


def handle_raw(conn: Connection, raw) -> int:
    """Разбор одного сообщения потока. Возвращает 1, если учли сделку.
    Ответы на SUBSCRIBE ({"result": null, "id": 1}) и мусор пропускаем."""
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return 0
    if not isinstance(msg, dict):
        return 0
    data = msg.get("data", msg)
    if not isinstance(data, dict) or data.get("e") != "aggTrade":
        return 0
    symbol = str(data.get("s") or "").upper()
    if _venue_of.get(symbol) != conn.venue:
        return 0
    try:
        price = float(data["p"])
        trade_ms = int(data.get("T") or data.get("E"))
    except (KeyError, TypeError, ValueError):
        return 0
    if price <= 0:
        return 0
    on_trade(symbol, price, trade_ms, gen=conn.gen)
    return 1


def stream_url(venue: str, symbols) -> str:
    base = settings.BINANCE_FUTURES_WS_URL if venue == "futures" else settings.BINANCE_WS_URL
    streams = "/".join(f"{s.lower()}@aggTrade" for s in sorted(symbols))
    return f"{base.rstrip('/')}/stream?streams={streams}"


async def _subscribe_missing(ws, conn: Connection) -> None:
    """Новая монета на уже открытом соединении — подписка сообщением."""
    missing = conn.wanted - conn.subscribed
    if not missing:
        return
    conn.subscribed |= missing   # до await: второй вызов не подпишет повторно
    params = [f"{s.lower()}@aggTrade" for s in sorted(missing)]
    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": next(_ids)}))


async def _watchdog(ws, conn: Connection) -> None:
    """Раз в секунду: подписать новые монеты и проверить, что данные идут.
    Ни одного сообщения NO_DATA_RECONNECT_MS — закрываем соединение, цикл
    run_venue переподключится."""
    try:
        while True:
            await asyncio.sleep(1.0)
            await _subscribe_missing(ws, conn)
            if now_ms() - conn.last_msg_ms > NO_DATA_RECONNECT_MS:
                conn.last_error = f"нет данных {NO_DATA_RECONNECT_MS // 1000} с"
                log.warning("Поток цены Binance (%s): %s — переподключаю", conn.venue, conn.last_error)
                await ws.close()
                return
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — сокет уже сломан: закрываем, run_venue переподключится
        conn.last_error = (f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:200]
        try:
            await ws.close()
        except Exception:  # noqa: BLE001
            pass


async def _pump(ws, conn: Connection) -> None:
    watchdog = asyncio.create_task(_watchdog(ws, conn))
    try:
        async for raw in ws:
            conn.last_msg_ms = now_ms()
            handle_raw(conn, raw)
            if conn.wanted - conn.subscribed:
                await _subscribe_missing(ws, conn)
    finally:
        watchdog.cancel()


async def run_venue(venue: str) -> None:
    """Держит соединение одной площадки (спот или фьючерсы) и переподключается
    при обрыве. Binance сам закрывает соединение раз в 24 ч — это нормально."""
    backoff = RECONNECT_MIN_SEC
    while True:
        conn = _conns[venue]
        if not conn.wanted:
            ev = _wake_event(venue)
            ev.clear()
            if not conn.wanted:
                await ev.wait()
            continue
        symbols = set(conn.wanted)
        url = stream_url(venue, symbols)
        started = time.time()
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                          open_timeout=10, close_timeout=2) as ws:
                conn.gen += 1
                conn.subscribed = symbols
                conn.is_open = True
                conn.open_since_ms = conn.last_msg_ms = now_ms()
                log.info("Поток цены Binance (%s) подключён: %s", venue, ", ".join(sorted(symbols)))
                await _pump(ws, conn)
            # Сервер закрыл соединение штатно (раз в 24 ч) или его закрыл сторож
            log.info("Поток цены Binance (%s) закрыт, переподключение через %.0f с", venue, backoff)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — обрыв потока не должен ронять бота
            conn.last_error = (f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:200]
            log.warning("Поток цены Binance (%s) оборвался (%s), переподключение через %.0f с",
                        venue, conn.last_error, backoff)
        finally:
            conn.is_open = False
        conn.reconnects += 1
        if time.time() - started > STABLE_CONN_SEC:
            backoff = RECONNECT_MIN_SEC
        await asyncio.sleep(backoff)
        backoff = min(RECONNECT_MAX_SEC, backoff * 2)


async def run_forever() -> None:
    await asyncio.gather(*(run_venue(v) for v in VENUES))


def fresh_symbols(symbols) -> list[str]:
    return [s for s in symbols if latest(s) is not None]


def last_error() -> str:
    errs = [f"{c.venue}: {c.last_error}" for c in _conns.values() if c.wanted and c.last_error and not c.is_open]
    return "; ".join(errs)
