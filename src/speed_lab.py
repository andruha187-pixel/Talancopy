"""
⚡ Лаборатория скорости: насколько стакан Polymarket отстаёт от Binance.

Зачем. По отчётам (шаг ~3 с) рынок догоняет Binance почти сразу: рывок
вероятности на 10¢ рынок проходит за те же 3 с, а за следующие 3 с добавляет
~1¢ — меньше наших издержек (полспреда + комиссия). Если окно «купить по
старой цене» есть, оно меньше секунды. Здесь мы его меряем: при рывке цены на
Binance записываем верх стакана обеих сторон рынка до и после, с точностью до
миллисекунд по нашим часам. Ничего не покупает.

Как. Слушаем сделки Binance (price_stream) и изменения стакана Polymarket
(book_stream). По каждой монете знаем текущий 5m-рынок (main._active_markets),
страйк (цена Binance на начало окна) и волатильность (EWMA квадратов
секундных доходностей). «Честная» вероятность UP = Φ(ln(P/K) / (σ·√секунд до
конца)). Рывок — вероятность сдвинулась на ≥ SPEED_LAB_JUMP (5¢) за
≤ SPEED_LAB_WINDOW_MS (1 с). Тогда событие: верх стакана обеих сторон за 2 с
до рывка и SPEED_LAB_RECORD_MS (6 с) после — каждое изменение лучших bid/ask
со временем получения и временем сервера Polymarket, — и цена Binance.

Что считаем (сторона, которую рывок сделал дороже, — «наша»):
  pre_move       — на сколько середина нашей стороны сдвинулась за время
                   самого рывка, ещё до нашего сигнала;
  reprice_ms     — через сколько мс после сигнала исчез старый ask;
  ask_<L>        — ask нашей стороны через L мс: столько заплатил бы бот,
                   реагирующий за L мс (0/100/250/500/1000/2000);
  edge_<L>       — середина нашей стороны через 6 с минус ask_<L> и комиссия:
                   запас покупки «по старой цене», доля доллара на акцию;
  move           — сдвиг середины нашей стороны за 6 с.
Задержки: Binance → бот (наши часы минус время сделки), Polymarket → бот
(наши часы минус timestamp сообщения), запрос к CLOB раз в минуту.

Другие биржи (src/venues.py: фьючерсы Binance, Coinbase, OKX, Bybit) и
Chainlink: их цена за 2 с до рывка и 3 с после. venue_x — когда каждая
прошла половину хода Binance спот (мс от сигнала; Binance спот — «bn»): кто
двигается первым и успевает ли Polymarket раньше какой-нибудь биржи.

Результат — таблица speed_events, CSV в 4-часовом отчёте, сводка в 🎯 → ⚡.
"""
from __future__ import annotations
import asyncio
import collections
import json
import logging
import math
import time
from statistics import median

from config import settings
from src import book_stream, chainlink_feed, price_stream, runtime_state, storage, venues

log = logging.getLogger("speed_lab")

PRE_MS = 2000                  # сколько стакана до рывка кладём в событие
MIN_LEFT_SEC = 20.0            # ближе к концу рынка рывки не ловим
LATENCIES_MS = (0, 100, 250, 500, 1000, 2000)
SIGMA_MIN_SAMPLES = 120        # ~2 мин секундных доходностей — раньше σ не считаем
SIGMA_ALPHA = 1.0 / 600        # EWMA ~10 минут
TOP_KEEP_MS = 5000             # сколько истории верха стакана держим на токен
RESAMPLE_MS = 250              # цена та же — вероятность пересчитываем не чаще (тикает время)
BINANCE_PATH_STEP_MS = 100     # путь Binance в событии — не чаще точки на 100 мс
STRIKE_FROM_TRADE_SEC = 5      # запасной страйк — первая сделка не позже 5 с от начала окна
MAX_EVENTS_PER_MARKET = 6      # рывков на рынок (их много у страйка — хватит и части)…
BIG_JUMP = 0.10                # …кроме крупных: от 10¢ пишем всегда
DEPTH_SPAN = 0.03              # глубина asks нашей стороны в момент рывка: до ask + 3¢
DEPTH_LEVELS = 8
MAX_PATH = 600                 # точек пути в событии — не больше (страховка от «шторма» стакана)
VENUE_STEP_MS = 100            # пути бирж в базе — не чаще точки на 100 мс (моменты считаем по всем сделкам)
VENUE_REC_MS = 3000            # и до 3 с после сигнала: дальше опережение не интересно
VENUE_MAX_POINTS = 3000        # сделок биржи в событии — не больше
VENUE_NAMES = {"bn": "Binance спот", **venues.NAMES, "cl": "Chainlink"}
FINALIZE_EVERY_SEC = 0.2
RTT_EVERY_SEC = 60.0
CLEANUP_EVERY_SEC = 3600.0
TF_KEY = "5m"


def jump() -> float:
    return float(settings.SPEED_LAB_JUMP)


def window_ms() -> int:
    return int(settings.SPEED_LAB_WINDOW_MS)


def record_ms() -> int:
    return int(settings.SPEED_LAB_RECORD_MS)


def cooldown_ms() -> int:
    return int(settings.SPEED_LAB_COOLDOWN_MS)


def enabled() -> bool:
    return bool(runtime_state._state.get("speed_lab_enabled", settings.SPEED_LAB_ENABLED))


def set_enabled(on: bool) -> None:
    runtime_state.set("speed_lab_enabled", bool(on))
    if not on:
        for st in _assets.values():
            st.event = None


def now_ms() -> int:
    return int(time.time() * 1000)


def fee(p: float | None) -> float:
    """Комиссия тейкера на одну акцию по цене p."""
    return float(settings.FEE_RATE) * p * (1 - p) if p is not None else 0.0


def phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


class _Asset:
    def __init__(self, asset: str):
        self.asset = asset
        self.sec: int | None = None          # секунда последней сделки (время Binance)
        self.sec_close: float | None = None  # последняя цена этой секунды
        self.prev_sec: int | None = None     # прошлая секунда со сделками и её цена закрытия
        self.prev_close: float | None = None
        self.var = 0.0                       # EWMA квадрата секундной лог-доходности
        self.n = 0
        self.slug: str | None = None         # рынок, к которому относятся страйк и окно
        self.strike: float | None = None
        self.minq: collections.deque = collections.deque()   # (ms, p, price), p растёт
        self.maxq: collections.deque = collections.deque()   # (ms, p, price), p падает
        self.last_p_ms = 0
        self.last_p_price: float | None = None
        self.cooldown_until = 0
        self.per_market = 0
        self.event: dict | None = None


_active_ref: dict | None = None          # main._active_markets ("btc:5m" -> ActiveMarket)
_assets: dict[str, _Asset] = {}
_symbol_asset: dict[str, str | None] = {}
_token_side: dict[str, tuple[str, str]] = {}      # токен -> (монета, "up"/"down")
_tops: dict[str, collections.deque] = {}          # токен -> (recv_ms, bid, ask, ask_size, server_ms)
_bn_delay: collections.deque = collections.deque(maxlen=5000)
_pm_delay: collections.deque = collections.deque(maxlen=5000)
_rtt: collections.deque = collections.deque(maxlen=500)
_vring: dict[tuple[str, str], collections.deque] = {}     # (биржа, монета) -> (recv_ms, цена)
_vdelay: dict[str, collections.deque] = {}                 # биржа -> recv − время сделки
_stats = {"events": 0, "saved": 0, "errors": 0}


def reset() -> None:
    """Для тестов."""
    global _active_ref
    _active_ref = None
    _assets.clear()
    _symbol_asset.clear()
    _token_side.clear()
    _tops.clear()
    _bn_delay.clear()
    _pm_delay.clear()
    _rtt.clear()
    _vring.clear()
    _vdelay.clear()
    for k in _stats:
        _stats[k] = 0


def install(active_markets: dict | None) -> None:
    """Подключить слушателей потоков и ссылку на активные рынки (идемпотентно)."""
    global _active_ref
    _active_ref = active_markets
    price_stream.add_listener(on_binance_trade)
    book_stream.add_listener(on_book_update)
    venues.add_listener(on_venue_trade)
    chainlink_feed.add_listener(on_chainlink)


def _market_for(asset: str):
    if _active_ref is None:
        return None
    return _active_ref.get(f"{asset}:{TF_KEY}")


def _asset_of_symbol(symbol: str) -> str | None:
    if symbol in _symbol_asset:
        return _symbol_asset[symbol]
    from src import binance_feed
    found = None
    for asset in settings.ASSETS:
        if binance_feed.symbol_for(asset).upper() == symbol.upper():
            found = asset
            break
    _symbol_asset[symbol] = found
    return found


def _switch_market(st: _Asset, market) -> None:
    """Новый рынок монеты: страйк и окно рывка — с нуля, токены — новые."""
    st.slug = market.slug
    st.strike = None
    st.minq.clear()
    st.maxq.clear()
    st.last_p_price = None
    st.per_market = 0
    for tok in [t for t, (a, _s) in _token_side.items() if a == st.asset]:
        _token_side.pop(tok, None)
        _tops.pop(tok, None)
    for tok, side in ((market.up_token_id, "up"), (market.down_token_id, "down")):
        if tok:
            _token_side[tok] = (st.asset, side)
            _tops[tok] = collections.deque()


def _update_sigma(st: _Asset, price: float, trade_ms: int) -> None:
    """Доходность — между ценами закрытия соседних секунд (последняя сделка
    секунды), а не между двумя соседними сделками."""
    sec = trade_ms // 1000
    if st.sec is None:
        st.sec, st.sec_close = sec, price
        return
    if sec > st.sec:
        # Секунда st.sec закончилась: её закрытие к закрытию прошлой секунды
        if st.prev_close and st.sec_close:
            r = math.log(st.sec_close / st.prev_close)
            v = r * r / max(1, st.sec - st.prev_sec)
            # Сначала — простое среднее (иначе первая же доходность весила бы в
            # EWMA ещё полчаса), с ~600-й секунды — EWMA
            st.var += max(SIGMA_ALPHA, 1.0 / (st.n + 1)) * (v - st.var)
            st.n += 1
        st.prev_sec, st.prev_close = st.sec, st.sec_close
        st.sec, st.sec_close = sec, price
    elif sec == st.sec:
        st.sec_close = price


def _push(st: _Asset, ms: int, p: float, price: float) -> None:
    """Скользящие минимум и максимум вероятности за окно рывка."""
    lim = ms - window_ms()
    while st.minq and st.minq[0][0] < lim:
        st.minq.popleft()
    while st.maxq and st.maxq[0][0] < lim:
        st.maxq.popleft()
    item = (ms, p, price)
    while st.minq and st.minq[-1][1] >= p:
        st.minq.pop()
    st.minq.append(item)
    while st.maxq and st.maxq[-1][1] <= p:
        st.maxq.pop()
    st.maxq.append(item)


def _path_binance(ev: dict, rel: int, price: float) -> None:
    pb = ev["path_b"]
    if pb and pb[-1][1] == price or len(pb) >= MAX_PATH:
        return
    if pb and pb[-1][0] // BINANCE_PATH_STEP_MS == rel // BINANCE_PATH_STEP_MS:
        pb[-1] = (rel, price)
    else:
        pb.append((rel, price))


def _bucket_add(seq, ms: int, price: float, step: int = VENUE_STEP_MS, cap: int = MAX_PATH) -> None:
    """Точка цены в путь: одна на интервал step (последняя в нём)."""
    if seq and seq[-1][1] == price:
        return
    if seq and seq[-1][0] // step == ms // step:
        seq[-1] = (ms, price)
    elif len(seq) < cap:
        seq.append((ms, price))


def _compress(path, step: int = VENUE_STEP_MS) -> list:
    out: list = []
    for ms, px in sorted(path):
        _bucket_add(out, ms, px, step)
    return out


# ------------------------------------------------------- другие биржи ----

def on_venue_trade(venue: str, asset: str, price: float, trade_ms: int, recv_ms: int) -> None:
    """Сделка с другой биржи (src/venues.py) или точка Chainlink."""
    if not enabled():
        return
    d = recv_ms - trade_ms
    if -5000 < d < 60000:
        dq = _vdelay.get(venue)
        if dq is None:
            dq = _vdelay[venue] = collections.deque(maxlen=2000)
        dq.append(d)
    ring = _vring.get((venue, asset))
    if ring is None:
        ring = _vring[(venue, asset)] = collections.deque()
    if not ring or ring[-1][1] != price:              # все смены цены, без повторов
        ring.append((recv_ms, price))
    lim = recv_ms - TOP_KEEP_MS
    while len(ring) >= 2 and ring[1][0] < lim:   # одну точку старше горизонта держим
        ring.popleft()
    st = _assets.get(asset)
    ev = st.event if st else None
    if ev is not None:
        rel = recv_ms - ev["t0"]
        path = ev["vpath"].setdefault(venue, [])
        if rel <= VENUE_REC_MS and len(path) < VENUE_MAX_POINTS and (not path or path[-1][1] != price):
            path.append((rel, price))


def on_chainlink(asset: str, ts_sec: float, value: float, rx_sec: float) -> None:
    try:
        on_venue_trade("cl", asset, float(value), int(float(ts_sec) * 1000), int(float(rx_sec) * 1000))
    except (TypeError, ValueError):
        pass


def _venue_pre(asset: str, t0: int) -> dict:
    """Пути бирж за PRE_MS до рывка (+ точка на начало окна)."""
    out = {}
    start = t0 - PRE_MS
    for (venue, a), ring in _vring.items():
        if a != asset:
            continue
        prior, inside = None, []
        for ms, px in ring:
            if ms < start:
                prior = px
            elif ms <= t0:
                inside.append((ms - t0, px))
        path = ([(-PRE_MS, prior)] if prior is not None else []) + inside
        if path:
            out[venue] = path
    return out


def _cross(path, ref_rel: int, sign: int, thr: float):
    """Первый момент после ref_rel, когда цена ушла от цены в ref_rel на thr (доля) в сторону sign."""
    ref = None
    for rel, px in path:
        if rel <= ref_rel:
            ref = px
        else:
            break
    if not ref or ref <= 0:
        return None
    for rel, px in path:
        if rel > ref_rel and px > 0 and sign * math.log(px / ref) >= thr:
            return rel
    return None


def venue_crossings(ev: dict) -> dict:
    """Когда каждая биржа прошла половину хода Binance спот за рывок (мс от сигнала)."""
    p0, p1 = ev.get("price0"), ev.get("price1")
    if not p0 or not p1 or p0 <= 0 or p1 <= 0 or p0 == p1:
        return {}
    thr = 0.5 * abs(math.log(p1 / p0))
    sign = 1 if p1 > p0 else -1
    ref_rel = ev["old_ms"] - ev["t0"]
    return {v: _cross(sorted(path), ref_rel, sign, thr) for v, path in (ev.get("vpath") or {}).items()}


# ------------------------------------------------------------ Binance ----

def on_binance_trade(symbol: str, price: float, trade_ms: int, recv_ms: int) -> None:
    if not enabled():
        return
    asset = _asset_of_symbol(symbol)
    if asset is None:
        return
    st = _assets.get(asset)
    if st is None:
        st = _assets[asset] = _Asset(asset)
    d = recv_ms - trade_ms
    if -5000 < d < 60000:
        _bn_delay.append(d)
    on_venue_trade("bn", asset, price, trade_ms, recv_ms)      # кольцо и путь Binance спот для сравнения бирж
    _update_sigma(st, price, trade_ms)
    ev = st.event
    if ev is not None:
        _path_binance(ev, recv_ms - ev["t0"], price)
    market = _market_for(asset)
    if market is None:
        return
    if market.slug != st.slug:
        _switch_market(st, market)
    start_ms = market.start_time * 1000
    if trade_ms < start_ms:
        return
    if st.strike is None:
        if market.strike_price:
            st.strike = float(market.strike_price)
        elif trade_ms - start_ms <= STRIKE_FROM_TRADE_SEC * 1000:
            st.strike = price
        else:
            return
    if st.n < SIGMA_MIN_SAMPLES or st.var <= 0 or not st.strike or st.strike <= 0 or price <= 0:
        return
    left = market.end_time - recv_ms / 1000.0
    if left < MIN_LEFT_SEC:
        return
    if price == st.last_p_price and recv_ms - st.last_p_ms < RESAMPLE_MS:
        return
    st.last_p_price, st.last_p_ms = price, recv_ms
    p = phi(math.log(price / st.strike) / (math.sqrt(st.var) * math.sqrt(left)))
    _push(st, recv_ms, p, price)
    if ev is not None or recv_ms < st.cooldown_until:
        return
    j = jump()
    if p - st.minq[0][1] >= j - 1e-12:
        old = st.minq[0]
    elif st.maxq[0][1] - p >= j - 1e-12:
        old = st.maxq[0]
    else:
        return
    if st.per_market >= MAX_EVENTS_PER_MARKET and abs(p - old[1]) < max(j, BIG_JUMP):
        return
    _start_event(st, market, recv_ms, trade_ms, old, (recv_ms, p, price), left)


def _ask_size(token: str, ask: float | None):
    b = book_stream.get_book(token)
    if not b or ask is None:
        return None
    return b.get("asks", {}).get(ask)


def _start_event(st: _Asset, market, t0: int, trade_ms: int, old, new, left: float) -> None:
    direction = 1 if new[1] > old[1] else -1
    fav = market.up_token_id if direction > 0 else market.down_token_id
    book = book_stream.get_book(fav)
    if not book or not book.get("asks"):
        return                       # стакана нашей стороны нет — мерить нечего
    path_book = []
    start = t0 - PRE_MS
    for tok, side in ((market.up_token_id, "u"), (market.down_token_id, "d")):
        prior, inside = None, []
        for r in _tops.get(tok, ()):
            if r[0] < start:
                prior = r
            elif r[0] <= t0:
                inside.append(r)
        if prior is not None:
            path_book.append((-PRE_MS, side, prior[1], prior[2], prior[3], None))
        elif not inside:
            a = book_stream.best_ask(tok)
            path_book.append((-PRE_MS, side, book_stream.best_bid(tok), a, _ask_size(tok, a), None))
        for r in inside:
            path_book.append((r[0] - t0, side, r[1], r[2], r[3], (r[4] - t0) if r[4] else None))
    asks = sorted(book["asks"].items())
    ask0 = asks[0][0]
    ev = {
        "asset": st.asset, "slug": market.slug, "t0": t0, "trade_ms": trade_ms, "old_ms": old[0],
        "left": left, "dir": direction, "p0": old[1], "p1": new[1], "price0": old[2], "price1": new[2],
        "strike": st.strike, "sigma": math.sqrt(st.var),
        "up": market.up_token_id, "down": market.down_token_id, "fav": fav,
        "fav_bid0": book_stream.best_bid(fav), "fav_ask0": ask0, "fav_ask_size0": asks[0][1],
        "depth0": [[p, round(s, 2)] for p, s in asks if p <= ask0 + DEPTH_SPAN + 1e-9][:DEPTH_LEVELS],
        "book_age": (t0 - book["received_ms"]) if book.get("received_ms") else None,
        "path_b": [], "path_book": path_book, "vpath": _venue_pre(st.asset, t0),
    }
    _path_binance(ev, old[0] - t0, old[2])
    _path_binance(ev, 0, new[2])
    st.event = ev
    st.cooldown_until = t0 + record_ms() + cooldown_ms()
    st.per_market += 1
    _stats["events"] += 1


# ----------------------------------------------------------- Polymarket ----

def on_book_update(token: str, recv_ms: int, server_ms: int | None) -> None:
    if not enabled():
        return
    if server_ms:
        d = recv_ms - server_ms
        if -5000 < d < 60000:
            _pm_delay.append(d)
    owner = _token_side.get(token)
    if owner is None:
        return
    bid, ask = book_stream.best_bid(token), book_stream.best_ask(token)
    dq = _tops.get(token)
    if dq is None:
        dq = _tops[token] = collections.deque()
    if dq and dq[-1][1] == bid and dq[-1][2] == ask:
        return
    size = _ask_size(token, ask)
    dq.append((recv_ms, bid, ask, size, server_ms))
    lim = recv_ms - TOP_KEEP_MS
    while len(dq) >= 2 and dq[1][0] < lim:   # одну запись старше горизонта держим: состояние на его начале
        dq.popleft()
    st = _assets.get(owner[0])
    ev = st.event if st else None
    # После рывка пишем только «нашу» сторону: стаканы сторон зеркальные
    # (bid одной = 1 − ask другой), до рывка в событии есть обе
    if ev is not None and token == ev["fav"] and len(ev["path_book"]) < MAX_PATH:
        ev["path_book"].append((recv_ms - ev["t0"], "u" if owner[1] == "up" else "d", bid, ask, size,
                                (server_ms - ev["t0"]) if server_ms else None))


# ------------------------------------------------------------- итоги ----

def _at(series: list, ms: int):
    """(bid, ask) по последнему изменению не позже ms."""
    best = None
    for r in series:
        if r[0] <= ms:
            best = r
        else:
            break
    return (best[2], best[3]) if best is not None else (None, None)


def _mid(bid, ask):
    return (bid + ask) / 2 if bid is not None and ask is not None else None


def _r(x, nd=4):
    return round(x, nd) if x is not None else None


def compute(ev: dict) -> dict:
    """Метрики события (см. описание модуля)."""
    side = "u" if ev["dir"] > 0 else "d"
    s = sorted((r for r in ev["path_book"] if r[1] == side), key=lambda r: r[0])
    bid0, ask0 = _at(s, 0)
    if ask0 is None:
        ask0 = ev.get("fav_ask0")
    if bid0 is None:
        bid0 = ev.get("fav_bid0")
    mid0 = _mid(bid0, ask0)
    rec = record_ms()
    bid_e, ask_e = _at(s, rec)
    mid_end = _mid(bid_e, ask_e)
    mid_old = _mid(*_at(s, ev["old_ms"] - ev["t0"]))
    reprice = None
    if ask0 is not None:
        for r in s:
            if 0 < r[0] <= rec and (r[3] is None or r[3] > ask0 + 1e-9):
                reprice = r[0]
                break
    need = 0.5 * abs(ev["p1"] - ev["p0"])
    mid_reprice = None
    if mid0 is not None:
        for r in s:
            m = _mid(r[2], r[3])
            if 0 < r[0] <= rec and m is not None and m - mid0 >= need - 1e-9:
                mid_reprice = r[0]
                break
    out = {
        "fav_bid0": bid0, "fav_ask0": ask0, "reprice_ms": reprice, "mid_reprice_ms": mid_reprice,
        "mid_end": _r(mid_end), "bid_end": bid_e,
        "move": _r(mid_end - mid0) if (mid_end is not None and mid0 is not None) else None,
        "pre_move": _r(mid0 - mid_old) if (mid0 is not None and mid_old is not None) else None,
    }
    for lat in LATENCIES_MS:
        a = _at(s, lat)[1]
        if lat == 0 and a is None:
            a = ask0
        out[f"ask_{lat}"] = a
        out[f"edge_{lat}"] = _r(mid_end - a - fee(a)) if (a is not None and mid_end is not None) else None
    return out


def _row(ev: dict) -> dict:
    m = compute(ev)
    row = {
        "ts": int(ev["t0"] // 1000), "t0_ms": ev["t0"], "asset": ev["asset"], "market_slug": ev["slug"],
        "sec_left": round(ev["left"], 1), "direction": "UP" if ev["dir"] > 0 else "DOWN",
        "p0": _r(ev["p0"]), "p1": _r(ev["p1"]), "jump_ms": ev["t0"] - ev["old_ms"],
        "price0": ev["price0"], "price1": ev["price1"], "strike": ev["strike"],
        "sigma_1s": float(f"{ev['sigma']:.6g}"), "fav_ask_size0": ev["fav_ask_size0"], "book_age_ms": ev["book_age"],
        "bn_delay_ms": _median(_bn_delay), "pm_delay_ms": _median(_pm_delay), "rtt_ms": _median(_rtt),
        "outcome": None,
        "depth0": json.dumps(ev["depth0"], separators=(",", ":")),
        "book_path": json.dumps([list(r) for r in sorted(ev["path_book"], key=lambda r: r[0])], separators=(",", ":")),
        "binance_path": json.dumps([list(r) for r in ev["path_b"]], separators=(",", ":")),
        "venue_x": json.dumps(venue_crossings(ev), separators=(",", ":")),
        "venue_paths": json.dumps({v: [list(r) for r in _compress(p)] for v, p in (ev.get("vpath") or {}).items()},
                                  separators=(",", ":")),
    }
    row.update(m)
    return row


def finalize_due(now: int | None = None) -> int:
    """Закрыть события, у которых кончилось время записи. Возвращает, сколько записали."""
    now = now_ms() if now is None else now
    rows = []
    for st in _assets.values():
        ev = st.event
        if ev is None or now < ev["t0"] + record_ms():
            continue
        st.event = None
        try:
            rows.append(_row(ev))
        except Exception as exc:  # noqa: BLE001 — запись не должна ронять бота
            _stats["errors"] += 1
            log.warning("speed_lab: не посчитал событие %s: %s", ev.get("slug"), exc)
    if rows:
        try:
            storage.save_speed_events(rows)
            _stats["saved"] += len(rows)
        except Exception as exc:  # noqa: BLE001
            _stats["errors"] += len(rows)
            log.warning("speed_lab: не записал %d событий: %s", len(rows), exc)
            return 0
    return len(rows)


def _median(dq) -> float | None:
    vals = list(dq)
    return float(median(vals)) if vals else None


async def _probe_rtt() -> None:
    """Время ответа CLOB Polymarket (публичный /book по текущему токену) —
    примерно столько занимает дорога ордера туда и обратно."""
    import httpx
    tok = next(iter(_token_side), None)
    if tok is None:
        return
    url = settings.POLY_HOST.rstrip("/") + "/book"
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.get(url, params={"token_id": tok})       # соединение (TLS) — не в счёт
        t = time.perf_counter()
        resp = await client.get(url, params={"token_id": tok})
        if resp.status_code == 200:
            _rtt.append((time.perf_counter() - t) * 1000)


async def run_forever(active_markets: dict | None = None) -> None:
    install(active_markets)
    last_rtt = last_clean = 0.0
    while True:
        await asyncio.sleep(FINALIZE_EVERY_SEC)
        if not enabled():
            continue
        try:
            finalize_due()
        except Exception as exc:  # noqa: BLE001
            log.warning("speed_lab: %s", exc)
        mono = time.monotonic()
        if mono - last_rtt >= RTT_EVERY_SEC:
            last_rtt = mono
            try:
                await _probe_rtt()
            except Exception:  # noqa: BLE001
                pass
        if mono - last_clean >= CLEANUP_EVERY_SEC:
            last_clean = mono
            try:
                storage.cleanup_speed(int(time.time() - float(settings.SPEED_LAB_KEEP_DAYS) * 86400))
            except Exception:  # noqa: BLE001
                pass


# -------------------------------------------------------------- тексты ----

def _pct(n: int, total: int) -> str:
    return f"{n / total * 100:.0f}%" if total else "—"


def _avg(vals: list) -> float | None:
    return sum(vals) / len(vals) if vals else None


def _cents(x: float | None) -> str:
    return f"{x * 100:+.2f}¢" if x is not None else "—"


def latency_line() -> str:
    lat = []
    for name, dq in (("Binance → бот", _bn_delay), ("Polymarket → бот", _pm_delay), ("запрос к CLOB", _rtt)):
        m = _median(dq)
        if m is not None:
            lat.append(f"{name} {m:.0f} мс")
    for v, dq in _vdelay.items():
        m = _median(dq)
        if m is not None and v != "bn":
            lat.append(f"{VENUE_NAMES.get(v, v)} → бот {m:.0f} мс")
    return ("Наши задержки (медиана): " + " · ".join(lat)) if lat else ""


def hold_pnl(rows: list[dict], lat: int) -> tuple[float | None, int]:
    """Средний итог на акцию, если купить ask_<lat> и держать до конца рынка."""
    vals = []
    for r in rows:
        a = r.get(f"ask_{lat}")
        if a is None or r.get("outcome") not in ("UP", "DOWN"):
            continue
        win = r["outcome"] == r["direction"]
        vals.append((1 - a if win else -a) - fee(a))
    return _avg(vals), len(vals)


def summary_lines(since_ts: int = 0) -> list[str]:
    rows = storage.speed_rows_since(since_ts)
    n = len(rows)
    head = f"≥ {jump() * 100:g}¢ вероятности за {window_ms() / 1000:g} с"
    if not n:
        lines = [f"Рывков Binance ({head}) пока не было" + ("" if enabled() else " — запись выключена") + "."]
    else:
        by_asset: dict[str, int] = {}
        for r in rows:
            by_asset[r["asset"]] = by_asset.get(r["asset"], 0) + 1
        lines = [f"Рывков Binance: {n} ({head}): "
                 + " · ".join(f"{a.upper()} {k}" for a, k in sorted(by_asset.items(), key=lambda kv: -kv[1]))]
        size = _avg([abs(r["p1"] - r["p0"]) for r in rows if r["p0"] is not None and r["p1"] is not None])
        pre = _avg([r["pre_move"] for r in rows if r["pre_move"] is not None])
        if size is not None and pre is not None:
            lines.append(f"Рынок сдвинулся ещё во время рывка, до нашего сигнала: в среднем {_cents(pre)} "
                         f"из {size * 100:.1f}¢ рывка.")
        rep = [r["reprice_ms"] for r in rows]
        known = [x for x in rep if x is not None]
        lines.append("Старый ask продержался дольше " + " · ".join(
            f"{lat} мс — {_pct(sum(1 for x in rep if x is None or x > lat), n)}" for lat in (250, 500, 1000))
            + (f"; медиана {median(known):.0f} мс" if known else ""))
        parts = []
        for lat in (0, 250, 500, 1000):
            v = _avg([r[f"edge_{lat}"] for r in rows if r[f"edge_{lat}"] is not None])
            if v is not None:
                parts.append(f"{'сразу' if lat == 0 else f'{lat} мс'} {_cents(v)}")
        if parts:
            lines.append("Запас покупки через 6 с (на акцию, после комиссии), если купить: " + " · ".join(parts))
        hold = []
        for lat in (0, 500):
            v, k = hold_pnl(rows, lat)
            if v is not None:
                hold.append((lat, v, k))
        if hold:
            lines.append(f"Если держать до конца рынка ({hold[0][2]} с исходом): " + " · ".join(
                f"{'сразу' if lat == 0 else f'{lat} мс'} {_cents(v)}" for lat, v, _k in hold))
        lines += venue_lines(rows)
    lat = latency_line()
    if lat:
        lines.append(lat)
    return lines


def venue_lines(rows: list[dict]) -> list[str]:
    """Кто двигается первым: биржи против Binance спот и Polymarket против бирж."""
    leads: dict[str, list] = collections.defaultdict(list)
    first: collections.Counter = collections.Counter()
    pm = collections.defaultdict(lambda: [0, 0])        # биржа -> [событий, Polymarket раньше]
    n_first = 0
    for r in rows:
        try:
            vx = json.loads(r.get("venue_x") or "{}")
        except (TypeError, ValueError):
            continue
        known = {v: x for v, x in vx.items() if x is not None and v != "cl"}
        bn = known.get("bn")
        for v, x in vx.items():
            if x is not None and bn is not None and v != "bn":
                leads[v].append(x - bn)
        if len(known) >= 2:
            n_first += 1
            first[min(known, key=lambda v: (known[v], v != "bn"))] += 1
        mr = r.get("mid_reprice_ms")
        if mr is not None:
            for v, x in vx.items():
                if x is not None:
                    pm[v][0] += 1
                    pm[v][1] += mr < x
    out = []
    order = [v for v in VENUE_NAMES if v != "bn" and leads.get(v)]
    if order:
        out.append("Биржи против Binance спот (медиана, мс; минус — раньше): " + " · ".join(
            f"{VENUE_NAMES[v]} {median(leads[v]):+.0f}" for v in order))
    if n_first:
        out.append("Первой сдвинулась (без Chainlink): " + " · ".join(
            f"{VENUE_NAMES[v]} {_pct(k, n_first)}" for v, k in first.most_common()))
    pm_parts = [f"{VENUE_NAMES.get(v, v)} {_pct(b, n)}" for v, (n, b) in pm.items() if n]
    if pm_parts:
        out.append("Polymarket прошёл половину хода раньше биржи: " + " · ".join(pm_parts))
    return out


def menu_text() -> str:
    lines = [
        "⚡ Скорость рынка — " + ("запись идёт" if enabled() else "запись выключена") + " (ничего не покупает)",
        "",
        "Когда цена на Binance резко сдвигает шансы исхода, бот записывает стакан Polymarket до и после "
        "рывка с точностью до миллисекунд: сколько держится старая цена и сколько можно было бы на ней "
        "заработать при нашей скорости. Заодно — цены других бирж и Chainlink: кто двигается первым.",
        "",
        "За 24 часа:",
    ]
    lines += ["  " + x for x in summary_lines(int(time.time()) - 86400)]
    status = venues.status_line()
    if status:
        lines.append("  " + status)
    lines += [
        "",
        "Как читать: окно есть, если старый ask часто живёт дольше наших задержек, а запас при 250–500 мс "
        "стабильно выше +1¢. Около нуля или минус — быстрее рынка нам не успеть. Биржа с минусом — "
        "двигается раньше Binance спот: по ней бот мог бы замечать рывки раньше.",
        "Все события — файлом …_speed.csv в отчёте раз в 4 часа.",
    ]
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    return "\n".join([f"⚡ Скорость рынка {from_label} → {to_label} (UTC)"] + summary_lines(since_ts))
