"""
Быстрая проверка сигнала — раз в ~0.3 с, в дополнение к обычной раз в 3 с.

Зачем. По отчётам 25.09–08.10 у 32% рынков с сигналом условия входа держались
не дольше одной 3-секундной проверки: цена заходила в диапазон 0.90–0.95 и
уходила из него между тиками. Если те же тики проредить (проверка раз в 7,
10.5, 14 с), входов остаётся ~350, 321, 297 из 393 — каждая секунда между
проверками стоит входов. По этой зависимости проверка раз в ~0.5 с даёт
примерно +9–10% входов, винрейт у коротких сигналов такой же (~96–97%).

Как. Обычная проверка (main.py) каждые 3 с тянет свечи Binance REST-запросом,
считает индикаторы, читает стакан и пишет тик в базу — так и осталось.
Быстрая не делает ни одного сетевого запроса:
  - цена — из потока сделок Binance (src/price_stream.py);
  - стакан — живой WS-стакан Polymarket (src/book_stream.py), только свежий;
  - индикаторы — по свечам последнего REST-запроса обычной проверки, где
    последняя свеча досчитана сделками из потока (patch_klines): ATR, EMA и
    MACD такие же, какие посчитала бы обычная проверка в этот момент;
  - решение — та же strategy.evaluate с теми же порогами.
Сначала дешёвые условия (окно входа, цена в диапазоне, нет открытой сделки),
индикаторы — только если они прошли.

Вход — тот же executor.maybe_enter; блокировка на рынок (см. executor) не
даёт обеим проверкам купить один рынок дважды. С 11.10 вход ещё и ждёт
подтверждения (src/entry_confirm.py): сигнал должен быть и ~3 с назад —
быстрая проверка записывает туда результат каждой оценки рынка. В базу быстрая проверка пишет
только тики с сигналом (tick_source = "fast", не чаще раза в 3 с на рынок), а
сделку помечает entry_path = "fast" — ряд тиков раз в 3 с для анализа прежний.

Нет потока цены, стакан не свежий или свечи устарели — быстрая проверка по
монете молчит, обычная работает как раньше.
"""
from __future__ import annotations
import asyncio
import logging
import re
import time
from collections import Counter

import pandas as pd

from config import settings
from src import binance_feed, executor, indicators, polymarket_client, price_stream, runtime_state, storage, strategy
from src import entry_confirm, chainlink_feed, duo
from src.market_discovery import ActiveMarket
from src.polymarket_client import OrderBookSnapshot
from src.timeframes import TIMEFRAMES, TimeframeProfile

log = logging.getLogger("fast_signal")

KLINES_MAX_AGE_MS = 30_000       # свечи старше — быстрая проверка по монете молчит
WS_BEFORE_FETCH_MS = 1000        # поток цены подключён хотя бы за 1 с до запроса свечей
OTHER_BOOK_MAX_AGE_MS = 60_000   # стакан второй стороны нужен только для записи тика
SIGNAL_LOG_EVERY_SEC = 3.0       # тик с сигналом пишем не чаще раза в 3 с на рынок
ERROR_LOG_EVERY_SEC = 60.0
STARTUP_GRACE_SEC = 30.0         # первые секунды после старта поток ещё подключается

_klines: dict[str, tuple[pd.DataFrame, int]] = {}
_ind_memo: dict[str, tuple] = {}     # asset -> (ключ входных данных, индикаторы)
_last_logged: dict[str, float] = {}
_last_error_log: dict[str, float] = {}
_tasks: set = set()
# Чем закончилась каждая быстрая проверка (enter, range, window, no_price, …)
stats: Counter = Counter()
_started_at: float = time.time()


def reset() -> None:
    """Сброс состояния (для тестов)."""
    _klines.clear()
    _ind_memo.clear()
    _last_logged.clear()
    _last_error_log.clear()
    stats.clear()


def remember_klines(asset: str, df: pd.DataFrame, fetch_ms: int) -> None:
    """Обычная проверка отдаёт сюда свечи последнего REST-запроса и время,
    когда запрос ушёл (наши часы, мс)."""
    if df is None or len(df) < 2 or "open_time" not in df.columns:
        return
    _klines[asset] = (df, int(fetch_ms))


def patch_klines(df: pd.DataFrame, fetch_ms: int, ticker: price_stream.Ticker | None,
                 conn_open_since_ms: int, now_ms: int | None = None) -> pd.DataFrame | None:
    """Свечи последнего REST-запроса + сделки из потока после него.

    Обычно запрос был 0–3 с назад, и последняя свеча в нём — текущая минута:
    её максимум/минимум расширяем сделками потока за эту минуту, закрытие —
    последняя цена. Если с запроса минута сменилась, прошлую свечу
    досчитываем сделками потока, а новую добавляем (первая строка уходит —
    свечей столько же, сколько вернул бы новый запрос). Минута сменилась
    больше одного раза — свечи устарели, None.

    None и тогда, когда поток подключился позже, чем ушёл запрос: сделки между
    ними не видны ни там, ни там, и максимум/минимум свечи мог бы быть неверным.
    """
    if ticker is None or ticker.price is None or df is None or len(df) < 2:
        return None
    now_ms = price_stream.now_ms() if now_ms is None else now_ms
    if not conn_open_since_ms or conn_open_since_ms > fetch_ms - WS_BEFORE_FETCH_MS:
        return None
    if now_ms - fetch_ms > KLINES_MAX_AGE_MS:
        return None
    last_open = int(df["open_time"].iloc[-1])
    trade_minute = ticker.trade_ms // 60000 * 60000
    i_high, i_low, i_close = (df.columns.get_loc(c) for c in ("high", "low", "close"))

    if trade_minute == last_open:
        out = df.copy()
        row = len(out) - 1
        if ticker.minute_ms == last_open:
            out.iat[row, i_high] = max(float(out.iat[row, i_high]), ticker.high)
            out.iat[row, i_low] = min(float(out.iat[row, i_low]), ticker.low)
        out.iat[row, i_close] = ticker.price
        return out

    if trade_minute == last_open + 60_000 and ticker.minute_ms == trade_minute:
        out = df.copy()
        row = len(out) - 1
        prev = ticker.prev
        if prev is not None and prev[0] == last_open:
            # Сделки потока в прошлой минуте после запроса — досчитываем её
            out.iat[row, i_high] = max(float(out.iat[row, i_high]), prev[2])
            out.iat[row, i_low] = min(float(out.iat[row, i_low]), prev[3])
            out.iat[row, i_close] = prev[4]
        new = out.iloc[[row]].copy()
        new.iat[0, df.columns.get_loc("open_time")] = trade_minute
        if "open" in df.columns:
            new.iat[0, df.columns.get_loc("open")] = ticker.open
        new.iat[0, i_high] = ticker.high
        new.iat[0, i_low] = ticker.low
        new.iat[0, i_close] = ticker.price
        if "volume" in df.columns:
            new.iat[0, df.columns.get_loc("volume")] = 0.0
        return pd.concat([out.iloc[1:], new], ignore_index=True)

    return None


def indicators_now(asset: str, timeframe: TimeframeProfile, ticker: price_stream.Ticker,
                   now_ms: int | None = None) -> dict | None:
    """Индикаторы на этот момент: свечи обычной проверки + сделки потока.
    Если с прошлого раза не было ни новых свечей, ни новых сделок — тот же
    результат без пересчёта (~2–3 мс на pandas)."""
    cached = _klines.get(asset)
    if cached is None:
        return None
    df, fetch_ms = cached
    conn = price_stream.connection_for(ticker.symbol)
    if conn is None or not conn.is_open:
        return None
    now_ms = price_stream.now_ms() if now_ms is None else now_ms
    if now_ms - fetch_ms > KLINES_MAX_AGE_MS:
        return None
    key = (id(df), fetch_ms, conn.open_since_ms, ticker.trade_ms, ticker.price, ticker.minute_ms,
           ticker.high, ticker.low, ticker.prev)
    memo = _ind_memo.get(asset)
    if memo is not None and memo[0] == key:
        return memo[1]
    patched = patch_klines(df, fetch_ms, ticker, conn.open_since_ms, now_ms)
    if patched is None:
        return None
    ind = indicators.compute_indicator_snapshot(
        patched, timeframe.atr_period, timeframe.ema_fast, timeframe.ema_slow, timeframe.atr_lookback_for_regime,
    )
    _ind_memo[asset] = (key, ind)
    return ind


def _task_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.warning("Ошибка входа из быстрой проверки: %r", task.exception())


def _spawn(coro) -> None:
    # Вход — отдельной задачей: пока по одной монете исполняется ордер,
    # быстрая проверка остальных монет не ждёт.
    task = asyncio.get_running_loop().create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_task_done)


def check(asset: str, timeframe: TimeframeProfile, market: ActiveMarket, now: float | None = None) -> str:
    """Одна быстрая проверка одного рынка. Возвращает, чем закончилась:
    "enter" — сигнал, вход запущен; "wait" — сигнал есть, но ещё не
    подтверждён (~3 с назад его не было); остальное — почему нет (stats)."""
    now = time.time() if now is None else now
    if now >= market.end_time:
        return "expired"
    minutes_left = (market.end_time - now) / 60
    win_min, win_max = runtime_state.entry_window()
    if not (win_min <= minutes_left <= win_max):
        entry_confirm.record(market.slug, None, False, now)
        return "window"
    ticker = price_stream.latest(binance_feed.symbol_for(asset))
    if ticker is None:
        return "no_price"
    # Цена и страйк для сигнала: Chainlink (как Polymarket) или Binance. Свечи
    # (ATR/EMA/MACD) — всегда Binance, из потока выше.
    pv = chainlink_feed.view(asset, market, ticker.price, now)
    if pv.problem:
        entry_confirm.record(market.slug, None, False, now)
        return "no_chainlink"
    up = pv.price >= pv.strike
    token = market.up_token_id if up else market.down_token_id
    other_token = market.down_token_id if up else market.up_token_id
    book = polymarket_client.ws_snapshot(token)
    if book is None:
        return "no_book"
    lo, hi = runtime_state.entry_range(asset)
    if not (lo - 1e-9 <= book.best_ask <= hi + 1e-9):
        # Цена стороны вне диапазона — сигнала нет (для подтверждения входа)
        entry_confirm.record(market.slug, "UP" if up else "DOWN", False, now)
        return "range"
    if executor.entry_busy(market.slug, now):
        return "busy"
    # Бот двух стратегий: по рынку уже есть виртуальная сделка, и ни одна
    # стратегия сейчас войти не хочет (src/duo.py) — делать нечего.
    if (duo.market_done(market.slug, now) if duo.enabled()
            else storage.get_open_trade_for_market(market.slug)):
        return "open"
    ind = indicators_now(asset, timeframe, ticker)
    if ind is None:
        return "no_klines"
    other = (polymarket_client.ws_snapshot(other_token, max_age_ms=OTHER_BOOK_MAX_AGE_MS)
             or OrderBookSnapshot(best_bid=None, best_ask=None, ask_liquidity_usdc=0.0, source="ws"))
    up_book, down_book = (book, other) if up else (other, book)
    price = ind["close"] if pv.source == "binance" else pv.price
    decision = strategy.evaluate(
        current_price=price,
        strike_price=pv.strike,
        minutes_left=minutes_left,
        indicators=ind,
        up_book=up_book,
        down_book=down_book,
        min_minutes_left=win_min,
        max_minutes_left=win_max,
        atr_distance_mult=timeframe.atr_distance_mult,
        atr_spike_mult=timeframe.atr_spike_mult,
        asset=asset,
    )
    entry_confirm.record(market.slug, decision.direction, decision.should_enter, now)
    if not decision.should_enter:
        return "no_signal"
    if now - _last_logged.get(market.slug, 0.0) >= SIGNAL_LOG_EVERY_SEC:
        if len(_last_logged) > 2000:
            _last_logged.clear()
        _last_logged[market.slug] = now
        storage.log_signal(market.slug, price, pv.strike, decision,
                           indicators=ind, up_book=up_book, down_book=down_book, tick_source="fast",
                           price_source=pv.log_source, binance_price=ind["close"],
                           binance_strike=market.strike_price)
        log.info("⚡ %s | %s price=%.4f strike=%.4f dir=%s left=%.2fm score=%.1f ask=%.3f — сигнал быстрой проверки",
                 market.slug, pv.source, price, pv.strike, decision.direction, minutes_left,
                 decision.safety_score, book.best_ask)
    if not entry_confirm.confirmed(market.slug, decision.direction, now):
        # Сигнал есть, но ~3 с назад его не было — ждём, держится ли он
        return "wait"
    _spawn(executor.maybe_enter(market, decision, path="fast"))
    return "enter"


def _log_error(asset: str, exc: BaseException) -> None:
    now = time.time()
    if now - _last_error_log.get(asset, 0.0) >= ERROR_LOG_EVERY_SEC:
        _last_error_log[asset] = now
        log.exception("Ошибка быстрой проверки %s: %s", asset, exc)


async def run_forever(markets: dict) -> None:
    """Фоновая задача. markets — словарь main._active_markets
    ("btc:5m" -> ActiveMarket); его ведёт обычная проверка."""
    global _started_at
    _started_at = time.time()
    interval = max(0.1, float(settings.FAST_LOOP_INTERVAL_SEC))
    timeframes = {tf.label: tf for tf in TIMEFRAMES}
    while True:
        started = time.monotonic()
        if runtime_state.get("fast_entry_enabled") and not runtime_state.get("paused"):
            for key, market in list(markets.items()):
                asset, _, label = key.partition(":")
                tf = timeframes.get(label)
                if tf is None or not runtime_state.is_asset_enabled(asset):
                    continue
                try:
                    stats[check(asset, tf, market)] += 1
                except Exception as exc:  # noqa: BLE001 — сбой быстрой проверки не должен ронять бота
                    stats["error"] += 1
                    _log_error(asset, exc)
                # check синхронный — между монетами отдаём управление, чтобы
                # сообщения стакана и потока цены не ждали всю пачку
                await asyncio.sleep(0)
        await asyncio.sleep(max(0.05, interval - (time.monotonic() - started)))


def status_line() -> str:
    """Строка для меню и «🔎 Почему нет сделок» (без Markdown-символов)."""
    if not runtime_state.get("fast_entry_enabled"):
        return "⚡ Быстрый вход: выкл — сигнал проверяется раз в 3 с"
    if not settings.USE_LIVE_BOOK_STREAM:
        return "⚡ Быстрый вход: недоступен без живого стакана — сигнал проверяется раз в 3 с"
    interval = max(0.1, float(settings.FAST_LOOP_INTERVAL_SEC))
    head = f"⚡ Быстрый вход: вкл, проверка раз в {interval:g} с"
    assets = [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]
    if not assets:
        return head
    missing = [a for a in assets if price_stream.latest(binance_feed.symbol_for(a)) is None]
    if not missing:
        return f"{head}, цена Binance потоком ✅"
    if time.time() - _started_at < STARTUP_GRACE_SEC:
        return f"{head}, поток цены Binance подключается"
    if len(missing) == len(assets):
        # Текст ошибки идёт в меню с Markdown — убираем символы разметки
        err = re.sub(r"[_*`\[\]]", " ", price_stream.last_error())[:120].strip()
        return (f"{head}, но поток цены Binance сейчас не работает — пока проверка раз в 3 с"
                + (f" ({err})" if err else ""))
    names = ", ".join(a.upper() for a in missing)
    return f"{head}, цена Binance потоком: {len(assets) - len(missing)} из {len(assets)} монет (нет: {names})"


def entries_line(trades: list, columns: list) -> str:
    """'⚡ Входов быстрой проверкой: 3 из 10' по строкам trades (стратегия)."""
    i = {c: k for k, c in enumerate(columns)}
    if "entry_path" not in i:
        return ""
    strat = [t for t in trades if (t[i["source"]] or "strategy") == "strategy"]
    if not strat:
        return ""
    fast = sum(1 for t in strat if t[i["entry_path"]] == "fast")
    return f"⚡ Входов быстрой проверкой: {fast} из {len(strat)}"
