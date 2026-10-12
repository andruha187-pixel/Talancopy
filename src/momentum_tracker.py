"""
Исследовательский модуль — НЕ торгует, только наблюдает и записывает.

Идея (от пользователя): если достоверно известно, что цена одной стороны
контракта продолжит расти (скажем, с 0.70 до 0.75 или 0.80), можно купить
эту сторону по 0.70, дождаться роста, затем купить ПРОТИВОПОЛОЖНУЮ сторону
по подешевевшей цене — и в зависимости от того, насколько цена в сумме
разошлась, зафиксировать прибыль НЕЗАВИСИМО от итогового исхода рынка
(классический приём "хеджирование после движения цены", используется в
беттинге под названием "middling"). Мы предоставляем данные для проверки
этой идеи, а не саму торговлю — сначала нужно понять, насколько часто
и при каких условиях цена вообще проходит путь в 5-10 центов внутри
одного 5- или 15-минутного окна.

Как это работает:
- Для каждого активного рынка отслеживаем ЛУЧШУЮ (более дорогую) из двух
  сторон — как только её цена (best_ask) впервые попадает в диапазон
  [MOMENTUM_MIN_PRICE, MOMENTUM_MAX_PRICE] (по умолчанию 0.70-0.95),
  начинаем отслеживать контрольные точки с шагом MOMENTUM_STEP (0.05).
- Каждый раз, когда цена этой стороны впервые пересекает очередную
  контрольную точку (0.70 -> 0.75 -> 0.80 -> ...), пишем строку с полным
  снимком индикаторов НА ЭТОТ МОМЕНТ: RSI, MACD, ATR, EMA, полосы
  Боллинджера, объём относительно среднего, и дисбаланс стакана (доля
  объёма на покупку — давление цены, которого нет в ценовых индикаторах).
- После резолюции рынка помечаем все записи по нему итоговым исходом —
  так видно не только "дошла ли цена до следующей точки", но и кто в
  итоге выиграл рынок.

Результат — датасет вида "при каких условиях в момент X цена дошла до
X+0.05 (и через сколько времени), а при каких — нет". Анализировать эти
данные (искать реальную закономерность) нужно отдельно — сам модуль
только собирает.
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import binance_feed, book_stream, indicators, market_discovery, storage
from src.market_discovery import ActiveMarket
from src.timeframes import TimeframeProfile

log = logging.getLogger("momentum_tracker")

MOMENTUM_MIN_PRICE = 0.70
MOMENTUM_MAX_PRICE = 0.95
MOMENTUM_STEP = 0.05

# (market_slug, side) -> наивысшая уже залогированная контрольная точка
_last_checkpoint: dict[tuple[str, str], float] = {}


def _checkpoints_up_to(price: float) -> list[float]:
    """Все контрольные точки из диапазона, которые <= price, по порядку."""
    points = []
    p = MOMENTUM_MIN_PRICE
    while p <= MOMENTUM_MAX_PRICE + 1e-9:
        if p <= price + 1e-9:
            points.append(round(p, 2))
        p += MOMENTUM_STEP
    return points


async def check_market(market: ActiveMarket, timeframe: TimeframeProfile) -> None:
    """Вызывается на каждом тике основного бота (переиспользуем уже
    подписанный WS-стакан и уже посчитанные по Binance индикаторы — без
    лишних запросов к API)."""
    for side, token_id in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
        price = book_stream.best_ask(token_id)
        if price is None or price < MOMENTUM_MIN_PRICE:
            continue

        key = (market.slug, side)
        already = _last_checkpoint.get(key, MOMENTUM_MIN_PRICE - MOMENTUM_STEP)
        crossed = [cp for cp in _checkpoints_up_to(price) if cp > already + 1e-9]
        if not crossed:
            continue

        # Индикаторы считаем один раз (не на каждую пройденную точку —
        # если цена перепрыгнула сразу через несколько ступеней между
        # тиками, всем им ставим один и тот же снимок момента, это и есть
        # состояние на момент обнаружения).
        symbol = binance_feed.symbol_for(market.asset)
        try:
            klines = await binance_feed.get_klines(
                symbol,
                limit=max(100, timeframe.atr_lookback_for_regime + timeframe.atr_period + 5),
                interval=timeframe.kline_interval,
            )
            ind = indicators.compute_extended_snapshot(
                klines, timeframe.atr_period, timeframe.ema_fast, timeframe.ema_slow,
                timeframe.atr_lookback_for_regime,
            )
        except Exception as exc:  # noqa: BLE001 — не мешаем основному тику бота
            log.warning("Не удалось посчитать индикаторы для %s: %s", market.slug, exc)
            continue

        imbalance = book_stream.book_imbalance(token_id)
        minutes_left = max(0.0, (market.end_time - time.time()) / 60)

        for cp in crossed:
            storage.log_momentum_checkpoint(
                market.slug, market.asset, timeframe.label, side, cp, minutes_left, ind, imbalance,
            )

        _last_checkpoint[key] = crossed[-1]
        log.info("%s %s: контрольная точка %.2f (RSI=%.1f, MACD_bull=%s, imbalance=%s)",
                  market.slug, side, crossed[-1], ind.get("rsi") or -1, ind.get("macd_bullish"),
                  round(imbalance, 2) if imbalance is not None else None)


async def label_resolved(exclude_slugs: set[str] | None = None) -> None:
    """Общая фоновая задача (как executor.label_resolved_markets) — узнаём
    исход рынков, по которым уже есть незалейбленные контрольные точки."""
    for slug in storage.get_momentum_markets_needing_outcome(exclude_slugs=exclude_slugs, limit=10):
        try:
            outcome = await market_discovery.get_resolution(slug)
        except Exception as exc:  # noqa: BLE001
            log.warning("Не удалось узнать исход %s: %s", slug, exc)
            continue
        if outcome:
            storage.label_momentum_outcome(slug, outcome)


def cleanup_old_sessions(active_slugs: set[str]) -> None:
    """Чистим in-memory состояние по рынкам, которых уже нет среди активных
    (резолвились) — иначе словарь будет расти вечно."""
    stale = [k for k in _last_checkpoint if k[0] not in active_slugs]
    for k in stale:
        del _last_checkpoint[k]
