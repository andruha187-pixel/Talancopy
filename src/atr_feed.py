"""
ATR по каждой монете: Wilder ATR(ATR_PERIOD) по 1-минутным свечам Binance —
так же, как в прежних 5m-ботах (их колонка atr в отчётах), только без бага
«ломаного» билда: там цена Chainlink шла в сырых единицах (×10^18), и
расстояние в ATR получалось 10^17–10^21, то есть фильтр пропускал всё.
Здесь цена Chainlink уже в обычных единицах (chainlink_feed.normalize_price).

Последняя (ещё не закрытая) свеча входит в расчёт, как и раньше. Обновление —
раз в ATR_REFRESH_SEC секунд по каждой монете, в фоне.
"""
from __future__ import annotations
import asyncio
import logging
import time

import pandas as pd

from config import settings
from src import binance_feed

log = logging.getLogger("atr_feed")

MAX_AGE_SEC = 60.0            # ATR старше минуты не используем — ставки нет
KLINES = 100                  # столько 1m-свечей берём для расчёта

_atr: dict[str, tuple[float, float]] = {}      # монета -> (ATR, когда посчитан)
_errors: dict[str, str] = {}


def wilder_atr(df: pd.DataFrame, period: int) -> float | None:
    """ATR по Уайлдеру (EWM с alpha = 1/period) на последней свече."""
    if df is None or len(df) < 2:
        return None
    high, low, close = df["high"].astype(float), df["low"].astype(float), df["close"].astype(float)
    prev = close.shift(1)
    tr = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    val = float(tr.ewm(alpha=1 / period, adjust=False).mean().iloc[-1])
    return val if val > 0 else None


def get(asset: str, now: float | None = None) -> float | None:
    """ATR монеты, если посчитан не раньше чем MAX_AGE_SEC назад."""
    v = _atr.get(asset)
    now = time.time() if now is None else now
    if v is None or now - v[1] > MAX_AGE_SEC:
        return None
    return v[0]


def put(asset: str, value: float, ts: float | None = None) -> None:
    """Для тестов и ручной подстановки."""
    _atr[asset] = (float(value), time.time() if ts is None else ts)


def reset() -> None:
    _atr.clear()
    _errors.clear()


async def refresh(asset: str) -> float | None:
    try:
        df = await binance_feed.get_klines(binance_feed.symbol_for(asset), limit=KLINES, interval="1m")
        val = wilder_atr(df, max(1, int(settings.ATR_PERIOD)))
    except Exception as exc:  # noqa: BLE001 — Binance недоступен: ATR устареет, ставок по монете не будет
        _errors[asset] = str(exc)[:200]
        return None
    if val is not None:
        _atr[asset] = (val, time.time())
        _errors.pop(asset, None)
    return val


def status_line(assets, now: float | None = None) -> str:
    now = time.time() if now is None else now
    parts = [f"{a.upper()} {'✅' if get(a, now) is not None else '❌'}" for a in assets]
    line = "📐 ATR (Binance 1m): " + ", ".join(parts)
    bad = [a for a in assets if get(a, now) is None and a in _errors]
    if bad:
        line += f". Ошибка Binance ({bad[0].upper()}): {_errors[bad[0]]}"
    return line


async def run_forever() -> None:
    """Фоновая задача: ATR по всем монетам, раз в ATR_REFRESH_SEC секунд."""
    while True:
        assets = list(settings.ASSETS)
        pause = max(0.2, float(settings.ATR_REFRESH_SEC)) / max(1, len(assets))
        for a in assets:
            await refresh(a)
            await asyncio.sleep(pause)
