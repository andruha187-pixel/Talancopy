"""
Профиль таймфрейма для этого бота — ТОЛЬКО 5 минут. Это отдельный,
самостоятельный проект (форк основного 15m-бота), альтернативные таймфреймы
намеренно удалены, а не просто выключены — держим кодовую базу узкой
под одну конкретную задачу: тест 5-минутного рынка.

ВАЖНО: параметры ниже — ПРОПОРЦИОНАЛЬНО пересчитанные из того, что
реально сработало на 15-минутном боте (после калибровки по 961 реальной
сделке другого трейдера + по живым 4-часовым отчётам), а не откалиброванные
на реальных 5-минутных данных. Считай их отправной точкой для теста в
DRY_RUN, не готовым к LIVE значением.

- Индикаторы на 1m-свечах (тоньше некуда у Binance).
- ATR(7)/EMA(3,7) вместо ATR(14)/EMA(9,21) — короче период, пропорционально
  более быстрому рынку (5 минут вместо 15).
- Окно входа 1.0-3.5 минуты из 5 (пропорция окна 2-9 из 15 у 15m-версии).
- discovery_mode="deterministic" — 5-минутные рынки Polymarket используют
  тот же формат слага, что и 15m (тикер + unix-таймстемп начала окна),
  никакой человекочитаемой привязки к Eastern Time, как у часовых.
- Опрос раз в 3 секунды — рынок живёт всего 5 минут, нужна более частая
  проверка, чем у 15-минутного (там 5 секунд).
"""
from __future__ import annotations
from dataclasses import dataclass
import os


def _f(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _i(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _s(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class TimeframeProfile:
    label: str
    interval_minutes: int
    kline_interval: str
    atr_period: int
    ema_fast: int
    ema_slow: int
    atr_lookback_for_regime: int
    min_minutes_left: float
    max_minutes_left: float
    atr_distance_mult: float
    atr_spike_mult: float
    discovery_mode: str            # "deterministic" | "series" (см. market_discovery.py)
    poll_interval_seconds: int


TIMEFRAMES: list[TimeframeProfile] = [
    TimeframeProfile(
        label="5m",
        interval_minutes=5,
        kline_interval=_s("TF_5M_KLINE_INTERVAL", "1m"),
        atr_period=_i("TF_5M_ATR_PERIOD", 7),
        ema_fast=_i("TF_5M_EMA_FAST", 3),
        ema_slow=_i("TF_5M_EMA_SLOW", 7),
        atr_lookback_for_regime=_i("TF_5M_ATR_LOOKBACK", 30),
        min_minutes_left=_f("TF_5M_MIN_MINUTES_LEFT", 1.0),
        max_minutes_left=_f("TF_5M_MAX_MINUTES_LEFT", 3.5),
        atr_distance_mult=_f("TF_5M_ATR_DISTANCE_MULT", 1.5),
        atr_spike_mult=_f("TF_5M_ATR_SPIKE_MULT", 2.2),
        discovery_mode="deterministic",
        poll_interval_seconds=_i("TF_5M_POLL_SECONDS", 3),
    ),
]
