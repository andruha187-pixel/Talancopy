"""
Расчёт технических индикаторов поверх OHLC-данных с биржи.
Используем pandas — минимум кода, минимум шансов на ошибку в формулах.
"""
from __future__ import annotations
import pandas as pd


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average True Range по стандартной формуле (Wilder).
    df должен содержать колонки high, low, close.
    """
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1 / period, adjust=False).mean()


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[pd.Series, pd.Series, pd.Series]:
    """MACD: разница между быстрой и медленной EMA — реагирует на смену
    моментума раньше, чем простое сравнение EMA9 vs EMA21, потому что
    считает СКОРОСТЬ схождения/расхождения средних, а не просто их порядок.
    Возвращает (macd_line, signal_line, histogram)."""
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index — 0-100, >70 обычно трактуют как
    перекупленность, <30 как перепроданность. Для исследования моментума
    интереснее не сами пороги, а то, коррелирует ли текущий RSI с тем,
    продолжит ли цена ЭТОГО конкретного контракта Polymarket двигаться в
    ту же сторону в оставшееся до конца окна время."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))


def bollinger_bands(series: pd.Series, period: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Возвращает (upper, middle, lower). middle = SMA(period)."""
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = middle + num_std * std
    lower = middle - num_std * std
    return upper, middle, lower


def compute_extended_snapshot(df: pd.DataFrame, atr_period: int, ema_fast: int, ema_slow: int,
                               atr_regime_lookback: int, rsi_period: int = 14,
                               bb_period: int = 20) -> dict:
    """
    Расширенный набор индикаторов ДЛЯ ИССЛЕДОВАТЕЛЬСКОГО БОТА (momentum_tracker) —
    не используется в живой торговой стратегии, чтобы не менять поведение
    уже проверенного бота. Включает всё из compute_indicator_snapshot плюс
    RSI, полосы Боллинджера (ширина и позиция цены в них — %B) и объём
    относительно среднего.
    """
    base = compute_indicator_snapshot(df, atr_period, ema_fast, ema_slow, atr_regime_lookback)

    df = df.copy()
    df["rsi"] = rsi(df["close"], rsi_period)
    upper, middle, lower = bollinger_bands(df["close"], bb_period)
    df["bb_upper"], df["bb_middle"], df["bb_lower"] = upper, middle, lower

    last = df.iloc[-1]
    bb_width = float(last["bb_upper"] - last["bb_lower"]) if pd.notna(last["bb_upper"]) else None
    # %B: 0 = у нижней полосы, 1 = у верхней, >1/<0 = цена вышла за полосу
    bb_percent_b = None
    if bb_width and bb_width > 0:
        bb_percent_b = float((last["close"] - last["bb_lower"]) / bb_width)

    volume_last = float(df["volume"].iloc[-1])
    volume_avg = float(df["volume"].tail(atr_regime_lookback).mean())
    volume_ratio = (volume_last / volume_avg) if volume_avg else None

    base.update({
        "rsi": float(last["rsi"]) if pd.notna(last["rsi"]) else None,
        "bb_width": bb_width,
        "bb_percent_b": bb_percent_b,
        "volume_last": volume_last,
        "volume_avg": volume_avg,
        "volume_ratio": volume_ratio,
    })
    return base


def compute_indicator_snapshot(df: pd.DataFrame, atr_period: int, ema_fast: int,
                                ema_slow: int, atr_regime_lookback: int) -> dict:
    """
    Возвращает срез индикаторов на последней свече: ATR, EMA fast/slow,
    наклон EMA fast, отношение текущего ATR к его среднему (для детекта
    аномального всплеска волатильности), и MACD-гистограмму — она добавлена
    после реального случая (2026-09-17): серия проигрышей на UP-сделках,
    где EMA9>EMA21 всё ещё показывала "тренд вверх", а рынок уже развернулся
    вниз — EMA9/21 запаздывающий индикатор, MACD реагирует на смену
    моментума раньше и используется как третье, независимое подтверждение
    направления (см. strategy._score_trend_alignment).
    """
    df = df.copy()
    df["atr"] = atr(df, atr_period)
    df["ema_fast"] = ema(df["close"], ema_fast)
    df["ema_slow"] = ema(df["close"], ema_slow)
    _, _, hist = macd(df["close"])
    df["macd_hist"] = hist

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    ema_fast_slope = last["ema_fast"] - prev["ema_fast"]
    trend_up = last["ema_fast"] > last["ema_slow"]
    macd_bullish = last["macd_hist"] > 0

    atr_avg = df["atr"].tail(atr_regime_lookback).mean()
    atr_ratio = float(last["atr"] / atr_avg) if atr_avg and atr_avg > 0 else 1.0

    return {
        "close": float(last["close"]),
        "atr": float(last["atr"]),
        "atr_ratio_to_avg": atr_ratio,
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
        "ema_fast_slope": float(ema_fast_slope),
        "trend_up": bool(trend_up),
        "macd_histogram": float(last["macd_hist"]),
        "macd_bullish": bool(macd_bullish),
    }
