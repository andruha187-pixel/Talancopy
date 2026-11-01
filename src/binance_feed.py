"""
Свечи Binance для ATR (публичный REST API, ключ не нужен).

Фолбэк на фьючерсы (fapi.binance.com): если у монеты нет спотовой пары
(запрос вернул 400/404), пробуем тот же символ на фьючерсном хосте.
"""
from __future__ import annotations
import httpx
import pandas as pd

from config import settings

_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_base", "taker_quote", "ignore",
]

SYMBOL_MAP = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "bnb": "BNBUSDT",
    "xrp": "XRPUSDT",
    "hype": "HYPEUSDT",
    "doge": "DOGEUSDT",
}


def symbol_for(asset: str) -> str:
    return SYMBOL_MAP.get(asset.lower(), f"{asset.upper()}USDT")


async def _get_klines_from(base_url: str, symbol: str, interval: str, limit: int,
                            start_time_ms: int | None) -> pd.DataFrame:
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms

    url = f"{base_url}/fapi/v1/klines" if "fapi" in base_url else f"{base_url}/api/v3/klines"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw = resp.json()

    df = pd.DataFrame(raw, columns=_COLUMNS)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df


async def get_klines(symbol: str, limit: int = 100, interval: str = "1m",
                      start_time_ms: int | None = None) -> pd.DataFrame:
    """Тянем свечи с Binance; при ошибке на споте пробуем фьючерсы тем же символом."""
    try:
        return await _get_klines_from(settings.BINANCE_BASE_URL, symbol, interval, limit, start_time_ms)
    except httpx.HTTPStatusError:
        return await _get_klines_from(settings.BINANCE_FUTURES_URL, symbol, interval, limit, start_time_ms)
