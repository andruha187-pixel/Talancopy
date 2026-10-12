"""
Источник данных о цене — публичный REST API Binance (ключ не нужен).
Параметризован по символу — один и тот же код обслуживает все активы.

Фолбэк на фьючерсы (fapi.binance.com): часть монет может не иметь спотовой
пары на обычном Binance (на момент написания это под вопросом для HYPE) —
если спотовый запрос возвращает 400/404, пробуем тот же символ на
фьючерсном хосте. Если и там нет — актив просто пропускается на этом тике
(main.py логирует предупреждение и не падает).
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


# Где у символа реально нашлись свечи: "spot" или "futures" (HYPE). Поток
# цены (src/price_stream.py) берёт сделки с той же площадки, иначе цена из
# потока и свечи были бы с разных рынков.
_venue: dict[str, str] = {}


def venue_for(symbol: str) -> str:
    return _venue.get(symbol.upper(), "spot")


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
        df = await _get_klines_from(settings.BINANCE_BASE_URL, symbol, interval, limit, start_time_ms)
        _venue[symbol.upper()] = "spot"
        return df
    except httpx.HTTPStatusError:
        df = await _get_klines_from(settings.BINANCE_FUTURES_URL, symbol, interval, limit, start_time_ms)
        _venue[symbol.upper()] = "futures"
        return df


async def get_price_at(symbol: str, timestamp_sec: int) -> float:
    """
    Цена на начало минуты, в которую стартовал рынок на Polymarket. Это и
    есть "страйк" — рынок резолвится как Up, если цена в конце окна >=
    цены в начале окна. Всегда 1m-свеча, независимо от таймфрейма рынка —
    точность момента открытия важнее, чем таймфрейм индикаторов.
    """
    minute_start_ms = (timestamp_sec // 60) * 60 * 1000
    df = await get_klines(symbol, limit=1, interval="1m", start_time_ms=minute_start_ms)
    if df.empty:
        raise RuntimeError(f"Binance не вернул свечу для {symbol} timestamp={timestamp_sec}")
    return float(df.iloc[0]["open"])


async def get_last_price(symbol: str) -> float:
    async def _try(base_url: str) -> float:
        url = f"{base_url}/fapi/v1/ticker/price" if "fapi" in base_url else f"{base_url}/api/v3/ticker/price"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params={"symbol": symbol})
            resp.raise_for_status()
            return float(resp.json()["price"])

    try:
        return await _try(settings.BINANCE_BASE_URL)
    except httpx.HTTPStatusError:
        return await _try(settings.BINANCE_FUTURES_URL)
