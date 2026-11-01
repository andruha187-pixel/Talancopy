"""
Рынки Up/Down Polymarket по точному слагу и их исходы (Gamma API).

Слаг детерминированный: {монета}-updown-{интервал}-{unix-начало окна}, окна
выровнены по сетке UTC (floor(now / интервал) * интервал). Рынок следующего
окна существует заранее — бот находит его за минуту до старта.
"""
from __future__ import annotations
import json
from datetime import datetime

import httpx

from config import settings


def _expected_slug(asset: str, label: str, start_ts: int) -> str:
    return f"{asset}-updown-{label}-{start_ts}"


async def _fetch_event_by_slug(slug: str) -> dict | None:
    url = f"{settings.GAMMA_HOST}/events"
    params = {"slug": slug}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    if not data:
        return None
    return data[0] if isinstance(data, list) else data


def _parse_timestamp(value) -> int | None:
    """API отдаёт время то unix-секундами, то ISO-строкой ('...Z')."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return None


def _extract_market_fields(event: dict) -> tuple[str, list[str], int | None, int | None]:
    """condition_id, [up_token, down_token], start/end из объекта Event -> Market."""
    market = event["markets"][0] if "markets" in event and event["markets"] else event
    condition_id = market.get("conditionId") or market.get("condition_id")

    token_ids_raw = market.get("clobTokenIds")
    token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else (token_ids_raw or [])

    outcomes_raw = market.get("outcomes")
    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or ["Up", "Down"])

    up_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "up"), 0)
    down_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)
    up_token, down_token = token_ids[up_idx], token_ids[down_idx]

    if up_token == down_token:
        # Бывало у HYPE (21.09): один и тот же токен у UP и DOWN — торговать нельзя
        raise RuntimeError(
            f"up_token_id == down_token_id для {event.get('slug', '?')} — сломанная пара токенов, пропускаю"
        )

    start_time = _parse_timestamp(event.get("marketStartTime"))
    end_time = _parse_timestamp(event.get("endDate") or market.get("endDate"))
    return condition_id, [up_token, down_token], start_time, end_time


async def get_window_tokens(asset: str, label: str, start_ts: int) -> tuple[str, str] | None:
    """Токены (UP, DOWN) окна по точному слагу {asset}-updown-{label}-{start_ts}.
    None — такого рынка (пока) нет."""
    event = await _fetch_event_by_slug(_expected_slug(asset.lower(), label, int(start_ts)))
    if event is None:
        return None
    _cid, (up_token, down_token), _start, _end = _extract_market_fields(event)
    return up_token, down_token


async def get_resolution(slug: str) -> str | None:
    """"UP" / "DOWN", если рынок уже зарезолвился (outcomePrices: 1.0 у победителя), иначе None."""
    event = await _fetch_event_by_slug(slug)
    if event is None:
        return None
    market = event["markets"][0] if event.get("markets") else event
    if not market.get("closed"):
        return None

    outcomes_raw = market.get("outcomes")
    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or ["Up", "Down"])
    prices_raw = market.get("outcomePrices")
    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    if not prices:
        return None

    winner_idx = max(range(len(prices)), key=lambda i: float(prices[i]))
    return outcomes[winner_idx].upper()
