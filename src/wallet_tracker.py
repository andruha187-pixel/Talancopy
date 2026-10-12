"""
Отслеживание чужого кошелька на Polymarket — уведомления в реальном
времени (и опционально копитрейдинг) о его новых входах в позиции.

Источник данных — публичный API Polymarket (data-api.polymarket.com/
activity), тот же, что использовался в scripts/analyze_wallet.py для
разбора истории. Опрашиваем его раз в WALLET_TRACK_POLL_SECONDS секунд —
это не самый быстрый вариант из возможных (прямой листенер ончейн-логов
Polygon был бы быстрее на несколько секунд), но кардинально проще и
надёжнее, а для входа в 15-минутный рынок разница в 3-5 секунд
несущественна на фоне того, что сам кошелёк обычно входит за 3.7-4.4
минуты до конца окна (see scripts/analyze_wallet.py анализ).

Дедупликация — по last_seen_ts, сохраняется в bot_settings, переживает
рестарт: не будем повторно уведомлять про старые сделки после рестарта.
"""
from __future__ import annotations
import asyncio
import logging

import httpx

from config import settings
from src import runtime_state, telegram_notify, executor

log = logging.getLogger("wallet_tracker")

DATA_API = "https://data-api.polymarket.com"
_LAST_SEEN_KEY = "wallet_tracker_last_ts"


async def _fetch_recent_activity(address: str, limit: int = 20) -> list[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{DATA_API}/activity", params={
            "user": address, "limit": limit, "type": "TRADE", "side": "BUY",
        })
        resp.raise_for_status()
        data = resp.json()
    return data if isinstance(data, list) else []


def _get_last_seen_ts() -> int:
    from src import storage
    saved = storage.get_all_settings().get(_LAST_SEEN_KEY)
    try:
        return int(saved)
    except (TypeError, ValueError):
        import time
        return int(time.time())  # первый запуск — не уведомляем про всю историю разом


def _set_last_seen_ts(ts: int) -> None:
    from src import storage
    storage.set_setting(_LAST_SEEN_KEY, ts)


async def _handle_new_trade(trade: dict) -> None:
    slug = str(trade.get("slug", ""))
    direction = str(trade.get("outcome", "")).upper()
    price = trade.get("price")
    size_shares = trade.get("size")
    usdc_size = trade.get("usdcSize") or (price * size_shares if price and size_shares else None)
    token_id = str(trade.get("asset", ""))
    condition_id = str(trade.get("conditionId", ""))

    if runtime_state.get("wallet_notify_enabled"):
        if usdc_size:
            text = f"🐋 Кошелёк вошёл в позицию!\n{direction} {slug}\nЦена: {price} | Размер: {usdc_size:.2f} USDC"
        else:
            text = f"🐋 Кошелёк вошёл в позицию!\n{direction} {slug}\nЦена: {price}"
        await telegram_notify.notify(text)

    if runtime_state.get("wallet_copytrade_enabled") and token_id and direction in ("UP", "DOWN"):
        await executor.execute_copytrade(
            token_id=token_id,
            direction=direction,
            slug=slug,
            condition_id=condition_id,
            price_hint=float(price) if price is not None else None,
        )


async def wallet_tracker_loop() -> None:
    address = settings.WALLET_TRACK_ADDRESS
    if not address:
        return

    last_seen = _get_last_seen_ts()
    log.info("Слежу за кошельком %s (с ts=%s)", address, last_seen)

    while True:
        try:
            activities = await _fetch_recent_activity(address)
            new_ones = [a for a in activities if int(a.get("timestamp", 0)) > last_seen]
            # Активность отдаётся новыми-первыми — обрабатываем в хронологическом порядке
            for trade in sorted(new_ones, key=lambda a: a.get("timestamp", 0)):
                await _handle_new_trade(trade)
                last_seen = max(last_seen, int(trade.get("timestamp", 0)))
            if new_ones:
                _set_last_seen_ts(last_seen)
        except Exception as exc:  # noqa: BLE001 — трекер не должен ронять весь бот
            log.warning("Ошибка опроса активности кошелька: %s", exc)

        await asyncio.sleep(settings.WALLET_TRACK_POLL_SECONDS)
