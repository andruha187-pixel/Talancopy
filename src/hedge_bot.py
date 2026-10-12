"""
Хедж-стратегия, откалиброванная на реальных momentum-отчётах (2026-09-20):
1. Как только цена стороны рынка впервые достигает HEDGE_ENTRY_PRICE
   (по умолчанию 0.70) — покупаем эту сторону.
2. Если цена продолжает расти и достигает HEDGE_TRIGGER_PRICE (0.90) до
   конца окна — докупаем ПРОТИВОПОЛОЖНУЮ сторону в таком количестве акций,
   чтобы держать РОВНО одинаковое число акций с обеих сторон. При равном
   числе акций выплата фиксирована (ровно это число акций × $1) независимо
   от исхода — значит, и прибыль (выплата минус суммарные затраты)
   одинакова в обоих исходах, то есть зафиксирована.
3. Если цена НЕ доходит до HEDGE_TRIGGER_PRICE — остаёмся с односторонней
   позицией. Это не мелочь: по нашим данным именно эти случаи почти
   гарантированно проигрывают (застрявшая сторона выигрывает в ~4.5%
   случаев) — учитывай это в размере ставки.

Порог 0.90 (не 0.70-0.85) выбран по факту бэктеста на реальных отчётах:
чем позже хеджируешь, тем больше гарантированная маржа с каждого
успешного хеджа, и это перевешивает потери от возросшего числа случаев,
где хедж вообще не срабатывает. См. обсуждение в чате от 2026-09-20 —
на четырёх отчётах (366 сессий) хедж на 0.90 дал +$29.45, на 0.70-0.85 —
убыток, несмотря на то что сам хедж каждый раз безубыточен по построению.

ВАЖНО про комиссию: pnl_usdc здесь считается БЕЗ вычета комиссии тейкера
(7% для крипторынков на КАЖДУЮ ногу) — так же, как и у основной стратегии
в этом боте. Реальный итог на кошельке будет чуть хуже, чем показывают
отчёты. См. обсуждение комиссий в README.
"""
from __future__ import annotations
import logging

from config import settings
from src import book_stream, market_discovery, polymarket_client, runtime_state, storage, telegram_notify
from src.market_discovery import ActiveMarket
from src.timeframes import TimeframeProfile

log = logging.getLogger("hedge_bot")


async def _execute_entry(market: ActiveMarket, side: str, token_id: str, price_hint: float,
                          timeframe: TimeframeProfile) -> None:
    dry_run = runtime_state.get("dry_run")
    stake = runtime_state.get("hedge_stake_usdc")

    book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)
    available = book.ask_liquidity_usdc
    if available < settings.MIN_VIABLE_TRADE_USDC:
        return
    if available < stake:
        stake = round(available * 0.9, 2)

    tick = book.tick_size or book_stream.tick_size(token_id)
    reference_price = book.best_ask or price_hint
    if reference_price is None:
        return
    price_cap = polymarket_client.round_price_for_buy(
        min(reference_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, 0.99), tick,
    )

    order_id = "dry-run"
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify("❌ Хедж-бот: LIVE включён, но POLY_PRIVATE_KEY не задан — вход пропущен.")
            return
        try:
            resp = await polymarket_client.place_buy_order(token_id, price_cap, stake, tick)
        except Exception as exc:  # noqa: BLE001
            await telegram_notify.notify(f"❌ Хедж-бот: ошибка входа ({market.slug}): {exc}")
            return
        order_id = polymarket_client.response_field(resp, "order_id") or str(resp)

    shares = stake / price_cap
    storage.create_hedge_position(
        market.slug, market.asset, timeframe.label, side, price_cap, shares, stake, token_id, dry_run,
    )
    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else ''}🔷 Хедж-бот: вход {side} по {market.slug}\n"
        f"Цена: {price_cap:.3f} | Размер: {stake:.2f} USDC | ждём {runtime_state.get('hedge_trigger_price'):.2f} для хеджа"
    )


async def _execute_hedge(market: ActiveMarket, side: str, position_id: int, entry_shares: float,
                          opposite_token_id: str) -> None:
    dry_run = runtime_state.get("dry_run")

    opp_book = await polymarket_client.get_orderbook_cached(opposite_token_id, depth_levels=10)
    opp_price = opp_book.best_ask
    if opp_price is None:
        return  # нет стакана на другой стороне прямо сейчас — попробуем на следующем тике

    # Хотим РОВНО entry_shares акций на другой стороне — тогда выплата
    # фиксирована (entry_shares x $1) независимо от исхода.
    target_cost = entry_shares * opp_price
    available = opp_book.ask_liquidity_usdc
    if available < settings.MIN_VIABLE_TRADE_USDC:
        return
    if available < target_cost:
        target_cost = round(available * 0.9, 2)  # неполный хедж лучше, чем никакого

    tick = opp_book.tick_size or book_stream.tick_size(opposite_token_id)
    price_cap = polymarket_client.round_price_for_buy(
        min(opp_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, 0.99), tick,
    )

    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify("❌ Хедж-бот: LIVE включён, но POLY_PRIVATE_KEY не задан — хедж пропущен.")
            return
        try:
            resp = await polymarket_client.place_buy_order(opposite_token_id, price_cap, target_cost, tick)
        except Exception as exc:  # noqa: BLE001
            await telegram_notify.notify(f"❌ Хедж-бот: ошибка хеджа ({market.slug}): {exc}")
            return

    hedge_shares = target_cost / price_cap
    storage.mark_hedged(position_id, price_cap, hedge_shares, target_cost, opposite_token_id)

    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else ''}🔒 Хедж-бот: зафиксирован хедж по {market.slug} ({side})\n"
        f"Докупили противоположную сторону по {price_cap:.3f}, {hedge_shares:.2f} акций "
        f"(на входе было {entry_shares:.2f}) — прибыль зафиксирована независимо от исхода."
    )


async def check_market(market: ActiveMarket, timeframe: TimeframeProfile) -> None:
    if not runtime_state.get("hedge_bot_enabled"):
        return

    entry_price = runtime_state.get("hedge_entry_price")
    hedge_price = runtime_state.get("hedge_trigger_price")

    sides = [
        ("UP", market.up_token_id, market.down_token_id),
        ("DOWN", market.down_token_id, market.up_token_id),
    ]
    for side, token_id, opposite_token_id in sides:
        price = book_stream.best_ask(token_id)
        if price is None:
            continue

        existing = storage.get_open_hedge_position(market.slug, side)
        if existing is None:
            if price >= entry_price:
                await _execute_entry(market, side, token_id, price, timeframe)
        else:
            pos_id, entry_shares, entry_cost, status = existing
            if status == "open_unhedged" and price >= hedge_price:
                await _execute_hedge(market, side, pos_id, entry_shares, opposite_token_id)


async def settle_resolved() -> None:
    """Общая фоновая задача (как executor.settle_resolved_trades) — узнаём
    исход рынков с открытыми хедж-позициями и фиксируем итоговый PnL."""
    for pos_id, market_slug, side, entry_shares, entry_cost, hedge_shares, hedge_cost, status, dry_run in \
            storage.get_unsettled_hedge_positions():
        try:
            outcome = await market_discovery.get_resolution(market_slug)
        except Exception as exc:  # noqa: BLE001
            log.warning("Не удалось узнать исход %s: %s", market_slug, exc)
            continue
        if not outcome:
            continue

        won = outcome == side
        if status == "hedged":
            realized_shares = entry_shares if won else hedge_shares
            total_cost = entry_cost + (hedge_cost or 0)
            pnl = realized_shares * 1.0 - total_cost
        else:  # open_unhedged — не успели захеджировать до резолюции
            pnl = (entry_shares * 1.0 - entry_cost) if won else -entry_cost

        storage.settle_hedge_position(pos_id, outcome, pnl)
        emoji = "🟢" if pnl > 0 else "🔴"
        await telegram_notify.notify(
            f"{emoji} Хедж-бот: {market_slug} ({side}) зарезолвился {outcome}. "
            f"{'Хедж сработал' if status == 'hedged' else 'Без хеджа (не дошло до порога)'}. "
            f"PnL: {pnl:+.2f} USDC" + (" (dry run)" if dry_run else "")
        )
