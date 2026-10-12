"""
Точка входа. Архитектура: один независимый асинхронный поток на каждую
пару (актив, таймфрейм) — например, "btc/5m", "sol/5m" и т.д. Все потоки
делят общий book_stream (WS-стакан), общий Telegram-бот и общую БД.

Плюс быстрая проверка сигнала (src/fast_signal.py): раз в ~0.3 с по всем
монетам по данным в памяти — цена Binance из потока (src/price_stream.py),
живой стакан и свечи последнего тика. Обычный тик раз в 3 с остаётся: он
находит рынок, тянет свечи, пишет тики в базу и тоже может войти.

Плюс две общие фоновые задачи, которые не привязаны к конкретному потоку:
- settlement_loop: резолюция сделок и разметка исходов сигналов по ВСЕМ
  активам/таймфреймам разом (если делать это в каждом из 12 потоков
  отдельно — 12-кратное дублирование запросов к Gamma API впустую).
- reporting.report_loop: периодический CSV-отчёт в Telegram (общий по
  всем активам — разбивка по колонкам asset/timeframe уже в самих данных).
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import binance_feed, market_discovery, indicators, strategy
from src.market_discovery import ActiveMarket
from src import polymarket_client, storage, telegram_notify, executor, book_stream, runtime_state, reporting, wallet_tracker, momentum_tracker, hedge_bot
from src import price_stream, fast_signal, entry_confirm, chainlink_feed
from src.timeframes import TIMEFRAMES, TimeframeProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("polymarket-bot")

# Слаг сейчас активного рынка по каждому потоку ("btc:15m" -> "btc-updown-15m-...")
# — общий словарь, читает settlement_loop, чтобы не спрашивать Gamma API про
# рынки, которые заведомо ещё не могли зарезолвиться.
_active_slugs: dict[str, str] = {}

# Рынок на протяжении всего своего окна не меняется — кэшируем и спрашиваем
# Gamma API заново только когда окно истекло, а не на каждом тике (было:
# один и тот же рынок запрашивался каждые 3-5 секунд, до ~100 лишних
# запросов за один 5-минутный рынок).
_active_markets: dict[str, ActiveMarket] = {}

# Последнее состояние каждого потока — для команды /token и общего статуса.
_instance_state: dict[str, dict] = {}


async def _instance_tick(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"

    if not runtime_state.is_asset_enabled(asset):
        # Актив выключен кнопкой в Telegram — не тратим запросы к API,
        # просто помечаем в статусе, что поток на паузе, и выходим.
        _instance_state[key] = {
            "market_slug": "—", "asset": asset, "timeframe": timeframe.label,
            "direction": "выкл", "current_price": "—", "strike_price": "—",
            "minutes_left": "—", "safety_score": "—",
            "up_token_id": None, "down_token_id": None,
        }
        telegram_notify.set_state_ref(_instance_state)
        return

    cached = _active_markets.get(key)
    if cached is not None and time.time() < cached.end_time:
        market = cached
    else:
        # Окно истекло (или это первый тик) — отписываемся от токенов
        # СТАРОГО рынка для этого же потока прежде, чем подписаться на новый.
        # Без этого _subscribed растёт неограниченно с каждым новым окном.
        if cached is not None and settings.USE_LIVE_BOOK_STREAM:
            book_stream.unsubscribe([cached.up_token_id, cached.down_token_id])
        market = await market_discovery.get_active_market(asset, timeframe)
        _active_slugs[key] = market.slug
        _active_markets[key] = market
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream.subscribe([market.up_token_id, market.down_token_id])
        # Новое окно — сразу прогреваем рынок в SDK (метаданные для ордера),
        # чтобы в момент сигнала ордер уходил без лишнего запроса. Только для
        # монет, по которым сейчас идут реальные сделки.
        if not runtime_state.trade_is_dry(asset):
            asyncio.create_task(polymarket_client.prewarm_market([market.up_token_id, market.down_token_id]))

    if not runtime_state.get("dry_run"):
        asyncio.create_task(polymarket_client.prewarm_transport())

    symbol = binance_feed.symbol_for(asset)
    fetch_ms = int(time.time() * 1000)
    klines = await binance_feed.get_klines(
        symbol,
        limit=max(100, timeframe.atr_lookback_for_regime + timeframe.atr_period + 5),
        interval=timeframe.kline_interval,
    )
    # Свечи и монету — быстрой проверке: между тиками она досчитывает
    # последнюю свечу сделками из потока Binance (той же площадки, что свечи).
    fast_signal.remember_klines(asset, klines, fetch_ms)
    price_stream.watch(symbol, binance_feed.venue_for(symbol))
    ind = indicators.compute_indicator_snapshot(
        klines, timeframe.atr_period, timeframe.ema_fast, timeframe.ema_slow, timeframe.atr_lookback_for_regime,
    )
    binance_price = ind["close"]

    minutes_left = max(0.0, (market.end_time - time.time()) / 60)

    get_book = polymarket_client.get_orderbook_cached if settings.USE_LIVE_BOOK_STREAM else polymarket_client.get_orderbook
    up_book = await get_book(market.up_token_id)
    down_book = await get_book(market.down_token_id)

    # Окно входа — из настроек (⚙️ → ⏱ Окно входа), а не зашитое в профиль
    win_min, win_max = runtime_state.entry_window()
    # Цена и страйк: Chainlink (как Polymarket при расчёте исхода) или Binance —
    # по настройке; без свежей цены/страйка Chainlink сигнал не считается.
    pv = chainlink_feed.view(asset, market, binance_price)
    if pv.problem:
        decision = strategy.Decision(False, None, None, 0.0, minutes_left, 0.0, reasons=[pv.problem])
    else:
        decision = strategy.evaluate(
            current_price=pv.price,
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
    current_price = pv.price if pv.price is not None else binance_price
    strike_price = pv.strike if pv.strike is not None else market.strike_price
    # Результат оценки — для подтверждения сигнала (вход, только если сигнал
    # был и ~3 с назад, см. src/entry_confirm.py)
    entry_confirm.record(market.slug, decision.direction, decision.should_enter)

    storage.log_signal(market.slug, current_price, strike_price, decision,
                        indicators=ind, up_book=up_book, down_book=down_book,
                        price_source=pv.log_source, binance_price=binance_price,
                        binance_strike=market.strike_price)

    _instance_state[key] = {
        "market_slug": market.slug,
        "asset": asset,
        "timeframe": timeframe.label,
        "direction": decision.direction,
        "current_price": round(current_price, 2),
        "strike_price": round(strike_price, 2),
        "minutes_left": round(minutes_left, 2),
        "safety_score": decision.safety_score,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
    }
    telegram_notify.set_state_ref(_instance_state)

    active_book = up_book if decision.direction == "UP" else down_book
    log.info(
        "%s | %s price=%.4f strike=%.4f dir=%s left=%.1fm score=%.1f enter=%s book=%s reasons=%s",
        market.slug, pv.log_source, current_price, strike_price, decision.direction,
        minutes_left, decision.safety_score, decision.should_enter, active_book.source, decision.reasons,
    )

    await executor.maybe_enter(market, decision)

    if settings.MOMENTUM_TRACKER_ENABLED:
        try:
            await momentum_tracker.check_market(market, timeframe)
        except Exception as exc:  # noqa: BLE001 — исследовательский модуль не должен ронять торговлю
            log.warning("Ошибка momentum_tracker для %s: %s", market.slug, exc)

    try:
        await hedge_bot.check_market(market, timeframe)
    except Exception as exc:  # noqa: BLE001 — хедж-бот не должен ронять основную торговлю
        log.warning("Ошибка hedge_bot для %s: %s", market.slug, exc)


ERROR_RETRY_SECONDS = 30


async def _instance_loop(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"
    consecutive_failures = 0
    notified_dead = False
    while True:
        try:
            await _instance_tick(asset, timeframe)
            if notified_dead:
                await telegram_notify.notify(
                    f"✅ Поток {key} снова работает после {consecutive_failures} ошибок подряд."
                )
            consecutive_failures = 0
            notified_dead = False
        except Exception as exc:  # noqa: BLE001 — один сломанный поток не должен ронять остальные
            consecutive_failures += 1
            err = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
            log.exception("Ошибка в потоке %s (%d подряд): %s", key, consecutive_failures, err)
            if consecutive_failures == 10 and not notified_dead:
                notified_dead = True
                await telegram_notify.notify(
                    f"⚠️ Поток {key}: {consecutive_failures} ошибок подряд ({err}). "
                    f"Продолжаю пробовать раз в {ERROR_RETRY_SECONDS} с, сообщу, когда восстановится."
                )

        # Раньше после 10 ошибок подряд поток переходил на опрос раз в 10 минут
        # — для BTC 5m это значит пропустить 2 рынка из каждых 2-х (ошибка
        # обычно временная: таймаут Binance/Gamma). Теперь максимум 30 с.
        sleep_for = timeframe.poll_interval_seconds if consecutive_failures < 10 else ERROR_RETRY_SECONDS
        await asyncio.sleep(sleep_for)


async def settlement_loop() -> None:
    """Общая (не привязанная к конкретному активу) фоновая задача: резолюция
    сделок и разметка исходов сигналов по всем потокам разом. Стоп-лосс
    позиций — в отдельном быстром цикле stop_loss_loop."""
    while True:
        try:
            await executor.settle_resolved_trades()
            active = set(_active_slugs.values())
            await executor.label_resolved_markets(exclude_slugs=active)
            if settings.MOMENTUM_TRACKER_ENABLED:
                await momentum_tracker.label_resolved(exclude_slugs=active)
                momentum_tracker.cleanup_old_sessions(active)
            await hedge_bot.settle_resolved()
        except Exception as exc:  # noqa: BLE001
            log.exception("Ошибка в settlement_loop: %s", exc)
        await asyncio.sleep(10)


STOP_LOSS_CHECK_SECONDS = 2


async def stop_loss_loop() -> None:
    """Стоп-лосс позиции (если включён в Telegram) — проверка раз в ~2 с.
    Раньше он жил в settlement_loop и проверялся раз в 10 с: по отчётам
    25.09–05.10 проигрышная позиция падает с ~0.9 до ~0.4 за секунды, и на
    тех же сделках проверка раз в 10 с продавала заметно хуже, чем раз в 2–3 с.
    Выключенный стоп ничего не стоит: функция сразу выходит, без запросов."""
    while True:
        try:
            await executor.check_position_stop_losses()
        except Exception as exc:  # noqa: BLE001
            log.exception("Ошибка в stop_loss_loop: %s", exc)
        await asyncio.sleep(STOP_LOSS_CHECK_SECONDS)


async def main():
    # Инициализация БД и настроек — до старта любых фоновых задач.
    storage.init_db()
    runtime_state.init_from_db()

    # Защита от "тихого" LIVE без ключа: если в БД с прошлого раза сохранён
    # LIVE-режим, а сейчас POLY_PRIVATE_KEY не задан (новый хостинг, забыли
    # перенести переменную и т.п.) — принудительно откатываемся в DRY RUN,
    # а не пытаемся торговать клиентом без прав на ордера.
    forced_back_to_dry_run = False
    if not runtime_state.get("dry_run") and not settings.POLY_PRIVATE_KEY:
        runtime_state.set("dry_run", True)
        forced_back_to_dry_run = True

    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()

        await telegram_notify.clear_legacy_keyboard()
        dry_run = runtime_state.get("dry_run")
        assets_line = telegram_notify._assets_modes_line() or "(нет включённых)"
        timeframes_line = ", ".join(tf.label for tf in TIMEFRAMES)
        forced_note = (
            "\n⚠️ Был сохранён LIVE-режим с прошлого раза, но POLY_PRIVATE_KEY сейчас не задан — "
            "принудительно откатил в DRY RUN, чтобы не пытаться торговать без ключа."
            if forced_back_to_dry_run else ""
        )
        # Без постоянного тома каждый деплой начинается с пустой базы: режим
        # возвращается в DRY RUN, ставка/банк — к значениям по умолчанию,
        # статистика обнуляется. Говорим об этом сразу, а не когда заметят.
        new_db_note = (
            f"\n⚠️ База данных создана заново ({settings.DB_PATH}). Если бот уже работал раньше, "
            "значит данные не сохраняются между деплоями: подключи в Coolify постоянный том "
            "(Persistent Storage) на /app/data. Сейчас режим, ставка, банк и свои настройки "
            "монет — по умолчанию, статистика пустая."
            if storage.DB_WAS_NEW else ""
        )
        pending_note = telegram_notify._live_pending_note()
        fast_note = (
            f"⚡ Быстрый вход: вкл — сигнал проверяется раз в {max(0.1, settings.FAST_LOOP_INTERVAL_SEC):g} с "
            "по цене Binance из потока (⚙️ Настройки)."
            if runtime_state.get("fast_entry_enabled") and settings.USE_LIVE_BOOK_STREAM else
            "⚡ Быстрый вход: выкл — сигнал проверяется раз в 3 с."
        )
        fast_note += "\n" + (
            f"⏱ Подтверждение сигнала: вкл — вход, только если сигнал был и {settings.ENTRY_CONFIRM_SEC:g} с назад."
            if entry_confirm.enabled() else
            "⏱ Подтверждение сигнала: выкл — вход на первой проверке с сигналом."
        )
        fast_note += "\n" + (
            "📡 Цена сигнала: Chainlink — как у Polymarket. Первое окно после запуска по каждой "
            "монете бот пропустит: страйк Chainlink берётся из потока, а он начал писать только сейчас."
            if chainlink_feed.mode() == "chainlink" else
            "📡 Цена сигнала: Binance."
        )
        await telegram_notify.notify(
            f"🤖 Бот запущен. Режим: {'DRY RUN (без реальных сделок)' if dry_run else 'LIVE — реальные сделки!'}\n"
            f"Активы: {assets_line} (🔴 реальные сделки, 🧪 DRY)\nТаймфреймы: {timeframes_line}\n"
            + (f"{pending_note}\n" if pending_note else "")
            + f"{fast_note}\n"
            + "Открой /menu для управления (старт/стоп, размер позиции, стоп-лосс, настройки)."
            f"{forced_note}{new_db_note}"
        )

        book_stream_task = None
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream_task = asyncio.create_task(book_stream.run_forever())
        # Цена Binance потоком и быстрая проверка сигнала (раз в ~0.3 с).
        # Без живого стакана быстрой проверке не на что смотреть.
        price_stream_task = asyncio.create_task(price_stream.run_forever())
        # Цена Chainlink — потоки цен Polymarket (PolyBolt и старый RTDS)
        chainlink_task = asyncio.create_task(chainlink_feed.run_forever())
        fast_task = (asyncio.create_task(fast_signal.run_forever(_active_markets))
                     if settings.USE_LIVE_BOOK_STREAM else None)

        report_task = asyncio.create_task(reporting.report_loop())
        settlement_task = asyncio.create_task(settlement_loop())
        stop_loss_task = asyncio.create_task(stop_loss_loop())
        wallet_tracker_task = asyncio.create_task(wallet_tracker.wallet_tracker_loop())

        instance_tasks = [
            asyncio.create_task(_instance_loop(asset, timeframe))
            for asset in settings.ASSETS
            for timeframe in TIMEFRAMES
        ]

        try:
            await asyncio.gather(*instance_tasks)
        finally:
            if book_stream_task:
                book_stream_task.cancel()
            price_stream_task.cancel()
            chainlink_task.cancel()
            if fast_task:
                fast_task.cancel()
            report_task.cancel()
            settlement_task.cancel()
            stop_loss_task.cancel()
            wallet_tracker_task.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
