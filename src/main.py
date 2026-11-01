"""
Точка входа ⏱ бота позднего входа на 5m-рынках.

Фоновые задачи:
  - book_stream    — живой стакан Polymarket (WebSocket);
  - chainlink_feed — цена Chainlink (страйк, текущая цена, исход окна);
  - atr_feed       — ATR по 1-минутным свечам Binance;
  - late_bot       — 🔁 запись окон всех монет и ⏱ ставки (тест и реальные);
  - reporting      — файлы в Telegram раз в REPORT_INTERVAL_HOURS;
  - Telegram       — меню и уведомления.
"""
from __future__ import annotations
import asyncio
import logging

from config import settings
from src import atr_feed, book_stream, chainlink_feed, late_bot, reporting, runtime_state, storage, telegram_notify

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polymarket-5m-late-bot")


async def main():
    storage.init_db()
    runtime_state.init_from_db()
    if not late_bot.rule_since():
        late_bot.mark_rule_change()      # первый запуск: итоги «по нынешнему правилу» — с него

    # Защита от «тихого» LIVE без ключа: в базе сохранён LIVE, а ключа сейчас нет
    forced_dry = False
    if not runtime_state.get("dry_run") and not settings.POLY_PRIVATE_KEY:
        runtime_state.set("dry_run", True)
        forced_dry = True

    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()
        try:
            await telegram_notify.notify(telegram_notify.startup_text(forced_dry))
        except Exception as exc:  # noqa: BLE001
            log.warning("Стартовое сообщение не ушло: %s", exc)

        jobs = [chainlink_feed.run_forever(), atr_feed.run_forever(), late_bot.run_forever(), reporting.report_loop()]
        if settings.USE_LIVE_BOOK_STREAM:
            jobs.append(book_stream.run_forever())
        tasks = [asyncio.create_task(j) for j in jobs]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
