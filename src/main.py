"""
Точка входа копи-бота.

Фоновые задачи:
  источники сделок: блокчейн Polygon (по провайдеру), поток RTDS, опрос Data API;
  стакан WebSocket; выходы (стопы/тейки), разметка цен, резолюции, сверка позиций;
  прогрев SDK; отчёты; авто-поиск кошельков; очередь уведомлений; запись в БД.
"""
from __future__ import annotations

import asyncio
import logging
import signal

from config import settings
from src import book_stream, copier, db, exits, http, notifier, positions, reports, state, trader
from src.detector import detector, prune_loop
from src.discovery import scanner
from src.sources import chain, poller, rtds
from src.util import esc

logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("copy-bot")


def _on_wallets_changed() -> None:
    chain.wallets_changed()


async def _initial_sync() -> None:
    for w in state.wallets():
        if w.mode in ("paper", "live"):
            await copier.sync_holdings(w.address)
    if copier.needs_live():
        try:
            await trader.get_client()
            await trader.balance_usdc(max_age=0)
        except Exception as exc:  # noqa: BLE001
            notifier.notify(f"⚠️ Реальная торговля включена, но клиент Polymarket не создался: {esc(str(exc)[:300])}")


async def _warm_loop() -> None:
    """Прогрев кэшей по токенам, которые сейчас держат копируемые: их
    докупки и выходы пойдут без запросов в Gamma (фильтры) и без лишних
    запросов метаданных в SDK (реальный ордер)."""
    from src import markets
    await asyncio.sleep(30)
    while True:
        tokens: list[str] = []
        live_tokens: list[str] = []
        for w in state.wallets():
            if w.mode not in ("paper", "live"):
                continue
            book = copier.holdings.get(w.address, {})
            top = [t for t, _ in sorted(book.items(), key=lambda kv: -kv[1])[:25]]
            tokens += top
            if w.mode == "live":
                live_tokens += top
        live_tokens += [p.token_id for p in positions.open_positions(mode="live")]
        for t in dict.fromkeys(tokens):
            meta = markets.get(t)
            if meta is None or not meta.complete:
                await markets.ensure(t, timeout=5.0)
                await asyncio.sleep(0.2)
        if copier.needs_live():
            for t in dict.fromkeys(live_tokens):
                await trader.warm(t)
        await asyncio.sleep(270)


async def main() -> None:
    db.init_db()
    db.start_writer()
    state.load()
    positions.load()
    state.on_wallets_changed(_on_wallets_changed)
    detector.handler = copier.on_target_trade
    chain.build_listeners()

    if state.get("live_enabled") and not trader.live_possible():
        state.set("live_enabled", False)

    app = None
    if settings.TELEGRAM_BOT_TOKEN:
        from src import telegram_ui
        app = telegram_ui.build_app()
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
    else:
        log.warning("TELEGRAM_BOT_TOKEN не задан — работаю без Telegram (только логи)")

    ws = state.wallets()
    notifier.notify(
        "🤖 Копи-бот запущен.\n"
        f"Кошельков: {len(ws)} (🧪 {sum(w.mode == 'paper' for w in ws)}, 🔴 {sum(w.mode == 'live' for w in ws)})\n"
        f"Реальная торговля: {'🔴 ВКЛ' if state.get('live_enabled') else '⚪ выкл'}"
        + ("" if trader.live_possible() else " (ключ не задан — только виртуально)") +
        "\nИсточники: " + ", ".join(
            (["блокчейн ×%d" % len(chain.listeners)] if chain.listeners else [])
            + (["поток RTDS"] if settings.RTDS_ENABLED else [])
            + (["опрос Data API"] if settings.POLL_ENABLED else []))
        + "\nМеню: /menu")

    tasks = [
        asyncio.create_task(notifier.run_forever(), name="notifier"),
        asyncio.create_task(book_stream.run_forever(), name="book_stream"),
        asyncio.create_task(rtds.run_forever(), name="rtds"),
        asyncio.create_task(rtds.census_flush_loop(), name="census"),
        asyncio.create_task(poller.run_forever(), name="poller"),
        asyncio.create_task(exits.exit_monitor_loop(), name="exit_monitor"),
        asyncio.create_task(exits.mark_loop(), name="mark"),
        asyncio.create_task(exits.resolution_loop(), name="resolution"),
        asyncio.create_task(exits.reconcile_loop(), name="reconcile"),
        asyncio.create_task(trader.prewarm_loop(copier.needs_live), name="prewarm"),
        asyncio.create_task(_warm_loop(), name="warm"),
        asyncio.create_task(reports.report_loop(), name="reports"),
        asyncio.create_task(scanner.schedule_loop(), name="discovery_schedule"),
        asyncio.create_task(prune_loop(), name="prune"),
        asyncio.create_task(_initial_sync(), name="initial_sync"),
    ]
    tasks += [asyncio.create_task(lst.run_forever(), name=f"chain{lst.index}") for lst in chain.listeners]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    try:
        await stop.wait()
    finally:
        log.info("Останавливаюсь…")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if app is not None:
            try:
                await app.updater.stop()
                await app.stop()
                await app.shutdown()
            except Exception:  # noqa: BLE001
                pass
        await http.close_all()
        db.stop_writer()


def run() -> None:
    try:
        import uvloop  # быстрее стандартного цикла событий — меньше задержка обработки
        uvloop.install()
    except Exception:  # noqa: BLE001
        pass
    asyncio.run(main())


if __name__ == "__main__":
    run()
