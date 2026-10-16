"""
Периодический отчёт для анализа стратегии — раз в REPORT_INTERVAL_HOURS
формирует CSV-файлы из накопленных данных и шлёт их в Telegram файлом.

Два файла:
  signals_*.csv — КАЖДЫЙ тик за период, вошёл бот или нет, со всеми сырыми
                  индикаторами, компонентами score и (когда рынок уже
                  зарезолвился) фактическим исходом. Это основной датасет
                  для поиска реального edge — без меток исхода на
                  НЕ-торгованных сигналах анализ был бы смещён только на
                  те случаи, где бот и так решил войти.
  trades_*.csv  — только реально исполненные (или dry-run) сделки с PnL.

Момент последнего отчёта хранится в bot_settings (переживает рестарт) —
чтобы при перезапуске не задваивать период и не терять данные между ним.
"""
from __future__ import annotations
import asyncio
import csv
import logging
import os
import time
import zipfile

from config import settings
from src import storage, telegram_notify, loss_stats, diagnostics, runtime_state

log = logging.getLogger("reporting")

_LAST_REPORT_KEY = "last_report_ts"
# CSV ⚡ лаборатории скорости больше этого — шлём zip (лимит файла у бота 50 МБ)
SPEED_ZIP_OVER_BYTES = 15 * 1024 * 1024

# Сделка, открытая в последние минуты окна, резолвится уже после отчёта — и
# раньше её исход не попадал ни в один CSV (так «пропал» проигрыш 03.10 04:42).
# Поэтому в trades.csv добавляем сделки последних 15 минут прошлого окна —
# уже с исходом. Повторы между отчётами отличаются по id; подпись к отчёту
# считает только сделки своего окна.
TRADES_OVERLAP_SEC = 15 * 60


# Telegram отклоняет подпись к файлу длиннее 1024 символов (считает в UTF-16,
# эмодзи — за 2) — и тогда не уходит весь отчёт.
CAPTION_LIMIT = 1024


def _caption_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _fit_caption(text: str, limit: int = CAPTION_LIMIT) -> str:
    if _caption_len(text) <= limit:
        return text
    while text and _caption_len(text) > limit - 1:
        text = text[:-1]
    return text + "…"


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def _get_last_report_ts() -> int:
    saved = storage.get_all_settings().get(_LAST_REPORT_KEY)
    try:
        return int(saved)
    except (TypeError, ValueError):
        # Первый запуск — берём период отчёта назад от текущего момента,
        # а не всю историю с нуля.
        return int(time.time() - settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


async def build_and_send_report() -> None:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())

    signals = storage.get_signals_since(since_ts)
    trades = storage.get_trades_since(since_ts)
    momentum = storage.get_momentum_since(since_ts) if settings.MOMENTUM_TRACKER_ENABLED else []

    if not signals and not trades and not momentum:
        _set_last_report_ts(now_ts)
        return

    from_label = time.strftime("%Y%m%d-%H%M", time.gmtime(since_ts))
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime(now_ts))
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")

    signals_path = f"{base}_signals.csv"
    trades_path = f"{base}_trades.csv"

    _write_csv(signals_path, storage.SIGNALS_COLUMNS, signals)
    trades_for_csv = storage.get_trades_since(max(0, since_ts - TRADES_OVERLAP_SEC))
    _write_csv(trades_path, storage.TRADES_COLUMNS, trades_for_csv)

    entered = sum(1 for row in signals if row[storage.SIGNALS_COLUMNS.index("should_enter")])
    labeled = sum(1 for row in signals if row[storage.SIGNALS_COLUMNS.index("outcome")])
    closed_trades = [row for row in trades if row[storage.TRADES_COLUMNS.index("outcome")]]
    i_pnl, i_dry = storage.TRADES_COLUMNS.index("pnl_usdc"), storage.TRADES_COLUMNS.index("dry_run")
    # Реальные и виртуальные (DRY) сделки считаем раздельно — их PnL нельзя складывать
    closed_live = [r for r in closed_trades if not r[i_dry]]
    closed_dry = [r for r in closed_trades if r[i_dry]]
    wins = sum(1 for r in closed_live if (r[i_pnl] or 0) > 0)
    pnl_sum = sum(r[i_pnl] or 0 for r in closed_live)
    dry_line = ""
    if closed_dry:
        dry_wins = sum(1 for r in closed_dry if (r[i_pnl] or 0) > 0)
        dry_pnl = sum(r[i_pnl] or 0 for r in closed_dry)
        dry_line = (f"\nDRY-сделок закрыто: {len(closed_dry)} | побед: {dry_wins} | "
                    f"PnL: {dry_pnl:+.2f} (виртуально)")

    # По монетам — реальные (🔴) и виртуальные DRY-сделки (🧪) раздельно
    by_asset = storage.get_pnl_by_asset_mode(since_ts)
    asset_lines = "\n".join(
        f"  {a.upper()} {'🧪 DRY' if dry else '🔴 LIVE'}: {loss_stats.plural_trades(b['trades'])}, PnL {b['pnl_usdc']:+.2f}"
        + (" (виртуально)" if dry else "")
        for (a, dry), b in sorted(by_asset.items(), key=lambda kv: (kv[0][1], kv[0][0]))
    ) or "  (сделок за период не было)"

    try:
        honest = "\n\n" + loss_stats.short_line("Всего LIVE", storage.get_outcome_stats(live=True))
    except Exception:  # noqa: BLE001 — строка-сводка не должна ломать отчёт
        honest = ""

    caption = (
        f"📄 Отчёт {from_label} → {to_label} (UTC)\n"
        f"Тиков сигналов: {len(signals)} (с известным исходом: {labeled}) | вошли: {entered}\n"
        f"Реальных сделок закрыто: {len(closed_live)} | побед: {wins} | PnL: {pnl_sum:+.2f} USDC{dry_line}\n\n"
        f"По токенам за период:\n{asset_lines}"
        f"{honest}"
    )
    # Бот двух стратегий: вместо LIVE/DRY по монетам — итоги стратегий за период
    # (виртуальные, 1️⃣ и 2️⃣ отдельно), разбивка по монетам — в CSV (колонка strategy).
    if runtime_state._state.get("strategies_mode"):
        try:
            from src import duo_menu
            caption = (
                f"📄 Отчёт {from_label} → {to_label} (UTC)\n"
                f"Тиков сигналов: {len(signals)} (с известным исходом: {labeled}) | сигналов на вход: {entered}\n"
                f"Реальных сделок закрыто: {len(closed_live)} | побед: {wins} | PnL: {pnl_sum:+.2f} USDC\n\n"
                + "\n".join(duo_menu.report_lines(since_ts))
                + f"{honest}"
            )
        except Exception:  # noqa: BLE001 — строки стратегий не должны ломать отчёт
            pass
    # Почему не входили: рынки без сигнала (по тикам окна) и сигналы без сделки
    # (лимит позиций, ошибки ордеров и т.п. — см. src/diagnostics.py).
    extra_lines = []
    try:
        no_sig = diagnostics.no_signal_line(diagnostics.classify_markets(signals, storage.SIGNALS_COLUMNS))
        if no_sig:
            extra_lines.append("🔎 " + no_sig)
        skips = diagnostics.skip_line()
        if skips:
            extra_lines.append("⛔ Сигналы без сделки: " + skips)
    except Exception:  # noqa: BLE001 — диагностика не должна ломать отчёт
        pass
    # Сколько входов нашла быстрая проверка (раз в ~0.3 с) и работает ли поток
    # цены Binance — чтобы по отчётам было видно, что она даёт.
    try:
        from src import fast_signal  # здесь: fast_signal -> executor -> telegram_notify
        fast_entries = fast_signal.entries_line(trades, storage.TRADES_COLUMNS)
        status = fast_signal.status_line()
        if fast_entries:
            extra_lines.append(fast_entries + " (" + status.replace("⚡ ", "", 1) + ")")
        else:
            extra_lines.append(status)
    except Exception:  # noqa: BLE001
        pass
    # Сколько сигналов отсеяло подтверждение (сигнал должен держаться ~3 с)
    try:
        from src import entry_confirm
        extra_lines.append(entry_confirm.status_line())
    except Exception:  # noqa: BLE001
        pass
    # Источник цены сигнала и состояние потока Chainlink
    try:
        from src import chainlink_feed
        extra_lines.append(chainlink_feed.status_line())
    except Exception:  # noqa: BLE001
        pass
    # Свои настройки монет (диапазон/ставка) — чтобы по отчёту было видно,
    # при каких настройках торговала каждая монета.
    try:
        own = telegram_notify._overrides_summary()
    except Exception:  # noqa: BLE001
        own = ""
    if own:
        extra_lines.append("⚙️ Свои настройки: " + own)
    # Добавляем, пока влезает в лимит подписи
    for line in extra_lines:
        if _caption_len(caption + "\n" + line) <= CAPTION_LIMIT:
            caption += "\n" + line
    caption = _fit_caption(caption)
    diagnostics.reset_skips()

    await telegram_notify.send_document(signals_path, caption)
    await telegram_notify.send_document(trades_path, None)

    if momentum:
        momentum_path = f"{base}_momentum.csv"
        _write_csv(momentum_path, storage.MOMENTUM_COLUMNS, momentum)
        reached_by_cp: dict[float, int] = {}
        for row in momentum:
            cp = row[storage.MOMENTUM_COLUMNS.index("checkpoint_price")]
            reached_by_cp[cp] = reached_by_cp.get(cp, 0) + 1
        cp_lines = "\n".join(f"  {cp:.2f}: {n} раз" for cp, n in sorted(reached_by_cp.items()))
        momentum_caption = (
            f"🔬 Momentum-отчёт {from_label} → {to_label}\n"
            f"Контрольных точек зафиксировано: {len(momentum)}\n\n"
            f"По уровням:\n{cp_lines}"
        )
        await telegram_notify.send_document(momentum_path, momentum_caption)

    hedge_positions = storage.get_hedge_positions_since(since_ts)
    if hedge_positions:
        hedge_path = f"{base}_hedge.csv"
        _write_csv(hedge_path, storage.HEDGE_COLUMNS, hedge_positions)
        closed = [row for row in hedge_positions if row[storage.HEDGE_COLUMNS.index("status")] == "closed"]
        hedged_count = sum(1 for row in closed if row[storage.HEDGE_COLUMNS.index("hedge_price")] is not None)
        pnl_sum = sum(row[storage.HEDGE_COLUMNS.index("pnl_usdc")] or 0 for row in closed)
        hedge_caption = (
            f"🔒 Хедж-бот {from_label} → {to_label}\n"
            f"Позиций закрыто: {len(closed)} (захеджировано: {hedged_count}, "
            f"без хеджа: {len(closed) - hedged_count}) | PnL: {pnl_sum:+.2f} USDC (без учёта комиссии)"
        )
        await telegram_notify.send_document(hedge_path, hedge_caption)

    try:
        await _send_speed(base, from_label, to_label, since_ts, max(0, since_ts - TRADES_OVERLAP_SEC))
    except Exception as exc:  # noqa: BLE001 — лаборатория не должна ломать отчёт
        log.warning("Отчёт ⚡ скорости не ушёл: %s", exc)

    _set_last_report_ts(now_ts)


async def _send_speed(base: str, from_label: str, to_label: str, since_ts: int, rows_since: int) -> int:
    """⚡ Лаборатория скорости: события с rows_since отдельным файлом (в отчёте
    — период и 15 мин прошлого, уже с исходом рынка; повторы отличаются по id),
    сводка в подписи — с since_ts. После отправки файл удаляем — события и так
    лежат в базе SPEED_LAB_KEEP_DAYS дней. Возвращает число событий."""
    from src import speed_lab
    rows = storage.get_speed_events_since(rows_since)
    if not rows:
        return 0
    path = f"{base}_speed.csv"
    _write_csv(path, storage.SPEED_COLUMNS, rows)
    send_path = path
    if os.path.getsize(path) > SPEED_ZIP_OVER_BYTES:
        send_path = path + ".zip"
        with zipfile.ZipFile(send_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(path, arcname=os.path.basename(path))
    try:
        await telegram_notify.send_document(send_path, _fit_caption(speed_lab.report_caption(from_label, to_label,
                                                                                               since_ts)))
    finally:
        for p in {path, send_path}:
            try:
                os.remove(p)
            except OSError:
                pass
    return len(rows)


async def send_speed_all() -> int:
    """Кнопка «📤» в ⚡: все события из базы одним файлом."""
    now_ts = int(time.time())
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime(now_ts))
    base = os.path.join(settings.REPORTS_DIR, f"all_to_{to_label}")
    return await _send_speed(base, "всё в базе", to_label, 0, 0)


async def report_loop() -> None:
    """Фоновая задача: спит между отчётами, переживает произвольные
    интервалы рестарта за счёт хранения last_report_ts в БД."""
    while True:
        try:
            await build_and_send_report()
        except Exception:
            pass  # не роняем бота из-за проблем с отчётом; попробуем в следующий раз
        await asyncio.sleep(max(60, settings.REPORT_INTERVAL_HOURS * 3600))
