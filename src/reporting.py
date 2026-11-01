"""
Отчёт раз в REPORT_INTERVAL_HOURS — два файла в Telegram:
  *_windows.zip — окна 5m всех монет (и выключенных тоже): страйк, путь цены
                  [секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR] и исходы;
                  CSV внутри zip — путь занимает много места;
  *_trades.csv  — ставки ⏱ (тестовые и реальные).
В каждом — период и 30 минут прошлого (окна за это время уже с исходами).
Кнопка 📤 Файлы — всё, что в базе: окна по дням, частями до ~40 МБ.
Момент последнего отчёта хранится в bot_settings — переживает рестарт.
"""
from __future__ import annotations
import asyncio
import csv
import io
import logging
import os
import time
import zipfile

from config import settings
from src import late_bot, storage, telegram_notify

log = logging.getLogger("reporting")

_LAST_REPORT_KEY = "last_report_ts"
OVERLAP_SEC = 30 * 60
DAY = 86400
# Telegram отклоняет подпись к файлу длиннее 1024 символов (считает в UTF-16)
CAPTION_LIMIT = 1024
# Бот может прислать файл до 50 МБ — режем окна на части
ZIP_PART_LIMIT = 40 * 1024 * 1024


def _caption_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _fit_caption(text: str, limit: int = CAPTION_LIMIT) -> str:
    if _caption_len(text) <= limit:
        return text
    while text and _caption_len(text) > limit - 1:
        text = text[:-1]
    return text + "…"


def _csv_bytes(columns: list[str], rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        f.write(_csv_bytes(columns, rows))


def zip_windows(base: str, since_ts: int, until_ts: int) -> tuple[list[str], int]:
    """Окна [since_ts, until_ts) — в zip (CSV по UTC-дням; новая часть, если зип больше
    ZIP_PART_LIMIT). Возвращает (пути к zip, сколько окон)."""
    os.makedirs(os.path.dirname(base) or ".", exist_ok=True)
    paths: list[str] = []
    n = 0
    zf = None
    tag = os.path.basename(base)
    t = int(since_ts)
    try:
        while t < until_ts:
            t_end = min(int(until_ts), (t // DAY + 1) * DAY)
            rows = storage.windows_export(t, t_end)
            if rows:
                if zf is None:
                    path = f"{base}_windows" + (f"_{len(paths) + 1}" if paths else "") + ".zip"
                    zf = zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED)
                    paths.append(path)
                zf.writestr(f"{tag}_windows_{time.strftime('%Y%m%d', time.gmtime(t))}.csv",
                            _csv_bytes(storage.WINDOW_COLUMNS, rows))
                n += len(rows)
                if sum(i.compress_size for i in zf.infolist()) > ZIP_PART_LIMIT:
                    zf.close()
                    zf = None
            t = t_end
    except Exception:
        if zf is not None:
            zf.close()
            zf = None
        for p in paths:
            try:
                os.remove(p)
            except OSError:
                pass
        raise
    finally:
        if zf is not None:
            zf.close()
    return paths, n


async def _send(base: str, captions: dict, since_ts: int, rows_since: int, until_ts: int) -> int:
    """Окна (zip) и ставки (CSV) с rows_since. Возвращает число окон + ставок."""
    files: list[tuple[str, str]] = []
    try:
        zips, n_win = zip_windows(base, rows_since, until_ts)
        for i, path in enumerate(zips):
            cap = captions["windows"] + (f"\nЧасть {i + 1} из {len(zips)}." if len(zips) > 1 else "")
            files.append((path, cap))
        trades = storage.trades_export(rows_since)
        if trades:
            path = f"{base}_trades.csv"
            _write_csv(path, storage.TRADE_COLUMNS, trades)
            files.append((path, captions["trades"]))
        for path, caption in files:
            await telegram_notify.send_document(path, _fit_caption(caption))
    finally:
        for path, _cap in files:
            try:
                os.remove(path)
            except OSError:
                pass
    return n_win + len(trades)


def _get_last_report_ts() -> int:
    raw = storage.get_setting(_LAST_REPORT_KEY)
    try:
        return int(float(raw)) if raw else int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)
    except ValueError:
        return int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


def _label(ts: float) -> str:
    return time.strftime("%Y%m%d-%H%M", time.gmtime(ts))


async def build_and_send_report() -> int:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())
    from_label, to_label = _label(since_ts), _label(now_ts)
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")
    captions = {"windows": late_bot.report_caption(from_label, to_label, since_ts),
                "trades": "\n".join(late_bot.strategy_lines())}
    n = await _send(base, captions, since_ts, max(0, since_ts - OVERLAP_SEC), now_ts + 1)
    _set_last_report_ts(now_ts)
    return n


async def send_all() -> int:
    """Кнопка 📤: все окна и ставки из базы."""
    now_ts = int(time.time())
    to_label = _label(now_ts)
    first = storage.windows_first_ts()
    since = first if first is not None else now_ts
    captions = {"windows": "🔁 Окна 5m — всё в базе (путь цены — за последние "
                           f"{settings.REC_KEEP_DAYS} дн.)\n" + late_bot.report_caption(_label(since), to_label, since),
                "trades": "\n".join(late_bot.strategy_lines())}
    return await _send(os.path.join(settings.REPORTS_DIR, f"all_to_{to_label}"), captions, since, since, now_ts + 1)


async def report_loop() -> None:
    """Фоновая задача: отчёт раз в REPORT_INTERVAL_HOURS (момент последнего — в базе)."""
    interval = max(60.0, settings.REPORT_INTERVAL_HOURS * 3600)
    while True:
        wait = _get_last_report_ts() + interval - time.time()
        if wait > 0:
            await asyncio.sleep(min(wait, 300))
            continue
        try:
            await build_and_send_report()
        except Exception as exc:  # noqa: BLE001 — отчёт не должен ронять бота
            log.warning("Отчёт не ушёл: %s", exc)
            await asyncio.sleep(300)
