"""
Отчёты копирования (раз в REPORT_INTERVAL_HOURS и по кнопке): zip с CSV —
все попытки копирования с причинами пропусков, позиции с PnL, сделки
копируемых с задержкой каждого источника. Пришли этот архив в чат — по нему
видно, где теряются деньги (проскальзывание, задержка, плохие рынки).
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
from src import db, notifier, state
from src.stats import wallet_stats
from src.util import esc, money

log = logging.getLogger("reports")

_doc_sender = None   # async fn(path, caption)


def set_doc_sender(fn) -> None:
    global _doc_sender
    _doc_sender = fn


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def build_report(since_ts: int) -> tuple[str, str]:
    """Синхронно (через to_thread): путь к zip и подпись."""
    db.flush(3)
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)
    stamp_from = time.strftime("%Y%m%d-%H%M", time.gmtime(since_ts))
    stamp_to = time.strftime("%Y%m%d-%H%M", time.gmtime())
    path = os.path.join(settings.REPORTS_DIR, f"copy_{stamp_from}_to_{stamp_to}.zip")

    wallets = db.query("SELECT id, address, label, cfg, created_at, deleted FROM wallets")
    copies = db.query("SELECT * FROM copies WHERE ts >= ? ORDER BY id", (since_ts,))
    trades = db.query("SELECT * FROM target_trades WHERE first_ms >= ? ORDER BY id", (since_ts * 1000,))
    pos = db.query("SELECT * FROM positions WHERE status = 'open' OR COALESCE(closed_ts, 0) >= ? "
                   "OR opened_ts >= ? ORDER BY id", (since_ts, since_ts))
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("copies.csv", _csv_bytes(copies))
        z.writestr("positions.csv", _csv_bytes(pos))
        z.writestr("target_trades.csv", _csv_bytes(trades))
        z.writestr("wallets.csv", _csv_bytes(wallets))

    lines = [f"📄 Отчёт копирования {time.strftime('%d.%m %H:%M', time.gmtime(since_ts))}–"
             f"{time.strftime('%d.%m %H:%M', time.gmtime())} UTC"]
    for w in state.wallets():
        for mode in ("live", "paper"):
            s = wallet_stats(w.id, mode, since_ts)
            if not (s["copies"] or s["realized"] or s["open"] or s["skipped"]):
                continue
            icon = "🔴" if mode == "live" else "🧪"
            lines.append(f"{icon} {w.short()}: копий {s['copies']}, пропусков {s['skipped']}, "
                         f"PnL {money(s['total'], sign=True)}")
    caption = "\n".join(lines)
    return path, caption


async def send_report(since_ts: int | None = None) -> None:
    since = since_ts or int(state.get("last_report_ts") or (time.time() - settings.REPORT_INTERVAL_HOURS * 3600))
    path, caption = await asyncio.to_thread(build_report, since)
    state.set("last_report_ts", int(time.time()))
    if _doc_sender is None:
        log.info("Отчёт готов: %s", path)
        return
    try:
        await _doc_sender(path, caption)
    except Exception as exc:  # noqa: BLE001
        notifier.notify(f"⚠️ Не удалось отправить отчёт: {esc(str(exc)[:300])}")


async def report_loop() -> None:
    interval = max(0.25, settings.REPORT_INTERVAL_HOURS) * 3600
    while True:
        last = float(state.get("last_report_ts") or 0)
        wait = interval - (time.time() - last) if last else interval
        await asyncio.sleep(max(60.0, wait))
        if not state.wallets():
            state.set("last_report_ts", int(time.time()))
            continue
        try:
            await send_report()
        except Exception as exc:  # noqa: BLE001
            log.warning("Отчёт: %s", exc)
