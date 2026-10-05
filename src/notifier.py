"""
Очередь уведомлений в Telegram. Горячий путь копирования никогда не ждёт
Telegram: сообщение кладётся в очередь, отдельная задача отправляет.
Если сообщений много (несколько кошельков торгуют разом) — склеиваем их
в одно, чтобы не упереться в лимиты Telegram.
"""
from __future__ import annotations

import asyncio
import logging

log = logging.getLogger("notifier")

_queue: asyncio.Queue | None = None
_sender = None          # async fn(text: str, markup=None)
MAX_LEN = 3800


def set_sender(fn) -> None:
    global _sender
    _sender = fn


def _q() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue()
    return _queue


def notify(text: str, markup=None) -> None:
    """Неблокирующая отправка (HTML-разметка)."""
    try:
        _q().put_nowait((text, markup))
    except Exception:  # noqa: BLE001
        pass


async def run_forever() -> None:
    q = _q()
    while True:
        text, markup = await q.get()
        # Склеиваем накопившиеся простые сообщения (без кнопок).
        if markup is None:
            parts = [text]
            while not q.empty() and sum(len(p) for p in parts) < MAX_LEN:
                nxt_text, nxt_markup = q.get_nowait()
                if nxt_markup is not None:
                    await _send(nxt_text, nxt_markup)
                    continue
                parts.append(nxt_text)
            text = "\n\n".join(parts)
        await _send(text, markup)
        await asyncio.sleep(0.35)


async def _send(text: str, markup=None) -> None:
    if _sender is None:
        log.info("TG: %s", text[:300])
        return
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)] or [""]
    for i, chunk in enumerate(chunks):
        for attempt in range(3):
            try:
                await _sender(chunk, markup if i == len(chunks) - 1 else None)
                break
            except Exception as exc:  # noqa: BLE001
                wait = getattr(exc, "retry_after", None) or (1.5 * (attempt + 1))
                log.warning("Telegram не принял сообщение (%s), повтор через %ss", exc, wait)
                await asyncio.sleep(float(wait) if not hasattr(wait, "total_seconds") else wait.total_seconds())
