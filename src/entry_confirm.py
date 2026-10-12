"""
Подтверждение сигнала: входим, только если условия входа держатся ~3 с.

Зачем. Разбор проигрышей 10–11.10 (XRP #113, ETH #114, #142, #148): каждый
раз бот входил в момент, который не продержался и 3 секунд. Цена Binance
прыгала на 4–6 bp за секунду и возвращалась, или цена стороны в стакане
проходила через диапазон сверху вниз (0.99 → 0.94 → 0.87), и ордер
исполнялся уже вне диапазона: 0.84 при сигнале 0.90, 0.887 при 0.94.

Архив 25.09–11.10 (3-секундные тики, BTC/ETH/XRP/SOL/BNB/DOGE, ставка $30):
  вход на первом тике с сигналом       — 240 входов, 7 проигрышей, +$209;
  вход, если сигнал был и ~3 с назад   — 151 вход,   1 проигрыш,  +$237.
По периодам проигрышей: до 03.10 2 → 0, 03–09.10 1 → 0, с 10.10 4 → 1.
Реальные сделки 10.10 08:50 – 11.10 15:50 по Таллину: −$49 → +$6, все четыре
проигрыша выше этим правилом пропускаются. Стоп-лосс позиции (продать при
падении на 30–70%) на тех же данных только вредит: +$34…+$120 вместо +$209 —
цена часто проседает и возвращается.

Как. Обычная проверка (раз в 3 с) и быстрая (раз в ~0.3 с) записывают сюда
результат каждой оценки рынка: когда, в какую сторону, был ли сигнал. Вход
разрешён, если сигнал есть сейчас и был в ту же сторону в оценке, ближайшей
к моменту «ENTRY_CONFIRM_SEC назад» (ищем в окне от 4.5 до 2 с назад). Это та
же проверка, что в архиве: два тика подряд с разницей ~3 с. Что было между
ними, не важно — так и проверялось.

Выключается в Telegram (⚙️ Настройки → ⏱ Подтверждение сигнала).
"""
from __future__ import annotations
import time
from collections import deque

from config import settings
from src import runtime_state

LOOK_BEFORE_SEC = 1.5   # оценку «3 с назад» ищем в окне [t−3−1.5, t−3+1.0]
LOOK_AFTER_SEC = 1.0    # (при коротком подтверждении окно уже, см. confirmed)
KEEP_SEC = 15.0         # сколько секунд истории держим по рынку
MAX_MARKETS = 300       # больше рынков в памяти — чистим старые
MAX_TRACKED = 5000      # счётчики рынков для строки статуса

_hist: dict[str, deque] = {}
# Рынки, где был сигнал, и рынки, где он подтвердился — для строки статуса
_signal_slugs: set = set()
_confirmed_slugs: set = set()
_since: float = time.time()


def reset() -> None:
    """Сброс состояния (для тестов)."""
    global _since
    _hist.clear()
    _signal_slugs.clear()
    _confirmed_slugs.clear()
    _since = time.time()


def confirm_sec() -> float:
    try:
        return max(0.0, float(settings.ENTRY_CONFIRM_SEC))
    except (TypeError, ValueError):
        return 0.0


def enabled() -> bool:
    return bool(runtime_state.get("entry_confirm_enabled")) and confirm_sec() > 0


def _prune(now: float) -> None:
    for slug in [s for s, h in _hist.items() if not h or now - h[-1][0] > KEEP_SEC]:
        _hist.pop(slug, None)


def record(slug: str, direction: str | None, ok: bool, now: float | None = None) -> None:
    """Результат одной оценки рынка (обычной или быстрой проверкой)."""
    now = time.time() if now is None else now
    h = _hist.get(slug)
    if h is None:
        if len(_hist) >= MAX_MARKETS:
            _prune(now)
        h = _hist[slug] = deque()
    h.append((now, direction, bool(ok)))
    while h and now - h[0][0] > KEEP_SEC:
        h.popleft()


def confirmed(slug: str, direction: str | None, now: float | None = None) -> bool:
    """Сигнал в эту сторону был и ~ENTRY_CONFIRM_SEC назад. Без подтверждения
    (выключено в настройках) — всегда True."""
    if not enabled():
        return True
    now = time.time() if now is None else now
    sec = confirm_sec()
    target = now - sec
    # Окно поиска «той» оценки: при 3 с — от 4.5 до 2 с назад. При коротком
    # подтверждении окно сужается, чтобы не захватить только что сделанную оценку.
    lo = target - min(LOOK_BEFORE_SEC, sec / 2)
    hi = target + min(LOOK_AFTER_SEC, sec / 3)
    best = None
    for ts, d, ok in _hist.get(slug, ()):
        if lo <= ts <= hi:
            if best is None or abs(ts - target) < abs(best[0] - target):
                best = (ts, d, ok)
    result = best is not None and best[2] and best[1] == direction
    if len(_signal_slugs) > MAX_TRACKED:
        _signal_slugs.clear()
        _confirmed_slugs.clear()
    _signal_slugs.add(slug)
    if result:
        _confirmed_slugs.add(slug)
    return result


def status_line() -> str:
    """Строка для меню, «🔎 Почему нет сделок» и отчёта (без Markdown-символов)."""
    if not enabled():
        return "⏱ Подтверждение сигнала: выкл — вход на первой проверке с сигналом"
    sec = f"{confirm_sec():g}"
    signals = len(_signal_slugs)
    if not signals:
        return f"⏱ Подтверждение сигнала: вкл — вход, только если сигнал держится {sec} с"
    passed = len(_confirmed_slugs & _signal_slugs)
    since = time.strftime("%H:%M", time.gmtime(_since))
    return (f"⏱ Подтверждение сигнала {sec} с: с {since} UTC сигнал был на {signals} рынках, "
            f"продержался на {passed}, отсеяно {signals - passed}")
