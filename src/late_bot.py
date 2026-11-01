"""
⏱ Поздний вход на 5-минутных рынках Up/Down Polymarket.

Правило. За ~3 минуты до конца окна (по умолчанию с 3:10 до 3:00 до конца)
покупаем сторону, которая уже далеко ушла от страйка:
  - цена стороны (ask) 0.92–0.95;
  - цена Chainlink ушла от страйка (цены Chainlink на старте окна) не меньше
    чем на 0.10% и не меньше чем на 2 ATR (Wilder ATR(7) по 1m-свечам Binance).
Держим до конца окна — стоп-лосс и досрочная продажа на данных 25.09–20.10
только ухудшали итог. Всё перечисленное меняется кнопками в Telegram.

Откуда правило. На ~15 тыс. 5m-рынков (25.09–20.10) вход с этими условиями в
окне 3:10–3:00 до конца — 232 ставки, 98.3% выигрышей, около +3% на ставку;
соседние окна (2:50–3:10, 3:00–3:20) — 96.7–97.7%. При цене ~0.94 окупаемость —
от ~95% выигрышей, поэтому сначала — DRY.

Ставки — по включённым монетам (🪙): тестовая всегда, реальная — если монета 🔴
и бот в 🔴 LIVE, есть ключ и не достигнут дневной стоп. Одна ставка на окно.
Сумма — общая (⚙️) или своя у монеты (🪙 💵): фиксированная $ или % от банка
(баланс кошелька; не читается — банк, заданный вручную).

Реальный ордер (FAK, не дороже ask + 1¢) не исполнился или исполнился частично —
бот пробует снова каждые LIVE_RETRY_SEC, пока идёт окно входа и условия
выполняются (цена в диапазоне, та же сторона), и добирает до суммы ставки. В
Telegram — только итог («✅ ставка …»); окно без покупки пишется в ставки со
статусом SKIP_NOFILL и причиной (note). Об ошибках, которые повтором не лечатся
(баланс, ключ), — сообщение, но не чаще раза в час.

🔁 Запись: по каждому окну каждой монеты (и выключенной тоже) — страйк и путь
[секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR, bid UP, bid DOWN,
объём ask UP, объём ask DOWN, объём bid UP, объём bid DOWN] каждые REC_STEP_SEC
секунд с 4:30 до конца; исход — ранний (стакан / Chainlink) и официальный. По
этим файлам подбираются окно входа, пороги и монеты; бид и объёмы нужны, чтобы
честно проверить мейкерский вход (исполнилась бы лимитка на стороне-лидере).
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from config import settings
from src import atr_feed, book_stream, chainlink_feed, market_discovery, runtime_state, storage

log = logging.getLogger("late_bot")

STEP = 300
LABEL = "5m"
PREFETCH_SEC = 60              # рынок следующего окна ищем за минуту до начала…
PREFETCH_RETRY_SEC = 10        # …и снова каждые 10 с, если его ещё нет
DISCOVERY_RETRY_SEC = 5        # рынок текущего окна не нашёлся — снова через 5 с
LATE_RETRY_SEC = 30            # …после первой минуты окна — через 30 с
BOOK_WINDOW_SEC = 12.0         # исход по стакану: читаем в последние 12 с…
BOOK_LAST_SEC = 3.0            # …и верим последнему чтению, если оно не раньше чем за 3 с до конца
BOOK_LO, BOOK_HI = 0.03, 0.97
BOOK_MAX_AGE_MS = 30_000       # стакан без обновлений дольше — не входим (поток мог зависнуть)
EARLY_WAIT_SEC = 30            # ждём точку Chainlink после конца окна не дольше
STRIKE_WAIT_SEC = 10           # точки Chainlink с начала окна нет столько — берём ближайшую до него
SETTLE_EVERY_SEC = 60
SETTLE_LAG_SEC = 90            # официальный исход спрашиваем не раньше чем через 1.5 мин после конца
SETTLE_MAX_AGE_SEC = 2 * 86400
FLUSH_EVERY_SEC = 30
TRIM_EVERY_SEC = 3600
UNSUPPORTED_AFTER = 3          # рынка нет 3 окна подряд (с запуска) — монету пропускаем…
UNSUPPORTED_SKIP_SEC = 6 * 3600  # …на 6 часов
REQUEST_PAUSE_SEC = 0.2

MIN_ORDER_SHARES = 5.0         # меньше 5 акций биржа ордер не берёт
PRICE_SLIP = 0.01              # потолок ордера — ask + 1¢ (но не выше верхней границы цены + 1¢)
LIVE_RETRY_SEC = 0.5           # реальный ордер не исполнился — снова через полсекунды, пока идёт окно входа
LIVE_MAX_TRIES = 30            # проверок и ордеров на окно — не больше (окно входа ~10 с)
LIVE_MAX_ERRORS = 3            # непонятных ошибок подряд — хватит
ERROR_NOTE_EVERY_SEC = 3600    # об одной и той же ошибке ордера — не чаще раза в час
BANK_REFRESH_SEC = 60          # баланс кошелька (для ставок в % банка) — раз в минуту
MIN_TRADE_USDC = 1.0

# Кнопки ⚙️ Настроек (окно — секунд до конца: (от, до)); любое другое значение — ✏️ сообщением
WINDOW_PRESETS = ((190, 180), (200, 180), (190, 170))
PRICE_PRESETS = ((0.90, 0.95), (0.92, 0.95), (0.92, 0.96))
DIST_PRESETS = (0.05, 0.08, 0.10, 0.15)
ATR_PRESETS = (1.5, 2.0, 2.5, 0.0)
STAKE_PRESETS = (5.0, 10.0, 20.0, 50.0)
PCT_PRESETS = (1.0, 2.0, 5.0, 10.0)
STOP_PRESETS = (0.0, 30.0, 60.0, 150.0)


@dataclass
class Win:
    asset: str
    start: int
    end: int
    slug: str
    up: str
    down: str
    own_tokens: bool = False
    strike: float | None = None
    path: list = field(default_factory=list)
    last_rec_t: float = 1e9            # секунд до конца у последней точки пути
    book_res: str | None = None
    book_res_t: float = 0.0
    early_set: bool = False
    last_flush: float = 0.0
    decided: bool = False              # ставка сделана или окно входа прошло
    checked: bool = False              # в базе проверили, нет ли уже ставки по окну (рестарт)
    why: str = ""                      # последняя причина «не входим» в окне входа


_cur: dict[str, Win] = {}
_next: dict[str, Win] = {}
_prefetch_at: dict[tuple[str, int], float] = {}
_prefetching: set = set()
_opening: set = set()
_pending: list[Win] = []
_retry_at: dict[tuple[str, int], float] = {}
_supported: set = set()
_fails: dict[str, int] = defaultdict(int)
_skip_until: dict[str, float] = {}
_last_start: dict[str, int] = {}
_tasks: set = set()
_trade_note = {"stop_day": None}
_err_notes: dict[str, float] = {}      # текст ошибки ордера -> когда последний раз сообщали
_bank = {"wallet": None, "ts": 0.0, "error": ""}   # баланс кошелька USDC (для ставок в % банка)
_why = defaultdict(Counter)            # монета -> {причина: окон}
_stats = {"windows": 0, "striked": 0, "early": 0, "official": 0, "discovery_errors": 0, "last_error": ""}


def reset() -> None:
    for d in (_cur, _next, _prefetch_at, _retry_at, _fails, _skip_until, _last_start, _why):
        d.clear()
    _prefetching.clear()
    _opening.clear()
    _pending.clear()
    _supported.clear()
    _trade_note["stop_day"] = None
    _err_notes.clear()
    _bank.update(wallet=None, ts=0.0, error="")
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0


# ---------------------------------------------------------- настройки ----

def _rs(key: str):
    return runtime_state.get(key)


def enabled() -> bool:
    return bool(_rs("late_enabled"))


def window() -> tuple[int, int]:
    """(от, до) — секунд до конца окна: вход, пока от ≥ осталось ≥ до. Одно время
    («ровно 3:00») — это секунда перед ним: бот проверяет раз в секунду."""
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    hi, lo = max(a, b), min(a, b)
    return max(hi, lo + 1), lo


def price_range() -> tuple[float, float]:
    a, b = float(_rs("late_min_price")), float(_rs("late_max_price"))
    return min(a, b), max(a, b)


def min_dist() -> float:
    return float(_rs("late_min_dist"))


def min_atr() -> float:
    return float(_rs("late_min_atr"))


def stake() -> float:
    return float(_rs("late_stake"))


def stop() -> float:
    return float(_rs("late_stop"))


def rule_since() -> int:
    return int(_rs("late_rule_since") or 0)


def mark_rule_change(now: float | None = None) -> None:
    runtime_state.set("late_rule_since", int(time.time() if now is None else now))


def mmss(sec: float) -> str:
    sec = int(round(sec))
    return f"{sec // 60}:{sec % 60:02d}"


def plural(n: int, one: str, few: str, many: str) -> str:
    """1 ставка, 2 ставки, 5 ставок."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def bets_text(n: int) -> str:
    return f"{n} {plural(n, 'ставка', 'ставки', 'ставок')}"


def window_phrase() -> str:
    """«за 3:00–3:10 до конца» или «ровно за 3:00 до конца»."""
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    return f"ровно за {mmss(a)} до конца" if a == b else f"за {window_text()} до конца"


def window_text() -> str:
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    if a == b:
        return f"ровно {mmss(a)}"
    hi, lo = window()
    return f"{mmss(lo)}–{mmss(hi)}"


def rule_tag() -> str:
    """Правило коротко — пишется в каждую ставку (колонка rule)."""
    hi, lo = window()
    pl, ph = price_range()
    return f"{hi}-{lo}s|{pl:.2f}-{ph:.2f}|d{min_dist():g}%|atr{min_atr():g}"


def atr_text() -> str:
    return f"≥{min_atr():g} ATR" if min_atr() > 0 else "без ATR"


def rule_short() -> str:
    """Правило в одну строку: «за 3:00–3:10 до конца, цена 0.92–0.95, до страйка ≥0.1% и ≥2 ATR»."""
    pl, ph = price_range()
    atr = f" и {atr_text()}" if min_atr() > 0 else " (без ATR)"
    return f"{window_phrase()}, цена {pl:.2f}–{ph:.2f}, до страйка ≥{min_dist():g}%{atr}"


def rule_text() -> str:
    pl, ph = price_range()
    atr = f", не меньше {min_atr():g} ATR" if min_atr() > 0 else ", без условия по ATR"
    return (f"{window_phrase()} окна сторона по цене {pl:.2f}–{ph:.2f}, "
            f"цена Chainlink ушла от страйка не меньше чем на {min_dist():g}%{atr}")


def trade_assets() -> list[str]:
    return [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]


def coin_live(asset: str) -> bool:
    return runtime_state.is_asset_live(asset)


def live_coins() -> list[str]:
    return [a for a in trade_assets() if coin_live(a)]


# ------------------------------------------------- сумма ставки и банк ----

async def refresh_bank(force: bool = False) -> float | None:
    """Баланс кошелька USDC — не чаще раза в BANK_REFRESH_SEC (force — сразу, например после ставки)."""
    if not settings.POLY_PRIVATE_KEY:
        return None
    now = time.time()
    if not force and now - _bank["ts"] < BANK_REFRESH_SEC:
        return _bank["wallet"]
    _bank["ts"] = now
    from src import polymarket_client
    try:
        val = await polymarket_client.get_usdc_balance()
    except Exception as exc:  # noqa: BLE001
        _bank["error"] = str(exc)[:150]
        return _bank["wallet"]
    if val is not None:
        _bank.update(wallet=val, error="")
    elif not _bank["error"]:
        _bank["error"] = "SDK не отдал баланс"
    return _bank["wallet"]


def bank() -> tuple[float | None, str]:
    """(банк для ставок в %, откуда он): баланс кошелька, иначе заданный вручную; (None, "") — неизвестен."""
    if _bank["wallet"] is not None:
        return float(_bank["wallet"]), "баланс кошелька"
    manual = float(_rs("late_bank_manual") or 0.0)
    if manual > 0:
        return manual, "задан вручную"
    return None, ""


def stake_rule(asset: str) -> tuple[str, float]:
    """Как считается ставка монеты: ("общая", $), ("usd", $) или ("pct", %)."""
    return runtime_state.coin_stake(asset) or ("общая", stake())


def stake_for(asset: str | None) -> float:
    """Сумма ставки монеты до правила «не меньше 5 акций»: общая, своя фиксированная или % банка
    (банк неизвестен — общая)."""
    if asset is None:
        return stake()
    mode, value = stake_rule(asset)
    if mode == "pct":
        b, _src = bank()
        if b is None:
            return stake()
        return round(min(runtime_state.STAKE_LIMITS[1], b * value / 100.0), 2)
    return value


def stake_label(asset: str) -> str:
    """«$20», «5% банка (~$10.67)», «общая $5»."""
    mode, value = stake_rule(asset)
    if mode == "usd":
        return f"${value:g}"
    if mode == "pct":
        b, _src = bank()
        return f"{value:g}% банка" + (f" (~${b * value / 100:.2f})" if b is not None else f" (банк неизвестен — общая ${stake():g})")
    return f"общая ${value:g}"


def stake_button(asset: str) -> str:
    mode, value = stake_rule(asset)
    return {"usd": f"${value:g}", "pct": f"{value:g}%"}.get(mode, "общая")


def bet_usdc(cap: float, asset: str | None = None) -> float:
    """Сумма ставки: общая или своя у монеты, но не меньше 5 акций по потолку цены (иначе биржа отклонит)."""
    return max(stake_for(asset), round(MIN_ORDER_SHARES * cap + 0.01, 2))


def fee_for(usdc: float, price: float) -> float:
    return round(usdc * float(settings.FEE_RATE) * (1 - price), 4)


# --------------------------------------------------------------- окна ----

def slug_for(asset: str, start: int) -> str:
    return f"{asset}-updown-{LABEL}-{start}"


def book_winner(up: str, down: str) -> str | None:
    ua, da = book_stream.best_ask(up), book_stream.best_ask(down)
    if da is not None and da <= BOOK_LO and (ua is None or ua >= BOOK_HI):
        return "UP"
    if ua is not None and ua <= BOOK_LO and (da is None or da >= BOOK_HI):
        return "DOWN"
    return None


def _task_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.warning("late_bot: фоновая задача упала: %s", task.exception())


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_task_done)


async def drain() -> None:
    """Дождаться фоновых задач (реальные ордера, поиск рынков следующего окна)."""
    while _tasks:
        await asyncio.gather(*list(_tasks), return_exceptions=True)


def _prune(d: dict, now: float) -> None:
    if len(d) > 2000:
        for old in [x for x in d if x[1] < now - 86400]:
            d.pop(old, None)


async def _fetch_win(asset: str, start: int, now: float) -> Win | None:
    try:
        toks = await market_discovery.get_window_tokens(asset, LABEL, start)
    except Exception as exc:  # noqa: BLE001 — Gamma недоступна: попробуем позже
        _stats["discovery_errors"] += 1
        _stats["last_error"] = f"{asset}: {exc}"[:200]
        return None
    if not toks:
        return None
    up, down = toks
    own = bool(settings.USE_LIVE_BOOK_STREAM)
    if own and start > now:
        book_stream.subscribe([up, down], watch_after_ms=int(start * 1000))
    elif own:
        book_stream.subscribe([up, down])
    return Win(asset, start, start + STEP, slug_for(asset, start), up, down, own_tokens=own)


def _activate(w: Win) -> Win:
    storage.window_upsert(w.asset, w.start, slug=w.slug)
    _supported.add(w.asset)
    _fails[w.asset] = 0
    _stats["windows"] += 1
    return w


def _set_current(asset: str, w: Win, now: float) -> None:
    _cur[asset] = w
    if (runtime_state.is_asset_enabled(asset) and coin_live(asset) and enabled()
            and live_blocked(now) is None):
        from src import polymarket_client
        _spawn(polymarket_client.prewarm_market([w.up, w.down]))


def _maybe_open(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _opening or now < _retry_at.get(key, 0):
        return
    _opening.add(key)
    _spawn(_open(asset, start, now))


async def _open(asset: str, start: int, now: float) -> None:
    """Рынок текущего окна, если он не нашёлся заранее, — в фоне: запрос к Gamma
    (до 10 с) не должен держать тик остальных монет."""
    key = (asset, start)
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            _retry_at[key] = now + (LATE_RETRY_SEC if now - start > 60 else DISCOVERY_RETRY_SEC)
            _prune(_retry_at, now)
            return
        cur = _cur.get(asset)
        if _last_start.get(asset) != start or (cur is not None and cur.start == start):
            _release(w)                           # окно уже сменилось или рынок уже есть
            return
        _set_current(asset, _activate(w), now)
    finally:
        _opening.discard(key)


def _release(w: Win) -> None:
    if not w.own_tokens:
        return
    cur = _cur.get(w.asset)
    busy = {cur.up, cur.down} if cur is not None else set()
    toks = [t for t in (w.up, w.down) if t not in busy]
    if toks:
        book_stream.unsubscribe(toks)


def _maybe_prefetch(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _prefetching or now < _prefetch_at.get(key, 0):
        return
    _prefetching.add(key)
    _prefetch_at[key] = now + PREFETCH_RETRY_SEC
    _prune(_prefetch_at, now)
    _spawn(_prefetch(asset, start, now))


async def _prefetch(asset: str, start: int, now: float) -> None:
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            return
        old, cur = _next.get(asset), _cur.get(asset)
        if (old is not None and old.start == start) or (cur is not None and cur.start == start):
            return
        if old is not None:
            _release(old)
        _next[asset] = w
    finally:
        _prefetching.discard((asset, start))


def _num(v: float | None, digits: int = 8):
    return None if v is None else float(f"{v:.{digits}g}")


def _flush(w: Win) -> None:
    storage.window_upsert(w.asset, w.start, slug=w.slug, path=json.dumps(w.path, separators=(",", ":")))


def _sample(w: Win, now: float) -> None:
    """Точка пути каждые REC_STEP_SEC с. Поля [0..4] неизменны (совместимость со старыми
    записями и выгрузками): [секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR].
    Поля [5..10] добавлены для симуляции мейкерского входа — бид обеих сторон и объёмы:
    [bid UP, bid DOWN, объём(ask UP), объём(ask DOWN), объём(bid UP), объём(bid DOWN)]."""
    t_left = w.end - now
    if w.strike is None and now >= w.start:
        # страйк — цена Chainlink на начало окна: первая точка не раньше начала; если её
        # так и нет через STRIKE_WAIT_SEC — ближайшая в пределах 5 с (как в chainlink_feed)
        s = chainlink_feed.price_after(w.asset, w.start)
        if s is None and now >= w.start + STRIKE_WAIT_SEC:
            s = chainlink_feed.price_at(w.asset, w.start)
        if s is not None:
            w.strike = s
            _stats["striked"] += 1
            storage.window_upsert(w.asset, w.start, strike=_num(s))
    step = max(0.5, float(settings.REC_STEP_SEC))
    if settings.REC_TO_SEC <= t_left <= settings.REC_FROM_SEC and w.last_rec_t - t_left >= step - 0.5:
        bu, bus, ua, uas = book_stream.top_of_book(w.up)
        bd, bds, da, das = book_stream.top_of_book(w.down)
        price, age = chainlink_feed.latest(w.asset, now)
        if price is not None and age is not None and age > settings.CHAINLINK_MAX_AGE_SEC:
            price = None
        w.path.append([round(t_left, 1), ua, da, _num(price), _num(atr_feed.get(w.asset, now), 6),
                       bu, bd, _num(uas, 6), _num(das, 6), _num(bus, 6), _num(bds, 6)])
        w.last_rec_t = t_left
    if w.end - BOOK_WINDOW_SEC <= now < w.end:
        w.book_res, w.book_res_t = book_winner(w.up, w.down), now
    if now - w.last_flush >= FLUSH_EVERY_SEC:
        w.last_flush = now
        _flush(w)


def _close(w: Win) -> None:
    _flush(w)
    if w.own_tokens:
        book_stream.unsubscribe([w.up, w.down])
    _pending.append(w)


def resolve_pending(now: float | None = None) -> int:
    """Ранний исход закончившихся окон: стакан последних секунд, иначе Chainlink конец vs страйк."""
    now = time.time() if now is None else now
    done = 0
    for w in list(_pending):
        if now < w.end:
            continue
        book = w.book_res if w.book_res_t >= w.end - BOOK_LAST_SEC else None
        if book and not w.early_set:
            w.early_set = True
            storage.window_upsert(w.asset, w.start, early_outcome=book)
            done += 1
        cl_end = chainlink_feed.price_after(w.asset, w.end)
        if cl_end is None and now < w.end + EARLY_WAIT_SEC:
            continue
        _pending.remove(w)
        if not w.early_set and w.strike and cl_end:
            w.early_set = True
            storage.window_upsert(w.asset, w.start, early_outcome="UP" if cl_end >= w.strike else "DOWN",
                                  cl_end=_num(cl_end))
            done += 1
        elif cl_end:
            storage.window_upsert(w.asset, w.start, cl_end=_num(cl_end))
    _stats["early"] += done
    return done


async def settle(now: float | None = None) -> int:
    """Официальные исходы закончившихся окон (Gamma), по несколько за проход."""
    now = time.time() if now is None else now
    n = 0
    for asset, start, slug in storage.windows_unresolved(int(now - SETTLE_LAG_SEC), int(now - SETTLE_MAX_AGE_SEC)):
        try:
            res = await market_discovery.get_resolution(slug)
        except Exception:  # noqa: BLE001
            res = None
        if res in ("UP", "DOWN"):
            storage.window_upsert(asset, start, outcome=res)
            _stats["official"] += 1
            n += 1
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return n


# --------------------------------------------------------------- тик ----

def _new_window(asset: str, start: int, now: float) -> None:
    prev = _last_start.get(asset)
    _last_start[asset] = start
    if prev is None:
        return
    cur = _cur.get(asset)
    if cur is None or cur.start != prev:
        _fails[asset] += 1                     # прошлое окно так и не нашлось
        if (_fails[asset] >= UNSUPPORTED_AFTER and asset not in _supported
                and now >= _skip_until.get(asset, 0)):
            _skip_until[asset] = now + UNSUPPORTED_SKIP_SEC
            log.info("%s: рынков 5m нет %d окон подряд — пропускаю %d ч", asset, _fails[asset],
                     UNSUPPORTED_SKIP_SEC // 3600)


async def _tick_current(asset: str, start: int, now: float) -> None:
    w = _cur.get(asset)
    if w is not None and w.start != start:
        _close(w)
        _cur.pop(asset, None)
        w = None
    if w is None:
        nw = _next.get(asset)
        if nw is None or nw.start != start:
            if nw is not None and nw.start < start:
                _release(_next.pop(asset))
            _maybe_open(asset, start, now)        # найдётся — подхватим на следующем тике
            return
        w = _activate(_next.pop(asset))
        _set_current(asset, w, now)
    _sample(w, now)


def _tick_next(asset: str, nxt: int, now: float) -> None:
    if nxt - now > PREFETCH_SEC:
        return
    nw = _next.get(asset)
    if nw is None or nw.start != nxt:
        _maybe_prefetch(asset, nxt, now)


async def tick(now: float | None = None) -> None:
    now = time.time() if now is None else now
    start = int(now // STEP) * STEP
    for asset in settings.ASSETS:
        if _last_start.get(asset) != start:
            _new_window(asset, start, now)
        if now < _skip_until.get(asset, 0):
            continue
        await _tick_current(asset, start, now)
        _tick_next(asset, start + STEP, now)
    resolve_pending(now)
    await maybe_trade(now)


# ------------------------------------------------------------ правило ----

def evaluate(w: Win, now: float) -> tuple[bool, dict]:
    """Можно ли входить в окно w прямо сейчас. (да/нет, подробности/причина)."""
    price, age = chainlink_feed.latest(w.asset, now)
    if w.strike is None:
        return False, {"why": "нет страйка (бот не застал начало окна)"}
    if price is None or age is None or age > settings.CHAINLINK_MAX_AGE_SEC:
        return False, {"why": "нет свежей цены Chainlink"}
    if price == w.strike:
        return False, {"why": "цена на страйке"}
    side = "UP" if price > w.strike else "DOWN"
    token = w.up if side == "UP" else w.down
    ask = book_stream.best_ask(token)
    dist = abs(price - w.strike) / w.strike * 100
    atr = atr_feed.get(w.asset, now)
    datr = abs(price - w.strike) / atr if atr else None
    info = dict(side=side, token=token, ask=ask, price_now=price, dist=dist, atr=atr, datr=datr)
    lo, hi = price_range()
    if ask is None:
        info["why"] = "нет стакана"
    elif not book_stream.is_fresh(token, BOOK_MAX_AGE_MS):
        info["why"] = "стакан не обновлялся 30 с"
    elif not lo - 1e-9 <= ask <= hi + 1e-9:
        info["why"] = f"цена вне {lo:.2f}–{hi:.2f}"
    elif dist < min_dist() - 1e-12:
        info["why"] = f"до страйка меньше {min_dist():g}%"
    elif min_atr() > 0 and atr is None:
        info["why"] = "нет ATR (Binance)"
    elif min_atr() > 0 and datr < min_atr() - 1e-9:
        info["why"] = f"меньше {min_atr():g} ATR от страйка"
    else:
        return True, info
    return False, info


def dry_fill(token: str, cap: float, usdc: float) -> tuple[float | None, float]:
    """(средняя цена, USDC) покупки на usdc по уровням asks не дороже cap."""
    b = book_stream.get_book(token)
    spent = shares = 0.0
    for price, size in sorted((b or {}).get("asks", {}).items()):
        if price > cap + 1e-9 or spent >= usdc - 1e-9:
            break
        take = min(price * size, usdc - spent)
        spent += take
        shares += take / price
    return (round(spent / shares, 4), round(spent, 2)) if shares > 0 else (None, 0.0)


def today_live_pnl(now: float | None = None) -> float:
    now = time.time() if now is None else now
    day = int(now // 86400) * 86400
    return sum(t["pnl_usdc"] or 0.0 for t in storage.trades_since(day - 3600)
               if not t["dry_run"] and t["settled_ts"] and t["settled_ts"] >= day)


def live_blocked(now: float | None = None) -> str | None:
    """Почему реальная ставка сейчас нельзя (None — можно)."""
    if not settings.POLY_PRIVATE_KEY:
        return "нет ключа POLY_PRIVATE_KEY"
    if runtime_state.get("dry_run"):
        return "бот в DRY RUN"
    s = stop()
    if s > 0 and today_live_pnl(now) <= -s:
        return f"дневной стоп −${s:g}"
    return None


async def maybe_trade(now: float | None = None) -> int:
    """Входы по правилу. Возвращает, сколько тестовых ставок записали."""
    now = time.time() if now is None else now
    if not enabled():
        return 0
    hi_t, lo_t = window()
    start = int(now // STEP) * STEP
    done = 0
    for asset in trade_assets():
        w = _cur.get(asset)
        if w is None or w.start != start or w.decided:
            continue
        t_left = w.end - now
        if t_left > hi_t + 1e-9:
            continue                              # окно входа ещё не началось
        if t_left < lo_t - 1e-9:
            w.decided = True                      # окно входа прошло без ставки
            if w.why:
                _why[asset][w.why] += 1
            continue
        if not w.checked:
            w.checked = True
            if storage.trade_exists(asset, start, dry_run=True):
                w.decided = True                  # после перезапуска в том же окне — не дублируем
                continue
        ok, info = evaluate(w, now)
        if not ok:
            w.why = info["why"]
            continue
        done += _enter(w, info, now)
    return done


def _cap(ask: float) -> float:
    """Потолок цены ордера: ask + 1¢, но не выше верхней границы цены + 1¢."""
    return min(round(ask + PRICE_SLIP, 2), round(price_range()[1] + PRICE_SLIP, 2))


def _enter(w: Win, info: dict, now: float) -> int:
    """Тестовая ставка по стакану; если монета 🔴 — ещё и реальная (в фоне, чтобы не держать тик)."""
    ask = info["ask"]
    cap = _cap(ask)
    usdc = bet_usdc(cap, w.asset)
    price, spent = dry_fill(info["token"], cap, usdc)
    if price is None or spent < MIN_TRADE_USDC:
        w.why = "мало объёма в стакане"          # пробуем дальше, пока идёт окно входа
        return 0
    w.decided = True
    base = dict(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"], token_id=info["token"],
                t_left=round(w.end - now, 1), price_now=_num(info["price_now"]), strike=_num(w.strike),
                dist_pct=round(info["dist"], 4), dist_atr=round(info["datr"], 3) if info["datr"] else None,
                atr=_num(info["atr"], 6), ask=ask, rule=rule_tag())
    tid = storage.trade_log(**base, price=price, size_usdc=spent, shares=round(spent / price, 4),
                            fee_usdc=fee_for(spent, price), dry_run=1, status="DRY")
    if coin_live(w.asset) and not storage.trade_exists(w.asset, w.start, dry_run=False):
        _spawn(_enter_live(w, info, base, cap, usdc, now))
    return 1 if tid else 0


async def _live_buy(token: str, cap: float, usdc: float):
    """Реальная покупка FAK-ордером по цене не выше cap: (USDC, акций, id, статус) или None (не исполнился)."""
    from src import polymarket_client
    tick = book_stream.tick_size(token)
    cap = polymarket_client.round_price_for_buy(cap, tick)
    try:
        resp = await polymarket_client.place_buy_order(token, cap, usdc, tick, order_type="FAK")
    except Exception as exc:  # noqa: BLE001
        if polymarket_client.is_no_fill_error(exc):
            return None
        raise
    oid = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID")
    status = str(polymarket_client.response_field(resp, "status") or "matched")
    amounts = polymarket_client.filled_amounts(resp)
    if amounts is None or status.lower() == "delayed" or (amounts[0] <= 0 and status.lower() == "matched"):
        spent, shares = usdc, usdc / cap
    else:
        spent, shares = amounts
    if spent <= 0:
        return None
    return round(spent, 2), shares, str(oid or ""), status


async def _notify(text: str) -> None:
    try:
        from src import telegram_notify
        await telegram_notify.notify(text)
    except Exception:  # noqa: BLE001
        pass


async def _notify_error(asset: str, exc: BaseException) -> None:
    """Ошибка ордера, которую повтор не лечит (баланс, ключ), — в Telegram, но одна и та же не чаще
    раза в ERROR_NOTE_EVERY_SEC."""
    key = str(exc).strip().lower()[:80]
    now = time.time()
    if now - _err_notes.get(key, 0.0) < ERROR_NOTE_EVERY_SEC:
        return
    _err_notes[key] = now
    await _notify(f"❌ ⏱ {asset.upper()}: ошибка ордера — {exc}"[:400])


async def _enter_live(w: Win, info: dict, base: dict, cap: float, usdc: float, now: float) -> None:
    """Реальная покупка. Не исполнилось (или исполнилось частично) — снова через LIVE_RETRY_SEC, пока идёт
    окно входа и условия выполняются, добирая до суммы ставки. В Telegram — только итог покупки."""
    why = live_blocked(now)
    if why:
        day = int(now // 86400)
        if why.startswith("дневной стоп") and _trade_note["stop_day"] != day:
            _trade_note["stop_day"] = day
            await _notify(f"⏱ Реальные ставки остановлены до конца дня — {why}. Тестовые идут дальше.")
        return
    from src import polymarket_client
    t0 = time.monotonic()
    side, token = info["side"], info["token"]
    lo_t = window()[1]
    target = usdc
    wallet = _bank["wallet"]
    if wallet is not None and target > wallet:
        target = round(float(wallet) - 0.01, 2)          # на балансе меньше ставки — на сколько есть
    spent = shares = 0.0
    oids: list[str] = []
    status = ""
    orders = errors = 0
    last = ""
    first_fill = None
    cur_cap, cur_ask = cap, info["ask"]
    for i in range(LIVE_MAX_TRIES):
        t_now = now + (time.monotonic() - t0)
        if i:
            if w.end - t_now < lo_t - 1e-9:
                last = last or "окно входа кончилось"
                break
            ok, again = evaluate(w, t_now)
            if not ok or again["side"] != side:
                last = again.get("why") or "сторона сменилась"
                await asyncio.sleep(LIVE_RETRY_SEC)
                continue
            cur_ask = again["ask"]
            cur_cap = _cap(cur_ask)
        left = round(target - spent, 2)
        min_amt = max(MIN_TRADE_USDC, round(MIN_ORDER_SHARES * cur_cap + 0.01, 2))
        if left < min_amt:
            if not spent and wallet is not None:
                last = f"на балансе ${wallet:.2f} — меньше минимального ордера ${min_amt:.2f}"
                await _notify_error(w.asset, RuntimeError(f"на балансе ${wallet:.2f} — меньше ставки, "
                                                          "реальная не ушла"))
            break                                        # набрали (или остаток меньше минимального ордера)
        orders += 1
        try:
            res = await _live_buy(token, cur_cap, left)
        except Exception as exc:  # noqa: BLE001
            last = f"ошибка: {exc}"[:200]
            errors += 1
            if polymarket_client.is_fatal_error(exc) or errors >= LIVE_MAX_ERRORS:
                await _notify_error(w.asset, exc)
                break
            await asyncio.sleep(LIVE_RETRY_SEC)
            continue
        errors = 0
        if res is None:
            last = f"не исполнился по ≤{cur_cap:.2f}"
        else:
            got, got_shares, oid, status = res
            if first_fill is None:
                first_fill = (t_now, cur_ask)
            spent += got
            shares += got_shares
            if oid:
                oids.append(oid)
            if got >= left - 0.01:
                break                                    # исполнилось на всю сумму
            last = f"частично: ${spent:.2f} из ${target:.2f}"
        await asyncio.sleep(LIVE_RETRY_SEC)
    note = f"ордеров {orders}" + (f"; {last}" if last and spent < target - 0.01 else "")
    if spent <= 0:
        storage.trade_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                          status="SKIP_NOFILL", note=note[:300])
        return
    price = round(spent / shares, 4) if shares > 0 else cur_cap
    t_fill, ask_fill = first_fill
    row = dict(base, ts=int(t_fill), t_left=round(w.end - t_fill, 1), ask=ask_fill)
    storage.trade_log(**row, price=price, size_usdc=round(spent, 2), shares=round(shares, 4),
                      fee_usdc=fee_for(spent, price), dry_run=0, status=status or "matched",
                      order_id=",".join(oids)[:500], note=note[:300])
    _spawn(refresh_bank(force=True))
    await _notify(f"✅ ⏱ {w.asset.upper()}: ставка {side} ${spent:.2f} по {price:.3f} за "
                  f"{mmss(w.end - t_fill)} до конца (до страйка {info['dist']:.2f}%"
                  + (f", {info['datr']:.1f} ATR" if info["datr"] else "") + ")")


async def _official(t: dict, now: float, cache: dict) -> str | None:
    key = (t["asset"], t["start_ts"])
    if key in cache:
        return cache[key]
    o = storage.window_outcome(*key)
    if o is None and now - (t["start_ts"] + STEP) >= SETTLE_LAG_SEC:
        try:
            o = await market_discovery.get_resolution(t["slug"])
        except Exception:  # noqa: BLE001
            o = None
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    cache[key] = o
    return o


async def settle_trades(now: float | None = None) -> int:
    """Итог ставок по официальному исходу окна."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for t in storage.trades_open(int(now - 30)):
        if t["start_ts"] < now - SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(t, now, cache)
        if o not in ("UP", "DOWN"):
            continue
        win = o == t["side"]
        if str(t["status"] or "").startswith("SKIP"):
            pnl = 0.0
        else:
            pnl = (t["shares"] if win else 0.0) - t["size_usdc"] - (t["fee_usdc"] or 0.0)
        if storage.trade_settle(t["id"], o, round(pnl, 4)):
            n += 1
            if not t["dry_run"] and not str(t["status"] or "").startswith("SKIP"):
                await _notify(f"⏱ {'🟢' if win else '🔴'} {t['asset'].upper()}: исход {o}, ставка {t['side']} "
                              f"→ {pnl:+.2f} USDC")
    return n


def trim_paths(now: float | None = None) -> int:
    days = int(settings.REC_KEEP_DAYS)
    if days <= 0:
        return 0
    now = time.time() if now is None else now
    return storage.windows_trim(int(now - days * 86400))


# ------------------------------------------------------------- итоги ----

def trade_stats(since_ts: int = 0, live: bool = False, asset: str | None = None) -> dict:
    rows = [t for t in storage.trades_since(since_ts) if bool(t["dry_run"]) != live
            and (asset is None or t["asset"] == asset)]
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    wins = sum(1 for t in settled if t["outcome"] == t["side"])
    return {"bets": len(bets), "settled": len(settled), "wins": wins,
            "pnl": sum(t["pnl_usdc"] or 0.0 for t in settled),
            "staked": sum(t["size_usdc"] or 0.0 for t in settled),
            "avg_price": (sum(t["price"] for t in bets if t["price"]) / len(bets)) if bets else None}


def stats_line(name: str, st: dict) -> str:
    if not st["bets"]:
        return f"{name}: ставок нет"
    s = f"{name}: {bets_text(st['bets'])}"
    if st["avg_price"]:
        s += f" (ср. цена {st['avg_price']:.3f})"
    if st["settled"]:
        s += (f", выигр. {st['wins']}/{st['settled']} = {st['wins'] / st['settled'] * 100:.1f}%, "
              f"{st['pnl']:+.2f} $" + (f" ({st['pnl'] / st['staked'] * 100:+.1f}%)" if st["staked"] else ""))
    return s


def mode_text() -> str:
    coins = live_coins()
    if runtime_state.get("dry_run"):
        return "🧪 DRY RUN — реальных ордеров нет" + (
            f" (🔴 помечены {', '.join(a.upper() for a in coins)} — пойдут после «🔴 Включить LIVE»)" if coins else "")
    if not coins:
        return "🔴 LIVE, но ни одна включённая монета не 🔴 — реальных ставок нет"
    return f"🔴 LIVE — реальные ставки по {', '.join(a.upper() for a in coins)}"


def strategy_lines() -> list[str]:
    day = int(time.time() // 86400) * 86400
    since = rule_since()
    lines = [f"⏱ Правило: {rule_text()}. Ставка: {stakes_text()} (не меньше 5 акций). "
             + ("✅ включено" if enabled() else "⏸ выключено"),
             "  Режим: " + mode_text(),
             "  " + stats_line("Тест всего", trade_stats(0, live=False))]
    if since:
        lines.append("  " + stats_line(f"Тест по нынешнему правилу (с {time.strftime('%d.%m %H:%M', time.gmtime(since))} UTC)",
                                       trade_stats(since, live=False)))
    lines.append("  " + stats_line("Тест сегодня (UTC)", trade_stats(day, live=False)))
    real = trade_stats(0, live=True)
    if real["bets"] or live_coins():
        lines.append("  " + stats_line("Реальные всего", real))
        s = stop()
        lines.append(f"  Реальные сегодня: {today_live_pnl():+.2f} USDC"
                     + (f" (дневной стоп −${s:g})" if s > 0 else " (без дневного стопа)"))
        why = live_blocked() if live_coins() else None
        if why and why != "бот в DRY RUN":
            lines.append(f"  ⚠️ Реальные ставки сейчас не уходят: {why}")
    return lines


def stakes_text() -> str:
    """«общая $5; BTC 5% банка (~$10.67), ETH $20» — для экранов."""
    own = [f"{a.upper()} {stake_label(a)}" for a in settings.ASSETS if runtime_state.coin_stake(a)]
    return f"общая ${stake():g}" + (f"; {', '.join(own)}" if own else "")


def bank_line() -> str:
    b, src = bank()
    if b is not None:
        return f"🏦 Банк для ставок в %: ${b:.2f} ({src})"
    why = "нет ключа POLY_PRIVATE_KEY" if not settings.POLY_PRIVATE_KEY else (_bank["error"] or "ещё не читали")
    return f"🏦 Банк для ставок в %: неизвестен ({why}) — монеты с % ставят общую сумму; можно задать вручную"


def coin_lines() -> list[str]:
    out = []
    for a in settings.ASSETS:
        test, real = trade_stats(0, False, a), trade_stats(0, True, a)
        mark = f"{'✅' if runtime_state.is_asset_enabled(a) else '⏸'} {'🔴' if coin_live(a) else '🧪'}"
        if now_skipped(a):
            mark += " (рынков 5m нет)"
        s = f"{a.upper()} {mark} · 💵 {stake_label(a)} — " + stats_line("тест", test)
        if real["bets"] or coin_live(a):
            s += "; " + stats_line("реальные", real)
            miss = sum(1 for t in storage.trades_since(0) if t["asset"] == a and not t["dry_run"]
                       and str(t["status"] or "") == "SKIP_NOFILL")
            if miss:
                s += f" (не купилось за окно входа: {miss})"
        out.append(s)
    return out


def now_skipped(asset: str) -> bool:
    return time.time() < _skip_until.get(asset, 0)


def why_lines() -> list[str]:
    """Почему по включённым монетам окна прошли без ставки (с запуска)."""
    lines = []
    for a in trade_assets():
        c = _why.get(a)
        if c:
            top = ", ".join(f"{k} — {v}" for k, v in c.most_common(3))
            lines.append(f"  {a.upper()}: {top}")
    return (["Окна без ставки (с запуска, главные причины):"] + lines) if lines else []


def recorder_line() -> str:
    rec = storage.window_rows(int(time.time()) - 86400)
    return (f"🔁 Запись: окон за сутки {len(rec)} по {len({r['asset'] for r in rec})} монетам, со страйком "
            f"{sum(1 for r in rec if r['strike'])}, путь с {mmss(settings.REC_FROM_SEC)} до конца каждые "
            f"{settings.REC_STEP_SEC:g} с.")


def feed_warnings(now: float | None = None) -> list[str]:
    """Чего не хватает для ставок по включённым монетам: свежей цены Chainlink, ATR."""
    now = time.time() if now is None else now
    coins = trade_assets()
    if not enabled() or not coins:
        return []
    out = []
    if not any((lambda p: p[0] is not None and p[1] is not None and p[1] <= settings.CHAINLINK_MAX_AGE_SEC)(
            chainlink_feed.latest(a, now)) for a in coins):
        out.append("⚠️ Нет свежей цены Chainlink — ставок не будет (📊 — что с потоком цен).")
    if min_atr() > 0 and not any(atr_feed.get(a, now) is not None for a in coins):
        out.append("⚠️ Нет ATR с Binance — при условии по ATR ставок не будет (📊 — ошибка Binance).")
    return out


def chainlink_line(now: float | None = None) -> str:
    """Откуда идёт цена Chainlink и по каким монетам она свежая (без неё ставок нет)."""
    now = time.time() if now is None else now
    tr = chainlink_feed.transports()

    def state(t) -> str:
        if t is None:
            return "—"
        if t.disabled:
            return f"⏸ ({t.disabled})"
        if t.connected and now - t.last_data < 30:
            return "✅"
        return "❌" + (f" ({t.last_error[:80]})" if t.last_error else "")
    fresh = []
    for a in settings.ASSETS:
        price, age = chainlink_feed.latest(a, now)
        if price is not None and age is not None and age <= settings.CHAINLINK_MAX_AGE_SEC:
            fresh.append(a.upper())
    return (f"📡 Chainlink: PolyBolt {state(tr.get('polybolt'))}, RTDS {state(tr.get('rtds'))}; "
            f"свежая цена: {', '.join(fresh) if fresh else 'ни по одной монете — ставок не будет'}")


def stats_text() -> str:
    lines = ["📊 Статистика", ""] + strategy_lines() + ["", "По монетам (за всё время в базе):"]
    lines += ["  " + x for x in coin_lines()]
    w = why_lines()
    if w:
        lines += [""] + w
    if any(runtime_state.coin_stake(a) and runtime_state.coin_stake(a)[0] == "pct" for a in settings.ASSETS) \
            or settings.POLY_PRIVATE_KEY:
        lines += ["", bank_line()]
    lines += ["", recorder_line(), chainlink_line(), atr_feed.status_line(settings.ASSETS)]
    if _stats["discovery_errors"]:
        lines.append(f"Поиск рынков: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    rec = storage.window_rows(since_ts)
    lines = [f"🔁 Окна 5m {from_label} → {to_label} (UTC): {len(rec)}, со страйком {sum(1 for r in rec if r['strike'])}, "
             f"с официальным исходом {sum(1 for r in rec if r['outcome'])}"]
    st = trade_stats(since_ts, live=False)
    lines.append(stats_line("⏱ Тест за период", st))
    real = trade_stats(since_ts, live=True)
    miss = sum(1 for t in storage.trades_since(since_ts) if not t["dry_run"] and str(t["status"] or "") == "SKIP_NOFILL")
    if real["bets"] or miss:
        lines.append(stats_line("Реальные за период", real)
                     + (f"; не купилось за окно входа: {miss} (причины — в колонке note)" if miss else ""))
    return "\n".join(lines)


# ---------------------------------------------------------- цикл ----

async def _sleep_to_next_second() -> None:
    await asyncio.sleep(1.0 - (time.time() % 1.0) + 0.05)


async def _tick_loop() -> None:
    last_trim = 0.0
    while True:
        try:
            await tick()
            if time.monotonic() - last_trim >= TRIM_EVERY_SEC:
                last_trim = time.monotonic()
                trim_paths()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot: %s", exc)
        await _sleep_to_next_second()


async def _settle_loop() -> None:
    """Исходы окон и итоги ставок — отдельно от тика: запросы к Gamma не должны его держать.
    Здесь же — баланс кошелька для ставок в % банка."""
    while True:
        try:
            await refresh_bank()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot bank: %s", exc)
        await asyncio.sleep(SETTLE_EVERY_SEC)
        try:
            await settle()
            await settle_trades()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot settle: %s", exc)


async def run_forever() -> None:
    await asyncio.sleep(3)
    await asyncio.gather(_tick_loop(), _settle_loop())
