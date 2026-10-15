"""
Бот двух стратегий: один сигнал и два способа решать, когда входить
реальными деньгами.

Сигнал — логика «сломанного» бота 12–14.10 (⚙️ → 📏 Фильтр расстояния: выкл):
фаворит по цене из диапазона входа, тренд, время и волатильность как обычно,
расстояние от страйка не проверяется. Каждый сигнал бот записывает как
ВИРТУАЛЬНУЮ сделку (strategy='virt', dry_run=1) — по ним стратегии «читают»
рынок:

1️⃣ Два режима (🎯 Стратегии → кнопка режима):
   «🩹 Отыгрыш» (по умолчанию): 2️⃣ проиграла по монете — 1️⃣ один раз входит
   по этой же монете на следующем сигнале на сумму, выигрыш с которой (за
   вычетом комиссии) возвращает этот минус. Не больше банка монеты. Проиграл
   отыгрыш — повтора нет. Вход реальный, только если и минус 2️⃣ реальный,
   и 1️⃣ в «💵 реально»; иначе тестовый (тестовый минус реальными деньгами не
   отыгрываем, реальный при 1️⃣ в тесте — отыгрываем тестовым входом).
   «🔁 N после проигрыша»: после виртуального проигрыша монеты — N входов по
   ней (N своё у монеты, 🔢); проигрыш внутри серии начинает её заново.
2️⃣ «Пауза после проигрыша» (B). Вход, только если за последние X минут
   (30/60/120, по умолчанию 60) не было виртуальных проигрышей ни по одной
   монете — общая пауза (по умолчанию с 15.10: на архиве 18.09–12.10 и на
   13–15.10 она была лучше, проигрыши идут пачками сразу по нескольким
   монетам). Можно переключить на паузу у каждой монеты.

🧹 Фильтры входа 1️⃣ и 2️⃣: не позже чем за N мин до конца (по умолчанию 2) и
спред не шире X (по умолчанию выкл) — решение один раз на рынок, по первому
сигналу; признаки входа пишутся в каждую сделку.

У каждой стратегии свой режим: «🧪 тест» — её входы пишутся виртуально (со
своей меткой), «💵 реально» — реальный ордер, если бот в LIVE и монета
🔴 LIVE в 🪙 Активах. Ставка 2️⃣ (и 1️⃣ в режиме N) — фикс. или % банка
(💰 / ⚙️ монеты); банк у каждой монеты свой: старт (🎯 → 🏦) + результат
сделок стратегий этой монеты, реальные и тестовые — отдельно. Никакая
ставка не больше банка монеты (банк кончился — входов по ней нет).
Виртуальные сделки — всегда по $1.

Результат рынка бот узнаёт сам в момент конца — по стакану: в последние
секунды у стороны-победителя нет продавцов (или ask ≥ 0.97), а сторону-
проигравшего продают по ≤ 0.03. Отчёты 11–14.10 (1100+ рынков, ~13 000
проверок стакана за последние 13 с) — ни одного расхождения с официальным
исходом. Сравнение по нашему потоку Chainlink (цена на конце против страйка)
ошибалось в 11% рынков — оно больше не используется. Стакан не решил или
бот его не видел — ждёт официальную резолюцию (settlement_loop).
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import book_stream, runtime_state, storage

log = logging.getLogger("polymarket-bot")

VIRT, A, B = "virt", "A", "B"
STRATS = (A, B)
LABELS = {VIRT: "👻 Виртуальные", A: "1️⃣ После проигрыша", B: "2️⃣ Пауза после проигрыша"}
VIRT_STAKE = 1.0           # виртуальные сделки — по $1: их PnL = итог на $1 ставки
BANK_CACHE_SEC = 10.0
SHORT = {VIRT: "👻", A: "1️⃣", B: "2️⃣"}
_ENABLED_KEY = {A: "strat_a_enabled", B: "strat_b_enabled"}
_LIVE_KEY = {A: "strat_a_live", B: "strat_b_live"}

BOOK_WINDOW_SEC = 12.0     # смотрим стакан рынка в его последние 12 с
BOOK_LAST_SEC = 3.0        # решает чтение не раньше чем за 3 с до конца
BOOK_LO, BOOK_HI = 0.03, 0.97
BOOK_MAX_AGE_MS = 30_000   # стакан, который не обновлялся дольше, — не верим
EARLY_DELAY_SEC = 0.5      # через сколько после конца рынка применить исход по стакану
HISTORY_SEC = 6 * 3600     # сколько истории поднимать из базы при старте и держать в памяти
LOOP_SEC = 0.5
_UPDOWN = ("UP", "DOWN")

# slug -> {"id", "asset", "direction", "start", "end"} — виртуальные сделки
_virtual: dict[str, dict] = {}
# Слаги, чей исход ещё не применён к стратегиям (ждём конца рынка)
_pending: set[str] = set()
# slug -> (победитель по стакану или None, время чтения) — последнее чтение
# стакана в последние секунды рынка
_book_last: dict[str, tuple[str | None, float]] = {}
# slug -> исход, уже применённый к стратегиям (ранний или официальный)
_applied: dict[str, str] = {}
# (slug, "A"/"B") — стратегия уже вошла в этот рынок
_entered: set[tuple[str, str]] = set()
# Последний виртуальный проигрыш каждой монеты: asset -> {"end": конец рынка, "slug"}
_losses: dict[str, dict] = {}
# Сколько исходов узнали сами, сколько пришлось ждать официальный и сколько
# раз наш ранний исход не совпал с официальным
_stats = {"early": 0, "late": 0, "mismatch": 0}


def enabled() -> bool:
    """Режим двух стратегий (иначе бот работает по-старому: сигнал = сделка)."""
    return bool(runtime_state._state.get("strategies_mode", False))


def _asset_of(slug: str) -> str:
    return storage.parse_market_slug(slug)[0]


def _bounds(slug: str) -> tuple[int, int] | None:
    end = storage.market_end_ts(slug)
    if end is None:
        return None
    try:
        return int(slug.rsplit("-", 1)[1]), end
    except (ValueError, IndexError):
        return None


def strategy_enabled(strat: str) -> bool:
    return bool(runtime_state.get(_ENABLED_KEY[strat]))


def strategy_live(strat: str) -> bool:
    return bool(runtime_state.get(_LIVE_KEY[strat]))


def is_dry(strat: str, asset: str) -> bool:
    """Вход стратегии будет тестовым (виртуальным): стратегия в «🧪 тест», бот
    в DRY RUN или монета не 🔴 LIVE."""
    return not strategy_live(strat) or runtime_state.trade_is_dry(asset)


def entry_dry(strat: str, asset: str) -> bool:
    """Тестовым ли будет вход стратегии в этот раз. Отыгрыш 1️⃣ реальный, только
    если 1️⃣ в «💵 реально» и ждёт отыгрыша реальный минус 2️⃣: тестовый минус
    реальными деньгами не отыгрываем, а реальный при 1️⃣ в тесте отыгрываем
    тестовым входом — видно, как сработал бы."""
    if strat == A and a_mode() == "recover":
        return is_dry(A, asset) or recover_amount(asset, False) <= 0
    return is_dry(strat, asset)


# ------------------------------------------------- стратегия 1️⃣ (A) ----

def a_remaining(asset: str) -> int:
    """Сколько входов осталось в текущей серии по монете (0 — ждём проигрыша)."""
    rem = (runtime_state._state.get("duo_a_remaining") or {}).get(asset.lower(), 0)
    try:
        rem = int(rem)
    except (TypeError, ValueError):
        rem = 0
    return max(0, min(rem, runtime_state.strat_a_count(asset)))


def _set_a_remaining(asset: str, n: int) -> None:
    cur = dict(runtime_state._state.get("duo_a_remaining") or {})
    n = max(0, int(n))
    if cur.get(asset.lower(), 0) == n:
        return
    cur[asset.lower()] = n
    runtime_state.set("duo_a_remaining", cur)


def reset_a() -> None:
    """Включили/выключили стратегию 1️⃣ или сменили её режим — с чистого
    листа: ждём следующего проигрыша, а не доигрываем старые серии/отыгрыши."""
    runtime_state.set("duo_a_remaining", {})
    runtime_state.set("duo_recover", {})


def a_mode() -> str:
    """Режим 1️⃣: "recover" — отыгрыш минуса 2️⃣, "series" — N после проигрыша."""
    return "series" if runtime_state._state.get("strat_a_mode") == "series" else "recover"


def label(strat: str) -> str:
    if strat == A:
        return "1️⃣ Отыгрыш минуса 2️⃣" if a_mode() == "recover" else "1️⃣ После проигрыша"
    return LABELS[strat]


# --------------------------------------------- отыгрыш минуса 2️⃣ (1️⃣) ----

def recover_items(asset: str, dry: bool | None = None) -> list[dict]:
    items = (runtime_state._state.get("duo_recover") or {}).get(asset.lower(), [])
    return [it for it in items if dry is None or bool(it["dry"]) == bool(dry)]


def recover_amount(asset: str, dry: bool | None) -> float:
    """Сколько минуса 2️⃣ по монете ждёт отыгрыша: реального (dry=False),
    тестового (True) или всего (None)."""
    return round(sum(it["amount"] for it in recover_items(asset, dry)), 2)


def recover_target(asset: str, dry: bool) -> float:
    """Сколько отыгрывает вход 1️⃣: реальный — реальные минусы 2️⃣, тестовый —
    все, что ждут (и реальные, если 1️⃣ в тесте)."""
    return recover_amount(asset, None if dry else False)


def _set_recover(asset: str, items: list[dict]) -> None:
    cur = {a: list(v) for a, v in (runtime_state._state.get("duo_recover") or {}).items()}
    if items:
        cur[asset.lower()] = items
    else:
        cur.pop(asset.lower(), None)
    runtime_state.set("duo_recover", cur)


def _add_recover(asset: str, slug: str, amount: float, dry: bool) -> None:
    items = recover_items(asset)
    if any(it["slug"] == slug for it in items) or amount <= 0:
        return
    _set_recover(asset, items + [{"slug": slug, "amount": round(float(amount), 2), "dry": bool(dry)}])


def _remove_recover_slug(asset: str, slug: str) -> None:
    items = recover_items(asset)
    left = [it for it in items if it["slug"] != slug]
    if len(left) != len(items):
        _set_recover(asset, left)


def recovery_stake(target: float, price: float) -> float:
    """Ставка, выигрыш с которой за вычетом комиссии (ставка × FEE_RATE ×
    (1 − цена)) равен target. Пример: $5 по 0.92 → ≈$61.5."""
    p = min(max(float(price), 0.01), 0.99)
    per_dollar = (1 - p) * (1 / p - max(0.0, float(settings.FEE_RATE)))
    if target <= 0 or per_dollar <= 0:
        return 0.0
    return round(target / per_dollar, 2)


# ------------------------------------------------------- банки монет ----

_bank_cache: dict[tuple[str, bool], tuple[float, float]] = {}


def coin_bank(asset: str, dry: bool, max_age: float = BANK_CACHE_SEC) -> float:
    """Банк монеты: старт (🎯 → 🏦) + итог закрытых сделок стратегий этой
    монеты — реальных (dry=False) или тестовых (dry=True)."""
    key = (asset.lower(), bool(dry))
    now = time.monotonic()
    c = _bank_cache.get(key)
    if c is not None and max_age > 0 and now - c[0] <= max_age:
        return c[1]
    value = runtime_state.coin_bank_start(asset) + storage.coin_strategy_pnl(asset, dry)
    _bank_cache[key] = (now, value)
    return value


def invalidate_banks() -> None:
    _bank_cache.clear()


def stake_for(strat: str, asset: str, dry: bool, price: float) -> float:
    """Ставка входа: виртуальный — $1; 1️⃣ в режиме отыгрыша — сумма, которая
    вернёт минус 2️⃣; иначе — фикс. или % банка монеты. Всегда не больше банка
    монеты: банк кончился — 0, входа нет (пополнить — 🎯 → 🏦)."""
    if strat == VIRT:
        return VIRT_STAKE
    bank = max(0.0, coin_bank(asset, dry))
    if strat == A and a_mode() == "recover":
        stake = recovery_stake(recover_target(asset, dry), price)
    else:
        mode, usdc, pct = runtime_state.sizing(asset)
        stake = bank * pct / 100 if mode == "percent" else usdc
    stake = round(max(0.0, min(stake, bank)), 2)
    if 0 < stake < 1.0:   # меньше $1 Polymarket не примет
        return 1.0 if bank >= 1.0 else 0.0
    return stake


# ------------------------------------------------- стратегия 2️⃣ (B) ----

def pause_sec() -> int:
    return max(1, int(runtime_state.get("strat_b_pause_min"))) * 60


def scope() -> str:
    """Пауза 2️⃣: "all" — проигрыш любой монеты останавливает все (по
    умолчанию), "coin" — у каждой монеты своя."""
    return "coin" if runtime_state._state.get("strat_b_scope") == "coin" else "all"


# ------------------------------------------------------ фильтры входа ----
#
# Решают, пускать ли стратегии 1️⃣ и 2️⃣ в рынок, — один раз на рынок, в момент
# первого сигнала (виртуальной сделки), как на истории, по которой их
# выбирали. Виртуальные сделки пишутся всегда — по ним пауза и статистика.

FILTER_LABELS = {"time": "мало времени до конца", "spread": "широкий спред"}
REF_MIN_LEFT, REF_SPREAD = 2.0, 0.01   # пороги для сравнения в статистике, если фильтр выкл


def entry_filters() -> tuple[float, float]:
    """(минимум минут до конца, максимум спреда); 0 — фильтр выключен."""
    return (max(0.0, float(runtime_state.get("entry_min_minutes_left") or 0.0)),
            max(0.0, float(runtime_state.get("entry_max_spread") or 0.0)))


def decision_spread(decision) -> float | None:
    """Спред в момент сигнала: ask фаворита + ask другой стороны − 1."""
    opp = getattr(decision, "opp_ask", None)
    if opp is None or decision.entry_price is None:
        return None
    return round(decision.entry_price + opp - 1.0, 4)


def entry_filter_reason(decision) -> str | None:
    """"time" / "spread" — фильтры не пускают стратегии в рынок, None — пускают."""
    min_left, max_spread = entry_filters()
    if min_left > 0 and decision.minutes_left is not None and decision.minutes_left < min_left - 1e-9:
        return "time"
    if max_spread > 0:
        sp = decision_spread(decision)
        if sp is None or sp > max_spread + 1e-9:
            return "spread"
    return None


def entry_features(decision, filtered: str | None = None) -> dict:
    """Признаки входа для колонок сделки (storage.FEATURE_COLUMNS)."""
    return {"minutes_left": decision.minutes_left, "opp_ask": getattr(decision, "opp_ask", None),
            "spread": decision_spread(decision), "distance_pct": getattr(decision, "distance_pct", None),
            "distance_atr": decision.distance_atr, "atr_ratio": getattr(decision, "atr_ratio", None),
            "filtered": filtered or ""}


def filters_text() -> str:
    min_left, max_spread = entry_filters()
    return ", ".join([f"до конца ≥ {min_left:g} мин" if min_left > 0 else "по времени — выкл",
                      f"спред ≤ {max_spread * 100:g}¢" if max_spread > 0 else "по спреду — выкл"])


def market_filtered(slug: str) -> str:
    v = _virtual.get(slug)
    return (v.get("filtered") or "") if v else ""


def feature_split(since_ts: int = 0) -> list[tuple[str, dict]]:
    """Виртуальные сделки, разбитые по признакам фильтров (пороги — текущие,
    выключенный фильтр — опорный 2 мин / 1¢): [(название, итог)], итог —
    {"trades", "losses", "expected_losses", "pnl_usdc"}."""
    min_left, max_spread = entry_filters()
    ml = min_left or REF_MIN_LEFT
    sp = max_spread or REF_SPREAD
    groups = {f"до конца ≥ {ml:g} мин": [], f"< {ml:g} мин": [],
              f"спред ≤ {sp * 100:g}¢": [], f"спред > {sp * 100:g}¢": []}
    keys = list(groups)
    for price, direction, outcome, pnl, mleft, spread in storage.virtual_feature_rows(since_ts):
        row = (price, direction, outcome, pnl)
        if mleft is not None:
            groups[keys[0] if mleft >= ml - 1e-9 else keys[1]].append(row)
        if spread is not None:
            groups[keys[2] if spread <= sp + 1e-9 else keys[3]].append(row)
    out = []
    for name in keys:
        rows = groups[name]
        out.append((name, {"trades": len(rows), "losses": sum(1 for r in rows if r[1] != r[2]),
                           "expected_losses": sum(1.0 - float(r[0] or 0) for r in rows),
                           "pnl_usdc": sum(float(r[3] or 0) for r in rows)}))
    return out


def apply_v3_defaults() -> bool:
    """Разовый переход на настройки 15.10 (анализ архива 18.09–12.10 и сделок
    13–15.10): общая пауза 2️⃣ 60 мин, фильтр «не позже чем за 2 мин до
    конца», фильтр спреда выкл, обе стратегии в 🧪 тесте. Дальше настройки
    меняются только кнопками. True — применили сейчас."""
    if runtime_state.get("duo_defaults_v3"):
        return False
    runtime_state.set("strat_b_scope", "all")
    runtime_state.set("strat_b_pause_min", 60)
    runtime_state.set("entry_min_minutes_left", 2.0)
    runtime_state.set("entry_max_spread", 0.0)
    runtime_state.set("strat_a_live", False)
    runtime_state.set("strat_b_live", False)
    runtime_state.set("duo_defaults_v3", True)
    return True


def last_loss(asset: str | None = None) -> dict | None:
    """Последний виртуальный проигрыш монеты (asset) или любой монеты (None):
    {"asset", "end", "slug"}."""
    if asset is not None:
        v = _losses.get(asset.lower())
        return {"asset": asset.lower(), **v} if v else None
    best = None
    for a, v in _losses.items():
        if best is None or v["end"] > best["end"]:
            best = {"asset": a, **v}
    return best


def b_pause_until(now: float | None = None, asset: str | None = None) -> float | None:
    """До какого времени стратегия 2️⃣ на паузе для монеты asset (в режиме
    «по всем монетам» — для всех). None — можно входить."""
    now = time.time() if now is None else now
    ll = last_loss(asset if scope() == "coin" else None)
    if ll is None:
        return None
    until = ll["end"] + pause_sec()
    return until if until > now else None


def b_allowed(now: float | None = None, asset: str | None = None) -> bool:
    return b_pause_until(now, asset) is None


def paused_assets(now: float | None = None) -> dict[str, float]:
    """Монеты на паузе 2️⃣ сейчас: asset -> до какого времени."""
    now = time.time() if now is None else now
    out = {}
    for a in list(_losses):
        until = b_pause_until(now, a)
        if until is not None:
            out[a] = until
    return out


# ------------------------------------------------------------ решения ----

def wanted(asset: str, slug: str, now: float | None = None) -> list[str]:
    """Какие стратегии хотят войти в этот рынок прямо сейчас."""
    if market_filtered(slug):
        return []          # фильтры входа закрыли рынок для стратегий
    out = []
    if strategy_enabled(A) and (slug, A) not in _entered:
        if a_mode() == "series":
            if a_remaining(asset) > 0:
                out.append(A)
        elif recover_items(asset):
            out.append(A)
    if strategy_enabled(B) and b_allowed(now, asset) and (slug, B) not in _entered:
        out.append(B)
    return out


def has_virtual(slug: str) -> bool:
    return slug in _virtual


def market_done(slug: str, now: float | None = None) -> bool:
    """Для быстрой проверки: по рынку делать больше нечего — виртуальная
    сделка записана, и ни одна стратегия войти не хочет."""
    if slug not in _virtual:
        return False
    return not wanted(_asset_of(slug), slug, now)


# ------------------------------------------------- события executor ----

def on_virtual_logged(trade_id: int, slug: str, direction: str,
                      up_token: str | None = None, down_token: str | None = None,
                      filtered: str | None = None) -> None:
    b = _bounds(slug)
    if b is None:
        return
    _virtual[slug] = {"id": trade_id, "asset": _asset_of(slug), "direction": direction,
                      "start": b[0], "end": b[1], "up": up_token, "down": down_token,
                      "filtered": filtered or ""}
    if slug not in _applied:
        _pending.add(slug)
    _prune()


def on_strategy_entered(strat: str, slug: str, dry: bool | None = None) -> None:
    _entered.add((slug, strat))
    if strat == A:
        asset = _asset_of(slug)
        if a_mode() == "series":
            _set_a_remaining(asset, a_remaining(asset) - 1)
        elif recover_items(asset):
            # отыгрыш — одна попытка: всё, что ждало, закрыто (проиграл — повтора нет)
            _set_recover(asset, [])


# ------------------------------------------------------ исходы рынков ----

def _recompute_losses(now: float | None = None) -> None:
    """Последний виртуальный проигрыш каждой монеты — по базе (официальный
    исход, иначе ранний)."""
    now = time.time() if now is None else now
    best: dict[str, dict] = {}
    for _tid, slug, direction, outcome, early in storage.virtual_trades_since(int(now - HISTORY_SEC)):
        res = outcome if outcome in _UPDOWN else (early if early in _UPDOWN else None)
        if res is None or res == direction:
            continue
        b = _bounds(slug)
        a = _asset_of(slug)
        if b and (a not in best or b[1] > best[a]["end"]):
            best[a] = {"end": b[1], "slug": slug}
    _losses.clear()
    _losses.update(best)


def _apply(slug: str, direction: str, outcome: str) -> bool:
    """Применяет исход виртуальной сделки к стратегиям — один раз на рынок
    (повторный тот же исход ничего не меняет)."""
    prev = _applied.get(slug)
    if prev == outcome:
        return False
    _applied[slug] = outcome
    lost = outcome != direction
    prev_lost = prev is not None and prev != direction
    b = _bounds(slug)
    if b is None:
        return True
    asset = _asset_of(slug)
    if lost and not prev_lost:
        if strategy_enabled(A):
            if a_mode() == "series":
                # 1️⃣: серия по монете начинается — или начинается заново
                _set_a_remaining(asset, runtime_state.strat_a_count(asset))
            else:
                # 1️⃣: 2️⃣ проиграла в этом рынке — её минус ждёт отыгрыша
                tr = storage.strategy_trade(slug, B)
                if tr is not None:
                    _add_recover(asset, slug, float(tr[1] or 0), bool(tr[2]))
        # 2️⃣: пауза монеты считается от конца проигравшего рынка
        cur = _losses.get(asset)
        if cur is None or b[1] >= cur["end"]:
            _losses[asset] = {"end": b[1], "slug": slug}
    elif prev_lost and not lost:
        # Официальный исход поправил наш ранний «проигрыш» — паузы по базе,
        # и отыгрывать нечего
        _recompute_losses()
        _remove_recover_slug(asset, slug)
    return True


def book_winner(up_token: str | None, down_token: str | None) -> str | None:
    """Победитель по живому стакану в последние секунды рынка: у победителя
    нет продавцов (или ask ≥ 0.97), проигравшего продают по ≤ 0.03. None —
    стакан не решил, его нет или он давно не обновлялся."""
    if not up_token or not down_token:
        return None
    ub, db = book_stream.get_book(up_token), book_stream.get_book(down_token)
    if not ub or not db:
        return None
    fresh = max(ub.get("received_ms") or 0, db.get("received_ms") or 0)
    if book_stream.now_ms() - fresh > BOOK_MAX_AGE_MS:
        return None
    ua, da = book_stream.best_ask(up_token), book_stream.best_ask(down_token)
    if da is not None and da <= BOOK_LO and (ua is None or ua >= BOOK_HI):
        return "UP"
    if ua is not None and ua <= BOOK_LO and (da is None or da >= BOOK_HI):
        return "DOWN"
    return None


def check_pending(now: float | None = None) -> int:
    """Последние секунды рынков с виртуальной сделкой — читаем стакан; сразу
    после конца применяем исход по последнему чтению. Возвращает, сколько
    исходов применили."""
    now = time.time() if now is None else now
    done = 0
    for slug in list(_pending):
        v = _virtual.get(slug)
        if v is None or slug in _applied:
            _pending.discard(slug)
            _book_last.pop(slug, None)
            continue
        if now < v["end"]:
            if now >= v["end"] - BOOK_WINDOW_SEC:
                _book_last[slug] = (book_winner(v.get("up"), v.get("down")), now)
            continue
        if now < v["end"] + EARLY_DELAY_SEC:
            continue
        _pending.discard(slug)
        res, t = _book_last.pop(slug, (None, 0.0))
        if res is None or t < v["end"] - BOOK_LAST_SEC:
            continue   # стакан не решил или его не видели — ждём официальную резолюцию
        try:
            storage.set_early_outcome(v["id"], res)
        except Exception as exc:  # noqa: BLE001 — запись в базу не должна мешать стратегиям
            log.warning("Не записал ранний исход %s: %s", slug, exc)
        _apply(slug, v["direction"], res)
        _stats["early"] += 1
        done += 1
    return done


def on_official(slug: str, direction: str, outcome: str, early: str | None = None) -> None:
    """Официальная резолюция виртуальной сделки (settlement_loop)."""
    _pending.discard(slug)
    if outcome not in _UPDOWN:
        return
    known = _applied.get(slug) or (early if early in _UPDOWN else None)
    if known is None:
        _stats["late"] += 1
    elif known != outcome:
        _stats["mismatch"] += 1
        log.warning("Ранний исход %s (%s) не совпал с официальным (%s)", slug, known, outcome)
    if known is not None and slug not in _applied:
        _applied[slug] = known   # применён до перезапуска — не применяем второй раз
    _apply(slug, direction, outcome)


def _prune(now: float | None = None) -> None:
    now = time.time() if now is None else now
    if len(_virtual) < 500:
        return
    old = [s for s, v in _virtual.items() if v["end"] < now - HISTORY_SEC]
    for s in old:
        _virtual.pop(s, None)
        _applied.pop(s, None)
        _pending.discard(s)
        _book_last.pop(s, None)
    for key in [k for k in _entered if k[0] in old]:
        _entered.discard(key)


def init(now: float | None = None) -> None:
    """При старте: виртуальные сделки, исходы и входы стратегий за последние
    часы — из базы (состояние серий 1️⃣ хранится в настройках)."""
    now = time.time() if now is None else now
    _virtual.clear()
    _pending.clear()
    _book_last.clear()
    _applied.clear()
    _entered.clear()
    _losses.clear()
    for k in _stats:
        _stats[k] = 0
    since = int(now - HISTORY_SEC)
    for tid, slug, direction, outcome, early in storage.virtual_trades_since(since):
        b = _bounds(slug)
        if b is None:
            continue
        _virtual[slug] = {"id": tid, "asset": _asset_of(slug), "direction": direction, "start": b[0], "end": b[1]}
        res = outcome if outcome in _UPDOWN else (early if early in _UPDOWN else None)
        if res is not None:
            _applied[slug] = res
        elif outcome is None:
            _pending.add(slug)
    for slug, reason in storage.virtual_filtered_since(since).items():
        if slug in _virtual:
            _virtual[slug]["filtered"] = reason
    for slug, strat in storage.strategy_entries_since(since):
        _entered.add((slug, strat))
    _recompute_losses(now)


async def run_forever() -> None:
    """Фоновая задача: раз в 0.5 с смотрит стаканы рынков в их последние
    секунды и применяет исходы закончившихся."""
    while True:
        try:
            if enabled():
                check_pending()
        except Exception as exc:  # noqa: BLE001 — сбой не должен ронять бота
            log.exception("Ошибка проверки исходов стратегий: %s", exc)
        await asyncio.sleep(LOOP_SEC)


# ------------------------------------------------------- тексты/статус ----

def _ago(seconds: float) -> str:
    m = int(max(0, seconds) // 60)
    if m < 1:
        return "только что"
    if m < 60:
        return f"{m} мин назад"
    return f"{m // 60} ч {m % 60:02d} мин назад"


def _left(seconds: float) -> str:
    m = int(max(0, seconds) // 60) + (1 if seconds % 60 else 0)
    return f"{m} мин" if m < 60 else f"{m // 60} ч {m % 60:02d} мин"


def mode_text(strat: str) -> str:
    if not strategy_enabled(strat):
        return "выкл"
    return "💵 реально" if strategy_live(strat) else "🧪 тест"


def a_state_text(assets: list[str]) -> str:
    """Режим отыгрыша: 'SOL — отыграть $5.00 (тест); BTC, ETH — ждут минуса 2️⃣'.
    Режим N: 'SOL — осталось 2 из 3; BTC, ETH — ждут проигрыша; XRP — N=0'."""
    if a_mode() == "recover":
        todo, waiting = [], []
        for a in assets:
            if recover_items(a):
                dry = entry_dry(A, a)
                todo.append(f"{a.upper()} — отыграть ${recover_target(a, dry):.2f} "
                            f"({'тест' if dry else 'реально'})")
            else:
                waiting.append(a.upper())
        if waiting:
            todo.append(f"{', '.join(waiting)} — {'ждёт' if len(waiting) == 1 else 'ждут'} минуса 2️⃣")
        return "; ".join(todo) or "нет включённых монет"
    active, waiting, off = [], [], []
    for a in assets:
        n = runtime_state.strat_a_count(a)
        if n <= 0:
            off.append(a.upper())
        elif a_remaining(a) > 0:
            active.append(f"{a.upper()} — осталось {a_remaining(a)} из {n}")
        else:
            waiting.append(a.upper())
    parts = active[:]
    if waiting:
        parts.append(f"{', '.join(waiting)} — {'ждёт' if len(waiting) == 1 else 'ждут'} проигрыша")
    if off:
        parts.append(f"{', '.join(off)} — N=0, не входит")
    return "; ".join(parts) or "нет включённых монет"


def b_state_text(now: float | None = None) -> str:
    now = time.time() if now is None else now
    if scope() == "all":
        until = b_pause_until(now)
        ll = last_loss()
        if until is not None and ll:
            return f"пауза ещё {_left(until - now)} (проигрыш {ll['asset'].upper()} {_ago(now - ll['end'])})"
        if ll:
            return f"можно входить (последний проигрыш {ll['asset'].upper()} {_ago(now - ll['end'])})"
        return "можно входить (проигрышей за последние часы не было)"
    paused = paused_assets(now)
    if not paused:
        return "по всем монетам можно входить"
    parts = [f"{a.upper()} ещё {_left(u - now)}" for a, u in sorted(paused.items(), key=lambda kv: kv[1])]
    return "на паузе: " + ", ".join(parts) + "; остальные — можно"


def summary_line(now: float | None = None) -> str:
    """Одна строка для меню и сообщения о запуске."""
    now = time.time() if now is None else now
    b = mode_text(B)
    if strategy_enabled(B):
        if scope() == "all":
            until = b_pause_until(now)
            b += f", пауза ещё {_left(until - now)}" if until else ", можно входить"
        else:
            paused = paused_assets(now)
            b += (", на паузе: " + ", ".join(a.upper() for a in sorted(paused))) if paused else ", можно входить"
    a = mode_text(A)
    if strategy_enabled(A):
        a += ", отыгрыш" if a_mode() == "recover" else ", N после проигрыша"
    return f"🎯 Стратегии: 1️⃣ {a} · 2️⃣ {b} · 🧹 {filters_text()}"


def early_stats_text() -> str:
    e, late, mm = _stats["early"], _stats["late"], _stats["mismatch"]
    if not e and not late:
        return ""
    return (f"Исход рынка: по стакану в момент конца — {e}, ждал официальный — {late}"
            + (f", по стакану не совпал с официальным: {mm}" if mm else ""))


def compare_pause(since_ts: int = 0) -> dict[str, dict]:
    """Как сработала бы пауза 2️⃣ «у каждой монеты» и «общая» на виртуальных
    сделках с since_ts (вход — в момент виртуальной сделки, пауза — от конца
    проигравшего рынка, длина — текущая настройка). Чтобы по своим свежим
    данным видеть, какая лучше: {"coin"|"all": {"trades", "losses",
    "expected_losses", "pnl_usdc"}}."""
    import bisect
    rows = storage.virtual_results_since(since_ts)
    pause = pause_sec()
    loss_ends_all: list[int] = []
    loss_ends: dict[str, list[int]] = {}
    for ts, slug, _p, won, _pnl in rows:
        b = _bounds(slug)
        if b and not won:
            loss_ends_all.append(b[1])
            loss_ends.setdefault(_asset_of(slug), []).append(b[1])
    loss_ends_all.sort()
    for v in loss_ends.values():
        v.sort()

    def paused(ends: list[int], t: int) -> bool:
        i = bisect.bisect_right(ends, t)      # проигрыши, известные к моменту входа
        return i > 0 and t - ends[i - 1] < pause

    out = {sc: {"trades": 0, "losses": 0, "expected_losses": 0.0, "pnl_usdc": 0.0} for sc in ("coin", "all")}
    for ts, slug, price, won, pnl in rows:
        for sc, ends in (("coin", loss_ends.get(_asset_of(slug), [])), ("all", loss_ends_all)):
            if paused(ends, ts):
                continue
            o = out[sc]
            o["trades"] += 1
            o["losses"] += int(not won)
            o["expected_losses"] += 1.0 - price
            o["pnl_usdc"] += pnl or 0.0
    return out


def compare_pause_text(since_ts: int = 0) -> str:
    """'Пауза 2️⃣ на виртуальных: у каждой монеты 120 вх., k=0.95, PnL +1.20 ·
    общая 80 вх., k=0.83, PnL +2.10' или ''."""
    c = compare_pause(since_ts)
    if not c["coin"]["trades"] and not c["all"]["trades"]:
        return ""

    def one(name: str, o: dict) -> str:
        k = o["losses"] / o["expected_losses"] if o["expected_losses"] > 0 else 0.0
        return f"{name} {o['trades']} вх., k={k:.2f}, PnL {o['pnl_usdc']:+.2f}"
    return (f"📐 Пауза 2️⃣ на виртуальных сделках: {one('у каждой монеты', c['coin'])} · "
            f"{one('общая', c['all'])}")
