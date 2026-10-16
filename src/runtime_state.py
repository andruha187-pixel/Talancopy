"""
Настройки, которые можно менять на лету из Telegram (без передеплоя):
размер позиции, дневной стоп-лосс, порог safety score, диапазон входа,
пауза, режим DRY_RUN/LIVE.

Живут в памяти для быстрого доступа из strategy/executor на каждом тике,
но каждое изменение сразу пишется в SQLite (`bot_settings`) — переживает
рестарт процесса (важно на Render: контейнер может перезапуститься сам
по себе, не только по твоей команде).
"""
from __future__ import annotations
import json
import time

from config import settings
from src import storage
from src.timeframes import TIMEFRAMES

# Ключи «своих» настроек монеты (см. asset_overrides ниже)
RANGE_KEYS = ("min_entry_price", "max_entry_price")
SIZING_KEYS = ("sizing_mode", "trade_size_usdc", "bankroll_pct")
_OVERRIDE_FLOAT_KEYS = ("min_entry_price", "max_entry_price", "trade_size_usdc", "bankroll_pct")


def _parse_counts(raw) -> dict:
    """JSON {"sol": 8, "btc": 2} -> {"sol": 8, "btc": 2}: целые ≥ 0, монеты в
    нижнем регистре. Битые значения молча отбрасываем."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for asset, n in data.items():
        try:
            out[str(asset).lower()] = max(0, int(float(n)))
        except (TypeError, ValueError):
            pass
    return out


def _parse_floats(raw) -> dict:
    """JSON {"sol": 100} -> {"sol": 100.0} (суммы ≥ 0)."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for asset, v in data.items():
        try:
            out[str(asset).lower()] = max(0.0, float(v))
        except (TypeError, ValueError):
            pass
    return out


def _parse_recover(raw) -> dict:
    """JSON несыгранных минусов: {"sol": [{"slug", "amount", "dry"}, ...]}."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for asset, items in data.items():
        if not isinstance(items, list):
            continue
        clean = []
        for it in items:
            try:
                clean.append({"slug": str(it["slug"]), "amount": float(it["amount"]), "dry": bool(it["dry"])})
            except (KeyError, TypeError, ValueError):
                pass
        if clean:
            out[str(asset).lower()] = clean
    return out


def _parse_overrides(raw) -> dict:
    """JSON из базы -> {"sol": {"min_entry_price": 0.92, ...}, ...}. Битые или
    неизвестные поля молча отбрасываем: настройки не должны ронять старт."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for asset, ov in data.items():
        if not isinstance(ov, dict):
            continue
        clean: dict = {}
        for key in _OVERRIDE_FLOAT_KEYS:
            if key in ov:
                try:
                    clean[key] = float(ov[key])
                except (TypeError, ValueError):
                    pass
        if ov.get("sizing_mode") in ("fixed", "percent"):
            clean["sizing_mode"] = ov["sizing_mode"]
        if clean:
            out[str(asset).lower()] = clean
    return out


_DEFAULTS = {
    "paused": False,
    "dry_run": settings.DRY_RUN,
    "trade_size_usdc": settings.TRADE_SIZE_USDC,
    "daily_loss_limit_usdc": settings.DAILY_LOSS_LIMIT_USDC,
    "safety_score_threshold": settings.SAFETY_SCORE_THRESHOLD,
    "min_entry_price": settings.MIN_ENTRY_PRICE,
    "max_entry_price": settings.MAX_ENTRY_PRICE,
    # Мин. расстояние цены от страйка, % от цены (0 = выкл) — см. config.py.
    "min_distance_pct": settings.MIN_DISTANCE_PCT,
    # Окно входа: за сколько минут до конца 5-минутного рынка бот может войти.
    # Было зашито в профиль таймфрейма (1.0–3.5, пропорция от 15m, ни разу не
    # проверялось на 5m). Теперь меняется кнопкой в ⚙️ Настройках.
    "entry_window_min": TIMEFRAMES[0].min_minutes_left,
    "entry_window_max": TIMEFRAMES[0].max_minutes_left,
    # Сколько РЕАЛЬНЫХ позиций может быть открыто одновременно (по всем
    # монетам). Считаются только позиции в ещё идущих рынках: после конца
    # рынка исход уже не меняется, и слот не должен ждать, пока Gamma API
    # отметит рынок закрытым.
    "max_open_positions": settings.MAX_OPEN_POSITIONS,
    # Быстрая проверка сигнала раз в ~0.3 с по цене Binance из потока (см.
    # src/fast_signal.py). Обычная проверка раз в 3 с работает всегда.
    "fast_entry_enabled": settings.FAST_ENTRY_ENABLED,
    # Вход, только если сигнал был и ~3 с назад (см. src/entry_confirm.py).
    "entry_confirm_enabled": settings.ENTRY_CONFIRM_ENABLED,
    # По какой цене считать сигнал: "chainlink" (как Polymarket) или "binance"
    # (см. src/chainlink_feed.py).
    "price_source": settings.PRICE_SOURCE if settings.PRICE_SOURCE in ("chainlink", "binance") else "chainlink",
    # Версия применённого набора рекомендованных настроек (см. RECOMMENDED ниже).
    "preset_version": 0,
    # По умолчанию выключено: каждая прошедшая порог сделка идёт полным
    # TRADE_SIZE_USDC, без урезания по пограничности score.
    "size_scaling_enabled": False,
    # Стоп-лосс ОТДЕЛЬНОЙ позиции в процентах (не дневной!): если текущая
    # стоимость позиции (по best bid в стакане) упала настолько от суммы
    # входа — закрываем досрочно продажей, не дожидаясь резолюции рынка.
    "position_stop_loss_enabled": False,
    "position_stop_loss_pct": 50.0,
    # Какие активы сейчас реально торгуются — можно включать/выключать
    # по одному через Telegram, не трогая остальные и не передеплоя.
    # Хранится как строка через запятую (см. get/set_enabled_assets ниже).
    "enabled_assets": ",".join(a for a in (x.strip().lower() for x in settings.ENABLED_ASSETS.split(","))
                               if a in settings.ASSETS),
    # Режим по каждой монете. Реальные ордера идут только по монетам из этого
    # списка (и только когда сам бот в LIVE); остальные включённые монеты
    # торгуют в DRY RUN — виртуальные сделки, чтобы проверить стратегию на
    # каждой монете отдельно, не рискуя деньгами. По умолчанию LIVE только
    # BTC: любая новая монета начинает с DRY.
    "live_assets": ",".join(a for a in (x.strip().lower() for x in settings.LIVE_ASSETS.split(",")) if a),
    # Уведомления о виртуальных сделках монет в режиме DRY. По умолчанию
    # выключены, чтобы 5–6 монет не засыпали чат; всё видно в 📊 Статистике
    # и в 4-часовых отчётах.
    "notify_dry_assets": False,
    # Режим размера ставки: "fixed" (константа в USDC, trade_size_usdc) или
    # "percent" (доля от ТЕКУЩЕГО банка — starting_bankroll_usdc + вся
    # реализованная прибыль/убыток с начала). Percent-режим сам сжимается
    # при просадке и растёт при выигрышах — в отличие от fixed, который на
    # похудевшем банке становится относительно только агрессивнее.
    "sizing_mode": settings.SIZING_MODE if settings.SIZING_MODE in ("fixed", "percent") else "percent",
    "bankroll_pct": settings.BANKROLL_PCT,
    "starting_bankroll_usdc": settings.STARTING_BANKROLL_USDC,
    # Свои настройки отдельных монет поверх общих: диапазон входа и размер
    # ставки (режим fixed/percent со своими значениями). Пример:
    #   {"sol": {"min_entry_price": 0.92, "max_entry_price": 0.95},
    #    "btc": {"sizing_mode": "percent", "bankroll_pct": 10.0, "trade_size_usdc": 20.0}}
    # Чего у монеты нет — берётся общее значение и меняется вместе с ним.
    # Банк для режима % один на все монеты (кошелёк один).
    "asset_overrides": _parse_overrides(settings.ASSET_OVERRIDES),
    # Отслеживание чужого кошелька: уведомления всегда можно включить
    # отдельно от реального копирования сделок (copytrade) — по умолчанию
    # только уведомляем, ничего не покупаем автоматически.
    "wallet_notify_enabled": True,
    "wallet_copytrade_enabled": False,
    "copytrade_size_usdc": 5.0,
    # Хедж-бот: вход по ENTRY_PRICE, докупка противоположной стороны при
    # достижении HEDGE_PRICE — см. src/hedge_bot.py. Пороги пришли из
    # анализа реальных momentum-отчётов (2026-09-20): хедж на 0.90 дал
    # положительный PnL на бэктесте, хедж на 0.70-0.85 — отрицательный,
    # несмотря на то, что сам хедж всегда безубыточен по построению —
    # разница в том, сколько сессий вообще НЕ доходит до точки хеджа и
    # остаётся неприкрытой позицией (см. обсуждение в чате).
    "hedge_bot_enabled": False,  # хедж живёт отдельным ботом (polymarket-hedge-bot)
    "hedge_entry_price": 0.70,
    "hedge_trigger_price": 0.90,
    "hedge_stake_usdc": 5.0,
    # --- Две стратегии (src/duo.py) ---
    # Сигнал = виртуальная сделка, реальные входы — по стратегиям 1️⃣ и 2️⃣.
    "strategies_mode": settings.STRATEGIES_MODE,
    # Фильтр расстояния от страйка (1.5 ATR и минимум в %). Выкл — логика
    # «сломанного» бота: расстояние не проверяется.
    "distance_filter_enabled": settings.DISTANCE_FILTER_ENABLED,
    "strat_a_enabled": settings.STRAT_A_ENABLED,
    "strat_a_live": settings.STRAT_A_LIVE,
    # Сколько входов после проигрыша у каждой монеты (нет в словаре — STRAT_A_DEFAULT_N)
    "strat_a_counts": _parse_counts(settings.STRAT_A_COUNTS),
    "strat_b_enabled": settings.STRAT_B_ENABLED,
    "strat_b_live": settings.STRAT_B_LIVE,
    "strat_b_pause_min": max(1, settings.STRAT_B_PAUSE_MIN),
    # Пауза 2️⃣: "all" — общая на все монеты, "coin" — у каждой монеты своя
    "strat_b_scope": "coin" if settings.STRAT_B_SCOPE == "coin" else "all",
    # Фильтры входа стратегий: минимум минут до конца, максимум спреда (0 — выкл)
    "entry_min_minutes_left": max(0.0, settings.ENTRY_MIN_MINUTES_LEFT),
    "entry_max_spread": max(0.0, settings.ENTRY_MAX_SPREAD),
    # Разовый переход на настройки 15.10 (общая пауза, фильтр 2 мин, тест) уже был
    "duo_defaults_v3": False,
    # Режим 1️⃣: "recover" — отыгрыш минуса 2️⃣, "series" — N входов после проигрыша
    "strat_a_mode": "series" if settings.STRAT_A_MODE == "series" else "recover",
    # Стартовый банк монеты (нет в словаре — COIN_BANK_USDC)
    "coin_banks": {},
    # Отыгрыш 1️⃣: несыгранные минусы 2️⃣ по монетам
    # {"sol": [{"slug": ..., "amount": 5.0, "dry": true}, ...]}
    "duo_recover": {},
    # Состояние стратегии 1️⃣: сколько входов осталось в текущей серии по монете
    "duo_a_remaining": {},
    # ⚡ Лаборатория скорости: запись рывков Binance и стакана (src/speed_lab.py)
    "speed_lab_enabled": settings.SPEED_LAB_ENABLED,
}

# Типы приведения при чтении из SQLite (там всё хранится как TEXT)
_CASTERS = {
    "paused": lambda v: str(v).lower() == "true",
    "dry_run": lambda v: str(v).lower() == "true",
    "trade_size_usdc": float,
    "daily_loss_limit_usdc": float,
    "safety_score_threshold": float,
    "min_entry_price": float,
    "max_entry_price": float,
    "min_distance_pct": float,
    "entry_window_min": float,
    "entry_window_max": float,
    "max_open_positions": int,
    "fast_entry_enabled": lambda v: str(v).lower() == "true",
    "entry_confirm_enabled": lambda v: str(v).lower() == "true",
    "price_source": lambda v: str(v).lower() if str(v).lower() in ("chainlink", "binance") else "chainlink",
    "preset_version": int,
    "size_scaling_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_pct": float,
    "enabled_assets": str,
    "live_assets": str,
    "notify_dry_assets": lambda v: str(v).lower() == "true",
    "sizing_mode": str,
    "bankroll_pct": float,
    "starting_bankroll_usdc": float,
    "asset_overrides": _parse_overrides,
    "wallet_notify_enabled": lambda v: str(v).lower() == "true",
    "wallet_copytrade_enabled": lambda v: str(v).lower() == "true",
    "copytrade_size_usdc": float,
    "hedge_bot_enabled": lambda v: str(v).lower() == "true",
    "hedge_entry_price": float,
    "hedge_trigger_price": float,
    "hedge_stake_usdc": float,
    "strategies_mode": lambda v: str(v).lower() == "true",
    "distance_filter_enabled": lambda v: str(v).lower() == "true",
    "strat_a_enabled": lambda v: str(v).lower() == "true",
    "strat_a_live": lambda v: str(v).lower() == "true",
    "strat_a_counts": _parse_counts,
    "strat_b_enabled": lambda v: str(v).lower() == "true",
    "strat_b_live": lambda v: str(v).lower() == "true",
    "strat_b_pause_min": lambda v: max(1, int(float(v))),
    "strat_b_scope": lambda v: "coin" if str(v).lower() == "coin" else "all",
    "entry_min_minutes_left": lambda v: max(0.0, float(v)),
    "entry_max_spread": lambda v: max(0.0, float(v)),
    "duo_defaults_v3": lambda v: str(v).lower() == "true",
    "strat_a_mode": lambda v: "series" if str(v).lower() == "series" else "recover",
    "coin_banks": lambda v: _parse_floats(v),
    "duo_recover": lambda v: _parse_recover(v),
    "duo_a_remaining": _parse_counts,
    "speed_lab_enabled": lambda v: str(v).lower() == "true",
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    saved = storage.get_all_settings()
    for key, raw in saved.items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass
    _apply_preset_if_new()


# Рекомендованные настройки по итогам анализа отчётов 25-28.09 (5m и 15m BTC).
# Применяются ОДИН РАЗ при первом старте новой версии поверх того, что было
# сохранено в базе из Telegram (иначе старые значения из bot_settings, например
# порог 92, так и остались бы). Дальше можно спокойно менять из Telegram —
# повторно не перезапишутся, пока не поднимем PRESET_VERSION. Кнопка
# "⭐ Рекомендованные" в ⚙️ Настройках применяет их вручную ещё раз.
PRESET_VERSION = 2

# v2 (08.10): окно входа 1.0–4.5 мин вместо 1.0–3.5. История BTC 5m 25.09–06.10
# (2447 рынков): 28 входов в день вместо 23, прибыль на ту же ставку +40%,
# лучше в 9 днях из 11; добавочные входы — 2 проигрыша при 5.2 ожидаемых.
# Проверка на новых данных 06.10–08.10 (7 монет, 3638 рынков, в подборе не
# участвовали): 96 входов вместо 62, 3 проигрыша при 6.5 ожидаемых против
# 4 при 4.2; добавочные 42 входа — 0 проигрышей при 2.7 ожидаемых.
RECOMMENDED_WINDOW = (1.0, 4.5)
_OLD_DEFAULT_WINDOW_MAX = 3.5


def recommended() -> dict:
    return {
        "safety_score_threshold": 88.0,
        "min_entry_price": 0.90,
        "max_entry_price": 0.95,
        "min_distance_pct": settings.MIN_DISTANCE_PCT,
        "entry_window_min": RECOMMENDED_WINDOW[0],
        "entry_window_max": RECOMMENDED_WINDOW[1],
        "fast_entry_enabled": True,
        "entry_confirm_enabled": True,
        "price_source": "chainlink",
        # Хедж вынесен в отдельный бот; здесь он тратил бы тот же кошелёк
        # и в LIVE покупал бы по $5 на каждом рынке, где цена прошла 0.70.
        "hedge_bot_enabled": False,
    }


def apply_recommended() -> dict:
    rec = recommended()
    for key, value in rec.items():
        set(key, value)
    return rec


def _apply_preset_if_new() -> None:
    current = int(_state.get("preset_version") or 0)
    if current >= PRESET_VERSION:
        return
    if current < 1:
        apply_recommended()          # новая база — весь набор
    else:
        # База уже настроена из Telegram — порог, диапазон и прочее не трогаем,
        # меняем только окно входа, и только если оно старое по умолчанию.
        if abs(float(_state.get("entry_window_max") or 0) - _OLD_DEFAULT_WINDOW_MAX) < 1e-9:
            set("entry_window_min", RECOMMENDED_WINDOW[0])
            set("entry_window_max", RECOMMENDED_WINDOW[1])
    set("preset_version", PRESET_VERSION)


def get(key: str):
    return _state[key]


def set(key: str, value) -> None:
    _state[key] = value
    if key == "starting_bankroll_usdc":
        _invalidate_bank_cache()
    # Словари (asset_overrides) храним как JSON, а не как repr Python
    storage.set_setting(key, json.dumps(value, sort_keys=True) if isinstance(value, dict) else value)


def snapshot() -> dict:
    return dict(_state)


# --- Окно входа ---

ENTRY_WINDOW_LIMITS = (0.5, 4.9)   # 5-минутный рынок: не раньше 4.9 и не позже 0.5 мин до конца


def entry_window() -> tuple[float, float]:
    """(минимум, максимум) минут до конца рынка, когда разрешён вход."""
    return float(get("entry_window_min")), float(get("entry_window_max"))


def set_entry_window(lo: float, hi: float) -> tuple[float, float]:
    lo = round(max(ENTRY_WINDOW_LIMITS[0], min(lo, ENTRY_WINDOW_LIMITS[1] - 0.5)), 2)
    hi = round(min(ENTRY_WINDOW_LIMITS[1], max(hi, lo + 0.5)), 2)
    set("entry_window_min", lo)
    set("entry_window_max", hi)
    return lo, hi


# --- Включение/выключение отдельных активов ---

def get_enabled_assets() -> set[str]:
    raw = _state.get("enabled_assets", "") or ""
    return {a for a in raw.split(",") if a}


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def set_enabled_assets(assets: set[str]) -> None:
    set("enabled_assets", ",".join(sorted(assets)))


def toggle_asset(asset: str) -> bool:
    """Переключает состояние актива и возвращает новое (True = включён)."""
    asset = asset.lower()
    enabled = get_enabled_assets()
    if asset in enabled:
        enabled.discard(asset)
    else:
        enabled.add(asset)
    set_enabled_assets(enabled)
    return asset in enabled


# --- Режим каждой монеты: LIVE или DRY ---

def get_live_assets() -> set[str]:
    raw = _state.get("live_assets", "") or ""
    return {a for a in raw.split(",") if a}


def is_asset_live(asset: str) -> bool:
    """Монета помечена для реальной торговли (сама по себе — без учёта
    общего режима бота)."""
    return asset.lower() in get_live_assets()


def set_asset_live(asset: str, live: bool) -> None:
    assets = get_live_assets()
    if live:
        assets.add(asset.lower())
    else:
        assets.discard(asset.lower())
    set("live_assets", ",".join(sorted(assets)))


def trade_is_dry(asset: str) -> bool:
    """Будет ли сделка по монете виртуальной: да, если весь бот в DRY RUN
    или монета не помечена как LIVE."""
    return bool(get("dry_run")) or not is_asset_live(asset)


def asset_mode(asset: str) -> str:
    """'off' — монета выключена, 'live' — реальные сделки прямо сейчас,
    'dry' — виртуальные."""
    if not is_asset_enabled(asset):
        return "off"
    return "dry" if trade_is_dry(asset) else "live"


# --- Размер ставки: fixed или % от текущего банка ---

# (время, значение) последнего расчёта банка — для частых вызовов из
# strategy (каждый тик каждой монеты), чтобы не ходить в базу каждый раз.
_bank_cache: tuple[float, float] | None = None


def _invalidate_bank_cache() -> None:
    global _bank_cache
    _bank_cache = None


def current_bankroll(max_age: float = 0.0) -> float:
    """starting_bankroll_usdc + реализованная прибыль/убыток с начала.
    Считаем ТОЛЬКО реальные (не dry-run) сделки — иначе виртуальный PnL из
    периодов тестового прогона исказил бы размер реальных ставок (баг,
    найденный на реальных отчётах: банк считался завышенным на сумму
    прошлого dry-run PnL). Если сейчас DRY_RUN, наоборот, честнее было бы
    видеть, как рос бы виртуальный банк — но раз sizing реальных денег и
    dry-run использует один и тот же расчёт, отдаём предпочтение
    безопасности реальных ставок.

    Банк один на все монеты (кошелёк один), в том числе для монет со своим
    процентом. max_age > 0 — можно взять значение, посчитанное не раньше
    max_age секунд назад (для частых вызовов из strategy); сделки берут свежее."""
    global _bank_cache
    now = time.monotonic()
    if max_age > 0 and _bank_cache is not None and now - _bank_cache[0] <= max_age:
        return _bank_cache[1]
    pnl = storage.get_pnl_summary(0, live_only=True)["pnl_usdc"]
    value = get("starting_bankroll_usdc") + pnl
    _bank_cache = (now, value)
    return value


def compute_trade_size(asset: str | None = None, max_bank_age: float = 0.0) -> float:
    """Базовый размер ставки ДО масштабирования по score (см.
    executor._scale_trade_size) — либо константа, либо доля от банка.
    asset — монета: если у неё своя ставка (⚙️ в 🪙 Активах), берём её,
    иначе общую."""
    mode, trade_size_usdc, bankroll_pct = sizing(asset)
    if mode == "percent":
        if asset and _state.get("strategies_mode"):
            # Бот двух стратегий: у каждой монеты свой банк (src/duo.py);
            # для оценки берём реальный, если бот в LIVE, иначе тестовый.
            from src import duo   # здесь: duo импортирует runtime_state
            bank = duo.coin_bank(asset, dry=bool(get("dry_run")), max_age=max(max_bank_age, 0.0))
            return round(max(0.0, bank) * bankroll_pct / 100, 2)
        bankroll = max(0.0, current_bankroll(max_age=max_bank_age))
        return round(bankroll * bankroll_pct / 100, 2)
    return trade_size_usdc


# --- Свои настройки монеты: диапазон входа и ставка ---
#
# Монета без своих значений берёт общие (главное меню) и меняется вместе с
# ними. Первое изменение в ⚙️ монеты копирует текущие общие значения и
# дальше живёт отдельно; «↩️ как в общих» возвращает монету к общим.

MIN_ENTRY_FLOOR = 0.50
MIN_TRADE_SIZE_USDC = 1.0
BANKROLL_PCT_MIN = 0.5
BANKROLL_PCT_MAX = 50.0


def max_entry_cap() -> float:
    """Выше этой цены бот не покупает никогда (MAX_ENTRY_EXECUTION_PRICE):
    максимум диапазона выше неё в LIVE давал бы ордера, которые не
    исполнятся, а в DRY — виртуальные сделки по цене, которой в стакане нет."""
    return round(float(settings.MAX_ENTRY_EXECUTION_PRICE), 2)


def clamp_min_entry(value: float, max_entry: float) -> float:
    return round(min(max(value, MIN_ENTRY_FLOOR), max_entry - 0.01), 2)


def clamp_max_entry(value: float, min_entry: float) -> float:
    return round(max(min(value, max_entry_cap()), min_entry + 0.01), 2)


def _overrides() -> dict:
    return _state.get("asset_overrides") or {}


def _update_asset(asset: str, changes: dict | None = None, remove: tuple = ()) -> None:
    """Меняет/удаляет поля своих настроек монеты. Всегда собираем новый
    словарь (а не правим на месте), чтобы не испортить _DEFAULTS."""
    asset = asset.lower()
    data = {a: dict(v) for a, v in _overrides().items()}
    cur = data.get(asset, {})
    for key in remove:
        cur.pop(key, None)
    if changes:
        cur.update(changes)
    if cur:
        data[asset] = cur
    else:
        data.pop(asset, None)
    set("asset_overrides", data)


def has_own_range(asset: str | None) -> bool:
    if not asset:
        return False
    ov = _overrides().get(asset.lower(), {})
    return all(k in ov for k in RANGE_KEYS)


def has_own_sizing(asset: str | None) -> bool:
    if not asset:
        return False
    ov = _overrides().get(asset.lower(), {})
    return all(k in ov for k in SIZING_KEYS)


def assets_with_own_settings() -> list[str]:
    return [a for a in settings.ASSETS if has_own_range(a) or has_own_sizing(a)]


def entry_range(asset: str | None = None) -> tuple[float, float]:
    """(минимум, максимум) цены входа для монеты: свой диапазон, если задан,
    иначе общий. Максимум не выше потолка исполнения (max_entry_cap)."""
    if has_own_range(asset):
        ov = _overrides()[asset.lower()]
        lo, hi = float(ov["min_entry_price"]), float(ov["max_entry_price"])
    else:
        lo, hi = float(get("min_entry_price")), float(get("max_entry_price"))
    return lo, min(hi, max_entry_cap())


def set_asset_range(asset: str, lo: float | None = None, hi: float | None = None) -> tuple[float, float]:
    """Меняет свой диапазон монеты (одну или обе границы). Если своего ещё не
    было — начинает с текущего общего. Возвращает итоговый диапазон."""
    cur_lo, cur_hi = entry_range(asset)
    if lo is not None:
        cur_lo = clamp_min_entry(lo, cur_hi)
    if hi is not None:
        cur_hi = clamp_max_entry(hi, cur_lo)
    _update_asset(asset, {"min_entry_price": cur_lo, "max_entry_price": cur_hi})
    return cur_lo, cur_hi


def set_global_range(lo: float | None = None, hi: float | None = None) -> tuple[float, float]:
    """То же для общего диапазона (главное меню → 📈 Диапазон входа)."""
    cur_lo, cur_hi = entry_range(None)
    if lo is not None:
        cur_lo = clamp_min_entry(lo, cur_hi)
    if hi is not None:
        cur_hi = clamp_max_entry(hi, cur_lo)
    set("min_entry_price", cur_lo)
    set("max_entry_price", cur_hi)
    return cur_lo, cur_hi


def reset_asset_range(asset: str) -> None:
    _update_asset(asset, remove=RANGE_KEYS)


def sizing(asset: str | None = None) -> tuple[str, float, float]:
    """(режим 'fixed'|'percent', фикс. сумма USDC, % банка) для монеты: свои,
    если заданы, иначе общие."""
    if has_own_sizing(asset):
        ov = _overrides()[asset.lower()]
        return ov["sizing_mode"], float(ov["trade_size_usdc"]), float(ov["bankroll_pct"])
    return get("sizing_mode"), float(get("trade_size_usdc")), float(get("bankroll_pct"))


def set_asset_sizing(asset: str, mode: str | None = None, trade_size_usdc: float | None = None,
                     bankroll_pct: float | None = None) -> None:
    """Своя ставка монеты: режим и/или значение. Если своей ещё не было —
    начинаем с текущей общей (режим и оба значения)."""
    cur_mode, cur_usdc, cur_pct = sizing(asset)
    if mode in ("fixed", "percent"):
        cur_mode = mode
    if trade_size_usdc is not None:
        cur_usdc = trade_size_usdc
    if bankroll_pct is not None:
        cur_pct = bankroll_pct
    _update_asset(asset, {
        "sizing_mode": cur_mode,
        "trade_size_usdc": round(max(MIN_TRADE_SIZE_USDC, cur_usdc), 2),
        "bankroll_pct": round(min(BANKROLL_PCT_MAX, max(BANKROLL_PCT_MIN, cur_pct)), 2),
    })


def reset_asset_sizing(asset: str) -> None:
    _update_asset(asset, remove=SIZING_KEYS)


def reset_asset(asset: str) -> None:
    _update_asset(asset, remove=RANGE_KEYS + SIZING_KEYS)


# --- Две стратегии: N входов после проигрыша по монете (стратегия 1️⃣) ---

STRAT_A_MAX_N = 50
STRAT_B_PAUSE_PRESETS = (30, 60, 120)
# Фильтры входа (🎯 → 🧹): 0 — выкл
ENTRY_MIN_LEFT_PRESETS = (0.0, 1.5, 2.0, 2.5, 3.0)
ENTRY_SPREAD_PRESETS = (0.0, 0.01, 0.02)


def strat_a_count(asset: str) -> int:
    """Сколько входов делает стратегия 1️⃣ после проигрыша монеты (0 — по
    монете не входит)."""
    counts = _state.get("strat_a_counts") or {}
    n = counts.get(asset.lower())
    return int(n) if n is not None else max(0, int(settings.STRAT_A_DEFAULT_N))


def set_strat_a_count(asset: str, n: int) -> int:
    n = max(0, min(STRAT_A_MAX_N, int(n)))
    counts = dict(_state.get("strat_a_counts") or {})
    counts[asset.lower()] = n
    set("strat_a_counts", counts)
    return n


# --- Свой банк у каждой монеты (бот двух стратегий) ---

COIN_BANK_PRESETS = (25, 50, 75, 100, 200)


def coin_bank_start(asset: str) -> float:
    banks = _state.get("coin_banks") or {}
    v = banks.get(asset.lower())
    return float(v) if v is not None else max(0.0, float(settings.COIN_BANK_USDC))


def set_coin_bank_start(asset: str, value: float) -> float:
    value = round(max(0.0, float(value)), 2)
    banks = dict(_state.get("coin_banks") or {})
    banks[asset.lower()] = value
    set("coin_banks", banks)
    return value
