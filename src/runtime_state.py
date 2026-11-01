"""
Настройки, которые меняются на лету из Telegram: общий режим DRY RUN / LIVE,
монеты (вкл/выкл, 🧪 тест / 🔴 реально) и правило входа ⏱ (окно, цена,
расстояние до страйка, ATR, ставка, дневной стоп).

Живут в памяти, каждое изменение сразу пишется в SQLite (`bot_settings`) и
переживает рестарт — если база на постоянном томе (/app/data).
"""
from __future__ import annotations
import json

from config import settings
from src import storage


def _coins_csv(raw: str) -> str:
    return ",".join(sorted({a.strip().lower() for a in (raw or "").split(",") if a.strip()}))


def _bool(v) -> bool:
    return str(v).lower() == "true"


# Пределы, чтобы кнопки и ввод вручную не сделали правило бессмысленным
WINDOW_LIMITS = (5, 295)            # окно входа — секунд до конца 5-минутного окна
PRICE_LIMITS = (0.50, 0.99)
STAKE_LIMITS = (1.0, 1000.0)
PCT_LIMITS = (0.1, 100.0)           # ставка монеты — % от банка
BANK_LIMITS = (0.0, 1_000_000.0)    # банк вручную (0 — не задан)


def _stakes_json(v) -> str:
    """Ставки монет: {"btc": {"mode": "usd", "value": 20}, "eth": {"mode": "pct", "value": 5}} — проверенный JSON."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {}
    for asset, cfg in (raw or {}).items():
        mode, value = str(cfg.get("mode")), float(cfg.get("value"))
        if mode == "usd":
            value = min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], value))
        elif mode == "pct":
            value = min(PCT_LIMITS[1], max(PCT_LIMITS[0], value))
        else:
            continue
        out[str(asset).lower()] = {"mode": mode, "value": round(value, 2)}
    return json.dumps(out, sort_keys=True)


def _sec(v) -> int:
    return int(min(WINDOW_LIMITS[1], max(WINDOW_LIMITS[0], int(float(v)))))


def _price(v) -> float:
    return round(min(PRICE_LIMITS[1], max(PRICE_LIMITS[0], float(v))), 2)


_DEFAULTS = {
    "dry_run": settings.DRY_RUN,
    "late_coins_on": _coins_csv(settings.LATE_COINS),
    "late_coins_live": _coins_csv(settings.LATE_LIVE_COINS),
    "late_enabled": settings.LATE_ENABLED,
    "late_from_sec": _sec(max(settings.LATE_FROM_SEC, settings.LATE_TO_SEC)),
    "late_to_sec": _sec(min(settings.LATE_FROM_SEC, settings.LATE_TO_SEC)),
    "late_min_price": _price(min(settings.LATE_MIN_PRICE, settings.LATE_MAX_PRICE)),
    "late_max_price": _price(max(settings.LATE_MIN_PRICE, settings.LATE_MAX_PRICE)),
    "late_min_dist": max(0.0, settings.LATE_MIN_DIST_PCT),
    "late_min_atr": max(0.0, settings.LATE_MIN_ATR),
    "late_stake": min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], settings.LATE_STAKE_USDC)),
    "late_stop": max(0.0, settings.LATE_STOP_USDC),
    # с какого момента действует нынешнее правило (итоги «по нынешнему правилу» — с него)
    "late_rule_since": 0,
    # своя ставка у монеты: фиксированная $ или % от банка (нет записи — общая ставка late_stake)
    "late_coin_stakes": "{}",
    # банк для ставок в % — если баланс кошелька не читается (0 — не задан)
    "late_bank_manual": max(0.0, settings.LATE_BANK_USDC),
}

_CASTERS = {
    "dry_run": _bool,
    "late_coins_on": str,
    "late_coins_live": str,
    "late_enabled": _bool,
    "late_from_sec": _sec,
    "late_to_sec": _sec,
    "late_min_price": _price,
    "late_max_price": _price,
    "late_min_dist": lambda v: max(0.0, float(v)),
    "late_min_atr": lambda v: max(0.0, float(v)),
    "late_stake": lambda v: min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], float(v))),
    "late_stop": lambda v: max(0.0, float(v)),
    "late_rule_since": lambda v: int(float(v)),
    "late_coin_stakes": _stakes_json,
    "late_bank_manual": lambda v: min(BANK_LIMITS[1], max(BANK_LIMITS[0], float(v))),
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    for key, raw in storage.get_all_settings().items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass


def get(key: str):
    return _state.get(key)


def set(key: str, value) -> None:
    _state[key] = value
    storage.set_setting(key, value)


# --- Монеты: включена ли и реальные ли ставки ---

def _csv_set(key: str) -> set[str]:
    return {a for a in str(_state.get(key) or "").split(",") if a}


def get_enabled_assets() -> set[str]:
    return _csv_set("late_coins_on")


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def toggle_asset(asset: str) -> bool:
    """Включить/выключить монету; возвращает новое состояние (True — включена)."""
    asset = asset.lower()
    on = get_enabled_assets()
    on.symmetric_difference_update({asset})
    set("late_coins_on", ",".join(sorted(on)))
    return asset in on


def get_live_assets() -> set[str]:
    return _csv_set("late_coins_live")


def is_asset_live(asset: str) -> bool:
    return asset.lower() in get_live_assets()


def set_asset_live(asset: str, live: bool) -> None:
    coins = get_live_assets()
    if live:
        coins.add(asset.lower())
    else:
        coins.discard(asset.lower())
    set("late_coins_live", ",".join(sorted(coins)))


# --- Своя ставка монеты: ("usd", $) или ("pct", % банка); None — общая ставка ---

def coin_stakes() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_stakes") or "{}"))
    except ValueError:
        return {}


def coin_stake(asset: str) -> tuple[str, float] | None:
    cfg = coin_stakes().get(asset.lower())
    return (cfg["mode"], float(cfg["value"])) if cfg else None


def set_coin_stake(asset: str, mode: str | None, value: float | None = None) -> None:
    """mode "usd" / "pct" — своя ставка; None — снова общая."""
    stakes = coin_stakes()
    if mode is None:
        stakes.pop(asset.lower(), None)
    else:
        stakes[asset.lower()] = {"mode": mode, "value": float(value)}
    set("late_coin_stakes", _stakes_json(stakes))
