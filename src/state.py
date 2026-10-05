"""
Настройки, которые меняются из Telegram на ходу, и реестр копируемых
кошельков. Всё держится в памяти (горячий путь не ходит в БД) и
сохраняется в SQLite при каждом изменении — переживает рестарт/деплой.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, fields

from src import db
from src.util import norm_addr

log = logging.getLogger("state")

# ------------------------------------------------------------------ общие ----

DEFAULTS: dict = {
    "paused": False,              # пауза новых входов (выходы работают всегда)
    "live_enabled": False,        # общий выключатель реальной торговли
    "notify_paper": True,         # уведомлять о виртуальных сделках
    "notify_skips": False,        # уведомлять о пропусках (шумно)
    "daily_loss_limit_live": 0.0,  # общий дневной стоп по реальным, $ (0 = выкл)
    "max_live_exposure": 0.0,     # потолок суммы открытых реальных позиций, $ (0 = выкл)
    "exit_slip_cents": 5.0,       # допустимое проскальзывание на выходе, центов
    "auto_claim": True,           # забирать выигрыш после резолюции автоматически
    "discovery_auto_hours": 0.0,  # авто-поиск кошельков раз в N часов (0 = выкл)
    "last_discovery_ts": 0.0,
    "last_report_ts": 0,
}

_settings: dict = dict(DEFAULTS)


def get(key: str):
    return _settings.get(key, DEFAULTS.get(key))


def set(key: str, value) -> None:  # noqa: A001 — имя как в остальных ботах
    _settings[key] = value
    db.upsert_row("settings", {"key": key, "value": json.dumps(value)})


# --------------------------------------------------------------- кошельки ----

@dataclass
class WalletCfg:
    id: int
    address: str
    label: str = ""
    mode: str = "paper"              # off | paper | live
    # Размер: процент от суммы входа копируемого или фиксированная сумма
    size_mode: str = "pct"           # pct | fixed
    size_pct: float = 10.0
    size_fixed: float = 5.0
    max_usdc: float = 25.0           # потолок одной нашей сделки, $
    min_target_usdc: float = 10.0    # их сделки меньше этого — шум, не копируем
    accumulate_small: bool = True    # копить доли < $1, пока не наберётся мин. ордер
    # Исполнение
    max_slip_cents: float = 2.0      # не дороже их цены больше чем на N центов…
    max_slip_pct: float = 10.0       # …и больше чем на N % (важно для дешёвых исходов)
    min_price: float = 0.03          # копируем входы только в этом диапазоне цен
    max_price: float = 0.97
    # Выходы
    copy_sells: bool = True          # продают они — продаём ту же долю
    copy_adds: bool = True           # копировать докупки в уже открытую позицию
    sl_pct: float = 0.0              # стоп-лосс, % от нашей цены входа (0 = выкл)
    tp_pct: float = 0.0              # тейк-профит, % (0 = выкл)
    trail_pct: float = 0.0           # трейлинг-стоп от максимума, % (0 = выкл)
    max_hold_h: float = 0.0          # закрыть через N часов (0 = выкл)
    # Риск
    max_position_usdc: float = 100.0  # потолок вложений в один рынок, $
    max_open: int = 20               # максимум открытых позиций по кошельку
    daily_loss_limit: float = 0.0    # дневной стоп по кошельку (реальные), $
    # Фильтры рынков
    skip_fast: bool = True           # не копировать 5м/15м/часовые крипто-рынки
    min_minutes_left: float = 30.0   # и рынки, которым до конца меньше N минут
    skip_live_sports: bool = False   # не копировать входы в уже идущий матч
    copy_maker_fills: bool = True    # копировать исполнения их лимиток (мейкер)
    notify: bool = True
    created_at: int = 0

    def short(self) -> str:
        return self.label or f"{self.address[:6]}…{self.address[-4:]}"

    def to_json(self) -> str:
        d = asdict(self)
        for k in ("id", "address", "label", "created_at"):
            d.pop(k, None)
        return json.dumps(d)

    @classmethod
    def from_row(cls, row: dict) -> "WalletCfg":
        cfg = {}
        try:
            cfg = json.loads(row.get("cfg") or "{}")
        except ValueError:
            pass
        known = {f.name for f in fields(cls)}
        clean = {k: v for k, v in cfg.items() if k in known}
        return cls(id=int(row["id"]), address=row["address"], label=row.get("label") or "",
                   created_at=int(row.get("created_at") or 0), **clean)


_wallets: dict[int, WalletCfg] = {}
_by_addr: dict[str, WalletCfg] = {}
_listeners: list = []
_active: frozenset = frozenset()


def _recompute_active() -> None:
    global _active
    _active = frozenset(w.address for w in _wallets.values() if w.mode in ("paper", "live"))


def on_wallets_changed(cb) -> None:
    """cb() вызывается после добавления/удаления/смены режима — источники
    сделок перестраивают подписки."""
    _listeners.append(cb)


def _fire() -> None:
    _recompute_active()
    for cb in list(_listeners):
        try:
            cb()
        except Exception as exc:  # noqa: BLE001
            log.warning("listener error: %s", exc)


def load() -> None:
    global _settings
    _settings = dict(DEFAULTS)
    for row in db.query("SELECT key, value FROM settings"):
        try:
            _settings[row["key"]] = json.loads(row["value"])
        except (TypeError, ValueError):
            pass
    _wallets.clear()
    _by_addr.clear()
    for row in db.query("SELECT * FROM wallets WHERE deleted = 0 ORDER BY id"):
        w = WalletCfg.from_row(row)
        _wallets[w.id] = w
        _by_addr[w.address] = w
    _recompute_active()


def wallets() -> list[WalletCfg]:
    return list(_wallets.values())


def wallet(wid: int) -> WalletCfg | None:
    return _wallets.get(int(wid))


def by_address(addr: str) -> WalletCfg | None:
    return _by_addr.get((addr or "").lower())


def active_addresses() -> frozenset:
    """Кошельки, за которыми реально следим (режим не «выкл»). Кэшируется —
    вызывается на каждое сообщение потока RTDS."""
    return _active


def add_wallet(address: str, label: str = "", mode: str = "paper", **overrides) -> WalletCfg:
    addr = norm_addr(address)
    if not addr:
        raise ValueError("Это не адрес кошелька (нужно 0x и 40 символов)")
    existing = db.query_one("SELECT * FROM wallets WHERE address = ?", (addr,))
    now = int(time.time())
    if existing:
        w = WalletCfg.from_row(existing)
        w.mode = mode
        if label:
            w.label = label
        for k, v in overrides.items():
            if hasattr(w, k):
                setattr(w, k, v)
        db.execute_sync("UPDATE wallets SET deleted = 0, label = ?, cfg = ? WHERE id = ?",
                        (w.label, w.to_json(), w.id))
    else:
        tmp = WalletCfg(id=0, address=addr, label=label, mode=mode, created_at=now)
        for k, v in overrides.items():
            if hasattr(tmp, k):
                setattr(tmp, k, v)
        wid = db.execute_sync(
            "INSERT INTO wallets (address, label, cfg, created_at, deleted) VALUES (?, ?, ?, ?, 0)",
            (addr, label, tmp.to_json(), now))
        tmp.id = int(wid)
        w = tmp
    _wallets[w.id] = w
    _by_addr[w.address] = w
    _fire()
    return w


def update_wallet(wid: int, **changes) -> WalletCfg | None:
    w = _wallets.get(int(wid))
    if not w:
        return None
    mode_changed = "mode" in changes and changes["mode"] != w.mode
    for k, v in changes.items():
        if hasattr(w, k) and k not in ("id", "address"):
            setattr(w, k, v)
    db.write("UPDATE wallets SET label = ?, cfg = ? WHERE id = ?", (w.label, w.to_json(), w.id))
    if mode_changed:
        _fire()
    return w


def remove_wallet(wid: int) -> None:
    w = _wallets.pop(int(wid), None)
    if w:
        _by_addr.pop(w.address, None)
        db.write("UPDATE wallets SET deleted = 1 WHERE id = ?", (w.id,))
        _fire()
