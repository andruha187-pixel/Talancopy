"""
SQLite-хранилище: окна 5m-рынков с путём цены (late_windows), ставки (late_trades)
и настройки из Telegram (bot_settings). Внешней БД не нужно.
"""
from __future__ import annotations
import os
import sqlite3
import time
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

-- 🔁 Окно 5m-рынка (по всем монетам, даже выключенным): страйк (цена Chainlink на
-- старте), путь [[секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR], …] и исход:
-- ранний (стакан в последние секунды / Chainlink) и официальный.
CREATE TABLE IF NOT EXISTS late_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    start_ts INTEGER NOT NULL,
    slug TEXT,
    strike REAL,
    path TEXT,
    cl_end REAL,
    early_outcome TEXT,
    outcome TEXT,
    UNIQUE(asset, start_ts)
);
CREATE INDEX IF NOT EXISTS idx_late_windows_start ON late_windows(start_ts);

-- ⏱ Ставки: тестовые (dry_run=1) и реальные (dry_run=0) — по одной на окно. Реальная, которая так и
-- не исполнилась за окно входа, — строка со status SKIP_NOFILL и причиной в note (в итоги не идёт).
CREATE TABLE IF NOT EXISTS late_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    asset TEXT,
    start_ts INTEGER,
    slug TEXT,
    side TEXT,
    token_id TEXT,
    t_left REAL,
    price_now REAL,
    strike REAL,
    dist_pct REAL,
    dist_atr REAL,
    atr REAL,
    ask REAL,
    price REAL,
    size_usdc REAL,
    shares REAL,
    fee_usdc REAL,
    dry_run INTEGER,
    status TEXT,
    order_id TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    settled_ts INTEGER,
    rule TEXT,
    note TEXT,
    UNIQUE(asset, start_ts, dry_run)
);
CREATE INDEX IF NOT EXISTS idx_late_trades_ts ON late_trades(ts);
"""


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# True, если при этом запуске файла базы ещё не было (создана с нуля). При
# каждом деплое без постоянного тома на /app/data так и будет.
DB_WAS_NEW = False


def init_db():
    global DB_WAS_NEW
    DB_WAS_NEW = not os.path.exists(settings.DB_PATH)
    with _conn() as conn:
        conn.executescript(_SCHEMA)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(late_trades)").fetchall()}
        if "note" not in cols:                                    # база прошлой версии
            conn.execute("ALTER TABLE late_trades ADD COLUMN note TEXT")


def get_all_settings() -> dict[str, str]:
    with _conn() as conn:
        return {k: v for k, v in conn.execute("SELECT key, value FROM bot_settings").fetchall()}


def get_setting(key: str) -> str | None:
    with _conn() as conn:
        row = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_setting(key: str, value) -> None:
    with _conn() as conn:
        conn.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?) "
                     "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, str(value)))


# --------------------------------------------------------------- окна ----

WINDOW_COLUMNS = ["id", "asset", "start_ts", "slug", "strike", "path", "cl_end", "early_outcome", "outcome"]
_WINDOW_FIELDS = set(WINDOW_COLUMNS) - {"id", "asset", "start_ts"}


def window_upsert(asset: str, start_ts: int, **fields) -> None:
    bad = set(fields) - _WINDOW_FIELDS
    if bad:
        raise ValueError(f"неизвестные поля: {bad}")
    with _conn() as conn:
        conn.execute("INSERT OR IGNORE INTO late_windows (asset, start_ts) VALUES (?, ?)", (asset, int(start_ts)))
        if fields:
            cols = sorted(fields)
            conn.execute(f"UPDATE late_windows SET {', '.join(f'{c} = ?' for c in cols)} "
                         "WHERE asset = ? AND start_ts = ?", [fields[c] for c in cols] + [asset, int(start_ts)])


def window_rows(since_ts: int = 0, with_path: bool = False) -> list[dict]:
    cols = [c for c in WINDOW_COLUMNS if with_path or c != "path"]
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(cols)} FROM late_windows WHERE start_ts >= ? "
                            "ORDER BY asset, start_ts", (since_ts,)).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def window_outcome(asset: str, start_ts: int) -> str | None:
    with _conn() as conn:
        row = conn.execute("SELECT outcome FROM late_windows WHERE asset = ? AND start_ts = ?",
                           (asset, int(start_ts))).fetchone()
    return row[0] if row and row[0] else None


def windows_unresolved(ended_before: int, since_ts: int, limit: int = 30) -> list[tuple[str, int, str]]:
    """Окна без официального исхода, закончившиеся раньше ended_before: (монета, старт, слаг)."""
    with _conn() as conn:
        return conn.execute(
            "SELECT asset, start_ts, slug FROM late_windows WHERE outcome IS NULL AND slug IS NOT NULL "
            "AND start_ts >= ? AND start_ts + 300 < ? ORDER BY start_ts DESC LIMIT ?",
            (since_ts, ended_before, limit)).fetchall()


def windows_trim(before_ts: int) -> int:
    """Стереть путь у окон старше before_ts (страйк и исходы остаются)."""
    with _conn() as conn:
        return conn.execute("UPDATE late_windows SET path = NULL WHERE start_ts < ? AND path IS NOT NULL",
                            (int(before_ts),)).rowcount


def windows_export(since_ts: int, until_ts: int | None = None) -> list[tuple]:
    """Окна с since_ts (и до until_ts, не включая) — для отчётов."""
    q = f"SELECT {', '.join(WINDOW_COLUMNS)} FROM late_windows WHERE start_ts >= ?"
    args: list = [int(since_ts)]
    if until_ts is not None:
        q += " AND start_ts < ?"
        args.append(int(until_ts))
    with _conn() as conn:
        return conn.execute(q + " ORDER BY start_ts, asset", args).fetchall()


def windows_first_ts() -> int | None:
    with _conn() as conn:
        row = conn.execute("SELECT MIN(start_ts) FROM late_windows").fetchone()
    return int(row[0]) if row and row[0] is not None else None


# ------------------------------------------------------------- ставки ----

TRADE_COLUMNS = [
    "id", "ts", "asset", "start_ts", "slug", "side", "token_id", "t_left", "price_now", "strike", "dist_pct",
    "dist_atr", "atr", "ask", "price", "size_usdc", "shares", "fee_usdc", "dry_run", "status", "order_id",
    "outcome", "pnl_usdc", "settled_ts", "rule", "note",
]


def trade_log(**f) -> int | None:
    """Записать ставку. None — по этому окну такая ставка уже есть."""
    cols = [c for c in TRADE_COLUMNS if c in f and c != "id"]
    with _conn() as conn:
        cur = conn.execute(f"INSERT OR IGNORE INTO late_trades ({', '.join(cols)}) "
                           f"VALUES ({', '.join('?' for _ in cols)})", [f[c] for c in cols])
        return cur.lastrowid if cur.rowcount else None


def trade_exists(asset: str, start_ts: int, dry_run: bool | None = None) -> bool:
    q = "SELECT 1 FROM late_trades WHERE asset = ? AND start_ts = ?"
    args: list = [asset, int(start_ts)]
    if dry_run is not None:
        q += " AND dry_run = ?"
        args.append(int(bool(dry_run)))
    with _conn() as conn:
        return conn.execute(q + " LIMIT 1", args).fetchone() is not None


def trades_open(ended_before: int) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(TRADE_COLUMNS)} FROM late_trades WHERE outcome IS NULL "
                            "AND start_ts + 300 < ? ORDER BY start_ts", (ended_before,)).fetchall()
    return [dict(zip(TRADE_COLUMNS, r)) for r in rows]


def trade_settle(trade_id: int, outcome: str, pnl_usdc: float) -> bool:
    with _conn() as conn:
        cur = conn.execute("UPDATE late_trades SET outcome = ?, pnl_usdc = ?, settled_ts = ? "
                           "WHERE id = ? AND outcome IS NULL", (outcome, pnl_usdc, int(time.time()), trade_id))
        return cur.rowcount > 0


def trades_since(since_ts: int) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(f"SELECT {', '.join(TRADE_COLUMNS)} FROM late_trades WHERE ts >= ? ORDER BY ts",
                            (since_ts,)).fetchall()
    return [dict(zip(TRADE_COLUMNS, r)) for r in rows]


def trades_export(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        return conn.execute(f"SELECT {', '.join(TRADE_COLUMNS)} FROM late_trades WHERE ts >= ? ORDER BY ts",
                            (since_ts,)).fetchall()


def coin_bank_flow(asset: str, since_ts: int) -> tuple[float, float]:
    """Для своего банка монеты: (закрытый результат её реальных ставок с since_ts, сколько сейчас стоит в
    открытых реальных ставках вместе с комиссией). Тестовые и неисполненные (SKIP_NOFILL, $0) не считаются."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(CASE WHEN outcome IS NOT NULL THEN COALESCE(pnl_usdc, 0) END), 0), "
            "COALESCE(SUM(CASE WHEN outcome IS NULL THEN COALESCE(size_usdc, 0) + COALESCE(fee_usdc, 0) END), 0) "
            "FROM late_trades WHERE asset = ? AND dry_run = 0 AND ts >= ? AND COALESCE(size_usdc, 0) > 0",
            (asset.lower(), int(since_ts))).fetchone()
    return float(row[0] or 0.0), float(row[1] or 0.0)
