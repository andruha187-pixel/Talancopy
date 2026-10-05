"""
SQLite: схема + фоновый поток записи.

Почему отдельный поток: sqlite3 синхронный, и запись с fsync на медленном
диске сервера занимает 5-30 мс. Если писать прямо из горячего цикла
копирования, каждая такая пауза — это задержка нашей сделки. Поэтому всё,
что пишется во время работы, кладётся в очередь, а отдельный поток пачками
сбрасывает её на диск. Чтение (статистика, отчёты) идёт через
asyncio.to_thread с собственным соединением — WAL позволяет читать
параллельно с записью.
"""
from __future__ import annotations

import logging
import os
import queue
import sqlite3
import threading
import time

from config import settings

log = logging.getLogger("db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS wallets (
    id INTEGER PRIMARY KEY,
    address TEXT UNIQUE,
    label TEXT,
    cfg TEXT,
    created_at INTEGER,
    deleted INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS target_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet_id INTEGER,
    wallet TEXT,
    tx_hash TEXT,
    token_id TEXT,
    side TEXT,
    shares REAL,
    usdc REAL,
    price REAL,
    trade_ts REAL,
    first_source TEXT,
    first_ms INTEGER,
    chain_ms INTEGER,
    rtds_ms INTEGER,
    poll_ms INTEGER,
    role TEXT,
    condition_id TEXT,
    slug TEXT,
    title TEXT,
    outcome TEXT,
    UNIQUE(wallet, tx_hash, token_id, side)
);
CREATE TABLE IF NOT EXISTS copies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER,
    wallet_id INTEGER,
    mode TEXT,
    action TEXT,
    reason TEXT,
    status TEXT,
    skip_reason TEXT,
    token_id TEXT,
    condition_id TEXT,
    slug TEXT,
    title TEXT,
    outcome TEXT,
    target_price REAL,
    target_usdc REAL,
    target_tx TEXT,
    detect_source TEXT,
    detect_latency_ms INTEGER,
    req_usdc REAL,
    req_shares REAL,
    price_cap REAL,
    best_quote REAL,
    fill_shares REAL,
    fill_usdc REAL,
    fill_price REAL,
    fee_usdc REAL,
    slippage_cents REAL,
    exec_ms INTEGER,
    order_id TEXT,
    position_id INTEGER,
    pnl_usdc REAL,
    err TEXT
);
CREATE INDEX IF NOT EXISTS idx_copies_wallet ON copies(wallet_id, mode, ts);
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY,
    wallet_id INTEGER,
    mode TEXT,
    token_id TEXT,
    condition_id TEXT,
    slug TEXT,
    title TEXT,
    outcome TEXT,
    end_ts REAL,
    shares REAL,
    cost_usdc REAL,
    fees_usdc REAL,
    proceeds_usdc REAL,
    realized_pnl REAL,
    opened_ts INTEGER,
    closed_ts INTEGER,
    status TEXT,
    close_reason TEXT,
    target_avg_price REAL,
    peak_bid REAL,
    last_bid REAL,
    entries INTEGER,
    redeemed INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_positions_wallet ON positions(wallet_id, mode, status);
CREATE TABLE IF NOT EXISTS census (
    wallet TEXT PRIMARY KEY,
    name TEXT,
    first_ts INTEGER,
    last_ts INTEGER,
    n_trades INTEGER,
    usdc REAL,
    big_trades INTEGER
);
CREATE TABLE IF NOT EXISTS discovery_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started INTEGER,
    finished INTEGER,
    mode TEXT,
    n_candidates INTEGER,
    n_analyzed INTEGER,
    archive_path TEXT,
    top_json TEXT
);
"""

_write_q: "queue.Queue[tuple]" = queue.Queue()
_writer_thread: threading.Thread | None = None
_stop = threading.Event()


def _connect() -> sqlite3.Connection:
    d = os.path.dirname(settings.DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = _connect()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def _writer_loop() -> None:
    conn = _connect()
    while not _stop.is_set() or not _write_q.empty():
        try:
            first = _write_q.get(timeout=0.5)
        except queue.Empty:
            continue
        batch = [first]
        # Пачка: всё, что успело накопиться, — одним коммитом.
        while len(batch) < 500:
            try:
                batch.append(_write_q.get_nowait())
            except queue.Empty:
                break
        try:
            for sql, params in batch:
                conn.execute(sql, params)
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("Ошибка записи в БД (%s) — пробую по одному", exc)
            conn.rollback()
            for sql, params in batch:
                try:
                    conn.execute(sql, params)
                    conn.commit()
                except Exception as exc2:  # noqa: BLE001
                    log.error("Не записалось: %s | %s | %s", exc2, sql[:80], str(params)[:200])
                    conn.rollback()
    conn.close()


def start_writer() -> None:
    global _writer_thread
    if _writer_thread and _writer_thread.is_alive():
        return
    _stop.clear()
    _writer_thread = threading.Thread(target=_writer_loop, name="db-writer", daemon=True)
    _writer_thread.start()


def stop_writer(timeout: float = 5.0) -> None:
    _stop.set()
    if _writer_thread:
        _writer_thread.join(timeout=timeout)


def flush(timeout: float = 5.0) -> None:
    """Дождаться, пока очередь записи опустеет (для отчётов и тестов)."""
    deadline = time.time() + timeout
    while not _write_q.empty() and time.time() < deadline:
        time.sleep(0.02)
    time.sleep(0.05)


def write(sql: str, params: tuple | list = ()) -> None:
    """Асинхронная (неблокирующая) запись — для горячего пути."""
    if _writer_thread is None or not _writer_thread.is_alive():
        # Писатель не запущен (тесты/старт) — пишем синхронно.
        conn = _connect()
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
        return
    _write_q.put((sql, tuple(params)))


def insert_row(table: str, row: dict) -> None:
    cols = ",".join(row.keys())
    marks = ",".join("?" for _ in row)
    write(f"INSERT INTO {table} ({cols}) VALUES ({marks})", list(row.values()))


def upsert_row(table: str, row: dict) -> None:
    cols = ",".join(row.keys())
    marks = ",".join("?" for _ in row)
    write(f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({marks})", list(row.values()))


# --- Синхронное чтение (вызывать через asyncio.to_thread в рабочем цикле) ---

def query(sql: str, params: tuple | list = ()) -> list[dict]:
    conn = _connect()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def query_one(sql: str, params: tuple | list = ()) -> dict | None:
    rows = query(sql, params)
    return rows[0] if rows else None


def execute_sync(sql: str, params: tuple | list = ()) -> int:
    """Синхронная запись с возвратом lastrowid — для редких операций
    (добавление кошелька и т.п.), где id нужен сразу."""
    conn = _connect()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()
