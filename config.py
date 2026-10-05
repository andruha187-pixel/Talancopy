"""
Конфигурация копи-бота. Всё — из переменных окружения (Coolify → Environment
Variables), ничего секретного в коде. Настройки, которые удобно крутить на
ходу (размеры, стопы, фильтры по каждому кошельку), живут не здесь, а в БД
и меняются из Telegram — см. src/state.py.
"""
import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    try:
        return float(val) if val not in (None, "") else default
    except ValueError:
        return default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    try:
        return int(val) if val not in (None, "") else default
    except ValueError:
        return default


def _get_list(name: str, default: str) -> list:
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


# Биржевые контракты Polymarket V2 (с 28.04.2026). Сделки любого кошелька
# видны в событиях OrderFilled этих контрактов — это самый быстрый
# публичный способ увидеть чужую сделку (блок Polygon ~2 с).
DEFAULT_EXCHANGES = ",".join([
    "0xE111180000d2663C0091e4f400237545B87B996B",  # CTF Exchange V2
    "0xe2222d279d744050d28e00520010520000310F59",  # Neg Risk CTF Exchange V2 (A)
    "0xe2222d002000ba0053cef3375333610f64600036",  # Neg Risk CTF Exchange V2 (B)
])


@dataclass
class Settings:
    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- Polymarket (нужно только для реальной торговли) ---
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")

    CLOB_HOST: str = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    DATA_API_HOST: str = os.getenv("DATA_API_HOST", "https://data-api.polymarket.com")
    CLOB_WS_URL: str = os.getenv("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")

    # --- Источники обнаружения сделок копируемых кошельков ---
    # 1) Блокчейн Polygon (WebSocket RPC, подписка на логи биржи) — самый
    #    быстрый. Можно перечислить несколько провайдеров через запятую — бот
    #    слушает все сразу и берёт того, кто прислал первым.
    CHAIN_ENABLED: bool = _get_bool("CHAIN_ENABLED", True)
    POLYGON_WSS_URLS: list = field(default_factory=lambda: _get_list(
        "POLYGON_WSS_URLS", "wss://polygon-bor-rpc.publicnode.com"))
    EXCHANGE_ADDRESSES: list = field(default_factory=lambda: [
        a.lower() for a in _get_list("EXCHANGE_ADDRESSES", DEFAULT_EXCHANGES)])
    # 2) Поток активности Polymarket (RTDS, topic "activity") — все сделки
    #    площадки в реальном времени, без ключей.
    RTDS_ENABLED: bool = _get_bool("RTDS_ENABLED", True)
    RTDS_WS_URL: str = os.getenv("RTDS_WS_URL", "wss://ws-live-data.polymarket.com")
    # Исполнения из RTDS (type "trades") как сигнал к копированию. Если в
    # «🩺 Источники» появятся расхождения сторон — выключи (false).
    RTDS_FILLS_TRIGGER: bool = _get_bool("RTDS_FILLS_TRIGGER", True)
    # 3) Опрос Data API v2 (/v2/activity) — страховка, ловит всё, что
    #    пропустили потоки (обрывы связи и т.п.). Медленнее остальных.
    POLL_ENABLED: bool = _get_bool("POLL_ENABLED", True)
    POLL_INTERVAL_SEC: float = _get_float("POLL_INTERVAL_SEC", 2.0)
    DATA_API_MAX_RPS: float = _get_float("DATA_API_MAX_RPS", 6.0)

    # Склейка частичных исполнений одного ордера копируемого (одна
    # транзакция = одна наша сделка). Для потоков, где ордер приходит
    # целиком (тейкер-ордер в блокчейне), ожидания нет вообще.
    AGGREGATE_WINDOW_MS: int = _get_int("AGGREGATE_WINDOW_MS", 200)

    # Сделки копируемого старше этого (сек) не копируем — цена уже другая.
    MAX_SIGNAL_AGE_SEC: float = _get_float("MAX_SIGNAL_AGE_SEC", 60.0)

    # --- Хранилище и отчёты ---
    DB_PATH: str = os.getenv("DB_PATH", "data/copy.db")
    REPORTS_DIR: str = os.getenv("REPORTS_DIR", "data/reports")
    REPORT_INTERVAL_HOURS: float = _get_float("REPORT_INTERVAL_HOURS", 4.0)

    # --- Поиск кошельков ---
    DISCOVERY_RPS: float = _get_float("DISCOVERY_RPS", 3.0)
    # Сколько кошельков анализировать глубоко в быстром / полном поиске.
    DISCOVERY_QUICK_WALLETS: int = _get_int("DISCOVERY_QUICK_WALLETS", 120)
    DISCOVERY_FULL_WALLETS: int = _get_int("DISCOVERY_FULL_WALLETS", 350)
    # Для скольких лучших мерить «проскальзывание при копировании».
    DISCOVERY_DRIFT_WALLETS: int = _get_int("DISCOVERY_DRIFT_WALLETS", 40)
    # Сколько лучших кошельков класть в архив с полными сделками/позициями.
    DISCOVERY_ARCHIVE_DETAIL: int = _get_int("DISCOVERY_ARCHIVE_DETAIL", 40)
    # Перепись активных кошельков из потока RTDS (кандидаты для поиска).
    CENSUS_ENABLED: bool = _get_bool("CENSUS_ENABLED", True)
    CENSUS_MIN_TRADE_USDC: float = _get_float("CENSUS_MIN_TRADE_USDC", 50.0)

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
