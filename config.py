"""
Конфигурация бота. Все настройки — из переменных окружения (.env), см.
.env.example. Ключи не хардкодим.

Бот один: ⏱ поздний вход на 5-минутных рынках Up/Down Polymarket — за ~3 минуты
до конца окна покупаем сторону, которая уже далеко ушла от страйка (src/late_bot.py).
Почти всё ниже — только стартовые значения: дальше настройки меняются кнопками в
Telegram и хранятся в базе.
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _coins(raw: str) -> list:
    return [a.strip().lower() for a in (raw or "").split(",") if a.strip()]


@dataclass
class Settings:
    # --- Монеты ---
    # ASSETS — какие 5m-рынки бот пишет (все, даже выключенные — для подбора по архивам).
    # LATE_COINS — по каким ставит стратегия при старте (✅); LATE_LIVE_COINS — какие
    # сразу 🔴 (реальные ставки, когда бот в 🔴 LIVE; по умолчанию ни одной).
    # По умолчанию ✅ — монеты, у которых правило «3:00–3:10» было лучшим на
    # данных 25.09–20.10: BTC, ETH, XRP, DOGE, BNB. SOL — нет (на последних днях
    # 38 из 41), HYPE — давно нет данных.
    ASSETS: list = field(default_factory=lambda: _coins(os.getenv("ASSETS", "btc,eth,sol,xrp,bnb,doge,hype")))
    LATE_COINS: str = os.getenv("LATE_COINS", "btc,eth,xrp,doge,bnb")
    LATE_LIVE_COINS: str = os.getenv("LATE_LIVE_COINS", "")

    # --- Режим ---
    # true — ни одного реального ордера (общий выключатель, 🔴 Включить LIVE в меню)
    DRY_RUN: bool = _get_bool("DRY_RUN", True)

    # --- ⏱ Правило входа (всё — кнопками в ⚙️ Настройках) ---
    LATE_ENABLED: bool = _get_bool("LATE_ENABLED", True)
    # Окно входа — секунд до конца окна: с LATE_FROM_SEC до LATE_TO_SEC (3:10 → 3:00)
    LATE_FROM_SEC: int = _get_int("LATE_FROM_SEC", 190)
    LATE_TO_SEC: int = _get_int("LATE_TO_SEC", 180)
    # Цена стороны-лидера (ask)
    LATE_MIN_PRICE: float = _get_float("LATE_MIN_PRICE", 0.92)
    LATE_MAX_PRICE: float = _get_float("LATE_MAX_PRICE", 0.95)
    # Цена Chainlink ушла от страйка не меньше чем на столько процентов…
    LATE_MIN_DIST_PCT: float = _get_float("LATE_MIN_DIST_PCT", 0.10)
    # …и не меньше чем на столько ATR (0 — без условия по ATR)
    LATE_MIN_ATR: float = _get_float("LATE_MIN_ATR", 2.0)
    # Сумма входа, USDC (но не меньше 5 акций — меньший ордер биржа не берёт)
    LATE_STAKE_USDC: float = _get_float("LATE_STAKE_USDC", 5.0)
    # Дневной стоп реальных ставок, USDC (UTC-сутки; 0 — без стопа)
    LATE_STOP_USDC: float = _get_float("LATE_STOP_USDC", 30.0)
    # Банк для ставок «% от банка» (у монеты в 🪙), если баланс кошелька прочитать нельзя
    # (нет ключа или SDK не отдаёт баланс). 0 — не задан: тогда такие монеты ставят общую сумму.
    LATE_BANK_USDC: float = _get_float("LATE_BANK_USDC", 0.0)

    # --- ATR: Wilder по 1-минутным свечам Binance (как в прежних 5m-ботах) ---
    ATR_PERIOD: int = _get_int("ATR_PERIOD", 7)
    ATR_REFRESH_SEC: float = _get_float("ATR_REFRESH_SEC", 10.0)
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
    BINANCE_FUTURES_URL: str = os.getenv("BINANCE_FUTURES_URL", "https://fapi.binance.com")

    # --- Цена Chainlink (по ней Polymarket решает исход): страйк и текущая цена ---
    CHAINLINK_MAX_AGE_SEC: float = _get_float("CHAINLINK_MAX_AGE_SEC", 5.0)
    CHAINLINK_USE_POLYBOLT: bool = _get_bool("CHAINLINK_USE_POLYBOLT", True)
    CHAINLINK_POLYBOLT_URL: str = os.getenv("CHAINLINK_POLYBOLT_URL", "wss://ws-live-v2.polymarket.com/ws")
    CHAINLINK_USE_RTDS: bool = _get_bool("CHAINLINK_USE_RTDS", True)
    CHAINLINK_RTDS_URL: str = os.getenv("CHAINLINK_RTDS_URL", "wss://ws-live-data.polymarket.com")
    POLY_API_KEY: str = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET: str = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE: str = os.getenv("POLY_API_PASSPHRASE", "")

    # --- Polymarket / CLOB ---
    POLY_HOST: str = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    POLY_CHAIN_ID: int = _get_int("POLY_CHAIN_ID", 137)
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")
    USE_LIVE_BOOK_STREAM: bool = _get_bool("USE_LIVE_BOOK_STREAM", True)
    FEE_RATE: float = _get_float("FEE_RATE", 0.07)

    # --- 🔁 Запись окон (все монеты): путь цены для подбора времени и монет по архивам ---
    # с REC_FROM_SEC до REC_TO_SEC секунд до конца, каждые REC_STEP_SEC секунд
    REC_FROM_SEC: int = _get_int("REC_FROM_SEC", 270)
    REC_TO_SEC: int = _get_int("REC_TO_SEC", 5)
    REC_STEP_SEC: float = _get_float("REC_STEP_SEC", 2.0)
    REC_KEEP_DAYS: int = _get_int("REC_KEEP_DAYS", 14)

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- База и отчёты ---
    DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")
    REPORT_INTERVAL_HOURS: float = _get_float("REPORT_INTERVAL_HOURS", 4.0)
    REPORTS_DIR: str = os.getenv("REPORTS_DIR", "data/reports")


settings = Settings()
