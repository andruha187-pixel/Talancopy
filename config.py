"""
Централизованная конфигурация бота. Все настройки берутся из переменных
окружения (.env), см. .env.example. Ничего не хардкодим — особенно ключи.
"""
import json
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


@dataclass
class Settings:
    # --- Активы ---
    # Список монет через запятую — на каждую заводится независимый поток по
    # каждому таймфрейму (см. src/timeframes.py). HYPE убран: в стакане по цене
    # 0.90–0.94 у него обычно ~$100 (у BTC ~$3650, ETH ~$1220, SOL ~$900), в
    # половине случаев меньше. BNB и DOGE есть, но по умолчанию выключены
    # (см. ENABLED_ASSETS) — включаются кнопкой в 🪙 Активах.
    ASSETS: list = field(default_factory=lambda: [
        a.strip().lower() for a in os.getenv("ASSETS", "btc,eth,sol,xrp,bnb,doge").split(",") if a.strip()
    ])

    # --- Binance (источник цены/индикаторов) ---
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
    # Фьючерсный хост — фолбэк для монет без спотовой пары на Binance (HYPE
    # на споте под вопросом на момент написания; на фьючерсах есть точно).
    BINANCE_FUTURES_URL: str = os.getenv("BINANCE_FUTURES_URL", "https://fapi.binance.com")
    # Цена потоком (WebSocket, сделки aggTrade) — для быстрой проверки сигнала,
    # см. src/price_stream.py. Фьючерсные потоки Binance с 23.04.2026 живут
    # по адресу /market (старый wss://fstream.binance.com/stream отключён).
    BINANCE_WS_URL: str = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:443")
    BINANCE_FUTURES_WS_URL: str = os.getenv("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/market")
    ATR_LOOKBACK_FOR_REGIME: int = _get_int("ATR_LOOKBACK_FOR_REGIME", 60)

    # --- Быстрая проверка сигнала (src/fast_signal.py) ---
    # Кроме обычной проверки раз в 3 с (REST-свечи Binance + стакан) бот раз в
    # FAST_LOOP_INTERVAL_SEC проверяет сигнал по данным в памяти: цена Binance
    # из потока, живой стакан Polymarket, свечи из последнего запроса. По
    # отчётам 25.09–08.10 у 32% рынков с сигналом условия входа держались не
    # дольше одной 3-секундной проверки. Включается/выключается и в Telegram
    # (⚙️ Настройки → ⚡ Быстрый вход).
    FAST_ENTRY_ENABLED: bool = _get_bool("FAST_ENTRY_ENABLED", True)
    FAST_LOOP_INTERVAL_SEC: float = _get_float("FAST_LOOP_INTERVAL_SEC", 0.3)

    # --- Цена Chainlink (src/chainlink_feed.py) ---
    # По какой цене считать сигнал: "chainlink" — как Polymarket при расчёте
    # исхода (цена и страйк Chainlink, Binance только для свечей), "binance" —
    # как раньше. Переключается и в Telegram (⚙️ Настройки → 📡 Цена сигнала).
    PRICE_SOURCE: str = os.getenv("PRICE_SOURCE", "chainlink").strip().lower()
    # Цена Chainlink старше этого — сигнала нет (вход запрещён)
    CHAINLINK_MAX_AGE_SEC: float = _get_float("CHAINLINK_MAX_AGE_SEC", 5.0)
    # Новый поток цен Polymarket (PolyBolt, нужны API-ключи CLOB) и старый
    # RTDS (без ключей, объявлен устаревшим). Работают оба, цены сливаются.
    CHAINLINK_USE_POLYBOLT: bool = _get_bool("CHAINLINK_USE_POLYBOLT", True)
    CHAINLINK_POLYBOLT_URL: str = os.getenv("CHAINLINK_POLYBOLT_URL", "wss://ws-live-v2.polymarket.com/ws")
    CHAINLINK_USE_RTDS: bool = _get_bool("CHAINLINK_USE_RTDS", True)
    CHAINLINK_RTDS_URL: str = os.getenv("CHAINLINK_RTDS_URL", "wss://ws-live-data.polymarket.com")
    # API-ключи CLOB для PolyBolt. Не заданы — бот берёт их из SDK-клиента или
    # выводит из POLY_PRIVATE_KEY (только чтение существующих).
    POLY_API_KEY: str = os.getenv("POLY_API_KEY", "")
    POLY_API_SECRET: str = os.getenv("POLY_API_SECRET", "")
    POLY_API_PASSPHRASE: str = os.getenv("POLY_API_PASSPHRASE", "")

    # --- Подтверждение сигнала (src/entry_confirm.py) ---
    # Вход, только если сигнал был и ENTRY_CONFIRM_SEC секунд назад (в ту же
    # сторону). Архив 25.09–11.10, 240 входов: проигрышей 7 → 1, прибыль
    # +$209 → +$237 на ставке $30, входов на 37% меньше. Включается/выключается
    # и в Telegram (⚙️ Настройки → ⏱ Подтверждение сигнала).
    ENTRY_CONFIRM_ENABLED: bool = _get_bool("ENTRY_CONFIRM_ENABLED", True)
    ENTRY_CONFIRM_SEC: float = _get_float("ENTRY_CONFIRM_SEC", 3.0)

    # --- Polymarket / CLOB ---
    POLY_HOST: str = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    POLY_CHAIN_ID: int = _get_int("POLY_CHAIN_ID", 137)
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")
    # ПРИМЕЧАНИЕ: с переходом на polymarket-client (актуальный официальный SDK)
    # этот параметр больше не используется — SDK сам определяет тип кошелька
    # (EOA/proxy/deposit wallet) через AsyncSecureClient.create(). Оставлен
    # в конфиге на случай отката на другой клиент, полем можно не заниматься.
    POLY_SIGNATURE_TYPE: int = _get_int("POLY_SIGNATURE_TYPE", 0)

    # --- Стратегия входа (общее для всех активов/таймфреймов) ---
    MIN_ENTRY_PRICE: float = _get_float("MIN_ENTRY_PRICE", 0.90)
    MAX_ENTRY_PRICE: float = _get_float("MAX_ENTRY_PRICE", 0.95)
    SAFETY_SCORE_THRESHOLD: float = _get_float("SAFETY_SCORE_THRESHOLD", 88.0)
    MIN_BOOK_LIQUIDITY_USDC: float = _get_float("MIN_BOOK_LIQUIDITY_USDC", 25.0)
    # Минимальное расстояние цены от страйка в % от цены (0 = фильтр выключен).
    # Зачем: расхождение в ATR обманчиво в тихом рынке — при маленьком ATR
    # "3-4 ATR" это всего $40-70 для BTC, и за 5-7 минут до конца 15m-рынка
    # такое расстояние легко съедается. Бэктест 15m BTC (25-28.09, 139 рынков):
    # без фильтра −$17, с минимумом ~$60 (0.07% при BTC ~84k) — +$38, из 4
    # проигрышей остался 1. На 5m по отчётам 25.09–08.10 сначала ничего не дал,
    # но по всем архивам до 10.10 минимум 0.05% срезал проигрыши примерно в 4
    # раза при той же прибыли (в выходные ATR маленький, и 1.5 ATR — это всего
    # несколько долларов BTC). С 10.10 по умолчанию 0.05% — в том числе для
    # кнопки «⭐ Рекомендованные». Меняется из Telegram (⚙️ Настройки).
    MIN_DISTANCE_PCT: float = _get_float("MIN_DISTANCE_PCT", 0.05)

    # --- Латентность / исполнение (прогрев стакана и транспорта) ---
    USE_LIVE_BOOK_STREAM: bool = _get_bool("USE_LIVE_BOOK_STREAM", True)
    LIVE_ENTRY_MAX_SLIPPAGE: float = _get_float("LIVE_ENTRY_MAX_SLIPPAGE", 0.01)
    MAX_ENTRY_EXECUTION_PRICE: float = _get_float("MAX_ENTRY_EXECUTION_PRICE", 0.97)

    # --- Управление капиталом ---
    # Дефолты откалиброваны под небольшой депозит (~$60) — см. обсуждение
    # в README: фиксированный (не растущий в % от банка) размер ставки,
    # чтобы не разгонять риск компаундингом на непроверенной вживую
    # стратегии. Меняй TRADE_SIZE_USDC пропорционально своему реальному
    # депозиту — это НЕ универсальная константа.
    # Бот двух стратегий начинает с фиксированной ставки $1: стратегии новые,
    # сначала проверяются малыми деньгами. Ставка общая для виртуальных сделок
    # и обеих стратегий; меняется в 💰 Размер позиции (и своя у монеты в ⚙️).
    TRADE_SIZE_USDC: float = _get_float("TRADE_SIZE_USDC", 1.0)
    # Режим % от банка остаётся доступен: банк = STARTING_BANKROLL_USDC +
    # реальный результат с начала базы; поставь свой реальный баланс.
    SIZING_MODE: str = os.getenv("SIZING_MODE", "fixed").strip().lower()
    BANKROLL_PCT: float = _get_float("BANKROLL_PCT", 8.0)
    STARTING_BANKROLL_USDC: float = _get_float("STARTING_BANKROLL_USDC", 300.0)
    # Какие монеты включены и по каким монетам стратегиям можно входить
    # реальными деньгами (🔴 LIVE в 🪙 Активах). По умолчанию включены BTC, ETH,
    # SOL, XRP: на истории 18.09–12.10 BNB и DOGE были хуже всех (k 1.32 и 1.24,
    # и в спокойные часы тоже в минусе), а с ними стратегия 2 давала меньше.
    ENABLED_ASSETS: str = os.getenv("ENABLED_ASSETS", "btc,eth,sol,xrp")
    LIVE_ASSETS: str = os.getenv("LIVE_ASSETS", "")
    # Свои диапазоны и ставки монет (JSON). На альтах вход по 0.95 не окупался
    # (архив 25.09–11.10: 51 вход по 0.95 — 5 проигрышей при 2.6 ожидаемых),
    # поэтому у всех, кроме BTC, максимум 0.94.
    ASSET_OVERRIDES: str = os.getenv("ASSET_OVERRIDES", json.dumps({
        "eth": {"min_entry_price": 0.90, "max_entry_price": 0.94},
        "xrp": {"min_entry_price": 0.90, "max_entry_price": 0.94},
        "sol": {"min_entry_price": 0.90, "max_entry_price": 0.94},
        "bnb": {"min_entry_price": 0.90, "max_entry_price": 0.94},
        "doge": {"min_entry_price": 0.90, "max_entry_price": 0.94},
    }))
    # Общий потолок ОДНОВРЕМЕННО открытых позиций по ВСЕМ активам/таймфреймам
    # разом — без этого при 12 параллельных потоках (6 монет x 2 таймфрейма)
    # можно случайно открыть 12 позиций разом, если все совпадут по времени.
    # В боте двух стратегий по одному рынку может быть две реальные позиции
    # (по одной на стратегию), а монеты часто входят в одно окно вместе —
    # поэтому по умолчанию 8 (4 монеты × 2 стратегии).
    MAX_OPEN_POSITIONS: int = _get_int("MAX_OPEN_POSITIONS", 8)
    # Ниже этой суммы даже пробовать не стоит — комиссии и слиппедж съедят
    # выгоду. Если в стакане меньше этого объёма по нужной цене — тик тихо
    # пропускается (не считается ошибкой, просто рынок сейчас неликвиден).
    MIN_VIABLE_TRADE_USDC: float = _get_float("MIN_VIABLE_TRADE_USDC", 2.0)
    DAILY_LOSS_LIMIT_USDC: float = _get_float("DAILY_LOSS_LIMIT_USDC", 60.0)

    # --- Исследовательский модуль (momentum_tracker) ---
    # Не торгует — просто пишет, при каких условиях (RSI/MACD/дисбаланс
    # стакана/и т.д.) цена стороны контракта проходит контрольные точки
    # 0.70/0.75/.../0.95. См. src/momentum_tracker.py.
    MOMENTUM_TRACKER_ENABLED: bool = _get_bool("MOMENTUM_TRACKER_ENABLED", True)

    # --- Масштабирование ставки по уверенности сигнала ---
    # Ставка = TRADE_SIZE_USDC только при score >= SIZE_SCALING_MAX_SCORE.
    # На самом пороге (score == threshold) ставка = TRADE_SIZE_USDC * MIN_FRACTION.
    # Между ними — линейная интерполяция. Так пограничные сигналы (score чуть
    # выше порога) автоматически получают меньшую ставку, а не полный размер.
    SIZE_SCALING_MIN_FRACTION: float = _get_float("SIZE_SCALING_MIN_FRACTION", 0.3)
    SIZE_SCALING_MAX_SCORE: float = _get_float("SIZE_SCALING_MAX_SCORE", 95.0)

    # --- Режим работы ---
    DRY_RUN: bool = _get_bool("DRY_RUN", True)          # True = только сигналы, ордера не шлём

    # --- Две стратегии (src/duo.py) ---
    # Каждый сигнал бот записывает как виртуальную сделку, а реальные входы
    # делают две стратегии по результатам виртуальных. false — бот работает
    # по-старому: сигнал = сделка (реальная по монетам с 🔴 LIVE).
    STRATEGIES_MODE: bool = _get_bool("STRATEGIES_MODE", True)
    # Фильтр расстояния цены от страйка (1.5 ATR и минимум в %). Выключен —
    # логика «сломанного» бота 12–14.10: фаворит по цене из диапазона входа,
    # тренд, время и волатильность как обычно, расстояние не проверяется.
    DISTANCE_FILTER_ENABLED: bool = _get_bool("DISTANCE_FILTER_ENABLED", False)
    # 1️⃣ «После проигрыша»: после виртуального проигрыша монеты — N входов по
    # ней (N своё у монеты: STRAT_A_COUNTS='{"sol": 8, "btc": 2}', остальным
    # STRAT_A_DEFAULT_N). Проигрыш внутри серии начинает её заново.
    STRAT_A_ENABLED: bool = _get_bool("STRAT_A_ENABLED", True)
    # Режим стратегии 1️⃣: recover — «отыгрыш»: после проигрыша 2️⃣ по монете
    # один вход по этой монете на сумму, выигрыш с которой (за вычетом
    # комиссии) возвращает этот минус; series — N входов после виртуального
    # проигрыша монеты (как было).
    STRAT_A_MODE: str = os.getenv("STRAT_A_MODE", "recover").strip().lower()
    STRAT_A_LIVE: bool = _get_bool("STRAT_A_LIVE", False)
    STRAT_A_DEFAULT_N: int = _get_int("STRAT_A_DEFAULT_N", 2)
    STRAT_A_COUNTS: str = os.getenv("STRAT_A_COUNTS", "")
    # 2️⃣ «Пауза после проигрыша»: вход по монете, только если за последние
    # STRAT_B_PAUSE_MIN минут не было виртуальных проигрышей.
    # STRAT_B_SCOPE: all — проигрыш любой монеты останавливает все (по
    # умолчанию: на архиве 18.09–12.10 и на 13–15.10 общая пауза была лучше),
    # coin — у каждой монеты своя пауза.
    STRAT_B_ENABLED: bool = _get_bool("STRAT_B_ENABLED", True)
    STRAT_B_LIVE: bool = _get_bool("STRAT_B_LIVE", False)
    STRAT_B_PAUSE_MIN: int = _get_int("STRAT_B_PAUSE_MIN", 60)
    STRAT_B_SCOPE: str = os.getenv("STRAT_B_SCOPE", "all").strip().lower()
    # Фильтры входа стратегий 1️⃣ и 2️⃣ (виртуальные сделки пишутся всегда):
    # не входить, если до конца рынка меньше ENTRY_MIN_MINUTES_LEFT минут и
    # если спред (ask фаворита + ask другой стороны − 1) шире ENTRY_MAX_SPREAD.
    # 0 — фильтр выключен. Решение — один раз на рынок, в момент первого
    # сигнала (виртуальной сделки). Пороги меняются в 🎯 → 🧹.
    ENTRY_MIN_MINUTES_LEFT: float = _get_float("ENTRY_MIN_MINUTES_LEFT", 2.0)
    ENTRY_MAX_SPREAD: float = _get_float("ENTRY_MAX_SPREAD", 0.0)
    # Свой банк у каждой монеты (для ставки «% банка» и как потолок отыгрыша):
    # стартовая сумма, дальше банк = старт + результат сделок стратегий этой
    # монеты (реальные и тестовые — отдельно). Своя сумма у монеты — 🎯 → 🏦.
    COIN_BANK_USDC: float = _get_float("COIN_BANK_USDC", 75.0)
    # Комиссия тейкера Polymarket: ставка × FEE_RATE × (1 − цена). Нужна, чтобы
    # отыгрыш возвращал минус уже после комиссии.
    FEE_RATE: float = _get_float("FEE_RATE", 0.07)

    # --- ⚡ Лаборатория скорости (src/speed_lab.py) ---
    # Только запись, ничего не покупает: при рывке цены на Binance (честная
    # вероятность UP сдвинулась на ≥ SPEED_LAB_JUMP за ≤ SPEED_LAB_WINDOW_MS)
    # пишет стакан обеих сторон рынка за 2 с до и SPEED_LAB_RECORD_MS после —
    # чтобы узнать, сколько миллисекунд Polymarket держит старую цену.
    SPEED_LAB_ENABLED: bool = _get_bool("SPEED_LAB_ENABLED", True)
    SPEED_LAB_JUMP: float = _get_float("SPEED_LAB_JUMP", 0.05)
    SPEED_LAB_WINDOW_MS: int = _get_int("SPEED_LAB_WINDOW_MS", 1000)
    SPEED_LAB_RECORD_MS: int = _get_int("SPEED_LAB_RECORD_MS", 6000)
    SPEED_LAB_COOLDOWN_MS: int = _get_int("SPEED_LAB_COOLDOWN_MS", 3000)
    SPEED_LAB_KEEP_DAYS: float = _get_float("SPEED_LAB_KEEP_DAYS", 3.0)

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- Storage ---
    DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")

    # --- Отчёты для анализа стратегии ---
    REPORT_INTERVAL_HOURS: float = _get_float("REPORT_INTERVAL_HOURS", 4.0)
    REPORTS_DIR: str = os.getenv("REPORTS_DIR", "data/reports")

    # --- Отслеживание чужого кошелька (уведомления + опциональный копитрейдинг) ---
    WALLET_TRACK_ADDRESS: str = os.getenv("WALLET_TRACK_ADDRESS", "0x4707b735acce66b2ccb5600086de2329685cd1ce")
    WALLET_TRACK_POLL_SECONDS: int = _get_int("WALLET_TRACK_POLL_SECONDS", 3)


settings = Settings()
