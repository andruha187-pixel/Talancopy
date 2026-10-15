"""
Исполнение решений стратегии + учёт открытых позиций и их резолюции.
Управление капиталом: не больше max_open_positions реальных позиций в ещё
идущих рынках одновременно (⚙️ Настройки), стоп по дневному лимиту убытков
(стоп-лосс, настраивается кнопкой в Telegram) — при достижении лимита новые
входы блокируются до следующего дня. Каждый сигнал, который не стал сделкой,
учитывается с причиной в src/diagnostics.py («🔎 Почему нет сделок»).
"""
from __future__ import annotations
import asyncio
import time

from config import settings
from src import storage, polymarket_client, telegram_notify, book_stream, runtime_state, loss_stats, diagnostics
from src import entry_confirm, duo
from src.market_discovery import ActiveMarket, get_resolution
from src.strategy import Decision


def _daily_loss_exceeded() -> bool:
    """Дневной стоп-лосс считается только по РЕАЛЬНЫМ сделкам и блокирует
    только реальные входы: виртуальный минус DRY-монеты не должен
    останавливать торговлю BTC, а сбор данных по DRY-монетам не должен
    обрываться из-за реальных убытков."""
    today_start = int(time.time() // 86400) * 86400
    summary = storage.get_pnl_summary(today_start, live_only=True)
    limit = runtime_state.get("daily_loss_limit_usdc")
    return summary["pnl_usdc"] <= -abs(limit)


def _quiet(asset: str, dry_run: bool) -> bool:
    """Не слать в Telegram виртуальные сделки монет, переведённых в DRY
    (иначе 5–6 монет засыплют чат). Монеты, помеченные как LIVE, уведомляют
    как раньше, даже если весь бот сейчас в DRY RUN. Включается кнопкой
    «🔔 Уведомления DRY-монет» в 🪙 Активах.
    Бот двух стратегий: молчат все виртуальные и тестовые сделки (их
    десятки в час), пишут только реальные."""
    if duo.enabled():
        return bool(dry_run) and not runtime_state.get("notify_dry_assets")
    return (bool(dry_run) and not runtime_state.is_asset_live(asset)
            and not runtime_state.get("notify_dry_assets"))


async def _notify(asset: str, dry_run: bool, text: str) -> None:
    if not _quiet(asset, dry_run):
        await telegram_notify.notify(text)


def _scale_trade_size(base_size: float, score: float, threshold: float) -> float:
    """
    Размер ставки, зависящий от уверенности сигнала. На самом пороге
    (score == threshold) используем только MIN_FRACTION от базового размера,
    на score >= SIZE_SCALING_MAX_SCORE — полный размер. Между ними —
    линейная интерполяция. Идея: пограничный сигнал (score едва прошёл
    порог) — это по определению самый рискованный случай, не стоит на нём
    ставить так же, как на уверенном.
    """
    min_fraction = settings.SIZE_SCALING_MIN_FRACTION
    max_score = settings.SIZE_SCALING_MAX_SCORE
    span = max(max_score - threshold, 1e-6)
    progress = min(1.0, max(0.0, (score - threshold) / span))
    fraction = min_fraction + (1 - min_fraction) * progress
    return round(base_size * fraction, 2)


_liquidity_skip_notified: set[str] = set()

# Вход в рынок ищут две проверки: обычная раз в 3 с (main.py) и быстрая раз в
# ~0.3 с (src/fast_signal.py). Обе зовут maybe_enter, а между проверкой «нет
# открытой сделки» и записью сделки в базу проходит время исполнения ордера —
# без блокировки на рынок они могли бы купить один рынок дважды.
_entry_locks: dict[str, asyncio.Lock] = {}
# Рынок -> время, раньше которого не повторяем вход после неудачного ордера
# (быстрая проверка иначе слала бы новую серию ордеров каждые 0.3 с).
_retry_after: dict[str, float] = {}
ENTRY_RETRY_COOLDOWN_SEC = 1.0
# Рынки, по которым прямо сейчас исполняется реальный ордер, — занимают слот
# лимита позиций до записи сделки в базу: иначе два сигнала по разным монетам
# в одну секунду оба прошли бы проверку лимита.
_inflight_live: set[str] = set()


def _lock_for(slug: str) -> asyncio.Lock:
    lock = _entry_locks.get(slug)
    if lock is None:
        if len(_entry_locks) > 500:
            for key in [k for k, v in _entry_locks.items() if not v.locked()]:
                _entry_locks.pop(key, None)
            for key in [k for k, t in _retry_after.items() if t < time.time()]:
                _retry_after.pop(key, None)
        lock = _entry_locks[slug] = asyncio.Lock()
    return lock


def _cool_down(slug: str) -> None:
    """Не повторять вход по рынку ENTRY_RETRY_COOLDOWN_SEC: ордер не исполнился
    или вход упёрся в лимит позиций / дневной стоп / пустой стакан. Быстрая
    проверка иначе повторяла бы то же самое каждые 0.3 с."""
    _retry_after[slug] = time.time() + ENTRY_RETRY_COOLDOWN_SEC


def entry_busy(slug: str, now: float | None = None) -> bool:
    """По рынку уже идёт вход или недавно не исполнился ордер — новый вход
    сейчас не начинаем."""
    lock = _entry_locks.get(slug)
    if lock is not None and lock.locked():
        return True
    return (time.time() if now is None else now) < _retry_after.get(slug, 0.0)


# id сделок, по которым уже сообщили о неудачной продаже по стоп-лоссу
_sl_fail_notified: set[int] = set()
# день (unix_ts // 86400), за который уже сообщили о дневном стоп-лоссе
_daily_limit_notified_day: int | None = None


# Попытки исполнить вход: первая по потолку сигнала, следующие — по свежему
# стакану и только пока цена в диапазоне входа монеты. Раньше после отказа
# FOK бот повторял ту же цену с половиной ставки: 08.10 в 22:07 отказали оба
# ордера BTC и BNB ($9 и $4.50) — цена уже ушла, а половина ставки по 0.95
# ($4.50 = 4.7 акции) даже меньше минимального ордера в 5 акций.
MAX_FILL_ATTEMPTS = 3
RETRY_PAUSE_SEC = 0.3
MIN_ORDER_SHARES = 5.0


async def _execute_live_buy(market: ActiveMarket, token_id: str, amount: float, cap: float, tick: float,
                            range_max: float):
    """Реальная покупка FAK-ордерами: исполняется всё, что есть в стакане по
    цене не выше потолка, остаток отменяется — без «всё или ничего». Если
    исполнилось не всё или ничего — до двух повторов по свежему стакану через
    0.3 с, пока лучший ask не выше максимума диапазона и до конца рынка
    больше 45 с. Возвращает (USDC, акций, id ордеров, статус, попыток,
    последний потолок) или None, если не исполнилось ничего."""
    filled_usdc = filled_shares = 0.0
    order_ids: list[str] = []
    status = None
    last_err = None
    remaining = round(amount, 2)
    attempts = 0
    while attempts < MAX_FILL_ATTEMPTS:
        attempts += 1
        try:
            resp = await polymarket_client.place_buy_order(token_id, cap, remaining, tick, order_type="FAK")
        except Exception as exc:  # noqa: BLE001
            if not polymarket_client.is_no_fill_error(exc):
                diagnostics.record_skip(market.slug, market.asset, "order_error", str(exc))
                await telegram_notify.notify(f"❌ Ошибка при выставлении ордера ({market.slug}): {exc}")
                break
            last_err = exc
        else:
            oid = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID")
            if oid:
                order_ids.append(str(oid))
            status = polymarket_client.response_field(resp, "status") or "SUBMITTED"
            amounts = polymarket_client.filled_amounts(resp)
            if amounts is None or str(status).lower() == "delayed" or (amounts[0] <= 0 and str(status).lower() == "matched"):
                # Сумм исполнения в ответе нет (или ордер принят с задержкой) —
                # считаем, что исполнился целиком, как было с FOK.
                usdc, shares = remaining, remaining / cap
            else:
                usdc, shares = amounts
            if usdc > 0:
                filled_usdc += usdc
                filled_shares += shares
                remaining = round(amount - filled_usdc, 2)
            else:
                last_err = f"исполнено 0 (статус {status})"
        if attempts >= MAX_FILL_ATTEMPTS:
            break
        if filled_usdc > 0 and remaining < max(settings.MIN_VIABLE_TRADE_USDC, MIN_ORDER_SHARES * cap):
            break  # остаток меньше минимального ордера — хватит
        if market.end_time - time.time() < 45:
            break
        await asyncio.sleep(RETRY_PAUSE_SEC)
        book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)
        if book.best_ask is None or book.best_ask > range_max + 1e-9:
            last_err = last_err or "цена ушла выше диапазона"
            break
        cap = polymarket_client.round_price_for_buy(
            min(book.best_ask + settings.LIVE_ENTRY_MAX_SLIPPAGE, settings.MAX_ENTRY_EXECUTION_PRICE, range_max),
            book.tick_size or tick,
        )
        available = polymarket_client.fillable_usdc(token_id, cap, book)
        if available < settings.MIN_VIABLE_TRADE_USDC:
            last_err = f"в стакане по цене до {cap:.2f} всего {available:.2f} USDC"
            continue
        remaining = min(remaining, round(available * 0.9, 2))

    if filled_usdc <= 0:
        if last_err is not None:
            diagnostics.record_skip(market.slug, market.asset, "liquidity", f"{attempts} попыток: {last_err}")
            if market.slug not in _liquidity_skip_notified:
                _liquidity_skip_notified.add(market.slug)
                await telegram_notify.notify(
                    f"⚠️ Сигнал по {market.slug} пропущен: ордер не исполнился за {attempts} "
                    f"попытки — цена ушла или заявки сняли ({last_err})."
                )
        return None
    return filled_usdc, filled_shares, ",".join(order_ids) or "matched", status or "matched", attempts, cap


async def maybe_enter(market: ActiveMarket, decision: Decision, path: str = "slow") -> None:
    """Вход по сигналу. path — какая проверка нашла сигнал: "slow" (раз в
    3 с) или "fast" (быстрая, src/fast_signal.py); пишется в сделку."""
    if not decision.should_enter:
        return
    # Сигнал должен держаться ~3 с: был и в оценке рынка ~3 с назад, в ту же
    # сторону (src/entry_confirm.py). Иначе — ждём следующей проверки.
    if not entry_confirm.confirmed(market.slug, decision.direction):
        return
    if entry_busy(market.slug):
        return
    # Между entry_busy и захватом блокировки нет await — второй вызов по тому
    # же рынку увидит блокировку занятой и выйдет сразу, а не будет ждать.
    async with _lock_for(market.slug):
        await _enter_locked(market, decision, path)


async def _daily_limit_hit(market: ActiveMarket) -> bool:
    """Дневной стоп-лосс по реальным сделкам достигнут — сообщить (раз в день)
    и отложить повтор входа."""
    global _daily_limit_notified_day
    if not _daily_loss_exceeded():
        return False
    day = int(time.time() // 86400)
    if _daily_limit_notified_day != day:
        _daily_limit_notified_day = day
        await telegram_notify.notify(
            f"🛑 Стоп-лосс достигнут (лимит {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC/день) — "
            f"реальные входы заблокированы до конца дня (виртуальные продолжаются). "
            f"Поменять лимит можно в 🛑 Стоп-лосс в меню."
        )
    diagnostics.record_skip(market.slug, market.asset, "daily")
    _cool_down(market.slug)
    return True


async def _enter_duo(market: ActiveMarket, decision: Decision, path: str) -> None:
    """Бот двух стратегий (src/duo.py): по сигналу — виртуальная сделка, потом
    входы стратегий 1️⃣ и 2️⃣, если они сейчас хотят в этот рынок."""
    slug, asset = market.slug, market.asset
    # 1) Виртуальная сделка — по каждому сигналу, в том числе на паузе ⏸: по
    # ним стратегии «читают» рынок. Фильтры входа (🎯 → 🧹) решают здесь же,
    # один раз на рынок: пускать ли в него стратегии.
    if not duo.has_virtual(slug):
        row = storage.get_open_trade_for_market(slug, strategy=duo.VIRT)
        if row is not None:
            duo.on_virtual_logged(row[0], slug, row[2], market.up_token_id, market.down_token_id,
                                  filtered=storage.trade_filtered(row[0]))
        else:
            filtered = duo.entry_filter_reason(decision)
            tid = await _enter_sized(market, decision, path, True, strategy=duo.VIRT,
                                     size_override=duo.VIRT_STAKE,
                                     features=duo.entry_features(decision, filtered))
            if tid is None:
                return  # не записалась (мало ликвидности) — повторим на следующем сигнале
            duo.on_virtual_logged(tid, slug, decision.direction, market.up_token_id, market.down_token_id,
                                  filtered=filtered)
    reason = duo.market_filtered(slug)
    if reason:
        diagnostics.record_skip(slug, asset, f"filter_{reason}")
        return
    want = duo.wanted(asset, slug)
    if not want:
        return
    if runtime_state.get("paused"):
        diagnostics.record_skip(slug, asset, "paused")
        return
    # 2) Стратегии — по очереди; у каждой своя сделка в рынке. Ставку считает
    # src/duo.py: фикс. или % банка монеты; у 1️⃣ в режиме отыгрыша — сумма,
    # которая вернёт минус 2️⃣ по этой монете (по худшей цене — потолку ордера).
    _lo, range_max = runtime_state.entry_range(asset)
    cap = min(decision.entry_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, settings.MAX_ENTRY_EXECUTION_PRICE, range_max)
    for strat in want:
        dry_run = duo.entry_dry(strat, asset)
        key = f"{slug}#{strat}"
        stake = duo.stake_for(strat, asset, dry_run, cap)
        note = ""
        if strat == duo.A and duo.a_mode() == "recover":
            target = duo.recover_target(asset, dry_run)
            note = (f"🩹 Отыгрыш минуса 2️⃣: ${target:.2f} → ставка ${stake:.2f} (по цене до {cap:.2f}, "
                    f"банк {asset.upper()} ${duo.coin_bank(asset, dry_run):.2f})")
        if not dry_run:
            if await _daily_limit_hit(market):
                continue   # реальный вход нельзя, тестовый другой стратегии — можно
            open_live = storage.count_open_trades(live_only=True, running_only=True)
            if open_live + len(_inflight_live - {key}) >= runtime_state.get("max_open_positions"):
                diagnostics.record_skip(slug, asset, "cap")
                _cool_down(slug)
                continue
            _inflight_live.add(key)
        try:
            tid = await _enter_sized(market, decision, path, dry_run, strategy=strat,
                                     size_override=stake, note=note)
        finally:
            _inflight_live.discard(key)
        if tid is not None:
            duo.on_strategy_entered(strat, slug, dry_run)


async def _enter_locked(market: ActiveMarket, decision: Decision, path: str) -> None:
    global _daily_limit_notified_day
    if duo.enabled():
        await _enter_duo(market, decision, path)
        return
    if runtime_state.get("paused"):
        diagnostics.record_skip(market.slug, market.asset, "paused")
        return
    if storage.get_open_trade_for_market(market.slug):
        return  # уже есть позиция в этом рынке
    # Реальная или виртуальная сделка — по общему режиму и режиму монеты
    # (🪙 Активы): DRY-монеты торгуют виртуально, даже когда бот в LIVE.
    dry_run = runtime_state.trade_is_dry(market.asset)
    if not dry_run:
        if _daily_loss_exceeded():
            # Сообщаем один раз за день, а не на каждом тике с сигналом
            day = int(time.time() // 86400)
            if _daily_limit_notified_day != day:
                _daily_limit_notified_day = day
                await telegram_notify.notify(
                    f"🛑 Стоп-лосс достигнут (лимит {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC/день) — "
                    f"реальные входы заблокированы до конца дня (DRY-монеты продолжают). "
                    f"Поменять лимит можно в 🛑 Стоп-лосс в меню."
                )
            diagnostics.record_skip(market.slug, market.asset, "daily")
            _cool_down(market.slug)
            return
        open_live = storage.count_open_trades(live_only=True, running_only=True)
        if open_live + len(_inflight_live - {market.slug}) >= runtime_state.get("max_open_positions"):
            # Общий потолок РЕАЛЬНЫХ позиций по всем активам разом — при
            # нескольких потоках несколько сигналов могут совпасть по времени.
            # Виртуальные сделки DRY-монет место не занимают; позиции в уже
            # закончившихся рынках (ждут резолюции) — тоже. Рынки, по которым
            # ордер исполняется прямо сейчас, место уже занимают.
            diagnostics.record_skip(market.slug, market.asset, "cap")
            _cool_down(market.slug)
            return
        _inflight_live.add(market.slug)
    try:
        await _enter_sized(market, decision, path, dry_run)
    finally:
        _inflight_live.discard(market.slug)


_STRAT_TAG = {"virt": "👻 ", "A": "1️⃣ ", "B": "2️⃣ "}


def _dry_fill_price(token_id: str, book, cap: float, usdc: float) -> float:
    """Цена, по которой виртуальная/тестовая сделка исполнилась бы ордером
    сейчас: средняя по уровням asks не дороже потолка на сумму ставки. Раньше
    писали сам потолок (ask + 1¢), а реальные ордера 13–15.10 исполнялись в
    среднем ровно по ask сигнала — тестовая статистика выходила хуже реальной
    примерно на 1¢ с доллара. Уровней нет — потолок, как раньше."""
    levels = book.asks
    if levels is None:
        b = book_stream.get_book(token_id)
        levels = list((b.get("asks") or {}).items()) if b else None
    if not levels:
        return cap
    spent = shares = 0.0
    for price, size in sorted((float(p), float(sz)) for p, sz in levels):
        if price > cap + 1e-9 or spent >= usdc - 1e-9:
            break
        take = min(price * size, usdc - spent)
        spent += take
        shares += take / price
    return round(spent / shares, 4) if shares > 0 else cap


async def _enter_sized(market: ActiveMarket, decision: Decision, path: str, dry_run: bool,
                       strategy: str | None = None, size_override: float | None = None,
                       note: str = "", features: dict | None = None) -> int | None:
    """Вход с размером ставки. Возвращает id записанной сделки или None, если
    сделки нет. strategy — метка сделки в боте двух стратегий (src/duo.py),
    size_override — ставка, посчитанная там же (без масштабирования по score),
    note — строка в уведомление."""
    # Ставка и диапазон — свои у монеты (⚙️ в 🪙 Активах), иначе общие
    base_size = runtime_state.compute_trade_size(market.asset) if size_override is None else size_override
    range_min, range_max = runtime_state.entry_range(market.asset)
    if base_size <= 0:
        diagnostics.record_skip(market.slug, market.asset, "no_bank")
        _cool_down(market.slug)
        return None  # банк в режиме % исчерпан — ставить нечего
    score_threshold = runtime_state.get("safety_score_threshold")
    if runtime_state.get("size_scaling_enabled") and size_override is None:
        trade_size = _scale_trade_size(base_size, decision.safety_score, score_threshold)
    else:
        trade_size = base_size

    token_id = market.up_token_id if decision.direction == "UP" else market.down_token_id

    # Проверяем реальную глубину стакана ПЕРЕД отправкой — FOK отменяет
    # ордер целиком, если не может закрыть всю сумму по цене не хуже
    # потолка. Если объём меньше запрошенного — торгуем тем, что реально
    # есть (с запасом на случай, что часть заберут раньше нас), а не
    # отправляем заведомо обречённый на отмену ордер на полную сумму.
    # Раньше глубину брали ТОЛЬКО из WS-кэша (book_stream). Когда WS-стакан
    # по токену пустой или протух, стратегия видела цену через REST-фолбэк,
    # а здесь получала ликвидность 0 и молча пропускала вход. Так 28.09
    # (16:19-20:19) 15m-бот выдал 18 сигналов по 3 рынкам и не открыл ни
    # одной сделки — у всех сигналов book_source был "rest". Теперь берём
    # стакан так же, как стратегия: WS, если свежий, иначе REST.
    book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)

    # Между тем, как strategy.evaluate() прочитала ask, и моментом реальной
    # отправки ордера проходит какое-то время (сеть + подпись). Даём себе
    # небольшой запас на слиппедж, но не платим больше жёсткого потолка, и
    # обязательно выравниваем по тику — иначе CLOB отклонит ордер с неверным
    # шагом цены прямо в критичный момент.
    tick = book.tick_size or book_stream.tick_size(token_id)
    # Потолок исполнения НЕ может быть выше максимума из «📈 Диапазон
    # входа». Раньше запас на слиппедж (+0.01) добавлялся поверх: сигнал
    # по 0.95 при максимуме 0.95 уходил с потолком 0.96 и реально
    # исполнялся по 0.96. Оба реальных проигрыша BTC 5m за 25-28.09
    # (−$21.86 и −$27.11) были именно такими сделками.
    raw_cap = min(
        decision.entry_price + settings.LIVE_ENTRY_MAX_SLIPPAGE,
        settings.MAX_ENTRY_EXECUTION_PRICE,
        range_max,
    )
    execution_price = polymarket_client.round_price_for_buy(raw_cap, tick)

    # Ликвидность считаем только по уровням НЕ дороже потолка: FOK-ордер
    # исполняется целиком или никак, а раньше в расчёт шли все 10 уровней
    # стакана, в том числе дороже потолка. На тонких стаканах (DOGE, HYPE) и
    # на сигналах по 0.95 ордер на полную ставку тогда отменялся.
    available_liquidity = polymarket_client.fillable_usdc(token_id, execution_price, book)
    if available_liquidity < settings.MIN_VIABLE_TRADE_USDC:
        diagnostics.record_skip(market.slug, market.asset, "liquidity")
        _cool_down(market.slug)
        if market.slug not in _liquidity_skip_notified:
            _liquidity_skip_notified.add(market.slug)
            await _notify(market.asset, dry_run,
                f"⚠️ Сигнал по {market.slug} пропущен: в стакане по цене до {execution_price:.2f} всего "
                f"{available_liquidity:.2f} USDC (источник: {book.source})."
            )
        return None
    if available_liquidity < trade_size:
        trade_size = round(available_liquidity * 0.9, 2)

    status = "DRY_RUN"
    order_id = "dry-run"
    first_cap = execution_price
    attempts = 1
    entry_logged = _dry_fill_price(token_id, book, execution_price, trade_size) if dry_run else execution_price
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            # Не должно случиться благодаря проверкам в telegram_notify/main.py,
            # но лучше явная ошибка тут, чем AttributeError глубоко внутри SDK.
            await telegram_notify.notify(
                "❌ LIVE включён, но POLY_PRIVATE_KEY не задан — вход пропущен. "
                "Проверь переменные окружения и передеплой."
            )
            return None
        result = await _execute_live_buy(market, token_id, trade_size, execution_price, tick, range_max)
        if result is None:
            _cool_down(market.slug)
            return None
        filled_usdc, filled_shares, order_id, status, attempts, execution_price = result
        trade_size = round(filled_usdc, 2)
        # Реальная средняя цена исполнения (раньше в базу шёл потолок ордера)
        entry_logged = round(filled_usdc / filled_shares, 4) if filled_shares > 0 else execution_price

    trade_id = storage.log_trade(
        market_slug=market.slug,
        condition_id=market.condition_id,
        direction=decision.direction,
        entry_price=entry_logged,
        size_usdc=trade_size,
        order_id=order_id,
        status=status,
        dry_run=dry_run,
        token_id=token_id,
        signal_ask=decision.entry_price,
        safety_score=decision.safety_score,
        range_min=range_min,
        range_max=range_max,
        entry_path=path,
        features=features if features is not None else duo.entry_features(decision),
        strategy=strategy,
    )

    mode_tag = ("🧪 [тест] " if strategy in ("A", "B") else "🧪 [DRY RUN] ") if dry_run else "✅ "
    await _notify(market.asset, dry_run,
        f"{_STRAT_TAG.get(strategy, '')}{mode_tag}Вход {decision.direction} по {market.slug}"
        f"{' ⚡ быстрая проверка' if path == 'fast' else ''}\n"
        f"Ask на сигнале: {decision.entry_price:.3f} | Потолок исполнения: {first_cap:.3f} "
        f"(тик {tick:g}) | Размер: {trade_size:.2f} из {base_size:.2f} USDC (score {decision.safety_score}/{score_threshold:.0f})\n"
        + (f"Исполнено по средней {entry_logged:.3f} за {attempts} попытки\n" if not dry_run and attempts > 1 else "")
        + f"Диапазон {range_min:.2f}–{range_max:.2f} | Расхождение: {decision.distance_atr} ATR | "
        f"До конца рынка: {decision.minutes_left:.1f} мин"
        + (f"\n{note}" if note else "")
    )
    return trade_id


async def execute_copytrade(token_id: str, direction: str, slug: str, condition_id: str,
                             price_hint: float | None) -> None:
    """
    Повторяет вход отслеживаемого кошелька (см. src/wallet_tracker.py).
    Отдельный от maybe_enter путь — здесь нет своего score, входим просто
    потому что вошёл он. Управляется СВОИМ тумблером
    (wallet_copytrade_enabled), а не общей паузой ⏸ Стоп — та относится
    только к входам по своим сигналам (maybe_enter). Риск-лимиты (дневной
    стоп-лосс, потолок открытых позиций) — общие с основной стратегией,
    это про капитал в целом, а не про то, какая часть бота его тратит.
    """
    if storage.get_open_trade_for_market(slug):
        return  # уже скопировали (или у нас своя позиция) в этом рынке
    dry_run = runtime_state.get("dry_run")
    if not dry_run:
        if _daily_loss_exceeded():
            return  # дневной лимит уже сработал — не копируем, пока не настанет новый день
        if storage.count_open_trades(live_only=True, running_only=True) >= runtime_state.get("max_open_positions"):
            return

    trade_size = runtime_state.get("copytrade_size_usdc")

    # Токен мог быть не в нашем WS-кэше вообще (мы его раньше не отслеживали) —
    # подписываемся сразу и берём стакан через get_orderbook_cached, который
    # сам падает в REST, если WS ещё не успел прогреться.
    if settings.USE_LIVE_BOOK_STREAM:
        book_stream.subscribe([token_id])
    book = await polymarket_client.get_orderbook_cached(token_id, depth_levels=10)
    available_liquidity = book.ask_liquidity_usdc
    if available_liquidity < settings.MIN_VIABLE_TRADE_USDC:
        return
    if available_liquidity < trade_size:
        trade_size = round(available_liquidity * 0.9, 2)

    tick = book.tick_size
    reference_price = price_hint or book.best_ask
    if reference_price is None:
        return
    raw_cap = min(reference_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, settings.MAX_ENTRY_EXECUTION_PRICE)
    execution_price = polymarket_client.round_price_for_buy(raw_cap, tick)

    status = "DRY_RUN"
    order_id = "dry-run"
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify(
                "❌ Копитрейдинг: LIVE включён, но POLY_PRIVATE_KEY не задан — вход пропущен."
            )
            return
        try:
            resp = await polymarket_client.place_buy_order(token_id, execution_price, trade_size, tick)
        except Exception as exc:  # noqa: BLE001
            await telegram_notify.notify(f"❌ Копитрейдинг: ошибка при выставлении ордера ({slug}): {exc}")
            return
        order_id = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID") or str(resp)
        status = polymarket_client.response_field(resp, "status") or "SUBMITTED"

    storage.log_trade(
        market_slug=slug,
        condition_id=condition_id,
        direction=direction,
        entry_price=execution_price,
        size_usdc=trade_size,
        order_id=order_id,
        status=status,
        dry_run=dry_run,
        token_id=token_id,
        source="copytrade",
    )

    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else '✅ '}📋 Скопирован вход {direction} по {slug}\n"
        f"Цена кошелька: {reference_price:.3f} | Потолок исполнения: {execution_price:.3f} | "
        f"Размер: {trade_size:.2f} USDC"
    )


async def label_resolved_markets(exclude_slugs: set[str] | None = None) -> None:
    """
    Подписывает исходом ВСЕ ещё не подписанные сигналы прошлых рынков —
    основа для отчёта/анализа: без метки "что реально произошло" по
    каждому тику нельзя понять, какой сигнал был бы правильным, даже если
    бот в тот рынок не входил. exclude_slugs — слаги сейчас активных
    рынков по ВСЕМ активам/таймфреймам (их исход ещё не может быть
    известен, спрашивать API бессмысленно).
    """
    for slug in storage.get_markets_needing_outcome(exclude_slugs=exclude_slugs, limit=10):
        outcome = await get_resolution(slug)
        if outcome:
            storage.label_signals_outcome(slug, outcome)


async def settle_resolved_trades() -> None:
    # По одному рынку бывает несколько сделок (бот двух стратегий: виртуальная
    # + 1️⃣ + 2️⃣) — Gamma API спрашиваем один раз на рынок за проход.
    resolved: dict[str, str | None] = {}
    for (trade_id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id,
         strategy, early) in storage.get_unsettled_trades(with_strategy=True):
        if market_slug not in resolved:
            resolved[market_slug] = await get_resolution(market_slug)
        outcome = resolved[market_slug]
        if outcome is None:
            continue

        shares = size_usdc / entry_price
        won = outcome == direction
        pnl = (shares * 1.0 - size_usdc) if won else -size_usdc
        if not storage.settle_trade(trade_id, outcome, pnl):
            continue  # уже закрыта стоп-лоссом, пока ждали ответ Gamma API
        if strategy == duo.VIRT:
            try:
                duo.on_official(market_slug, direction, outcome, early)
            except Exception:  # noqa: BLE001 — стратегии не должны ломать резолюцию
                pass
        elif strategy in ("A", "B"):
            duo.invalidate_banks()   # банк монеты изменился

        asset, _label = storage.parse_market_slug(market_slug)
        emoji = "🟢" if won else "🔴"
        text = (
            f"{_STRAT_TAG.get(strategy, '')}{emoji} Рынок {market_slug} зарезолвился: {outcome}. "
            f"Наша ставка: {direction}. PnL: {pnl:+.2f} USDC"
            + ((" (тест)" if strategy in ("A", "B") else " (dry run)") if dry_run else "")
        )
        if not won:
            # Проигрыш по 0.95 — это «заложенные» ~5%, а не обязательно ошибка:
            # показываем, сколько таких проигрышей ожидалось по ценам входа.
            try:
                st = storage.get_outcome_stats(live=not dry_run, asset=asset, strategy=strategy)
                label = f"{asset.upper()}{' ' + duo.SHORT[strategy] if strategy in duo.SHORT else ''}"
                label += (" тест" if strategy in ("A", "B") else " DRY") if dry_run else ""
                text += "\n" + loss_stats.loss_context(st, entry_price, label)
            except Exception:  # noqa: BLE001 — приписка не должна ломать резолюцию
                pass
        await _notify(asset, dry_run, text)


async def check_position_stop_losses() -> None:
    """
    Стоп-лосс ОТДЕЛЬНОЙ позиции в процентах (не дневной!) — включается и
    настраивается кнопкой в Telegram. Пока рынок ещё не зарезолвился, но
    цена ушла против нас настолько, что текущая стоимость позиции упала на
    position_stop_loss_pct% и больше от суммы входа — закрываем продажей
    прямо сейчас, не дожидаясь исхода, чтобы не рисковать потерять 100%.

    Оценка stops по best bid из живого стакана (book_stream) — тот же
    источник, что и для входа, без лишнего REST-запроса, если стакан свежий.

    Вызывается из отдельного быстрого цикла main.stop_loss_loop (раз в ~2 с;
    раньше — раз в 10 с из settlement_loop). Что показали отчёты 18.09–06.10
    (~2700 рынков BTC 5m и все 213 реальных сделок): проигрышная позиция
    падает с ~0.9 до ~0.4 за несколько секунд, поэтому стоп продаёт обычно уже
    около −50…−80%, а примерно 4 из 10 позиций, просевших на 50%, потом всё
    равно выигрывают. На реальных сделках стоп 50% дал бы +$66 вместо +$114 —
    прибыль он не увеличивает. По умолчанию выключен.
    """
    if not runtime_state.get("position_stop_loss_enabled"):
        return
    stop_pct = runtime_state.get("position_stop_loss_pct")

    for (trade_id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id,
         strategy, _early) in storage.get_unsettled_trades(with_strategy=True):
        if not token_id:
            continue  # старые сделки до появления этого поля — пропускаем, не падаем
        if strategy == duo.VIRT:
            continue  # виртуальные — «чтение рынка» стратегиями: им нужен исход рынка, а не стоп

        book = await polymarket_client.get_orderbook_cached(token_id)
        if book.best_bid is None:
            continue  # нет ставок на продажу прямо сейчас — не с чем сравнивать

        shares = size_usdc / entry_price
        current_value = shares * book.best_bid
        loss_pct = (current_value - size_usdc) / size_usdc * 100  # отрицательное число при убытке

        if loss_pct > -abs(stop_pct):
            continue  # просадка ещё не достигла порога

        pnl_estimate = current_value - size_usdc
        # Продаём 99% расчётного количества: entry_price — это потолок
        # исполнения, а комиссия/округление могут оставить на кошельке чуть
        # меньше акций, чем size/price — тогда FOK-продажа всего объёма
        # отклонялась бы с "not enough balance". Остаток (~1%) дождётся
        # резолюции как обычно.
        sell_shares = shares * 0.99

        if dry_run:
            if not storage.settle_trade(trade_id, "STOPPED", pnl_estimate):
                continue  # рынок уже зарезолвился и сделка закрыта
            await _notify(storage.parse_market_slug(market_slug)[0], dry_run,
                f"🧪 [DRY RUN] 📉 Стоп-лосс позиции сработал бы: {market_slug} ({direction})\n"
                f"Просадка {loss_pct:.1f}% (порог {stop_pct:.0f}%) | Оценка PnL: {pnl_estimate:+.2f} USDC"
            )
            continue

        if not settings.POLY_PRIVATE_KEY:
            if trade_id not in _sl_fail_notified:
                _sl_fail_notified.add(trade_id)
                await telegram_notify.notify(
                    f"❌ Стоп-лосс сработал для {market_slug}, но POLY_PRIVATE_KEY не задан — "
                    "продать не могу. Проверь переменные окружения."
                )
            continue

        try:
            resp = await polymarket_client.place_sell_order(token_id, sell_shares, min_price=book.best_bid * 0.98)
        except Exception as exc:  # noqa: BLE001
            # Цикл крутится раз в ~2 с — сообщаем об ошибке один раз на сделку,
            # а попытки продать продолжаются молча до резолюции рынка.
            if trade_id not in _sl_fail_notified:
                _sl_fail_notified.add(trade_id)
                await telegram_notify.notify(
                    f"❌ Не удалось закрыть позицию по стоп-лоссу ({market_slug}): {exc}\n"
                    "Пробую дальше каждые ~2 с, пока рынок не закроется."
                )
            continue

        status = polymarket_client.response_field(resp, "status") or "SOLD"
        storage.settle_trade(trade_id, "STOPPED", pnl_estimate)
        await telegram_notify.notify(
            f"📉 Стоп-лосс сработал: {market_slug} ({direction}) продано по ~{book.best_bid:.3f}\n"
            f"Просадка {loss_pct:.1f}% (порог {stop_pct:.0f}%) | Оценка PnL: {pnl_estimate:+.2f} USDC | статус: {status}"
        )
