"""
Telegram-бот: форекс-сводка + прогнозы перед важными релизами + сканер сигналов.

Логика (проверяется раз в 5 минут, но реально что-то делает только по расписанию):
1. Каждый день бот смотрит календарь (ForexFactory) на сегодня.
2. Если сегодня ЕСТЬ важные (High impact) релизы по основным валютам:
   - за 1 час до КАЖДОГО такого релиза бот присылает прогноз-предположение.
   - ИСКЛЮЧЕНИЕ: за 1 час до Non-Farm Payrolls (NFP) — отдельный расширенный
     разбор (не 3 предложения, а до 6 структурированных пунктов).
3. Если сегодня важных релизов НЕТ:
   - один раз, перед открытием NYSE (9:30 по Нью-Йорку), присылает общую
     сводку факторов, которые могут повлиять на рынок сегодня.
4. /start подписывает на уведомления и сразу присылает сводку календаря на
   сегодня. /forecast, /btc, /pairs, /indices, /news, /ask — см. описания
   команд в BOT_COMMANDS ниже. /signals — сканер модели Liquidity Sweep +
   Rejection по EUR/USD, GBP/USD, USD/JPY, XAU/USD на 1H/4H (требует
   TWELVE_DATA_API_KEY).

Защита от устаревших фактов: у Claude есть дата отсечки обучающих данных —
в каждый запрос автоматически добавляется инструкция не называть людей по
имени, если оно не упомянуто в переданных актуальных данных/заголовках.

Источники данных:
- Экономический календарь: ForexFactory (кэш 15 мин).
- Новости: ForexLive, FXStreet, Investing.com, DailyFX, TradingEconomics,
  Oilprice, Kitco, MarketWatch, CNBC + пресс-релизы ФРС, ЕЦБ, Банка Англии.
- Курсы фиатных пар: Frankfurter.app (данные ЕЦБ, без ключа).
- Цена BTC: Coinbase (без ключа).
- Индексы (DAX 40, Nasdaq, S&P 500): Yahoo Finance chart API (без ключа).
- Позиционирование трейдеров: CFTC Commitment of Traders (раз в неделю).
- Свечи для сканера сигналов: Twelve Data (НУЖЕН ключ, см. ниже).

ВАЖНО: это фоновый процесс, должен работать круглосуточно на чём-то always-on
(VPS/сервер), не на ноутбуке, который выключается.

Нужные вводные:
1. BOT_TOKEN          — токен от @BotFather (обязателен).
2. ANTHROPIC_API_KEY  — ключ с platform.claude.com (опционален). Без него бот
   всё равно работает: календарь и сырые цифры отправляются как обычно,
   просто вместо AI-анализа будет пометка, что AI недоступен, и сырые данные
   без интерпретации.
3. CLAUDE_MODEL       — необязательно, по умолчанию "claude-haiku-4-5-20251001".
4. TWELVE_DATA_API_KEY — ОБЯЗАТЕЛЕН для сканера сигналов (/signals и фоновый
   market_signal_loop). Без него /signals прямо отвечает, что сканер выключен,
   а фоновый цикл сигналов вообще не запускается. Получить ключ: регистрация
   на twelvedata.com, бесплатный план обычно даёт форекс-пары; XAU/USD
   (золото) на части бесплатных планов может быть недоступен — если именно
   по золоту сигналы никогда не приходят и в логах ошибка авторизации/плана
   именно для XAU/USD, это ожидаемо для бесплатного тарифа.

Зависимости (requirements.txt):
    aiogram, aiohttp, feedparser, anthropic, deep-translator, tzdata

Запуск:
    pip install -r requirements.txt
    export BOT_TOKEN="..."
    export ANTHROPIC_API_KEY="..."
    export TWELVE_DATA_API_KEY="..."
    python forex_bot.py
"""

import asyncio
import json
import logging
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import feedparser
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, KeyboardButton, ReplyKeyboardMarkup
from anthropic import AsyncAnthropic

BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_TOKEN_HERE")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_ENABLED = bool(ANTHROPIC_API_KEY)
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-haiku-4-5-20251001")

# Рыночный сканер: реальные OHLC-свечи Twelve Data.
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")
TWELVE_DATA_URL = "https://api.twelvedata.com/time_series"
SIGNAL_SCAN_INTERVAL = 15 * 60  # API вызываем максимум раз в 15 минут
SIGNAL_SYMBOLS = ["EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD"]
SIGNAL_TIMEFRAMES = ("1h", "4h")
SIGNAL_LOOKBACK = 10            # sweep экстремума предыдущих 10 свечей
SIGNAL_EMA_PERIOD = 50          # фильтр направления
SIGNAL_ATR_PERIOD = 14
SIGNAL_RR = 2.0                 # историческая проверка TP = 2R
SIGNAL_MAX_HOLD_BARS = 20       # максимум 20 свечей на исход сделки

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
RSS_FEEDS = [
    "https://www.forexlive.com/feed/news",
    "https://www.fxstreet.com/rss/news",
    "https://www.investing.com/rss/news_25.rss",
    "https://oilprice.com/rss/main",
    "https://www.kitco.com/rss/KitcoNews.xml",
    "https://www.dailyfx.com/feeds/all",
    "https://tradingeconomics.com/rss/news.aspx",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.ecb.europa.eu/rss/press.xml",
    "https://www.bankofengland.co.uk/rss/news",
]
CRYPTO_RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]
EQUITY_RSS_FEEDS = [
    "https://www.marketwatch.com/rss/topstories",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
]
COINGECKO_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
COINGECKO_CHART_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=30&interval=daily"
COINBASE_STATS_URL = "https://api.exchange.coinbase.com/products/BTC-USD/stats"
COINBASE_CANDLES_URL = "https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=86400"

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=3mo&interval=1d"
INDEX_ASSETS = {
    "DAX40": {"symbol": "^GDAXI", "label": "DAX 40"},
    "NASDAQ": {"symbol": "^IXIC", "label": "Nasdaq Composite"},
    "SP500": {"symbol": "^GSPC", "label": "S&P 500"},
}

FRANKFURTER_URL = "https://api.frankfurter.app/{start}..{end}"

COT_DATASET_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
COT_CONTRACT_NAMES = {
    "EUR": "EURO FX",
    "GBP": "BRITISH POUND STERLING",
    "JPY": "JAPANESE YEN",
    "CHF": "SWISS FRANC",
    "AUD": "AUSTRALIAN DOLLAR",
    "CAD": "CANADIAN DOLLAR",
    "NZD": "NEW ZEALAND DOLLAR",
}
COT_CACHE_TTL = timedelta(days=1)

CALENDAR_CACHE_TTL = timedelta(minutes=15)

MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"}
IMPACT_EMOJI = {"High": "🔴", "Medium": "🟠", "Low": "🟡", "Holiday": "⚪️"}

POPULAR_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "XAUUSD", "BTCUSD"]

NY_TZ = ZoneInfo("America/New_York")
NYSE_OPEN = time(9, 30)
PRE_EVENT_LEAD = timedelta(hours=1)
PRE_NYSE_LEAD = timedelta(minutes=30)
POLL_INTERVAL = 5 * 60

DATA_DIR = Path(__file__).parent
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
STATE_FILE = DATA_DIR / "daily_state.json"
SIGNAL_STATE_FILE = DATA_DIR / "market_signal_state.json"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_calendar_cache: dict = {"data": None, "fetched_at": None}
_cot_cache: dict = {}

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if CLAUDE_ENABLED else None


# ---------- Хранилище ----------

def load_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text())
    return default


def save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def get_subscribers() -> set[int]:
    return set(load_json(SUBSCRIBERS_FILE, []))


def add_subscriber(chat_id: int) -> None:
    subs = get_subscribers()
    if chat_id not in subs:
        subs.add(chat_id)
        save_json(SUBSCRIBERS_FILE, list(subs))


def load_state() -> dict:
    state = load_json(STATE_FILE, {})
    today_str = datetime.now(NY_TZ).date().isoformat()
    if state.get("date") != today_str:
        state = {"date": today_str, "notified_events": [], "quiet_summary_sent": False}
        save_json(STATE_FILE, state)
    return state


def save_state(state: dict) -> None:
    save_json(STATE_FILE, state)


# ---------- Экономический календарь ----------

def parse_event_time(raw: str):
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


async def fetch_calendar() -> list[dict]:
    now = datetime.now(timezone.utc)
    if _calendar_cache["data"] is not None and now - _calendar_cache["fetched_at"] < CALENDAR_CACHE_TTL:
        return _calendar_cache["data"]
    async with aiohttp.ClientSession() as session:
        async with session.get(CALENDAR_URL, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()
    _calendar_cache["data"] = data
    _calendar_cache["fetched_at"] = now
    return data


def filter_today(events: list[dict], impacts: tuple[str, ...]) -> list[dict]:
    today = datetime.now(timezone.utc).date()
    result = []
    for ev in events:
        dt = parse_event_time(ev.get("date", ""))
        if dt is None or dt.astimezone(timezone.utc).date() != today:
            continue
        if ev.get("country") not in MAJOR_CURRENCIES:
            continue
        if ev.get("impact") not in impacts:
            continue
        ev = {**ev, "_dt": dt, "_id": f"{ev.get('country')}_{ev.get('title')}_{ev.get('date')}"}
        result.append(ev)
    result.sort(key=lambda e: e["_dt"])
    return result


def format_calendar_summary(events: list[dict]) -> str:
    if not events:
        return "📅 Сегодня нет значимых экономических событий по основным валютам."
    lines = ["📊 <b>Экономическая сводка на сегодня</b>\n"]
    for ev in events:
        emoji = IMPACT_EMOJI.get(ev.get("impact"), "⚪️")
        time_str = ev["_dt"].astimezone().strftime("%H:%M")
        lines.append(
            f"{emoji} <b>{time_str} {ev.get('country')}</b> — {ev.get('title')}\n"
            f"    Прогноз: {ev.get('forecast') or '—'} | Пред.: {ev.get('previous') or '—'}"
        )
    lines.append("\n<i>Источник: ForexFactory calendar</i>")
    return "\n".join(lines)


# ---------- Новости (контекст для прогнозов) ----------

def fetch_recent_headlines(limit: int = 12) -> list[str]:
    return _fetch_rss_headlines(RSS_FEEDS, limit)


def fetch_crypto_headlines(limit: int = 10) -> list[str]:
    return _fetch_rss_headlines(CRYPTO_RSS_FEEDS, limit)


def fetch_equity_headlines(limit: int = 10) -> list[str]:
    return _fetch_rss_headlines(EQUITY_RSS_FEEDS, limit)


def fetch_all_headlines(limit: int = 20) -> list[str]:
    combined = RSS_FEEDS + EQUITY_RSS_FEEDS + CRYPTO_RSS_FEEDS
    return _fetch_rss_headlines(combined, limit)


def _fetch_rss_headlines(feeds: list[str], limit: int) -> list[str]:
    headlines = []
    for url in feeds:
        try:
            feed = feedparser.parse(url)
            headlines.extend(e.get("title", "") for e in feed.entries[:8])
        except Exception:
            logger.exception(f"Не удалось прочитать RSS: {url}")
    return headlines[:limit]


def translate_to_ru(texts: list[str]) -> list[str]:
    if not texts:
        return texts
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source="auto", target="ru").translate_batch(texts)
    except Exception:
        logger.exception("Не удалось перевести заголовки, оставляю оригинал")
        return texts


# ---------- Технические индикаторы ----------

def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-period:]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def compute_ma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def compute_technicals(closes: list[float]) -> dict:
    return {
        "rsi14": compute_rsi(closes, 14),
        "ma20": compute_ma(closes, 20),
        "ma50": compute_ma(closes, 50),
    }


def format_technicals(tech: dict, decimals: int = 2) -> str:
    rsi = tech.get("rsi14")
    ma20 = tech.get("ma20")
    ma50 = tech.get("ma50")

    if rsi is not None:
        if rsi >= 70:
            rsi_note = "перекуплен"
        elif rsi <= 30:
            rsi_note = "перепродан"
        else:
            rsi_note = "нейтрально"
        rsi_line = f"RSI(14): {rsi} ({rsi_note})"
    else:
        rsi_line = "RSI(14): недостаточно данных"

    if ma20 is not None and ma50 is not None:
        trend = "восходящий (MA20 выше MA50)" if ma20 > ma50 else "нисходящий (MA20 ниже MA50)"
        ma_line = f"MA20: {ma20:,.{decimals}f} | MA50: {ma50:,.{decimals}f} — тренд {trend}"
    else:
        ma_line = "MA20/MA50: недостаточно данных"

    return f"{rsi_line}\n{ma_line}"


# ---------- Данные по биткоину ----------

async def fetch_btc_market_data() -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(COINBASE_STATS_URL, timeout=15) as resp:
            resp.raise_for_status()
            stats = await resp.json()
        async with session.get(COINBASE_CANDLES_URL, timeout=15) as resp:
            resp.raise_for_status()
            candles = await resp.json()

    current_price = float(stats["last"])
    open_price = float(stats.get("open") or current_price)
    change_24h = ((current_price - open_price) / open_price * 100) if open_price else 0.0

    candles_asc = list(reversed(candles))
    highs = [float(c[2]) for c in candles_asc]
    lows = [float(c[1]) for c in candles_asc]
    closes = [float(c[4]) for c in candles_asc]
    highs_7 = highs[-7:] if len(highs) >= 7 else highs
    lows_7 = lows[-7:] if len(lows) >= 7 else lows
    highs_30 = highs[-30:] if len(highs) >= 30 else highs
    lows_30 = lows[-30:] if len(lows) >= 30 else lows

    return {
        "price": current_price,
        "change_24h": change_24h,
        "low_7d": min(lows_7) if lows_7 else current_price,
        "high_7d": max(highs_7) if highs_7 else current_price,
        "low_30d": min(lows_30) if lows_30 else current_price,
        "high_30d": max(highs_30) if highs_30 else current_price,
        **compute_technicals(closes),
    }


def format_btc_raw(data: dict) -> str:
    base = (
        f"Текущая цена: ${data['price']:,.0f} ({data['change_24h']:+.2f}% за 24ч)\n"
        f"Диапазон за 7 дней: ${data['low_7d']:,.0f} – ${data['high_7d']:,.0f}\n"
        f"Диапазон за 30 дней: ${data['low_30d']:,.0f} – ${data['high_30d']:,.0f}"
    )
    return base + "\n" + format_technicals(data, decimals=0)


# ---------- Фондовые индексы ----------

async def fetch_index_data(symbol: str) -> dict:
    url = YAHOO_CHART_URL.format(symbol=symbol)
    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

    result = data["chart"]["result"][0]
    meta = result["meta"]
    closes = result["indicators"]["quote"][0]["close"]
    closes = [c for c in closes if c is not None]

    current = meta.get("regularMarketPrice", closes[-1] if closes else 0.0)
    prev_close = meta.get("previousClose") or meta.get("chartPreviousClose") or current
    change_pct = ((current - prev_close) / prev_close * 100) if prev_close else 0.0
    closes_7d = closes[-7:] if len(closes) >= 7 else closes
    closes_30 = closes[-30:] if len(closes) >= 30 else closes

    return {
        "price": current,
        "change_pct": change_pct,
        "low_7d": min(closes_7d) if closes_7d else current,
        "high_7d": max(closes_7d) if closes_7d else current,
        "low_30d": min(closes_30) if closes_30 else current,
        "high_30d": max(closes_30) if closes_30 else current,
        **compute_technicals(closes),
    }


def format_index_raw(label: str, data: dict) -> str:
    base = (
        f"{label}: {data['price']:,.0f} ({data['change_pct']:+.2f}% за посл. сессию)\n"
        f"Диапазон за 7 дней: {data['low_7d']:,.0f} – {data['high_7d']:,.0f}\n"
        f"Диапазон за 30 дней: {data['low_30d']:,.0f} – {data['high_30d']:,.0f}"
    )
    return base + "\n" + format_technicals(data, decimals=0)


# ---------- Реальные курсы фиатных валютных пар ----------

async def fetch_fx_price_data(base: str, quote: str) -> dict | None:
    if base in ("XAU", "BTC") or quote in ("XAU", "BTC"):
        return None
    end = date.today()
    start = end - timedelta(days=120)
    url = FRANKFURTER_URL.format(start=start.isoformat(), end=end.isoformat()) + f"?from={base}&to={quote}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

    rates = data.get("rates", {})
    if not rates:
        return None
    sorted_dates = sorted(rates.keys())
    values = [rates[d][quote] for d in sorted_dates if quote in rates[d]]
    if not values:
        return None
    values_7d = values[-7:] if len(values) >= 7 else values
    values_30 = values[-30:] if len(values) >= 30 else values

    return {
        "current": values[-1],
        "low_7d": min(values_7d),
        "high_7d": max(values_7d),
        "low_30d": min(values_30),
        "high_30d": max(values_30),
        **compute_technicals(values),
    }


def format_fx_raw(data: dict) -> str:
    base = (
        f"Курс: {data['current']:.4f}\n"
        f"Диапазон за 7 дней: {data['low_7d']:.4f} – {data['high_7d']:.4f}\n"
        f"Диапазон за 30 дней: {data['low_30d']:.4f} – {data['high_30d']:.4f}"
    )
    return base + "\n" + format_technicals(data, decimals=4)


async def fetch_cot_positioning(currency: str) -> dict | None:
    name = COT_CONTRACT_NAMES.get(currency)
    if not name:
        return None

    cached = _cot_cache.get(currency)
    now = datetime.now(timezone.utc)
    if cached and now - cached[0] < COT_CACHE_TTL:
        return cached[1]

    params = {
        "$where": f"market_and_exchange_names like '%{name}%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "1",
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(COT_DATASET_URL, params=params, timeout=15) as resp:
            resp.raise_for_status()
            rows = await resp.json()
    if not rows:
        return None

    row = rows[0]
    try:
        long_pos = int(float(row.get("noncomm_positions_long_all", 0)))
        short_pos = int(float(row.get("noncomm_positions_short_all", 0)))
    except (TypeError, ValueError):
        return None

    result = {
        "date": row.get("report_date_as_yyyy_mm_dd", "")[:10],
        "long": long_pos,
        "short": short_pos,
        "net": long_pos - short_pos,
    }
    _cot_cache[currency] = (now, result)
    return result


def format_cot_raw(currency: str, cot: dict) -> str:
    bias = "нетто-лонг" if cot["net"] > 0 else "нетто-шорт" if cot["net"] < 0 else "нейтрально"
    return (
        f"COT по {currency} ({cot['date']}): крупные спекулянты {bias}, "
        f"лонги {cot['long']:,}, шорты {cot['short']:,}, нетто {cot['net']:+,}"
    )


# ---------- Генерация прогнозов через Claude ----------

PREDICT_PROMPT = """Ты аналитик форекс-рынка. Через час выходит статистика:

Страна/валюта: {currency}
Показатель: {title}
Прогноз рынка: {forecast}
Предыдущее значение: {previous}

Свежие новостные заголовки последних часов (могут быть не по теме — учитывай
только релевантные):
{headlines}

Дай короткий прогноз по-русски, максимум 3 предложения: как этот показатель
обычно влияет на валюту, что вероятно при факте лучше/хуже прогноза, и — если
показатель по USD — коротко про золото/нефть (доллар и золото обычно
двигаются в противофазе). Учитывай релевантные заголовки, если такие есть.
Без вступлений и общих фраз — сразу суть."""

NFP_KEYWORDS = ("non-farm", "nonfarm", "nfp", "payroll")

NFP_PROMPT = """Ты старший аналитик форекс-рынка. Через час выходит отчёт по
рынку труда США (Non-Farm Payrolls) — самый влиятельный макрорелиз месяца.
Это единственный релиз, где нужен развёрнутый разбор, а не короткая сводка.

Прогноз рынка по занятости: {forecast}
Предыдущее значение: {previous}

Свежие новостные заголовки последних часов (ищи там: пересмотры прошлых
месяцев, среднюю почасовую оплату/wage growth, уровень безработицы,
labor force participation, актуальные ожидания рынка по ставке ФРС/CME
FedWatch, риторику членов ФРС в последние дни, динамику доходности гособлигаций
и индекса доллара — используй только то, что реально есть в заголовках,
не выдумывай цифры, которых там нет):
{headlines}

Дай структурированный разбор по-русски, до 6 коротких пунктов:
- Что именно ожидает рынок (не только основная цифра, но и вторичные
  показатели — безработица, зарплаты — если они упомянуты в заголовках)
- Как текущие ожидания по ставке ФРС завязаны на этот отчёт (если заголовки
  дают контекст)
- Сценарий "значительно лучше прогноза" — что вероятно с USD, золотом, S&P 500
- Сценарий "значительно хуже прогноза" — что вероятно с USD, золотом, S&P 500
- Сценарий "около прогноза" — вероятна ли волатильность всё равно
- Один рискованный момент, на который стоит обратить внимание

Без вступлений и дисклеймеров, только конкретика. Если по какому-то пункту
в заголовках нет данных — пропусти его, не выдумывай."""

QUIET_PROMPT = """Ты аналитик форекс и товарных рынков. Сегодня нет
запланированных важных экономических релизов по основным валютам.

Свежие заголовки:
{headlines}

Выбери из них только 2-3 САМЫХ значимых для рынков фактора (макроэкономика,
геополитика, решения центробанков, OPEC+, крупные корпоративные/политические
события) — остальное это шум, игнорируй. Дай короткую сводку по-русски,
максимум 3 предложения: что происходит и как это может отразиться на
долларе/основных валютах, золоте (XAU/USD) или нефти (WTI/Brent) — только
если реально релевантно, не притягивай за уши. Без вступлений, без "стоит
отметить", без "следует учитывать" и подобных оборотов — сразу суть.

Если из заголовков ничего значимого не выделяется, ответь ровно одной фразой:
"Значимых факторов не просматривается." — и не выдумывай контекст."""

FORECAST_PROMPT = """Ты аналитик форекс, товарных и фондовых рынков. Вот
экономические события на сегодня (High/Medium impact) по основным валютам:
{calendar_summary}

Свежие новостные заголовки:
{headlines}

Дай короткое настроение (бычье/медвежье/нейтральное) по каждой из позиций:
USD, EUR, GBP, JPY, CHF, AUD, CAD, NZD, Золото (XAU), Нефть (WTI/Brent),
DAX 40, Nasdaq, S&P 500.

Формат — строго по одной строке на каждую позицию, на русском:
<эмодзи 📈 или 📉 или ➡️> <Валюта/актив>: <причина в 5-10 слов>

Если по позиции нет значимых факторов сегодня — напиши "нет выраженного драйвера".
Без вступления, без заключения, без дисклеймеров — только список из 13 строк."""

SCENARIO_BLOCK = """
После анализа обязательно добавь блок СТРОГО в этом формате, без вводных
фраз, риторики и повторов того, что уже сказано выше:

<b>Возможный сценарий</b>
Направление: ЛОНГ или ШОРТ (одно слово; если сигналы явно противоречат друг
другу — оба варианта, но не более 2 строк на каждый)
Вход: <диапазон двух конкретных чисел, напр. 78,000–78,500>
Стоп: <одно конкретное число>
Тейк: <одно конкретное число, при желании через запятую 2 цели>
Причина: <не больше 12 слов — что из RSI/MA/уровней это подтверждает>

Никаких фраз вроде "стоит учитывать", "рекомендуется рассмотреть",
"необходимо помнить" — только конкретные числа и короткая причина.
Одной строкой в самом конце: "Не финансовый совет.\""""

BTC_PROMPT = """Ты крипто-аналитик. Вот реальные рыночные данные по биткоину
(включая RSI и скользящие средние):

{raw_data}

Свежие крипто-новостные заголовки:
{headlines}

Дай короткий спекулятивный анализ по-русски, максимум 3 предложения: 1 зона
поддержки, 1 зона сопротивления (конкретные числа в USD), и одна фраза про
направление на основе RSI/MA. Без вступлений и общих фраз.
""" + SCENARIO_BLOCK

INDEX_PROMPT = """Ты аналитик фондового рынка. Вот реальные данные по индексу
{label} (включая RSI и скользящие средние):

{raw_data}

Свежие заголовки по рынкам и экономике:
{headlines}

Дай короткий анализ по-русски, максимум 3 предложения: 1 зона поддержки, 1
зона сопротивления (конкретные числа), и главный фактор, который сейчас
двигает индекс (если он есть в заголовках). Без вступлений и общих фраз.
""" + SCENARIO_BLOCK

NEWS_DIGEST_PROMPT = """Ты финансовый редактор. Вот сырые заголовки за
последние часы из разных источников (форекс, макро, акции, крипто):

{headlines}

Сделай дайджест по-русски в двух блоках (используй HTML-теги <b> для
заголовков блоков). Не пересказывай все заголовки подряд — выбери только
3-5 САМЫХ значимых для рынков, остальное это шум:

<b>Главное</b>
3-5 пунктов через тире, только самое важное, БЕЗ анализа — просто факты
своими словами (переведи и объедини похожие темы в один пункт).

<b>Что это может значить</b>
Максимум 2 предложения — краткий вывод, как это может повлиять на валюты,
индексы, золото, нефть или крипту. Без вступлений и общих фраз.

Если заголовки малозначимы или это в основном шум — так и скажи одной фразой
и не выдумывай значимость."""

ASK_PROMPT = """Ты финансовый ассистент, помогаешь с вопросами о рынках
(форекс, товары, индексы, крипто) и вообще любыми вопросами пользователя.

Актуальные цифры по активу из вопроса (если распознан):
{asset_data}

Немного свежего рыночного контекста (может быть не по теме вопроса —
используй только если релевантно):
{headlines}

Вопрос пользователя: {question}

Если в разделе "Актуальные цифры" есть данные — используй именно их для ответа
на вопрос о цене/курсе, не говори, что у тебя нет доступа к текущим данным.
Ответь по-русски, по делу, без лишних вступлений. Если вопрос не по
финансовой теме — всё равно ответь как обычный полезный ассистент."""

PAIR_PROMPT = """Ты аналитик форекс-рынка. Валютная пара: {pair_label}.

События сегодня по {base}: {base_events}
События сегодня по {quote}: {quote_events}

Ценовые данные и технические индикаторы (RSI, MA20/MA50 считаются от {base}
относительно {quote}, чем выше — тем сильнее {base}):
{price_levels}

Позиционирование крупных трейдеров (COT):
{cot_info}

Свежие новостные заголовки:
{headlines}

Напиши короткий анализ по-русски, максимум 3 предложения: главный фактор,
который сейчас двигает пару, в чью пользу он складывается ({base} или
{quote}) с учётом RSI/MA20/MA50, и если есть COT — совпадает ли позиционирование
крупных игроков с этим направлением. Без вступлений и общих фраз.
""" + SCENARIO_BLOCK


NO_KEY_NOTICE = "🤖 <i>AI-анализ пока недоступен (ANTHROPIC_API_KEY не настроен/не оплачен) — ниже сырые данные без интерпретации.</i>\n\n"

FACTUAL_GUARD = (
    "\n\nВАЖНО: не полагайся на собственные знания о том, кто СЕЙЧАС занимает "
    "должности (главы центробанков, президенты, министры и т.п.) — эта "
    "информация могла устареть. Называй конкретное имя человека только если "
    "оно явно упомянуто в переданных выше данных или заголовках. Если имя не "
    "упомянуто — используй должность без имени (например, «глава ФРС», "
    "«президент США») вместо угадывания, кто это. Дата сегодня: {today}."
)


async def ask_claude(prompt: str, fallback: str, max_tokens: int = 500) -> str:
    if not CLAUDE_ENABLED:
        return NO_KEY_NOTICE + fallback
    guarded_prompt = prompt + FACTUAL_GUARD.format(today=date.today().isoformat())
    try:
        resp = await claude.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": guarded_prompt}],
        )
        return resp.content[0].text.strip()
    except Exception:
        logger.exception("Ошибка запроса к Claude, отдаю сырые данные")
        return NO_KEY_NOTICE + fallback


async def send_to_subscribers(text: str) -> None:
    for chat_id in get_subscribers():
        try:
            await bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
        except Exception:
            logger.exception(f"Не удалось отправить сообщение в {chat_id}")


# ---------- Анализ по конкретной валютной паре ----------

def parse_pair(raw: str) -> tuple[str, str] | None:
    cleaned = raw.strip().upper().replace("/", "").replace(" ", "").replace("-", "")
    if len(cleaned) != 6:
        return None
    base, quote = cleaned[:3], cleaned[3:]
    known = MAJOR_CURRENCIES | {"XAU", "BTC"}
    if base not in known or quote not in known:
        return None
    return base, quote


def events_summary_for(events: list[dict], currency: str) -> str:
    relevant = [e for e in events if e.get("country") == currency]
    if not relevant:
        return "нет значимых событий сегодня"
    parts = []
    for e in relevant:
        parts.append(f"{e.get('title')} (прогноз {e.get('forecast') or '—'}, пред. {e.get('previous') or '—'})")
    return "; ".join(parts)


async def build_pair_analysis(base: str, quote: str) -> str:
    pair_label = f"{base}/{quote}"
    try:
        raw_events = await fetch_calendar()
        events = filter_today(raw_events, ("High", "Medium"))
    except Exception:
        events = []

    base_events = events_summary_for(events, base)
    quote_events = events_summary_for(events, quote)

    price_notes = []
    if "BTC" in (base, quote):
        try:
            btc_data = await fetch_btc_market_data()
            price_notes.append(format_btc_raw(btc_data))
        except Exception:
            logger.exception("Не удалось получить данные BTC для анализа пары")
    else:
        try:
            fx_data = await fetch_fx_price_data(base, quote)
            if fx_data:
                price_notes.append(format_fx_raw(fx_data))
        except Exception:
            logger.exception("Не удалось получить курс для пары")

    cot_notes = []
    for ccy in (base, quote):
        try:
            cot = await fetch_cot_positioning(ccy)
        except Exception:
            logger.exception(f"Не удалось получить COT для {ccy}")
            cot = None
        if cot:
            cot_notes.append(format_cot_raw(ccy, cot))

    headlines = fetch_crypto_headlines() if "BTC" in (base, quote) else fetch_recent_headlines()

    price_levels_text = "\n".join(price_notes) or "нет данных по цене"
    cot_text = "\n".join(cot_notes) or "нет данных по позиционированию"

    prompt = PAIR_PROMPT.format(
        pair_label=pair_label,
        base=base,
        quote=quote,
        base_events=base_events,
        quote_events=quote_events,
        price_levels=price_levels_text,
        cot_info=cot_text,
        headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
    )
    fallback = (
        f"По {base}: {base_events}\n"
        f"По {quote}: {quote_events}\n"
        f"{price_levels_text}\n"
        f"{cot_text}"
    )
    analysis = await ask_claude(prompt, fallback, max_tokens=650)
    return f"💱 <b>{pair_label}</b>\n\n{analysis}"


def pairs_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=f"{p[:3]}/{p[3:]}", callback_data=f"pair:{p}")
        for p in POPULAR_PAIRS
    ]
    rows = [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- Рыночный сигнальщик: Liquidity Sweep + Rejection ----------

def _f(value) -> float:
    return float(value)


def _ema(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if len(values) < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    prev = seed
    for i in range(period, len(values)):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def _atr(candles: list[dict], period: int = SIGNAL_ATR_PERIOD) -> list[float | None]:
    trs: list[float] = []
    for i, c in enumerate(candles):
        if i == 0:
            tr = c["high"] - c["low"]
        else:
            pc = candles[i - 1]["close"]
            tr = max(c["high"] - c["low"], abs(c["high"] - pc), abs(c["low"] - pc))
        trs.append(tr)
    out: list[float | None] = [None] * len(candles)
    for i in range(period - 1, len(candles)):
        out[i] = sum(trs[i - period + 1:i + 1]) / period
    return out


async def fetch_twelve_1h(symbol: str, outputsize: int = 5000) -> list[dict]:
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("TWELVE_DATA_API_KEY не задан")
    params = {
        "symbol": symbol,
        "interval": "1h",
        "outputsize": outputsize,
        "timezone": "UTC",
        "order": "asc",
        "apikey": TWELVE_DATA_API_KEY,
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(TWELVE_DATA_URL, params=params, timeout=25) as resp:
            resp.raise_for_status()
            data = await resp.json()
    if data.get("status") == "error" or "values" not in data:
        raise RuntimeError(data.get("message") or f"Twelve Data: {data}")
    now = datetime.now(timezone.utc)
    candles = []
    for v in data["values"]:
        dt = datetime.fromisoformat(v["datetime"]).replace(tzinfo=timezone.utc)
        if dt + timedelta(hours=1) > now:
            continue
        candles.append({
            "dt": dt,
            "open": _f(v["open"]), "high": _f(v["high"]),
            "low": _f(v["low"]), "close": _f(v["close"]),
        })
    return candles


def aggregate_4h(candles: list[dict]) -> list[dict]:
    groups: dict[datetime, list[dict]] = {}
    for c in candles:
        dt = c["dt"]
        start = dt.replace(hour=(dt.hour // 4) * 4, minute=0, second=0, microsecond=0)
        groups.setdefault(start, []).append(c)
    out = []
    for start in sorted(groups):
        g = sorted(groups[start], key=lambda x: x["dt"])
        if len(g) != 4 or [x["dt"].hour for x in g] != [(start.hour + i) % 24 for i in range(4)]:
            continue
        out.append({"dt": start, "open": g[0]["open"], "high": max(x["high"] for x in g),
                    "low": min(x["low"] for x in g), "close": g[-1]["close"]})
    return out


def detect_sweep(candles: list[dict], i: int, emas: list[float | None], atrs: list[float | None]) -> dict | None:
    """Объективная модель: sweep предыдущего N-bar high/low + возврат + EMA50.

    ВАЖНО: emas/atrs передаются уже посчитанными на весь ряд заранее.
    Раньше эта функция сама пересчитывала EMA/ATR по ВСЕМУ массиву при
    каждом вызове — а backtest_sweeps вызывает её на каждую из тысяч свечей,
    так что расчёт превращался в O(n²): пересчёт O(n) внутри цикла из n
    итераций. На 5000 свечах это десятки миллионов операций на чистом
    Python — синхронный CPU-bound код, который блокирует весь event loop
    целиком (не только эту команду, а вообще всего бота), и именно поэтому
    /signals "зависал" без вообще какого-либо ответа: asyncio.wait_for не
    может прервать код, если у него нет ни одной точки await, где event loop
    мог бы вклиниться и проверить дедлайн. Теперь emas/atrs считаются один
    раз снаружи и просто передаются сюда — вся функция стала O(1) на вызов,
    а весь бэктест — O(n) вместо O(n²)."""
    if i < max(SIGNAL_LOOKBACK, SIGNAL_EMA_PERIOD, SIGNAL_ATR_PERIOD):
        return None
    c = candles[i]
    rng = c["high"] - c["low"]
    if rng <= 0 or emas[i] is None or atrs[i] is None:
        return None
    prev = candles[i - SIGNAL_LOOKBACK:i]
    prev_high = max(x["high"] for x in prev)
    prev_low = min(x["low"] for x in prev)
    close_pos = (c["close"] - c["low"]) / rng

    if c["high"] > prev_high and c["close"] < prev_high and close_pos <= 0.45 and c["close"] < emas[i]:
        entry = c["close"]
        stop = c["high"] + 0.10 * atrs[i]
        risk = stop - entry
        if risk > 0:
            return {"side": "SHORT", "level": prev_high, "extreme": c["high"], "entry": entry,
                    "stop": stop, "target": entry - SIGNAL_RR * risk, "risk": risk}

    if c["low"] < prev_low and c["close"] > prev_low and close_pos >= 0.55 and c["close"] > emas[i]:
        entry = c["close"]
        stop = c["low"] - 0.10 * atrs[i]
        risk = entry - stop
        if risk > 0:
            return {"side": "LONG", "level": prev_low, "extreme": c["low"], "entry": entry,
                    "stop": stop, "target": entry + SIGNAL_RR * risk, "risk": risk}
    return None


def backtest_sweeps(candles: list[dict]) -> dict:
    """Консервативный backtest: если SL и TP внутри одной свечи — считаем SL первым.
    EMA/ATR считаем один раз на весь ряд (не на каждую итерацию, см. detect_sweep)."""
    closes = [c["close"] for c in candles]
    emas = _ema(closes, SIGNAL_EMA_PERIOD)
    atrs = _atr(candles)

    results = []
    start = max(SIGNAL_LOOKBACK, SIGNAL_EMA_PERIOD, SIGNAL_ATR_PERIOD)
    for i in range(start, len(candles) - 1):
        sig = detect_sweep(candles, i, emas, atrs)
        if not sig:
            continue
        outcome = None
        for j in range(i + 1, min(len(candles), i + 1 + SIGNAL_MAX_HOLD_BARS)):
            b = candles[j]
            if sig["side"] == "LONG":
                stop_hit, tp_hit = b["low"] <= sig["stop"], b["high"] >= sig["target"]
            else:
                stop_hit, tp_hit = b["high"] >= sig["stop"], b["low"] <= sig["target"]
            if stop_hit:
                outcome = -1.0
                break
            if tp_hit:
                outcome = SIGNAL_RR
                break
        if outcome is not None:
            results.append(outcome)
    wins = [r for r in results if r > 0]
    losses = [r for r in results if r < 0]
    gross_win, gross_loss = sum(wins), abs(sum(losses))
    pf = gross_win / gross_loss if gross_loss else (999.0 if gross_win else 0.0)
    equity = peak = max_dd = 0.0
    for r in results:
        equity += r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "trades": len(results), "win_rate": (100 * len(wins) / len(results)) if results else 0.0,
        "pf": pf, "avg_r": (sum(results) / len(results)) if results else 0.0, "max_dd_r": max_dd,
    }


def _price(v: float, symbol: str) -> str:
    if "JPY" in symbol:
        return f"{v:.3f}"
    if "XAU" in symbol:
        return f"{v:.2f}"
    return f"{v:.5f}"


def signal_message(symbol: str, tf: str, candle: dict, sig: dict, stats: dict) -> str:
    side_emoji = "🟢" if sig["side"] == "LONG" else "🔴"
    return (
        f"{side_emoji} <b>{symbol} · {sig['side']} · {tf.upper()}</b>\n"
        f"Модель: Liquidity Sweep + Rejection + EMA50\n"
        f"Свеча: {candle['dt'].strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"Снят уровень: {_price(sig['level'], symbol)} | экстремум: {_price(sig['extreme'], symbol)}\n"
        f"Закрытие / вход: <b>{_price(sig['entry'], symbol)}</b>\n\n"
        f"🛑 SL: {_price(sig['stop'], symbol)}\n"
        f"🎯 TP (2R): {_price(sig['target'], symbol)}\n\n"
        f"📊 <b>История на загруженных свечах</b>\n"
        f"Сделок: {stats['trades']} | Win rate: {stats['win_rate']:.1f}%\n"
        f"PF: {stats['pf']:.2f} | Avg: {stats['avg_r']:+.2f}R | Max DD: {stats['max_dd_r']:.1f}R\n"
        f"<i>Статистика не гарантирует будущий результат.</i>"
    )


def load_signal_state() -> dict:
    return load_json(SIGNAL_STATE_FILE, {"sent": []})


def save_signal_state(state: dict) -> None:
    state["sent"] = state.get("sent", [])[-500:]
    save_json(SIGNAL_STATE_FILE, state)


async def scan_market_signals(send: bool = True) -> list[str]:
    """Один проход по 4 инструментам. EMA/ATR считаем один раз на серию
    и переиспользуем и для live-проверки, и внутри backtest_sweeps."""
    if not TWELVE_DATA_API_KEY:
        return []
    state = load_signal_state()
    sent = set(state.get("sent", []))
    found: list[str] = []
    for symbol in SIGNAL_SYMBOLS:
        try:
            h1 = await fetch_twelve_1h(symbol)
            datasets = {"1h": h1, "4h": aggregate_4h(h1)}
            for tf, candles in datasets.items():
                if len(candles) < SIGNAL_EMA_PERIOD + 5:
                    continue
                closes = [c["close"] for c in candles]
                emas = _ema(closes, SIGNAL_EMA_PERIOD)
                atrs = _atr(candles)
                i = len(candles) - 1
                sig = detect_sweep(candles, i, emas, atrs)
                if not sig:
                    continue
                key = f"{symbol}|{tf}|{candles[i]['dt'].isoformat()}|{sig['side']}"
                if key in sent:
                    continue
                stats = backtest_sweeps(candles)
                msg = signal_message(symbol, tf, candles[i], sig, stats)
                found.append(msg)
                if send:
                    await send_to_subscribers(msg)
                    sent.add(key)
        except Exception:
            logger.exception(f"Ошибка market scanner: {symbol}")
    state["sent"] = list(sent)
    save_signal_state(state)
    return found


async def market_signal_loop() -> None:
    if not TWELVE_DATA_API_KEY:
        logger.warning("TWELVE_DATA_API_KEY не задан — market scanner отключён")
        return
    await asyncio.sleep(10)
    while True:
        try:
            await scan_market_signals(send=True)
        except Exception:
            logger.exception("Ошибка цикла market scanner")
        await asyncio.sleep(SIGNAL_SCAN_INTERVAL)


# ---------- Постоянная клавиатура ----------

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📡 Сигналы"), KeyboardButton(text="🔮 Прогноз")],
            [KeyboardButton(text="📈 Индексы"), KeyboardButton(text="₿ Биткоин")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )

# ---------- Хендлеры бота ----------

@dp.message(CommandStart())
async def on_start(message: Message) -> None:
    add_subscriber(message.chat.id)
    await message.answer("Подписал на уведомления. Собираю сводку на сегодня...", reply_markup=main_keyboard())
    try:
        raw_events = await fetch_calendar()
    except Exception as e:
        await message.answer(f"Не удалось получить календарь: {e}")
        return
    events = filter_today(raw_events, ("High", "Medium"))
    await message.answer(format_calendar_summary(events), parse_mode="HTML")


async def handle_signals_command(message: Message) -> None:
    logger.info("SIGNALS COMMAND RECEIVED chat_id=%s text=%r", message.chat.id, message.text)
    if not TWELVE_DATA_API_KEY:
        await message.answer("TWELVE_DATA_API_KEY не настроен — сканер сигналов выключен.")
        return

    await message.answer("🔎 Команда /signals получена. Проверяю EUR/USD, GBP/USD, USD/JPY и XAU/USD на 1H/4H...")
    try:
        found = await asyncio.wait_for(scan_market_signals(send=False), timeout=90)
    except asyncio.TimeoutError:
        logger.exception("SIGNALS scanner timeout")
        await message.answer("⚠️ Сканер не успел получить рыночные данные за 90 секунд. Попробуй /signals ещё раз.")
        return
    except Exception as e:
        logger.exception("SIGNALS scanner failed")
        await message.answer(f"⚠️ Ошибка сканера сигналов: {type(e).__name__}: {e}")
        return

    if not found:
        await message.answer("Сейчас новой подтверждённой модели Liquidity Sweep + Rejection нет.")
    else:
        for msg in found:
            await message.answer(msg, parse_mode="HTML")


@dp.message(F.text == "📡 Сигналы")
async def on_signals_button(message: Message) -> None:
    await handle_signals_command(message)


@dp.message(Command("signals"))
async def on_signals(message: Message) -> None:
    await handle_signals_command(message)


@dp.message(F.text == "🔮 Прогноз")
async def on_forecast_button(message: Message) -> None:
    await on_forecast(message)


@dp.message(F.text == "📈 Индексы")
async def on_indices_button(message: Message) -> None:
    await on_indices(message)


@dp.message(F.text == "₿ Биткоин")
async def on_btc_button(message: Message) -> None:
    await on_btc(message)


@dp.message(Command("forecast"))
async def on_forecast(message: Message) -> None:
    await message.answer("Строю прогноз по валютам, золоту, нефти и индексам...")
    try:
        raw_events = await fetch_calendar()
    except Exception as e:
        await message.answer(f"Не удалось получить календарь: {e}")
        return
    events = filter_today(raw_events, ("High", "Medium"))
    headlines = fetch_recent_headlines() + fetch_equity_headlines()
    prompt = FORECAST_PROMPT.format(
        calendar_summary=format_calendar_summary(events),
        headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
    )
    fallback = format_calendar_summary(events)
    forecast = await ask_claude(prompt, fallback)
    await message.answer(f"🔮 <b>Быстрый прогноз по рынкам</b>\n\n{forecast}", parse_mode="HTML")


@dp.message(Command("btc"))
async def on_btc(message: Message) -> None:
    logger.info(f"/btc от {message.chat.id}")
    await message.answer("Собираю данные по биткоину...")
    try:
        data = await fetch_btc_market_data()
    except Exception as e:
        logger.exception("Ошибка получения данных BTC")
        await message.answer(f"Не удалось получить данные по BTC: {e}")
        return
    headlines = fetch_crypto_headlines()
    raw_data = format_btc_raw(data)
    prompt = BTC_PROMPT.format(
        raw_data=raw_data,
        headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
    )
    analysis = await ask_claude(prompt, raw_data, max_tokens=650)
    text = f"₿ <b>Биткоин: ${data['price']:,.0f} ({data['change_24h']:+.2f}% 24ч)</b>\n\n{analysis}"
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("pairs"))
async def on_pairs(message: Message) -> None:
    await message.answer("Выбери пару:", reply_markup=pairs_keyboard())


async def build_index_analysis(key: str) -> str:
    asset = INDEX_ASSETS[key]
    data = await fetch_index_data(asset["symbol"])
    raw_data = format_index_raw(asset["label"], data)
    headlines = fetch_equity_headlines() + fetch_recent_headlines(limit=6)
    prompt = INDEX_PROMPT.format(
        label=asset["label"],
        raw_data=raw_data,
        headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
    )
    analysis = await ask_claude(prompt, raw_data, max_tokens=650)
    return f"📈 <b>{asset['label']}: {data['price']:,.0f} ({data['change_pct']:+.2f}%)</b>\n\n{analysis}"


def indices_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(text=asset["label"], callback_data=f"index:{key}")
        for key, asset in INDEX_ASSETS.items()
    ]
    return InlineKeyboardMarkup(inline_keyboard=[buttons])


@dp.message(Command("indices"))
async def on_indices(message: Message) -> None:
    await message.answer("Выбери индекс:", reply_markup=indices_keyboard())


@dp.callback_query(F.data.startswith("index:"))
async def on_index_callback(callback: CallbackQuery) -> None:
    key = callback.data.split(":", 1)[1]
    await callback.answer()
    if key not in INDEX_ASSETS:
        return
    await callback.message.answer(f"Собираю данные по {INDEX_ASSETS[key]['label']}...")
    try:
        text = await build_index_analysis(key)
    except Exception as e:
        logger.exception("Ошибка анализа индекса")
        await callback.message.answer(f"Не удалось получить данные: {e}")
        return
    await callback.message.answer(text, parse_mode="HTML")


@dp.message(Command("news"))
async def on_news(message: Message) -> None:
    await message.answer("Собираю дайджест новостей...")
    headlines = fetch_all_headlines()
    if not headlines:
        await message.answer("Не удалось получить свежие заголовки.")
        return
    if CLAUDE_ENABLED:
        prompt = NEWS_DIGEST_PROMPT.format(headlines="\n".join(f"- {h}" for h in headlines))
        digest = await ask_claude(prompt, "")
    else:
        translated = translate_to_ru(headlines)
        digest = NO_KEY_NOTICE + "<b>Главное (сырые заголовки)</b>\n" + "\n".join(f"- {h}" for h in translated)
    await message.answer(f"🗞 <b>Дайджест новостей</b>\n\n{digest}", parse_mode="HTML")


async def gather_asset_context(question: str) -> tuple[str, list[str]]:
    q = question.lower()
    parts = []
    errors = []

    if any(k in q for k in ("btc", "биткоин", "битко", "bitcoin")):
        try:
            data = await fetch_btc_market_data()
            parts.append("Биткоин (реальные данные):\n" + format_btc_raw(data))
        except Exception as e:
            logger.exception("Не удалось получить BTC для /ask")
            errors.append(f"BTC: {e}")

    index_map = [
        (("dax",), "DAX40"),
        (("nasdaq", "насдак"), "NASDAQ"),
        (("s&p", "sp500", "s&p500", "спп 500", "снп 500"), "SP500"),
    ]
    seen_indices = set()
    for keywords, key in index_map:
        if key in seen_indices or key in q or any(k in q for k in keywords):
            if key in seen_indices:
                continue
            seen_indices.add(key)
            try:
                asset = INDEX_ASSETS[key]
                data = await fetch_index_data(asset["symbol"])
                parts.append(f"{asset['label']} (реальные данные):\n" + format_index_raw(asset["label"], data))
            except Exception as e:
                logger.exception(f"Не удалось получить индекс {key} для /ask")
                errors.append(f"{key}: {e}")

    match = re.search(r"\b([a-zA-Z]{3})\s*/?\s*([a-zA-Z]{3})\b", question)
    if match:
        pair = parse_pair(match.group(1) + match.group(2))
        if pair and "BTC" not in pair:
            try:
                fx = await fetch_fx_price_data(*pair)
                if fx:
                    parts.append(f"{pair[0]}/{pair[1]} (реальный курс):\n" + format_fx_raw(fx))
            except Exception as e:
                logger.exception("Не удалось получить курс пары для /ask")
                errors.append(f"{pair[0]}/{pair[1]}: {e}")

    return "\n\n".join(parts), errors


async def answer_question(message: Message, question: str) -> None:
    if not question.strip():
        await message.answer("Напиши вопрос после команды, например: /ask что будет с долларом на этой неделе?")
        return
    asset_data, errors = await gather_asset_context(question)
    if errors and not asset_data:
        await message.answer("Не удалось получить актуальные данные: " + "; ".join(errors))
        return
    headlines = fetch_recent_headlines(limit=8)
    prompt = ASK_PROMPT.format(
        asset_data=asset_data or "нет данных по конкретному активу — вопрос, видимо, не о цене конкретного инструмента",
        headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
        question=question.strip(),
    )
    fallback_parts = ["AI-ответ недоступен без ANTHROPIC_API_KEY."]
    if asset_data:
        fallback_parts.append(asset_data)
    fallback_parts.append("Свежие заголовки:\n" + ("\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков"))
    fallback = "\n\n".join(fallback_parts)
    answer = await ask_claude(prompt, fallback)
    await message.answer(answer, parse_mode="HTML")


@dp.message(Command("ask"))
async def on_ask(message: Message) -> None:
    parts = message.text.split(maxsplit=1)
    question = parts[1] if len(parts) > 1 else ""
    await answer_question(message, question)


@dp.message(F.text & ~F.text.startswith("/"))
async def on_free_text(message: Message) -> None:
    await answer_question(message, message.text)


@dp.callback_query(F.data.startswith("pair:"))
async def on_pair_callback(callback: CallbackQuery) -> None:
    code = callback.data.split(":", 1)[1]
    pair = parse_pair(code)
    await callback.answer()
    if pair is None:
        return
    await callback.message.answer(f"Собираю анализ по {pair[0]}/{pair[1]}...")
    text = await build_pair_analysis(*pair)
    await callback.message.answer(text, parse_mode="HTML")


# ---------- Планировщик ----------

async def scheduler_loop() -> None:
    while True:
        try:
            state = load_state()
            raw_events = await fetch_calendar()
            high_events = filter_today(raw_events, ("High",))
            now_utc = datetime.now(timezone.utc)

            if high_events:
                for ev in high_events:
                    if ev["_id"] in state["notified_events"]:
                        continue
                    trigger_at = ev["_dt"].astimezone(timezone.utc) - PRE_EVENT_LEAD
                    event_time = ev["_dt"].astimezone(timezone.utc)
                    if now_utc > event_time:
                        state["notified_events"].append(ev["_id"])
                        save_state(state)
                    elif now_utc >= trigger_at:
                        headlines = fetch_recent_headlines()
                        raw_fallback = (
                            f"Прогноз рынка: {ev.get('forecast') or 'нет данных'}\n"
                            f"Предыдущее значение: {ev.get('previous') or 'нет данных'}"
                        )
                        is_nfp = any(kw in ev.get("title", "").lower() for kw in NFP_KEYWORDS)
                        if is_nfp:
                            prediction = await ask_claude(NFP_PROMPT.format(
                                forecast=ev.get("forecast") or "нет данных",
                                previous=ev.get("previous") or "нет данных",
                                headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
                            ), raw_fallback, max_tokens=700)
                        else:
                            prediction = await ask_claude(PREDICT_PROMPT.format(
                                currency=ev.get("country"),
                                title=ev.get("title"),
                                forecast=ev.get("forecast") or "нет данных",
                                previous=ev.get("previous") or "нет данных",
                                headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
                            ), raw_fallback)
                        time_str = ev["_dt"].astimezone().strftime("%H:%M")
                        emoji = "🇺🇸" if is_nfp else "🔮"
                        text = (
                            f"{emoji} <b>Через час: {ev.get('country')} — {ev.get('title')} ({time_str})</b>\n\n"
                            f"{prediction}"
                        )
                        await send_to_subscribers(text)
                        state["notified_events"].append(ev["_id"])
                        save_state(state)
            else:
                ny_now = datetime.now(NY_TZ)
                if ny_now.weekday() >= 5:
                    if not state["quiet_summary_sent"]:
                        state["quiet_summary_sent"] = True
                        save_state(state)
                else:
                    market_open_ny = datetime.combine(ny_now.date(), NYSE_OPEN, tzinfo=NY_TZ)
                    trigger_at_ny = market_open_ny - PRE_NYSE_LEAD
                    if state["quiet_summary_sent"]:
                        pass
                    elif ny_now > market_open_ny:
                        state["quiet_summary_sent"] = True
                        save_state(state)
                    elif ny_now >= trigger_at_ny:
                        headlines = fetch_recent_headlines()
                        fallback_headlines = headlines if CLAUDE_ENABLED else translate_to_ru(headlines)
                        fallback = "Важных релизов сегодня нет. Свежие заголовки:\n" + (
                            "\n".join(f"- {h}" for h in fallback_headlines) or "нет свежих заголовков"
                        )
                        summary = await ask_claude(QUIET_PROMPT.format(
                            headlines="\n".join(f"- {h}" for h in headlines) or "нет свежих заголовков",
                        ), fallback)
                        text = "🗞 <b>Сводка перед открытием NYSE</b>\n\n" + summary
                        await send_to_subscribers(text)
                        state["quiet_summary_sent"] = True
                        save_state(state)

            logger.info("Цикл планировщика завершён")
        except Exception:
            logger.exception("Ошибка в планировщике")

        await asyncio.sleep(POLL_INTERVAL)


BOT_COMMANDS = [
    BotCommand(command="start", description="Подписаться и получить сводку на сегодня"),
    BotCommand(command="forecast", description="Быстрый прогноз по валютам, золоту, нефти, индексам"),
    BotCommand(command="signals", description="Проверить торговые сигналы 1H/4H"),
    BotCommand(command="pairs", description="Кнопки: анализ по валютной паре"),
    BotCommand(command="indices", description="Кнопки: DAX 40 / Nasdaq / S&P 500"),
    BotCommand(command="btc", description="Разбор биткоина: поддержка/сопротивление"),
    BotCommand(command="news", description="Дайджест новостей: главное + что это значит"),
    BotCommand(command="ask", description="Задать любой вопрос боту"),
]


async def main() -> None:
    print("FOREXFX SIGNALS FIX BUILD (O(n) backtest)", flush=True)
    if BOT_TOKEN == "PUT_YOUR_TOKEN_HERE":
        raise RuntimeError("Установите переменную окружения BOT_TOKEN")
    if not CLAUDE_ENABLED:
        logger.warning("ANTHROPIC_API_KEY не задан — бот работает без AI-анализа, только сырые данные")
    if not TWELVE_DATA_API_KEY:
        logger.warning("TWELVE_DATA_API_KEY не задан — сканер сигналов (/signals) будет отвечать, что выключен")
    try:
        await bot.delete_my_commands()
    except Exception:
        logger.exception("Could not clear Telegram command menu")
    await bot.set_my_commands(BOT_COMMANDS)

    logger.info("Telegram menu refreshed; persistent keyboard enabled")

    asyncio.create_task(scheduler_loop())
    asyncio.create_task(market_signal_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
