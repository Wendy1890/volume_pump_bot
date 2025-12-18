import os
import time
import asyncio
import logging
from typing import Dict, List, Set

import ccxt
import pandas as pd

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

TG_TOKEN = os.getenv("TG_TOKEN", "")

PROXY_HOST = os.getenv("PROXY_HOST", "")
PROXY_PORT = os.getenv("PROXY_PORT", "")
PROXY_USER = os.getenv("PROXY_USER", "")
PROXY_PASS = os.getenv("PROXY_PASS", "")

if PROXY_HOST and PROXY_PORT:
    if PROXY_USER and PROXY_PASS:
        PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    else:
        PROXY_URL = f"http://{PROXY_HOST}:{PROXY_PORT}"
else:
    PROXY_URL = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
LOG = logging.getLogger("volpump")

MIN_VOL_USD_LAST = 20_000_000
MIN_VOL_RATIO = 5.0
HEARTBEAT_INTERVAL = 1 * 3600

AUTO_STATE: Dict[int, dict] = {}
PREVIOUS_SYMBOLS_CACHE: Dict[int, Set[str]] = {}

COMMON = {"enableRateLimit": True, "timeout": 15000}

EX_BYBIT = ccxt.bybit(COMMON)
EX_BYBIT.options.setdefault("defaultType", "swap")

EX_BINANCE = ccxt.binance(COMMON)
EX_BINANCE.options.setdefault("defaultType", "future")

EX_MEXC = ccxt.mexc(COMMON)
EX_MEXC.options.setdefault("defaultType", "swap")

EXCHANGES = {
    "Bybit": EX_BYBIT,
    "Binance": EX_BINANCE,
    "MEXC": EX_MEXC,
}

EXCHANGE_URLS = {
    "Binance": "https://www.binance.com/en/futures/{symbol}",
    "Bybit": "https://www.bybit.com/trade/usdt/{symbol}",
    "MEXC": "https://www.mexc.com/exchange/{symbol}",
}


def get_base_symbol(symbol: str) -> str:
    """Return the base asset from a trading pair (e.g. 'BTC/USDT' -> 'BTC')."""
    return symbol.split("/")[0]


def format_symbol_for_exchange(ex_name: str, symbol: str) -> str:
    """Format symbol according to a specific exchange URL pattern."""
    if ex_name == "Binance":
        return symbol.replace("/", "").replace(":", "")
    elif ex_name == "Bybit":
        return symbol.replace("/", "").replace(":", "")
    elif ex_name == "MEXC":
        return symbol.replace("/", "_").replace(":", "_")
    else:
        return symbol.replace("/", "")


def get_exchange_url(ex_name: str, symbol: str) -> str:
    """Return an exchange trading URL for the given symbol."""
    formatted_symbol = format_symbol_for_exchange(ex_name, symbol)
    if ex_name in EXCHANGE_URLS:
        return EXCHANGE_URLS[ex_name].format(symbol=formatted_symbol)
    return "#"


def get_usdt_futures(ex):
    out = []
    markets = ex.load_markets()
    for s, m in markets.items():
        if (m.get("swap") or m.get("future")) and m.get("linear") and m.get("quote") == "USDT":
            out.append(s)
    return out


def fetch_vol_48h(ex, symbol):

    try:
        data = ex.fetch_ohlcv(symbol, "1h", limit=48)
    except Exception:
        return None

    if not data or len(data) < 24:
        return None

    df = pd.DataFrame(data, columns=["time", "open", "high", "low", "close", "volume"])
    df["usd_vol"] = df["volume"] * df["close"]

    if len(df) >= 48:
        prev = df.iloc[:24]
        last = df.iloc[24:48]
    else:
        prev = df.iloc[:-24]
        last = df.iloc[-24:]

    prev_vol = float(prev["usd_vol"].sum())
    last_vol = float(last["usd_vol"].sum())

    if prev_vol <= 0:
        return None

    ratio = last_vol / prev_vol
    return {
        "prev_vol": prev_vol,
        "last_vol": last_vol,
        "ratio": ratio,
    }


def symbol_link(ex_name, symbol, is_new=False):

    base = symbol.split("/")[0] + "USDT"
    prefix = ex_name.upper()
    tv_url = f"https://www.tradingview.com/chart/?symbol={prefix}:{base}.P"
    exchange_url = get_exchange_url(ex_name, symbol)

    if is_new:
        return f"🆕 <b>{base}</b> (<a href=\"{tv_url}\">TV</a> • <a href=\"{exchange_url}\">{ex_name}</a>)"
    return f"<b>{base}</b> (<a href=\"{tv_url}\">TV</a> • <a href=\"{exchange_url}\">{ex_name}</a>)"


def deduplicate_by_best(hits: List[dict]) -> List[dict]:
    """
    Deduplicate hits by base coin, keeping only the best (max ratio) per coin.
    """
    best_by_coin: Dict[str, dict] = {}

    for hit in hits:
        base_coin = get_base_symbol(hit["symbol"])
        if base_coin not in best_by_coin:
            best_by_coin[base_coin] = hit
        else:
            if hit["ratio"] > best_by_coin[base_coin]["ratio"]:
                best_by_coin[base_coin] = hit

    return list(best_by_coin.values())


async def scan_exchange(ex_name, ex, previous_symbols: Set[str]):

    results: List[dict] = []
    symbols = get_usdt_futures(ex)

    try:
        tickers = await asyncio.to_thread(ex.fetch_tickers)
    except Exception as e:
        LOG.error(f"[{ex_name}] fetch_tickers error: {e}")
        return results

    candidates: List[str] = []

    for s in symbols:
        t = tickers.get(s)
        if not t:
            continue

        last = t.get("last") or t.get("close")
        if last is None:
            continue

        vol_quote = t.get("quoteVolume")
        if vol_quote is None:
            base_vol = t.get("baseVolume")
            if base_vol is not None:
                vol_quote = base_vol * last

        if vol_quote is None:
            continue

        if vol_quote >= MIN_VOL_USD_LAST:
            candidates.append(s)

    LOG.info(f"[{ex_name}] Candidates after volume prefilter: {len(candidates)} of {len(symbols)}")

    for s in candidates:
        data = await asyncio.to_thread(fetch_vol_48h, ex, s)
        if not data:
            continue

        if data["last_vol"] >= MIN_VOL_USD_LAST and data["ratio"] >= MIN_VOL_RATIO:
            base_coin = get_base_symbol(s)
            is_new = base_coin not in previous_symbols

            results.append({
                "exchange": ex_name,
                "symbol": s,
                "base_coin": base_coin,
                "ratio": data["ratio"],
                "last_vol": data["last_vol"],
                "prev_vol": data["prev_vol"],
                "is_new": is_new,
            })

    return results


async def autoscan_worker(context: ContextTypes.DEFAULT_TYPE):

    job = context.job
    chat_id = job.chat_id

    state = AUTO_STATE.get(chat_id)
    if not state:
        return

    now = int(time.time())
    hour_sec = 3600
    cur_hour_index = now // hour_sec
    last_closed_hour_ts = (cur_hour_index - 1) * hour_sec

    last_done_ts = state.get("last_hour_ts", 0)
    if last_done_ts == last_closed_hour_ts:
        return

    state["last_hour_ts"] = last_closed_hour_ts

    current_time_str = time.strftime("%H:%M:%S", time.localtime(now))
    LOG.info(f"[{chat_id}] New closed hour detected at {current_time_str}")

    previous_symbols = state.get("previous_symbols", set())
    if not previous_symbols and chat_id in PREVIOUS_SYMBOLS_CACHE:
        previous_symbols = PREVIOUS_SYMBOLS_CACHE[chat_id]

    LOG.info(f"[{chat_id}] Previous coins count: {len(previous_symbols)}")

    scan_start = time.time()
    all_hits: List[dict] = []

    for name, ex in EXCHANGES.items():
        hits = await scan_exchange(name, ex, previous_symbols)
        all_hits.extend(hits)

    scan_duration = time.time() - scan_start
    LOG.info(f"[{chat_id}] Scan time: {scan_duration:.2f}s")
    LOG.info(f"[{chat_id}] Hits before deduplication: {len(all_hits)}")

    if not all_hits:
        LOG.info(f"[{chat_id}] No signals")
        return

    unique_hits = deduplicate_by_best(all_hits)
    LOG.info(f"[{chat_id}] Unique coins after deduplication: {len(unique_hits)}")

    current_symbols = {h["base_coin"] for h in unique_hits}
    state["previous_symbols"] = current_symbols
    PREVIOUS_SYMBOLS_CACHE[chat_id] = current_symbols

    new_hits = [h for h in unique_hits if h["is_new"]]
    old_hits = [h for h in unique_hits if not h["is_new"]]

    new_hits.sort(key=lambda x: x["ratio"], reverse=True)
    old_hits.sort(key=lambda x: x["ratio"], reverse=True)

    sorted_hits = new_hits + old_hits

    from datetime import datetime
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_now = datetime.now(moscow_tz)
    current_hour = moscow_now.hour

    out = (
        f"🕐 <b>Scan {current_hour:02d}:00 MSK</b>\n"
        f"📊 Found: {len(sorted_hits)} coins (🆕 new: {len(new_hits)})\n"
        f"⏱️ Scan time: {scan_duration:.1f}s\n\n"
        "📊 <b>24h volume growth ≥ 500%</b>\n"
        "💰 Last 24h volume ≥ 20M$\n"
        "🎯 Best exchange per coin is shown\n\n"
    )

    for h in sorted_hits:
        link = symbol_link(h["exchange"], h["symbol"], h["is_new"])
        ratio = h["ratio"]
        growth_percent = (ratio - 1) * 100

        if h["is_new"]:
            out += (
                f"• {link}\n"
                f"  📈 Volume growth: <b>{ratio:.2f}×</b> (+{growth_percent:.0f}%)\n"
                f"  🔹 Prev 24h: ${h['prev_vol']:,.0f}\n"
                f"  🔸 Last 24h: <b>${h['last_vol']:,.0f}</b>\n\n"
            )
        else:
            out += (
                f"• {link}\n"
                f"  📈 Volume growth: {ratio:.2f}× (+{growth_percent:.0f}%)\n"
                f"  🔹 Prev 24h: ${h['prev_vol']:,.0f}\n"
                f"  🔸 Last 24h: ${h['last_vol']:,.0f}\n\n"
            )

    try:
        send_start = time.time()
        await context.bot.send_message(
            chat_id,
            out,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        send_duration = time.time() - send_start
        total_duration = time.time() - scan_start

        LOG.info(f"[{chat_id}] Message sent")
        LOG.info(f"[{chat_id}] Send: {send_duration:.2f}s | Total: {total_duration:.2f}s")
        LOG.info(
            f"[{chat_id}] Signals: {len(sorted_hits)} "
            f"({len(new_hits)} new, {len(old_hits)} repeated)"
        )

    except Exception as e:
        LOG.error(f"Send error: {e}")


async def heartbeat_job(context: ContextTypes.DEFAULT_TYPE):

    job = context.job
    chat_id = job.chat_id

    if chat_id not in AUTO_STATE:
        try:
            job.schedule_removal()
        except Exception:
            pass
        return

    try:
        await context.bot.send_message(
            chat_id,
            "🧪 Heartbeat: bot is running.",
        )
    except Exception as e:
        LOG.error(e)


async def start_autoscan(chat_id, context):
    """
    Start autoscan for a chat: checks every minute, scans on H1 close.
    """
    old = AUTO_STATE.get(chat_id)
    if old:
        for job in old["tasks"]:
            try:
                job.schedule_removal()
            except Exception:
                pass

    if chat_id in PREVIOUS_SYMBOLS_CACHE:
        del PREVIOUS_SYMBOLS_CACHE[chat_id]

    now = int(time.time())
    hour_sec = 3600
    cur_hour_index = now // hour_sec
    last_closed_hour_ts = (cur_hour_index - 1) * hour_sec

    AUTO_STATE[chat_id] = {
        "tasks": [],
        "last_hour_ts": last_closed_hour_ts,
        "previous_symbols": set(),
    }

    job_auto = context.job_queue.run_repeating(
        autoscan_worker,
        interval=60,
        first=10,
        chat_id=chat_id,
    )

    job_hb = context.job_queue.run_repeating(
        heartbeat_job,
        interval=HEARTBEAT_INTERVAL,
        first=HEARTBEAT_INTERVAL,
        chat_id=chat_id,
    )

    AUTO_STATE[chat_id]["tasks"] = [job_auto, job_hb]

    LOG.info(f"[{chat_id}] Autoscan started (check every minute)")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_message.chat_id

    from datetime import datetime
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_now = datetime.now(moscow_tz)

    welcome_text = (
        "🤖 <b>24h Volume Growth Monitor Bot</b>\n\n"
        "📌 <b>Signal conditions:</b>\n"
        "• Volume growth ≥ 500% (last 24h / previous 24h)\n"
        "• Last 24h volume ≥ 20M$\n\n"
        "🎯 <b>Output features:</b>\n"
        "• 🆕 New coins appear first\n"
        "• Best exchange is chosen per coin\n"
        "• All coins are sorted by growth strength\n"
        "• New coins are marked with 🆕\n\n"
        "🔗 <b>Links:</b>\n"
        "• TV — TradingView chart\n"
        "• Exchange — direct trading link\n\n"
        "⏰ <b>Schedule:</b>\n"
        "• Worker runs every minute\n"
        "• Scan is executed on each hourly candle close\n"
        "• Heartbeat every 1 hour\n\n"
        f"🕐 <i>Current time: {moscow_now.strftime('%H:%M:%S')} MSK</i>\n\n"
        "<b>Commands:</b>\n"
        "/autostop — stop autoscan\n"
        "/autostart — start autoscan\n"
        "/status — current status"
    )

    await update.message.reply_text(welcome_text, parse_mode=ParseMode.HTML)

    if chat_id not in AUTO_STATE:
        await start_autoscan(chat_id, context)
        await update.message.reply_text(
            "✅ Autoscan enabled.\n"
            f"Current time: {moscow_now.strftime('%H:%M:%S')} MSK\n\n"
            "ℹ️ <i>On the first run all coins are treated as new.</i>",
            parse_mode=ParseMode.HTML,
        )


async def cmd_autostart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    await start_autoscan(chat_id, context)

    from datetime import datetime
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_now = datetime.now(moscow_tz)

    await update.message.reply_text(
        "✅ Autoscan enabled.\n"
        f"Current time: {moscow_now.strftime('%H:%M:%S')} MSK\n\n"
        "ℹ️ <i>On restart the coin history is reset.</i>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_autostop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id

    old = AUTO_STATE.get(chat_id)
    if old:
        for job in old["tasks"]:
            try:
                job.schedule_removal()
            except Exception:
                pass
        del AUTO_STATE[chat_id]

    if chat_id in PREVIOUS_SYMBOLS_CACHE:
        del PREVIOUS_SYMBOLS_CACHE[chat_id]

    await update.message.reply_text("⛔ Autoscan stopped.")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id

    from datetime import datetime
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_now = datetime.now(moscow_tz)

    status_text = f"🕐 <b>Current time: {moscow_now.strftime('%H:%M:%S')} MSK</b>\n"

    if chat_id in AUTO_STATE:
        state = AUTO_STATE[chat_id]
        last_ts = state.get("last_hour_ts", 0)
        prev_symbols_count = len(state.get("previous_symbols", set()))

        status_text += "✅ <b>Autoscan is active</b>\n"
        status_text += "Worker checks every minute on hourly close.\n"

        if last_ts > 0:
            last_hour_dt = datetime.fromtimestamp(last_ts, moscow_tz)
            status_text += f"Last scan: {last_hour_dt.strftime('%H:%M')} MSK\n"
        else:
            status_text += "No scans have been run yet.\n"

        status_text += f"Tracked coins: {prev_symbols_count}\n"
    else:
        status_text += "⛔ <b>Autoscan is not active</b>\n"
        status_text += "Use /autostart to start it."

    await update.message.reply_text(status_text, parse_mode=ParseMode.HTML)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from datetime import datetime
    import pytz

    moscow_tz = pytz.timezone("Europe/Moscow")
    moscow_now = datetime.now(moscow_tz)

    await update.message.reply_text(
        "🤖 <b>Bot commands:</b>\n\n"
        "/start — description and auto start\n"
        "/autostart — enable autoscan\n"
        "/autostop — disable autoscan\n"
        "/status — current status\n\n"
        f"🕐 Current time: {moscow_now.strftime('%H:%M:%S')} MSK",
        parse_mode=ParseMode.HTML,
    )


def main():
    if not TG_TOKEN:
        LOG.error("TG_TOKEN environment variable is not set. Exiting.")
        raise SystemExit(1)

    builder = Application.builder().token(TG_TOKEN)

    if PROXY_URL:
        builder.proxy(PROXY_URL)

    app = builder.build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("autostart", cmd_autostart))
    app.add_handler(CommandHandler("autostop", cmd_autostop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    LOG.info("Volume pump bot started (with deduplication and exchange links)")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
