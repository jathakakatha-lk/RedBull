import os
import json
import time
import logging
import threading
from datetime import datetime
import requests
import pandas as pd
import numpy as np
from telebot import TeleBot, types
from flask import Flask

# ==========================================
# 1. FLASK ALIVE SERVER FOR RAILWAY HEALTHCHECK
# ==========================================
# Railway එකේ Healthcheck එක PASS වෙන්න මේ කොටස අනිවාර්යයි!
app = Flask(__name__)

@app.route('/')
def home():
    return "RED BULL MASTER BOT IS RUNNING ALIVE!", 200

@app.route('/webhook', methods=['POST', 'GET'])
def webhook_dummy():
    return "OK", 200

def run_flask():
    # Railway එකෙන් දෙන PORT එක ගන්නවා, නැත්නම් default 8080
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# ==========================================
# 2. CONFIGURATION & STATE
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BINANCE_API_URL = "https://fapi.binance.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")

bot = TeleBot(TELEGRAM_TOKEN)
STATE_FILE = "trade_state.json"

state = {
    "bot_active": True,
    "direct_mode": False,
    "reminder_active": True,
    "recovery_only": False,
    "signal_start": "08:00",
    "signal_end": "23:59",
    "fw_start": "00:00",
    "fw_end": "23:59",
    "symbol_list": [],
    "first_win_list": [],
    "blacklist": [],
    "active_trades": {},
    "accumulated_loss_pool": 0.0,
    "daily_stats": {
        "wins_count": 0, "wins_profit": 0.0,
        "loss_count": 0, "loss_amount": 0.0,
        "win_symbols": [], "loss_symbols": []
    },
    "background_tested_count": 0
}

state_lock = threading.Lock()

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                for k, v in saved.items(): state[k] = v
        except Exception as e:
            logging.error(f"Error loading state: {e}")

def save_state():
    with state_lock:
        try:
            with open(STATE_FILE, "w") as f: json.dump(state, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving state: {e}")

# ==========================================
# 3. BINANCE API & INDICATOR LOGIC
# ==========================================
def binance_request(endpoint, params=None):
    url = f"{BINANCE_API_URL}{endpoint}"
    for _ in range(3):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 429:
                time.sleep(5)
                continue
            if res.status_code == 200: return res.json()
        except:
            time.sleep(2)
    return None

def get_futures_symbols():
    data = binance_request("/fapi/v1/exchangeInfo")
    if not data: return []
    return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT']

def get_klines(symbol, interval, limit=5000):
    endpoint = "/fapi/v1/klines"
    all_candles = []
    end_time = None
    chunks = 4 if limit > 1500 else 1
    fetch_limit = 1250 if limit > 1500 else limit
    
    for _ in range(chunks):
        params = {"symbol": symbol, "interval": interval, "limit": fetch_limit}
        if end_time: params["endTime"] = end_time
        data = binance_request(endpoint, params)
        if not data: break
        all_candles = data + all_candles
        end_time = data[0][0] - 1
        if len(all_candles) >= limit: break
        time.sleep(0.2)
        
    if not all_candles: return None
    df = pd.DataFrame(all_candles[-limit:], columns=['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'])
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    return df

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def get_1h_zone(df):
    if df is None or len(df) < 500: return "NEUTRAL"
    close = df['close']
    ema80 = calculate_ema(close, 80)
    ema160 = calculate_ema(close, 160)
    ema500 = calculate_ema(close, 500)
    current_zone = "NEUTRAL"
    for i in range(len(df)):
        if (ema80.iloc[i] > ema160.iloc[i]) and (ema80.iloc[i-1] <= ema160.iloc[i-1]) and ema80.iloc[i] < ema500.iloc[i]:
            current_zone = "BUY_ZONE"
        elif (ema80.iloc[i] < ema160.iloc[i]) and (ema80.iloc[i-1] >= ema160.iloc[i-1]) and ema80.iloc[i] > ema500.iloc[i]:
            current_zone = "SELL_ZONE"
    return current_zone

def check_5m_signals(df, zone):
    if df is None or len(df) < 500: return None
    close, high, low = df['close'].values, df['high'].values, df['low'].values
    ema60 = calculate_ema(df['close'], 60).values
    ema80 = calculate_ema(df['close'], 80).values
    ema500 = calculate_ema(df['close'], 500).values
    
    # Simple 20-bar fractal check
    df['hh'] = df['high'].rolling(11, center=True).max()
    df['ll'] = df['low'].rolling(11, center=True).min()
    is_hh = (df['high'] == df['hh']).values
    is_ll = (df['low'] == df['ll']).values
    
    hh_broken, ll_broken = False, False
    last_hh, last_ll = None, None
    
    for i in range(20, len(df)):
        if is_hh[i]: last_hh = high[i]
        if is_ll[i]: last_ll = low[i]
        if zone == "BUY_ZONE" and ema60[i] > ema80[i] and ema60[i] < ema500[i]:
            if last_hh and close[i] > last_hh: hh_broken = True
            if hh_broken and is_ll[i]: return {"side": "BUY", "price": close[i]}
        elif zone == "SELL_ZONE" and ema60[i] < ema80[i] and ema60[i] > ema500[i]:
            if last_ll and close[i] < last_ll: ll_broken = True
            if ll_broken and is_hh[i]: return {"side": "SELL", "price": close[i]}
    return None

# ==========================================
# 4. SCANNERS & CORE LOOPS
# ==========================================
def run_symbol_scanner():
    raw_symbols = get_futures_symbols()
    state["symbol_list"] = [s for s in raw_symbols if s not in state["blacklist"]]
    save_state()
    bot.send_message(TELEGRAM_CHAT_ID, f"📋 **Symbol Scanner**\n\nකාසි ගණන: {len(state['symbol_list'])} ගබඩා කරගන්නා ලදී.")

def run_fwl_scanner():
    symbols = state["symbol_list"] if state["symbol_list"] else get_futures_symbols()
    valid_fwl = []
    for s in symbols[:20]:  # Rate limits ආරක්ෂා කරගැනීමට chunk එකක් ලෙස scan කරයි
        if s in state["blacklist"]: continue
        df_1h = get_klines(s, "1h", limit=100)
        if df_1h is not None and get_1h_zone(df_1h) != "NEUTRAL":
            valid_fwl.append(s)
            state["background_tested_count"] += 1
        time.sleep(0.5)
    state["first_win_list"] = valid_fwl
    save_state()
    
    formatted_coins = " ".join(valid_fwl).lower()
    report = f"⚡⛏️ FIRST WIN LIST REPORT\n━━━━━━━━━━━━━━━━━━━\n\n/fwl_add {formatted_coins}\n\nMr. MASTER👑"
    bot.send_message(TELEGRAM_CHAT_ID, report)

def live_monitor_loop():
    while True:
        try:
            active_symbols = list(state["active_trades"].keys())
            for symbol in active_symbols:
                trade = state["active_trades"].get(symbol)
                if not trade: continue
                
                ticker = binance_request("/fapi/v1/ticker/price", {"symbol": symbol})
                if not ticker: continue
                current_price = float(ticker['price'])
                
                is_tp = current_price >= trade["tp_price"] if trade["side"] == "BUY" else current_price <= trade["tp_price"]
                is_sl = current_price <= trade["sl_price"] if trade["side"] == "BUY" else current_price >= trade["sl_price"]
                
                if is_tp:
                    net_profit = (trade["margin"] * 0.30) - 0.01
                    state["daily_stats"]["wins_count"] += 1
                    state["daily_stats"]["wins_profit"] += net_profit
                    state["active_trades"].pop(symbol, None)
                    bot.send_message(TELEGRAM_CHAT_ID, f"🟢 Target Hit! {symbol} ලාභ පිට වැසුණා. Net: ${net_profit:.2f}")
                    save_state()
                elif is_sl:
                    loss_amount = (trade["margin"] * 0.27) + 0.01
                    trade["step"] += 1
                    trade["accumulated_loss"] += loss_amount
                    
                    if trade["step"] >= 3:
                        state["blacklist"].append(symbol)
                        state["accumulated_loss_pool"] += (trade["accumulated_loss"] / 4.0)
                        state["daily_stats"]["loss_count"] += 1
                        state["daily_stats"]["loss_amount"] += trade["accumulated_loss"]
                        state["active_trades"].pop(symbol, None)
                        bot.send_message(TELEGRAM_CHAT_ID, f"❌ RECOVERY FAILED: {symbol} Blacklist එකට එක් විය.")
                    else:
                        bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ STOP LOSS HIT (Step {trade['step']}/3): {symbol}\nඊළඟ 5M Fractal එකෙන් රිකවර් කිරීමට සූදානම්. ⏳")
                        trade["active_in_market"] = False
                    save_state()
            time.sleep(10)
        except Exception as e:
            logging.error(f"Error in Live Monitor: {e}")
            time.sleep(10)

def trade_scanner_loop():
    while True:
        try:
            now_str = datetime.now().strftime("%H:%M")
            if state["bot_active"] and (state["signal_start"] <= now_str <= state["signal_end"]):
                pool = get_futures_symbols() if state["direct_mode"] else state["first_win_list"]
                for symbol in pool:
                    if symbol in state["blacklist"]: continue
                    trade_state = state["active_trades"].get(symbol, {"step": 0, "accumulated_loss": 0.0, "active_in_market": False})
                    
                    if trade_state["active_in_market"]: continue
                    if state["recovery_only"] and trade_state["step"] == 0: continue
                    
                    df_1h = get_klines(symbol, "1h", limit=550)
                    df_5m = get_klines(symbol, "5m", limit=525)
                    zone = get_1h_zone(df_1h)
                    signal = check_5m_signals(df_5m, zone)
                    
                    if signal:
                        step = trade_state["step"]
                        margin = 0.80 if step == 0 else (0.80 * (2 ** step))
                        price = signal["price"]
                        side = signal["side"]
                        
                        sl_price = price * (1 - 0.027) if side == "BUY" else price * (1 + 0.027)
                        required_gains = (margin * 0.30) + trade_state["accumulated_loss"] + state["accumulated_loss_pool"]
                        if state["accumulated_loss_pool"] > 0: state["accumulated_loss_pool"] = 0.0
                        
                        price_diff = required_gains / (margin * 10)
                        tp_price = price * (1 + price_diff) if side == "BUY" else price * (1 - price_diff)
                        
                        state["active_trades"][symbol] = {
                            "symbol": symbol, "side": side, "step": step, "margin": margin,
                            "price": price, "tp_price": tp_price, "sl_price": sl_price,
                            "accumulated_loss": trade_state["accumulated_loss"], "active_in_market": True
                        }
                        save_state()
                        
                        sig_msg = (
                            f"🔔 NEW SIGNAL #{np.random.randint(10,99)} 🚨\n\n📍 Symbol: {symbol} | Side: {side}\n"
                            f"💵 Base Margin: ${margin:.1f} (10x)\n🎯 Target TP Price: {tp_price:.4f}\n🛑 {sl_price:.5f} : Stop Loss Price\n\n"
                            f"📈 Recovery Step: {step}/2\n🛡️ Protection SL: 27.0% (${margin*0.27:.3f})\n📊 Accumulated Loss: ${trade_state['accumulated_loss']:.3f}\n\nMr. MASTER👑"
                        )
                        bot.send_message(TELEGRAM_CHAT_ID, sig_msg)
                    time.sleep(15)
            time.sleep(15)
        except Exception as e:
            logging.error(f"Error in Trade Scanner: {e}")
            time.sleep(15)

# ==========================================
# 5. TELEGRAM HANDLERS (FIXED COMMANDS & KEYBOARD)
# ==========================================
# /menu විධානය නිවැරදිව ක්‍රියාත්මක වීමට Telebot Message Handler එකක් ලෙස සකසා ඇත.
@bot.message_handler(commands=['menu'])
def send_menu(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    markup.add(types.KeyboardButton('/bot_on'), types.KeyboardButton('/bot_off'))
    markup.add(types.KeyboardButton('/status'), types.KeyboardButton('/fwl_view'))
    markup.add(types.KeyboardButton('/Symbol_Scanner'), types.KeyboardButton('/Fwl_Scanner'))
    
    bot.reply_to(message, "🎮 **RED BULL MASTER CONTROL PANEL**\n\nපහත බොත්තම් (Buttons) භාවිතයෙන් ඔබට බොට් පාලනය කළ හැක.", reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['bot_on'])
def bot_on(message):
    state["bot_active"] = True
    save_state()
    bot.reply_to(message, "▶️ ස්කෑනර් පද්ධතිය සක්‍රීය කරන ලදී. (ON)")

@bot.message_handler(commands=['bot_off'])
def bot_off(message):
    state["bot_active"] = False
    save_state()
    bot.reply_to(message, "🛑 ස්කෑනර් පද්ධතිය තාවකාලිකව නවත්වන ලදී. (OFF)")

@bot.message_handler(commands=['status'])
def show_status(message):
    now_status = "ONLINE 🟢" if state["bot_active"] else "OFFLINE 🔴"
    msg = (
        "ℹ️ [RED BULL MASTER STATUS REPORT]\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"▶️ ස්කෑනර් එන්ට්‍රීම: {'සක්‍රීයයි (ON)' if state['bot_active'] else 'අක්‍රීයයි (OFF)'}\n"
        f"🔥 Verified ට්‍රේඩ් ගණන: {len(state['active_trades'])} / 3\n"
        f"🧪 Background Testing Trades: {state['background_tested_count']}\n"
        f"📢 මතක් කිරීමේ පද්ධතිය: {'සක්‍රීයයි 🔔' if state['reminder_active'] else 'අක්‍රීයයි 🔕'}\n"
        f"⚙️ Mode: {'DIRECT MODE 🚀' if state['direct_mode'] else 'NORMAL MODE 🔄'}\n"
        f"⏱️ BOT WINDOW STATUS : {now_status}\n"
        f"⏰ සිග්නල් දෙන කාලය: {state['signal_start']} - {state['signal_end']} දක්වා.\n"
        f"💵 මූලික ට්‍රේඩ් මාජින්: $0.8 | Leverage: 10x\n"
        f"🥇 First Win Coins ගණන: {len(state['first_win_list'])}\n"
        f"🚫 Blacklist Coins ගණන: {len(state['blacklist'])}\n"
    )
    bot.reply_to(message, msg)

@bot.message_handler(commands=['Symbol_Scanner'])
def trigger_symbol_scan(message):
    threading.Thread(target=run_symbol_scanner).start()
    bot.reply_to(message, "⚡ Manual Symbol Scan එකක් පසුබිමෙන් ආරම්භ වුණා.")

@bot.message_handler(commands=['Fwl_Scanner'])
def trigger_fwl_scan(message):
    threading.Thread(target=run_fwl_scanner).start()
    bot.reply_to(message, "⚡ Manual FWL Scan එකක් පසුබිමෙන් ආරම්භ වුණා.")

@bot.message_handler(commands=['fwl_view'])
def view_fwl(message):
    coins = ", ".join(state["first_win_list"]) if state["first_win_list"] else "හිස්"
    bot.reply_to(message, f"🥇 **වත්මන් First Win කාසි ලැයිස්තුව:**\n\n{coins}")

@bot.message_handler(commands=['fwl_add'])
def add_fwl_manual(message):
    args = message.text.split()[1:]
    for coin in args:
        if coin.upper() not in state["first_win_list"]: state["first_win_list"].append(coin.upper())
    save_state()
    bot.reply_to(message, "✅ කාසි First Win ලැයිස්තුවට එක් කරන ලදී.")

@bot.message_handler(commands=['clear_lists'])
def clear_fwl_list(message):
    state["first_win_list"] = []
    save_state()
    bot.reply_to(message, "🗑️ First Win ලැයිස්තුව හිස් කරන ලදී.")

# CRON SCHEDULER FOR MIDNIGHT & REPORTS
def cron_scheduler_loop():
    while True:
        now = datetime.now()
        if now.hour == 0 and now.minute == 0:
            run_symbol_scanner()
            run_fwl_scanner()
            time.sleep(60)
        if now.hour == 23 and now.minute == 59:
            bl_coins = " ".join(state["blacklist"]).lower() if state["blacklist"] else "නැත"
            report = (
                f"📊 ✨ FINAL PERFORMANCE REPORT\n━━━━━━━━━━━━━━━━━━━\n\n"
                f"🟢 Wins: {state['daily_stats']['wins_count']} ($ {state['daily_stats']['wins_profit']:.2f})\n"
                f"🔴 Loss: {state['daily_stats']['loss_count']} ($ {state['daily_stats']['loss_amount']:.2f})\n\n"
                f"━━━━━━━━━━━━━━━\nBacklist\n\nBacklist_add {bl_coins}\n\nMr. MASTER👑"
            )
            bot.send_message(TELEGRAM_CHAT_ID, report)
            state["daily_stats"] = {"wins_count": 0, "wins_profit": 0.0, "loss_count": 0, "loss_amount": 0.0, "win_symbols": [], "loss_symbols": []}
            save_state()
            time.sleep(60)
        time.sleep(10)

# ==========================================
# 6. ENGINE STARTPOINT
# ==========================================
if __name__ == "__main__":
    load_state()
    
    # 1. Flask alive server එක වෙනම Thread එකක run කරනවා (Railway Healthcheck සඳහා)
    threading.Thread(target=run_flask, daemon=True).start()
    
    # 2. අනෙක් Background වැඩසටහන් ක්‍රියාත්මක කරනවා
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=trade_scanner_loop, daemon=True).start()
    threading.Thread(target=cron_scheduler_loop, daemon=True).start()
    
    logging.info("Bot fully starting with multi-thread web compatibility...")
    bot.infinity_polling()
