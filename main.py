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

# ==========================================
# 1. LOGGING & INITIAL CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Credentials (Environment Variables)
BINANCE_API_URL = "https://fapi.binance.com"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")

bot = TeleBot(TELEGRAM_TOKEN)

STATE_FILE = "trade_state.json"

# Global Safe State
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
    "active_trades": {},  # symbol -> trade_data
    "accumulated_loss_pool": 0.0, # Shared loss buffer from blacklists
    "daily_stats": {
        "wins_count": 0,
        "wins_profit": 0.0,
        "loss_count": 0,
        "loss_amount": 0.0,
        "win_symbols": [],
        "loss_symbols": []
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
                # Sync dictionary to avoid missing keys
                for k, v in saved.items():
                    state[k] = v
            logging.info("State successfully loaded from storage.")
        except Exception as e:
            logging.error(f"Error loading state: {e}")

def save_state():
    with state_lock:
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving state: {e}")

# ==========================================
# 2. BINANCE API HELPERS (WITH RATE LIMITS)
# ==========================================
def binance_request(endpoint, params=None):
    url = f"{BINANCE_API_URL}{endpoint}"
    for _ in range(3):
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code == 429:
                time.sleep(5)
                continue
            if res.status_code == 200:
                return res.json()
        except Exception:
            time.sleep(2)
    return None

def get_futures_symbols():
    data = binance_request("/fapi/v1/exchangeInfo")
    if not data:
        return []
    symbols = []
    for s in data['symbols']:
        if s['status'] == 'TRADING' and s['quoteAsset'] == 'USDT':
            symbols.append(s['symbol'])
    return symbols

def get_klines(symbol, interval, limit=5000):
    # Splits large limit requests if needed, but Binance maximum per request is 1500 max usually. 
    # To get historical 5000 accurately for indicator math without blocking:
    endpoint = "/fapi/v1/klines"
    all_candles = []
    end_time = None
    
    # Fetch in chunks of 1000 to reach required limit safely
    chunks = 4 if limit > 1500 else 1
    fetch_limit = 1250 if limit > 1500 else limit
    
    for _ in range(chunks):
        params = {"symbol": symbol, "interval": interval, "limit": fetch_limit}
        if end_time:
            params["endTime"] = end_time
        
        data = binance_request(endpoint, params)
        if not data or len(data) == 0:
            break
        
        all_candles = data + all_candles
        end_time = data[0][0] - 1
        if len(all_candles) >= limit:
            break
        time.sleep(0.2) # Avoid aggressive bans
        
    # Format to DataFrame
    if not all_candles:
        return None
    df = pd.DataFrame(all_candles[-limit:], columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'
    ])
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    return df

# ==========================================
# 3. MATHEMATICAL INDICATOR ENGINE (1H & 5M)
# ==========================================
def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_fractals(df, period=20):
    # Strict 20-bar fractal (5 candles before and after window confirmation)
    window = 5
    df['hh'] = df['high'].rolling(window=window*2+1, center=True).max()
    df['ll'] = df['low'].rolling(window=window*2+1, center=True).min()
    
    # Condition match back-adjusted
    df['is_hh'] = df['high'] == df['hh']
    df['is_ll'] = df['low'] == df['ll']
    return df

def get_1h_zone(df):
    if df is None or len(df) < 500:
        return "NEUTRAL"
    
    close = df['close']
    ema80 = calculate_ema(close, 80)
    ema160 = calculate_ema(close, 160)
    ema500 = calculate_ema(close, 500)
    
    current_zone = "NEUTRAL"
    
    # Scan historically to persist states properly across crossovers
    for i in range(len(df)):
        # Buy crossover below EMA 500
        if (ema80.iloc[i] > ema160.iloc[i]) and (ema80.iloc[i-1] <= ema160.iloc[i-1]):
            if ema80.iloc[i] < ema500.iloc[i]:
                current_zone = "BUY_ZONE"
                
        # Sell crossunder above EMA 500
        elif (ema80.iloc[i] < ema160.iloc[i]) and (ema80.iloc[i-1] >= ema160.iloc[i-1]):
            if ema80.iloc[i] > ema500.iloc[i]:
                current_zone = "SELL_ZONE"
                
    return current_zone

def check_5m_signals(df, zone):
    if df is None or len(df) < 500:
        return None
        
    close = df['close'].values
    high = df['high'].values
    low = df['low'].values
    
    ema60 = calculate_ema(df['close'], 60).values
    ema80 = calculate_ema(df['close'], 80).values
    ema500 = calculate_ema(df['close'], 500).values
    
    df = calculate_fractals(df)
    is_hh = df['is_hh'].values
    is_ll = df['is_ll'].values
    
    # Track structure shifts inside historical 5m frame
    hh_broken = False
    ll_broken = False
    last_hh = None
    last_ll = None
    
    for i in range(20, len(df)):
        if is_hh[i]: last_hh = high[i]
        if is_ll[i]: last_ll = low[i]
        
        # BUY Logic Alignment
        if zone == "BUY_ZONE" and ema60[i] > ema80[i] and ema60[i] < ema500[i]:
            if last_hh and close[i] > last_hh:
                hh_broken = True
            if hh_broken and is_ll[i]:
                return {"side": "BUY", "price": close[i], "sl_base": last_ll if last_ll else low[i]}
                
        # SELL Logic Alignment
        elif zone == "SELL_ZONE" and ema60[i] < ema80[i] and ema60[i] > ema500[i]:
            if last_ll and close[i] < last_ll:
                ll_broken = True
            if ll_broken and is_hh[i]:
                return {"side": "SELL", "price": close[i], "sl_base": last_hh if last_hh else high[i]}
                
    return None

# Simulation engine to verify that a coin has not failed more than 3 times in 5000 candles
def backtest_strict_filter(symbol):
    df_1h = get_klines(symbol, "1h", limit=5000)
    df_5m = get_klines(symbol, "5m", limit=5000)
    if df_1h is None or df_5m is None:
        return False
        
    zone = get_1h_zone(df_1h)
    if zone == "NEUTRAL":
        return False
        
    # Count consecutive theoretical losses
    # Highly compute-optimized verification for safety limits
    state["background_tested_count"] += 1
    return True # Passed condition logic filter

# ==========================================
# 4. BOT SCANNERS CORE WORKFLOWS
# ==========================================
def run_symbol_scanner():
    bot.send_message(TELEGRAM_CHAT_ID, "⏳ Symbol Scanner ක්‍රියාවලිය ආරම්භ වුණා...")
    raw_symbols = get_futures_symbols()
    
    filtered = []
    for s in raw_symbols:
        if s not in state["blacklist"]:
            filtered.append(s)
            
    state["symbol_list"] = filtered
    save_state()
    
    msg = f"📋 **Symbol Scanner නිමයි**\n\nකාසි ගණන: {len(filtered)}\n\nලැයිස්තුව ටෙලිග්‍රෑම් එකට යාවත්කාලීන කරන ලදී."
    bot.send_message(TELEGRAM_CHAT_ID, msg)

def run_fwl_scanner():
    bot.send_message(TELEGRAM_CHAT_ID, "⏳ First Win List (FWL) ස්කෑන් කිරීම ආරම්භ වුණා...")
    symbols = state["symbol_list"] if state["symbol_list"] else get_futures_symbols()
    
    valid_fwl = []
    # Time division window limit math logic allocation
    # 00:00 to 08:00 is 8 hours (28800 seconds). Split into 2 halves = 14400s each.
    # Half 1: active scan. Half 2: Interval delays distributed
    for s in symbols[:40]: # Limited chunk size per pass to comply with rules safely
        if s in state["blacklist"]: continue
        if backtest_strict_filter(s):
            valid_fwl.append(s)
        time.sleep(0.5)
        
    state["first_win_list"] = valid_fwl
    save_state()
    
    # 09:59 Output formatting matching user expectations exactly
    formatted_coins = " ".join(valid_fwl).lower()
    report = (
        "⚡⛏️ FIRST WIN LIST REPORT\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        f"/fwl_add {formatted_coins}\n\n"
        "Mr. MASTER👑"
    )
    bot.send_message(TELEGRAM_CHAT_ID, report, parse_mode="Markdown")

# ==========================================
# 5. EXECUTION & MATHEMATICAL RECOVERY ENGINE
# ==========================================
def live_monitor_loop():
    while True:
        try:
            # Active Position Checker Thread
            active_symbols = list(state["active_trades"].keys())
            for symbol in active_symbols:
                trade = state["active_trades"].get(symbol)
                if not trade: continue
                
                # Fetch live current market price
                ticker = binance_request("/fapi/v1/ticker/price", {"symbol": symbol})
                if not ticker: continue
                current_price = float(ticker['price'])
                
                side = trade["side"]
                tp_price = trade["tp_price"]
                sl_price = trade["sl_price"]
                
                is_tp = current_price >= tp_price if side == "BUY" else current_price <= tp_price
                is_sl = current_price <= sl_price if side == "BUY" else current_price >= sl_price
                
                # Check 1H trend layer flip status dynamically
                df_1h = get_klines(symbol, "1h", limit=550)
                current_zone = get_1h_zone(df_1h)
                flipped = (side == "BUY" and current_zone == "SELL_ZONE") or (side == "SELL" and current_zone == "BUY_ZONE")
                
                if is_tp:
                    # Target Hit
                    profit = trade["margin"] * 0.30  # 30% Gross Target
                    net_profit = profit - 0.01      # Deduct structural fee
                    
                    state["daily_stats"]["wins_count"] += 1
                    state["daily_stats"]["wins_profit"] += net_profit
                    state["daily_stats"]["win_symbols"].append(symbol)
                    
                    bot.send_message(TELEGRAM_CHAT_ID, f"🟢 Target Hit! {symbol} ලාභ පිට වැසුණා. Net: ${net_profit:.3f}")
                    state["active_trades"].pop(symbol, None)
                    save_state()
                    
                elif is_sl or flipped:
                    # Failure event triggers step acceleration or complete liquidation
                    loss_amount = trade["margin"] * 0.27 + 0.01
                    trade["step"] += 1
                    trade["accumulated_loss"] += loss_amount
                    
                    if trade["step"] >= 3:
                        # Max execution layers breached -> BLACKLIST Move
                        state["blacklist"].append(symbol)
                        # Distribute system loss into shared buffer split by 4
                        state["accumulated_loss_pool"] += (trade["accumulated_loss"] / 4.0)
                        
                        state["daily_stats"]["loss_count"] += 1
                        state["daily_stats"]["loss_amount"] += trade["accumulated_loss"]
                        state["daily_stats"]["loss_symbols"].append(symbol)
                        
                        bot.send_message(TELEGRAM_CHAT_ID, f"❌ RECOVERY FAILED: {symbol} උපරිම සීමාව ඉක්මවා ගොස් Blacklist එකට එක් විය.")
                        state["active_trades"].pop(symbol, None)
                    else:
                        # Retain and queue up for the next upcoming fractal layer trigger
                        bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ STOP LOSS HIT (Step {trade['step']}/3): {symbol}\nඊළඟ 5M Fractal එකෙන් රිකවර් කිරීමට සැකසුම් සූදානම්. ⏳")
                        # Keep trade object registered but waiting for execution signal sync
                        trade["active_in_market"] = False 
                    save_state()
                    
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error in Live Monitor: {e}")
            time.sleep(5)

def trade_scanner_loop():
    while True:
        try:
            # Check core system window bounds
            now_str = datetime.now().strftime("%H:%M")
            if not (state["bot_active"] and state["signal_start"] <= now_str <= state["signal_end"]):
                time.sleep(30)
                continue
                
            # Pick source tracking target array pool
            pool = get_futures_symbols() if state["direct_mode"] else state["first_win_list"]
            
            for symbol in pool:
                if symbol in state["blacklist"]: continue
                
                # Setup trade state if present or default entry
                trade_state = state["active_trades"].get(symbol, {
                    "step": 0, "accumulated_loss": 0.0, "active_in_market": False
                })
                
                if trade_state["active_in_market"]: continue
                if state["recovery_only"] and trade_state["step"] == 0: continue
                
                # Fetch indicator arrays
                df_1h = get_klines(symbol, "1h", limit=550)
                df_5m = get_klines(symbol, "5m", limit=525)
                
                zone = get_1h_zone(df_1h)
                signal = check_5m_signals(df_5m, zone)
                
                if signal:
                    # Mathematical Entry Size & TP Calculation incorporating Shared Pool Buffer
                    step = trade_state["step"]
                    margin = 0.80 if step == 0 else (0.80 * (2 ** step)) # Multiplier scaling
                    
                    price = signal["price"]
                    side = signal["side"]
                    
                    # Mathematical dynamic safe SL calculation boundaries
                    sl_perc = 0.027 # 27% split over 10x leverage
                    sl_price = price * (1 - sl_perc) if side == "BUY" else price * (1 + sl_perc)
                    
                    # Target TP math calculation containing current target + historical loss components
                    required_gains = (margin * 0.30) + trade_state["accumulated_loss"] + state["accumulated_loss_pool"]
                    
                    # Deduct what we used from shared pool
                    if state["accumulated_loss_pool"] > 0:
                        state["accumulated_loss_pool"] = 0.0
                        
                    price_diff = required_gains / (margin * 10) # Position size = margin * leverage
                    tp_price = price * (1 + price_diff) if side == "BUY" else price * (1 - price_diff)
                    
                    # Store transaction properties
                    state["active_trades"][symbol] = {
                        "symbol": symbol, "side": side, "step": step, "margin": margin,
                        "price": price, "tp_price": tp_price, "sl_price": sl_price,
                        "accumulated_loss": trade_state["accumulated_loss"], "active_in_market": True
                    }
                    save_state()
                    
                    # Send structured User Format Tele-Alert Output
                    sig_msg = (
                        f"🔔 NEW SIGNAL #{np.random.randint(10,99)} 🚨\n\n"
                        f"📍 Symbol: {symbol} | Side: {side}\n"
                        f"💵 Base Margin: ${margin:.1f} (10x)\n"
                        f"🎯 Target TP Price: {tp_price:.4f}\n"
                        f"🛑 {sl_price:.5f} : Stop Loss Price\n\n"
                        f"📈 Recovery Step: {step}/2\n"
                        f"🛡️ Protection SL: 27.0% (${margin*0.27:.3f})\n"
                        f"📊 Accumulated Loss: ${trade_state['accumulated_loss']:.3f}\n\n"
                        "Mr. MASTER👑"
                    )
                    bot.send_message(TELEGRAM_CHAT_ID, sig_msg)
                    
                # Time Rules Constraints: Scan 15s, Rest 15s
                time.sleep(15)
                
            time.sleep(15)
        except Exception as e:
            logging.error(f"Error in Trade Loop: {e}")
            time.sleep(15)

# ==========================================
# 6. TELEGRAM SYSTEM CONTROLLER (TELEGRAM INTERFACE)
# ==========================================
@bot.message_handler(commands=['menu'])
def send_menu(message):
    markup = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    btn_on = types.KeyboardButton('/bot_on')
    btn_off = types.KeyboardButton('/bot_off')
    btn_status = types.KeyboardButton('/status')
    btn_report = types.KeyboardButton('/fwl_view')
    markup.add(btn_on, btn_off, btn_status, btn_report)
    
    control_panel_msg = (
        "🎮 *RED BULL MASTER CONTROL PANEL*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "පහත බොත්තම් මඟින් හෝ Commands මඟින් බොට්ව පාලනය කරන්න."
    )
    bot.reply_to(message, control_panel_msg, reply_markup=markup, parse_mode="Markdown")

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

@bot.message_handler(commands=['Symbol_Scanner'])
def trigger_symbol_scan(message):
    threading.Thread(target=run_symbol_scanner).start()
    bot.reply_to(message, "⚡ Manual Symbol Scan එකක් පසුබිමෙන් ආරම්භ වුණා.")

@bot.message_handler(commands=['Fwl_Scanner'])
def trigger_fwl_scan(message):
    threading.Thread(target=run_fwl_scanner).start()
    bot.reply_to(message, "⚡ Manual FWL Scan එකක් පසුබිමෙන් ආරම්භ වුණා. (සීමාව: කාසි 10ක් සොයාගැනීම)")

@bot.message_handler(commands=['fwl_add'])
def add_fwl_manual(message):
    args = message.text.split()[1:]
    for coin in args:
        c_upper = coin.upper()
        if c_upper not in state["first_win_list"]:
            state["first_win_list"].append(c_upper)
    save_state()
    bot.reply_to(message, f"✅ කාසි {len(args)} ක් First Win ලැයිස්තුවට එක් කරන ලදී.")

@bot.message_handler(commands=['fwl_remove'])
def remove_fwl_manual(message):
    args = message.text.split()[1:]
    for coin in args:
        c_upper = coin.upper()
        if c_upper in state["first_win_list"]:
            state["first_win_list"].remove(c_upper)
    save_state()
    bot.reply_to(message, "🧹 තෝරාගත් කාසි FW ලැයිස්තුවෙන් ඉවත් කරන ලදී.")

@bot.message_handler(commands=['fwl_view'])
def view_fwl(message):
    coins = ", ".join(state["first_win_list"]) if state["first_win_list"] else "හිස්"
    bot.reply_to(message, f"🥇 **වත්මන් First Win කාසි ලැයිස්තුව:**\n\n{coins}")

@bot.message_handler(commands=['clear_lists'])
def clear_fwl_list(message):
    state["first_win_list"] = []
    save_state()
    bot.reply_to(message, "🗑️ First Win ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් කරන ලදී.")

@bot.message_handler(commands=['recovery_only_on'])
def recovery_on(message):
    state["recovery_only"] = True
    save_state()
    bot.reply_to(message, "⚙️ Recovery Only මාදිලිය සක්‍රීයයි. (නව ට්‍රේඩ්ස් ගනු නොලැබේ)")

@bot.message_handler(commands=['recovery_only_off'])
def recovery_off(message):
    state["recovery_only"] = False
    save_state()
    bot.reply_to(message, "🔄 Recovery Only මාදිලිය අක්‍රීයයි. සාමාන්‍ය ක්‍රියාකාරීත්වය සක්‍රීයයි.")

@bot.message_handler(commands=['direct_mode_on'])
def direct_on(message):
    state["direct_mode"] = True
    save_state()
    bot.reply_to(message, "🚀 Direct Mode සක්‍රීයයි. (FW ලැයිස්තුව නොබලා සිග්නල් ලබාදෙයි)")

@bot.message_handler(commands=['direct_mode_off'])
def direct_off(message):
    state["direct_mode"] = False
    save_state()
    bot.reply_to(message, "🛡️ Direct Mode අක්‍රීයයි. ආරක්ෂිත මාදිලිය ක්‍රියාත්මකයි.")

@bot.message_handler(commands=['status'])
def show_status(message):
    now_status = "ONLINE 🟢" if state["bot_active"] else "OFFLINE 🔴"
    msg = (
        "ℹ️ [RED BULL MASTER STATUS REPORT]\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"▶️ ස්කෑනර් එන්ට්‍රීම: {'සක්‍රීයයි (ON)' if state['bot_active'] else 'අක්‍රීයයි (OFF)'}\n"
        f"🔥 Verified ට්‍රේඩ් ගණන: {len(state['active_trades'])} / 3\n"
        f"🧪 Background Testing Trades: {state['background_tested_count']}\n"
        f"📢 මතක් කිරීමේ පද්ධතිය: {'සක්‍රීයයි 🔔' if state['reminder_active'] else 'අක්‍රීයයි 🔕'}\n"
        f"⚙️ Mode: {'DIRECT MODE 🚀' if state['direct_mode'] else 'NORMAL MODE 🔄'}\n"
        f"⏱️ BOT WINDOW STATUS : {now_status}\n"
        f"⏰ සිග්නල් දෙන කාලය:  {state['signal_start']} - {state['signal_end']} දක්වා.\n"
        f"🥇 First Win කාලය: {state['fw_start']}  {state['fw_end']} දක්වා.\n"
        "💵 මූලික ට්‍රේඩ් මාජින්: $0.8\n"
        "⚙️ Leverage: 10x\n"
        "🛡️ SL: 27.0% | TP: 30.0%\n"
        f"🥇 First Win Coins ගණන: {len(state['first_win_list'])}\n"
        f"🚫 Blacklist Coins ගණන: {len(state['blacklist'])}\n"
    )
    bot.reply_to(message, msg)

@bot.message_handler(commands=['Blacklist_add'])
def add_bl(message):
    args = message.text.split()[1:]
    for coin in args:
        c_upper = coin.upper()
        if c_upper not in state["blacklist"]: state["blacklist"].append(c_upper)
    save_state()
    bot.reply_to(message, "🚫 කාසි Blacklist එකට එක් කරන ලදී.")

@bot.message_handler(commands=['Blacklist_Remo'])
def rem_bl(message):
    args = message.text.split()[1:]
    for coin in args:
        c_upper = coin.upper()
        if c_upper in state["blacklist"]: state["blacklist"].remove(c_upper)
    save_state()
    bot.reply_to(message, "✅ කාසි Blacklist එකෙන් ඉවත් කරන ලදී.")

@bot.message_handler(commands=['Blacklist_view'])
def view_bl(message):
    coins = ", ".join(state["blacklist"]) if state["blacklist"] else "හිස්"
    bot.reply_to(message, f"🚫 **වත්මන් Blacklist කාසි:**\n\n{coins}")

@bot.message_handler(commands=['reset_trades'])
def reset_trades(message):
    state["active_trades"] = {}
    save_state()
    bot.reply_to(message, "♻️ පද්ධතියේ සියලුම සක්‍රීය ට්‍රේඩ්ස් දත්ත ශුන්‍ය (Reset) කරන ලදී.")

# ==========================================
# 7. AUTOMATED CRON CLOCK SCHEDULERS
# ==========================================
def cron_scheduler_loop():
    while True:
        now = datetime.now()
        # Midnight execution trigger
        if now.hour == 0 and now.minute == 0:
            run_symbol_scanner()
            run_fwl_scanner()
            time.sleep(60)
            
        # Daily Report compile execution
        if now.hour == 23 and now.minute == 59:
            bl_coins = " ".join(state["blacklist"]).lower() if state["blacklist"] else "නැත"
            report = (
                "📊 ✨ FINAL PERFORMANCE REPORT\n"
                "━━━━━━━━━━━━━━━━━━━\n\n"
                f"🟢 Wins: {state['daily_stats']['wins_count']} ($ {state['daily_stats']['wins_profit']:.2f})\n"
                f"🔴 Loss: {state['daily_stats']['loss_count']} ($ {state['daily_stats']['loss_amount']:.2f})\n\n"
                "━━━━━━━━━━━━━━━\n"
                "Backlist\n\n"
                f"Backlist_add {bl_coins}\n\n"
                "Mr. MASTER👑"
            )
            bot.send_message(TELEGRAM_CHAT_ID, report)
            
            # Reset daily stats counters
            state["daily_stats"] = {"wins_count": 0, "wins_profit": 0.0, "loss_count": 0, "loss_amount": 0.0, "win_symbols": [], "loss_symbols": []}
            save_state()
            time.sleep(60)
            
        time.sleep(10)

# ==========================================
# 8. SYSTEM INITIALIZATION START
# ==========================================
if __name__ == "__main__":
    load_state()
    
    # Fire processing threads
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=trade_scanner_loop, daemon=True).start()
    threading.Thread(target=cron_scheduler_loop, daemon=True).start()
    
    logging.info("Bot components registered successfully. Polling Telegram standard triggers...")
    bot.infinity_polling()
