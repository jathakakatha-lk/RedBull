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
app = Flask(__name__)

@app.route('/')
def home():
    return "RED BULL MASTER BOT IS RUNNING ALIVE!", 200

@app.route('/webhook', methods=['POST', 'GET'])
def webhook_dummy():
    return "OK", 200

def run_flask():
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

from telebot.types import BotCommand
def set_bot_menu_commands(bot):
    commands = [
        BotCommand("symbol_scanner", "කාසි ගබඩා කිරීම / මැනුවල් ස්කෑන් කිරීම"),
        BotCommand("fwl_scanner", "කැන්ඩල් 5000ක් පරීක්ෂා කර FWL සකස් කිරීම (කාසි 10 සීමාව)"),
        BotCommand("fwl_view", "දැනට First Win ලැයිස්තුවේ ඇති කාසි පරීක්ෂා කිරීම"),
        BotCommand("clear_lists", "First Win ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් (Clear) කිරීම"),
        BotCommand("recovery_only_on", "අලුත් ට්‍රේඩ්ස් නැවතීම සහ රිකවරි පමණක් සක්‍රීය කිරීම"),
        BotCommand("recovery_only_off", "රිකවරි මාදිලිය අක්‍රීය කර සාමාන්‍ය ක්‍රමයට හැරවීම"),
        BotCommand("blacklist_view", "දැනට බ්ලැක්ලිස්ට් කර ඇති කාසි ලැයිස්තුව පරීක්ෂා කිරීම"),
        BotCommand("bot_on", "පද්ධතියේ සියලුම ස්කෑනර් ක්‍රියාවලීන් සක්‍රීය කිරීම (ON)"),
        BotCommand("bot_off", "ස්කෑනර් පද්ධතිය තාවකාලිකව නැවතීම (OFF)"),
        BotCommand("direct_mode_on", "FWL නොබලා සියලුම කාසි සඳහා සෘජුවම සිග්නල් දීම"),
        BotCommand("direct_mode_off", "Direct Mode අක්‍රීය කර ආරක්ෂිත FWL ක්‍රමය ක්‍රියාත්මක කිරීම"),
        BotCommand("reminder_on", "සක්‍රීය ට්‍රේඩ්ස් ඇති විට විනාඩියෙන් විනාඩියට මතක් කිරීම් සක්‍රීය කිරීම"),
        BotCommand("reminder_off", "විනාඩියෙන් විනාඩියට එන මතක් කිරීම් අක්‍රීය කිරීම"),
        BotCommand("status", "බොට්ගේ වත්මන් සමස්ත තත්ත්ව වාර්තාව ලබා ගැනීම"),
        BotCommand("check_health", "පසුබිම් පද්ධති ක්‍රියාකාරීත්වය බලන සෞඛ්‍ය වාර්තාව"),
        BotCommand("menu", "ප්‍රධාන Control Panel එක ලබා ගැනීම"),
        BotCommand("reset_trades", "දැනට පවතින සියලුම සක්‍රීය ට්‍රේඩ්ස් දත්ත ක්ෂණිකව මකා දැමීම")
    ]
    try:
        bot.set_my_commands(commands)
    except Exception as e:
        logging.error(f"Command set error: {e}")

set_bot_menu_commands(bot)

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
    for col in ['open', 'high', 'low', 'close']:
        df[col] = df[col].astype(float)
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
    
    if (ema80.iloc[-1] > ema160.iloc[-1]) and ema80.iloc[-1] < ema500.iloc[-1]:
        current_zone = "BUY_ZONE"
    elif (ema80.iloc[-1] < ema160.iloc[-1]) and ema80.iloc[-1] > ema500.iloc[-1]:
        current_zone = "SELL_ZONE"
    return current_zone

def check_5m_signals(df, zone):
    if df is None or len(df) < 500: return None
    close, high, low = df['close'].values, df['high'].values, df['low'].values
    ema60 = calculate_ema(df['close'], 60).values
    ema80 = calculate_ema(df['close'], 80).values
    ema500 = calculate_ema(df['close'], 500).values
    
    df['hh'] = df['high'].rolling(11, center=True).max()
    df['ll'] = df['low'].rolling(11, center=True).min()
    is_hh = (df['high'] == df['hh']).values
    is_ll = (df['low'] == df['ll']).values
    
    hh_broken, ll_broken = False, False
    last_hh, last_ll = None, None
    
    for i in range(len(df)-20, len(df)):
        if is_hh[i]: last_hh = high[i]
        if is_ll[i]: last_ll = low[i]
        
        if zone == "BUY_ZONE" and ema60[i] > ema80[i] and ema60[i] < ema500[i]:
            if last_hh and close[i] > last_hh: hh_broken = True
            if hh_broken and is_ll[i]: return {"side": "BUY", "price": close[i]}
        elif zone == "SELL_ZONE" and ema60[i] < ema80[i] and ema60[i] > ema500[i]:
            if last_ll and close[i] < last_ll: ll_broken = True
            if ll_broken and is_hh[i]: return {"side": "SELL", "price": close[i]}
    return None

# 🎯 NEW: TRUE BACKTEST FUNCTION (Candle 5000 Analysis)
def true_backtest_5000(df_1h, df_5m):
    try:
        if df_1h is None or df_5m is None or len(df_5m) < 500: return False
        
        # Approximate 1H zones using 5m equivalent EMAs for speed & accuracy
        ema60 = calculate_ema(df_5m['close'], 60).values
        ema80 = calculate_ema(df_5m['close'], 80).values
        ema500 = calculate_ema(df_5m['close'], 500).values
        
        ema80_1h_eq = calculate_ema(df_5m['close'], 960).values
        ema160_1h_eq = calculate_ema(df_5m['close'], 1920).values
        ema500_1h_eq = calculate_ema(df_5m['close'], 6000).values if len(df_5m) >= 6000 else ema500
        
        df_5m['hh'] = df_5m['high'].rolling(11, center=True).max()
        df_5m['ll'] = df_5m['low'].rolling(11, center=True).min()
        is_hh = (df_5m['high'] == df_5m['hh']).values
        is_ll = (df_5m['low'] == df_5m['ll']).values
        
        close, high, low = df_5m['close'].values, df_5m['high'].values, df_5m['low'].values
        
        hh_broken, ll_broken = False, False
        last_hh, last_ll = None, None
        in_trade = False
        trade_side = None
        tp_price, sl_price = 0, 0
        consecutive_losses = 0
        
        for i in range(20, len(df_5m)):
            if is_hh[i]: last_hh = high[i]
            if is_ll[i]: last_ll = low[i]
            
            if in_trade:
                if trade_side == "BUY":
                    if high[i] >= tp_price:
                        consecutive_losses = 0
                        in_trade = False
                    elif low[i] <= sl_price:
                        consecutive_losses += 1
                        in_trade = False
                        hh_broken = False
                elif trade_side == "SELL":
                    if low[i] <= tp_price:
                        consecutive_losses = 0
                        in_trade = False
                    elif high[i] >= sl_price:
                        consecutive_losses += 1
                        in_trade = False
                        ll_broken = False
                
                if consecutive_losses >= 3:
                    return False # ❌ ෆේල්: එක දිගට 3 පාරක් ලොස් වුණා
                continue
                
            zone = "NEUTRAL"
            if (ema80_1h_eq[i] > ema160_1h_eq[i]) and ema80_1h_eq[i] < ema500_1h_eq[i]: zone = "BUY_ZONE"
            elif (ema80_1h_eq[i] < ema160_1h_eq[i]) and ema80_1h_eq[i] > ema500_1h_eq[i]: zone = "SELL_ZONE"
            
            if zone == "BUY_ZONE" and ema60[i] > ema80[i] and ema60[i] < ema500[i]:
                if last_hh and close[i] > last_hh: hh_broken = True
                if hh_broken and is_ll[i]:
                    in_trade, trade_side = True, "BUY"
                    tp_price, sl_price = close[i] * (1 + 0.03), close[i] * (1 - 0.027)
                    hh_broken = False
            elif zone == "SELL_ZONE" and ema60[i] < ema80[i] and ema60[i] > ema500[i]:
                if last_ll and close[i] < last_ll: ll_broken = True
                if ll_broken and is_hh[i]:
                    in_trade, trade_side = True, "SELL"
                    tp_price, sl_price = close[i] * (1 - 0.03), close[i] * (1 + 0.027)
                    ll_broken = False
                    
        return True # ✅ පාස්: ලොස් වාර 3ක සීමාව පැන්නේ නැත
    except Exception as e:
        return False

# ==========================================
# 4. SCANNERS & CORE LOOPS
# ==========================================
def run_symbol_scanner():
    raw_symbols = get_futures_symbols()
    state["symbol_list"] = [s for s in raw_symbols if s not in state["blacklist"]]
    save_state()
    bot.send_message(TELEGRAM_CHAT_ID, f"📋 **Symbol Scanner**\n\nකාසි ගණන: {len(state['symbol_list'])} ගබඩා කරගන්නා ලදී.")

def run_fwl_scanner():
    bot.send_message(TELEGRAM_CHAT_ID, "⏳ ඔබේ නීති මාලාවට අනුව කැන්ඩල් 5000ක් පරීක්ෂා කරමින් (True Backtesting) FWL ස්කෑන් කිරීම ආරම්භ වුණා...")
    symbols = state["symbol_list"] if state["symbol_list"] else get_futures_symbols()
    if len(symbols) == 0: return

    delay_between_coins = (8 * 3600 / 2) / len(symbols)
    valid_fwl = []
    
    for s in symbols:
        if len(valid_fwl) >= 10: break
        if s in state["blacklist"]: continue
        
        df_1h = get_klines(s, "1h", limit=5000)
        df_5m = get_klines(s, "5m", limit=5000)
        
        # 🎯 TRUE BACKTEST පරීක්ෂාව
        if true_backtest_5000(df_1h, df_5m):
            valid_fwl.append(s)
                
        state["background_tested_count"] += 1
        time.sleep(max(0.5, delay_between_coins))
        
    state["first_win_list"] = valid_fwl
    save_state()
    
    formatted_coins = " ".join(valid_fwl).lower()
    report = (
        "⚡⛏️ FIRST WIN LIST REPORT (Backtested)\n"
        "━━━━━━━━━━━━━━━━━━━\n\n"
        f"`/fwl_add {formatted_coins}`\n\n"
        "Mr. MASTER👑"
    )
    bot.send_message(TELEGRAM_CHAT_ID, report, parse_mode="Markdown")

def live_monitor_loop():
    while True:
        try:
            active_symbols = list(state["active_trades"].keys())
            for symbol in active_symbols:
                trade = state["active_trades"].get(symbol)
                if not trade or not trade.get("active_in_market", False): continue
                
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
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error in Live Monitor: {e}")
            time.sleep(5)

def trade_scanner_loop():
    while True:
        try:
            now_str = datetime.now().strftime("%H:%M")
            if state["bot_active"] and (state["signal_start"] <= now_str <= state["signal_end"]):
                pool = get_futures_symbols() if state["direct_mode"] else state["first_win_list"]
                
                for symbol in pool:
                    if symbol in state["blacklist"]: continue
                    
                    # 🎯 SPAM FIX: දැනටමත් මාකට් එකේ ඉන්නවා නම් නැවත සිග්නල් එවීම නවත්වයි
                    if symbol in state["active_trades"] and state["active_trades"][symbol].get("active_in_market", False):
                        continue
                        
                    trade_state = state["active_trades"].get(symbol, {"step": 0, "accumulated_loss": 0.0, "active_in_market": False})
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
                        
                        # 🎯 MATH FIX: ප්‍රතිශතයක් (Percentage) ලෙස නිවැරදිව TP / SL සෑදීම
                        sl_percentage = 0.027  
                        sl_price = price * (1 - sl_percentage) if side == "BUY" else price * (1 + sl_percentage)
                        
                        required_gains = (margin * 0.30) + trade_state["accumulated_loss"] + state["accumulated_loss_pool"]
                        if state["accumulated_loss_pool"] > 0: state["accumulated_loss_pool"] = 0.0
                        
                        tp_percentage = required_gains / (margin * 10) 
                        tp_price = price * (1 + tp_percentage) if side == "BUY" else price * (1 - tp_percentage)
                        
                        state["active_trades"][symbol] = {
                            "symbol": symbol, "side": side, "step": step, "margin": margin,
                            "price": price, "tp_price": tp_price, "sl_price": sl_price,
                            "accumulated_loss": trade_state["accumulated_loss"], "active_in_market": True
                        }
                        save_state()
                        
                        sig_msg = (
                            f"🔔 NEW SIGNAL #{np.random.randint(10,99)} 🚨\n\n📍 Symbol: {symbol} | Side: {side}\n"
                            f"💵 Base Margin: ${margin:.2f} (10x)\n🎯 Target TP Price: {tp_price:.5f}\n🛑 Stop Loss Price: {sl_price:.5f}\n\n"
                            f"📈 Recovery Step: {step}/2\n🛡️ Protection SL: 27.0% (${margin*0.27:.3f})\n📊 Accumulated Loss: ${trade_state['accumulated_loss']:.3f}\n\nMr. MASTER👑"
                        )
                        bot.send_message(TELEGRAM_CHAT_ID, sig_msg)
                        time.sleep(2) 
                        
                time.sleep(15)
            else:
                time.sleep(15)
        except Exception as e:
            logging.error(f"Error in Trade Scanner: {e}")
            time.sleep(15)

# ==========================================
# 5. TELEGRAM HANDLERS
# ==========================================
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
        f"💵 මූලික ට්‍රේඩ් මාජින්: $0.80 | Leverage: 10x\n"
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
    
    try:
        bot.remove_webhook()
        time.sleep(1)
    except Exception as e:
        logging.error(f"Error removing webhook: {e}")
    
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=trade_scanner_loop, daemon=True).start()
    threading.Thread(target=cron_scheduler_loop, daemon=True).start()
    
    logging.info("Red Bull Master Bot is fully operational with True Backtesting and Bug Fixes!")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
