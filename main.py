import os
import time
import json
import threading
import requests
import datetime
import pandas as pd
import numpy as np
from binance.client import Client
from flask import Flask, request
import pytz  

# --- 1. CONFIGURATIONS ---
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 8080))

# 🕰️ BOT RUNNING TIMEZONE
BOT_TIMEZONE = "Asia/Colombo" 

client = Client(API_KEY, API_SECRET, requests_params={"timeout": 20})
client.API_URL = 'https://fapi.binance.com' 

app = Flask(__name__)
DB_FILE = "trade_state.json"

# --- 2. STATE MANAGEMENT & DATABASE ---
def load_data():
    default_state = {
        'active_positions': {},        
        'symbol_recovery_step': {},     
        'symbol_accumulated_loss': {},  
        'block_list': [],  
        'signal_count': 0, 
        'is_paused': False,
        'is_scanning': True,
        'max_signals': 3,
        'manual_initial_balance': 100.0,
        'alarm_active': True,
        'last_alarm_symbol': "NONE",
        'stats': {'wins': 0, 'loss': 0, 'total_pnl': 0.0, 'won_trades': [], 'lost_trades': []},
        'daily_stats': {'wins': 0, 'loss': 0, 'won_trades': [], 'last_reset_date': str(datetime.date.today())},
        'pending_acknowledgement': False,      
        'reminder_system_active': True,
        'recovery_only_mode': False,     
        
        # Dynamic Settings
        'base_margin': 0.80,            
        'margin_sl_pct': 50.0,         
        'fast_tp_pct': 25.0,           
        
        # Dynamic Time Window Settings (Stored in Hours and Minutes)
        'start_hour': 12,
        'start_minute': 30,
        'end_hour': 23,
        'end_minute': 59
    }
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f: 
                loaded_state = json.load(f)
                for k, v in default_state.items():
                    if k not in loaded_state: loaded_state[k] = v
                for k, v in default_state['stats'].items():
                    if k not in loaded_state['stats']: loaded_state['stats'][k] = v
                for k, v in default_state['daily_stats'].items():
                    if k not in loaded_state['daily_stats']: loaded_state['daily_stats'][k] = v
                return loaded_state
        except: pass
    return default_state

state = load_data()
state_lock = threading.Lock()

def sync_save():
    try:
        with state_lock:
            with open(DB_FILE, 'w') as f: json.dump(state, f)
    except Exception as e: print(f"Save Error: {e}")

def execute_telegram_send(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": str(TELEGRAM_CHAT_ID).strip(), "text": msg, "parse_mode": "HTML"}
    
    # Connection Reset වැළැක්වීමට 3 වතාවක් උත්සාහ කරයි
    for attempt in range(3):
        try:
            res = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code == 200:
                return True
            elif res.status_code == 429:
                retry_after = res.json().get('parameters', {}).get('retry_after', 5)
                time.sleep(retry_after)
        except Exception as e:
            print(f"Telegram Attempt {attempt+1} failed: {e}")
            time.sleep(2)
            
    print("Telegram Send Error: All 3 attempts failed.")
    return False

# --- 🕰️ 3. DYNAMIC LANKA TIME WINDOW ---
def is_ict_trading_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        current_hour = tz_now.hour
        current_minute = tz_now.minute
        
        total_minutes = (current_hour * 60) + current_minute
        
        with state_lock:
            start_time = (state.get('start_hour', 12) * 60) + state.get('start_minute', 30)
            end_time = (state.get('end_hour', 23) * 60) + state.get('end_minute', 59)
        
        if start_time <= total_minutes <= end_time: return True
        return False
    except Exception as e:
        print(f"Timezone Error: {e}")
        return True

# --- 🛡️ 4. 1-HOUR TREND ZONE FILTER (WITH BLOCK PREVENTING CACHE) ---
# පැයක Trend දත්ත සර්වර් මත තාවකාලිකව තබාගන්නා මතකය (Local Cache)
TREND_CACHE = {}
CACHE_DURATION_SEC = 900  # විනාඩි 15ක් (තත්පර 900) යනතුරු Binance වෙත නොගොස් මතකයේ ඇති දත්තම භාවිතා කරයි.

def get_1h_trend_zone(symbol):
    global TREND_CACHE
    now = time.time()
    
    # 1. පරීක්ෂා කිරීම: මෙම කාසියේ 1H Trend එක විනාඩි 15ක් ඇතුළත දැනටමත් ගණනය කර තිබේද?
    if symbol in TREND_CACHE:
        cache_time, cached_zone = TREND_CACHE[symbol]
        if now - cache_time < CACHE_DURATION_SEC:
            return cached_zone  # කලින් ගබඩා කරගත් නිවැරදි කලාපය (Zone) සෘජුවම ලබා දෙයි
            
    try:
        # 2. Cache එකක් නොමැති නම් හෝ කාලය ඉකුත් වී ඇත්නම් පමණක් Binance වෙතින් දත්ත ලබා ගනී
        res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=1000", timeout=15)
        df_1h = pd.DataFrame(res.json(), columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        closes = df_1h['close'].astype(float)
        
        if len(closes) < 505: 
            return "BUY_ZONE" 
        
        # EMA ගණනය කිරීම
        ema_80_series = closes.ewm(span=80, adjust=False).mean()
        ema_160_series = closes.ewm(span=160, adjust=False).mean()
        ema_500_series = closes.ewm(span=500, adjust=False).mean()
        
        # ආරම්භක Zone එක (Default)
        current_zone = "BUY_ZONE"
        
        # 3. 💡 අතීතයේ සිට වර්තමානය දක්වා (Past to Present) පිළිවෙළට ස්කෑන් කිරීම
        for idx in range(500, len(closes) - 1):
            prev_80 = ema_80_series.iloc[idx]
            prev_160 = ema_160_series.iloc[idx]
            curr_80 = ema_80_series.iloc[idx + 1]
            curr_160 = ema_160_series.iloc[idx + 1]
            curr_500 = ema_500_series.iloc[idx + 1]
            
            # 🟢 BUY ZONE එකක් ඇරඹෙන කොන්දේසිය:
            if prev_80 < prev_160 and curr_80 >= curr_160:
                if curr_80 < curr_500:
                    current_zone = "BUY_ZONE"
            
            # 🔴 SELL ZONE එකක් ඇරඹෙන කොන්දේසිය:
            elif prev_80 > prev_160 and curr_80 <= curr_160:
                if curr_80 > curr_500:
                    current_zone = "SELL_ZONE"
                    
        # 4. අලුතින් සොයාගත් Trend එක ඉදිරි විනාඩි 15 සඳහා මතක තබා ගැනීම (Cache කිරීම)
        TREND_CACHE[symbol] = (now, current_zone)
        return current_zone
        
    except Exception as e: 
        print(f"Error in 1H Trend Zone for {symbol}: {e}")
        # සර්වර් Error එකක් ආවොත්, Cache එකේ පරණ දත්ත තිබේ නම් එය ලබා දෙයි, නැතහොත් Default BUY ZONE ලබා දේ
        if symbol in TREND_CACHE:
            return TREND_CACHE[symbol][1]
        return "BUY_ZONE"

# --- 🎯 5. FRACTAL FINDER ENGINE (LEFT 20, RIGHT 20) ---
def find_strict_20_bar_fractal(df, side):
    highs = df['high'].astype(float).tolist()
    lows = df['low'].astype(float).tolist()
    length = len(df)
    
    if length < 42: return None
    i = length - 21 
    
    if side == "BUY": 
        current_low = lows[i]
        if all(current_low < lows[i - j] for j in range(1, 21)) and all(current_low < lows[i + j] for j in range(1, 21)):
            return current_low
    elif side == "SELL": 
        current_high = highs[i]
        if all(current_high > highs[i - j] for j in range(1, 21)) and all(current_high > highs[i + j] for j in range(1, 21)):
            return current_high
    return None

# --- 🌊 FLAT LINE COIN FILTER ---
def is_flat_line_coin(df):
    if len(df) < 30: return True
    
    highs = df['high'].astype(float).iloc[-20:]
    lows = df['low'].astype(float).iloc[-20:]
    closes = df['close'].astype(float).iloc[-20:]
    
    avg_candle_range = (highs - lows).mean()
    current_price = float(closes.iloc[-1])
    
    if current_price == 0: return True
    
    volatility_pct = (avg_candle_range / current_price) * 100
    if volatility_pct < 0.015: 
        return True
        
    recent_closes = df['close'].astype(float).iloc[-15:].tolist()
    if len(set(recent_closes)) <= 3:  
        return True
        
    return False

# --- 🧠 6. 5M INDICATOR ALIGNMENT ENGINE ---
def check_5m_indicator_alignment(df, zone):
    if len(df) < 510: return "NONE"
    closes = df['close'].astype(float)
    
    ema_60 = closes.ewm(span=60, adjust=False).mean().iloc[-1]
    ema_80 = closes.ewm(span=80, adjust=False).mean().iloc[-1]
    ema_500 = closes.ewm(span=500, adjust=False).mean().iloc[-1]
    
    if zone == "BUY_ZONE":
        if (ema_500 > ema_80) and (ema_80 > ema_60):
            fractal_low = find_strict_20_bar_fractal(df, "BUY")
            if fractal_low: return "BUY"
    elif zone == "SELL_ZONE":
        if (ema_500 < ema_80) and (ema_80 < ema_60):
            fractal_high = find_strict_20_bar_fractal(df, "SELL")
            if fractal_high: return "SELL"
    return "NONE"

# --- 🚀 7. CORE SCANNER ENGINE ---
def scan_markets():
    print("🚀 Next-Gen Fractal Recovery Scanner Engine Active...")
    while True:
        try:
            if not is_ict_trading_window():
                time.sleep(60); continue
                
            with state_lock:
                is_scanning = state.get('is_scanning', True)
                bot_paused = state.get('is_paused', False)
                active_positions = dict(state['active_positions'])
                max_signals = state.get('max_signals', 3)
                recovery_only = state.get('recovery_only_mode', False)
            
            if is_scanning and not bot_paused:
                res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
                symbols = [t['symbol'] for t in res.json() if t['symbol'].endswith("USDT") and float(t.get('lastPrice', 0)) < 0.7]
                
                for s in symbols:
                    if s in state.get('block_list', []): continue
                    if s in active_positions: continue
                    
                    with state_lock: coin_step = state['symbol_recovery_step'].get(s, 0)
                    zone_status = get_1h_trend_zone(s)
                    
                    try:
                        time.sleep(0.04)
                        k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=600", timeout=10)
                        df = pd.DataFrame(k_res.json(), columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
                        if is_flat_line_coin(df): continue
                        
                        signal_type = check_5m_indicator_alignment(df, zone_status)
                        if signal_type == "NONE": continue
                        
                        execute_trade = False
                        if recovery_only and coin_step == 0:
                            continue
                        
                        if zone_status == "SELL_ZONE" and signal_type == "SELL":
                            if coin_step > 0 or (len(active_positions) < max_signals): 
                                execute_trade = True
                        elif zone_status == "BUY_ZONE" and signal_type == "BUY":
                            if coin_step > 0 or (len(active_positions) < max_signals): 
                                execute_trade = True
                        
                        if execute_trade:
                            current_p = float(df['close'].iloc[-1])
                            execute_new_recovery_trade(s, signal_type, current_p)
                    except: pass
            time.sleep(5)
        except: time.sleep(15)

# --- 📈 8. LOSS RECOVERY ENTRY FUNCTION ---
def execute_new_recovery_trade(s, side, current_p):
    with state_lock:
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 50.0)
    
    leverage = 10
    position_size = current_margin * leverage 
    coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
    
    if side == "BUY":
        initial_sl = current_p * (1.0 - coin_sl_move_pct)
        if step == 0:
            target_profit_dollars = current_margin * (state.get('fast_tp_pct', 25.0) / 100.0)
            required_move_pct = target_profit_dollars / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
        else:
            binance_fee_est = position_size * 0.0008 
            required_return = accumulated_loss + binance_fee_est + 0.15 
            required_move_pct = required_return / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
    else: # SELL
        initial_sl = current_p * (1.0 + coin_sl_move_pct)
        if step == 0:
            target_profit_dollars = current_margin * (state.get('fast_tp_pct', 25.0) / 100.0)
            required_move_pct = target_profit_dollars / position_size
            initial_tp = current_p * (1.0 - required_move_pct)
        else:
            binance_fee_est = position_size * 0.0008 
            required_return = accumulated_loss + binance_fee_est + 0.15
            required_move_pct = required_return / position_size
            initial_tp = current_p * (1.0 - required_move_pct)

    with state_lock:
        state['signal_count'] += 1
        sig_id = state['signal_count']
        state['active_positions'][s] = {
            "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
            "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
            "initial_1h_zone": get_1h_trend_zone(s) 
        }
        if state.get('alarm_active', True): state['last_alarm_symbol'] = f"{s} ({side} Step {step})"
        if state.get('reminder_system_active', True): state['pending_acknowledgement'] = True

    msg = (f"🔔 <b>FRACTAL SIGNAL #{sig_id}</b> 🚨\n\n"
           f"📍 Symbol: <code>{s}</code> | Side: <b>{side}</b>\n"
           f"📈 Recovery Step: <b>{step}/3 (Total 4 Steps)</b>\n"
           f"💵 Base Margin: <b>${current_margin} (10x)</b>\n"
           f"🛡️ Protection SL: <b>{sl_margin_pct}% (${current_margin * 0.5})</b>\n"
           f"📊 Accumulated Loss: <b>${round(accumulated_loss, 4)}</b>\n\n"
           f"🎯 Target TP Price: <code>{round(initial_tp, 5)}</code>\n"
           f"🛑 Stop Loss Price: <code>{round(initial_sl, 5)}</code>\n\n"
           f"Mr. RedBull LOSS RECOVERY MASTER👑")
    execute_telegram_send(msg)
    sync_save()

# --- ⏰ 9. MINUTE-BY-MINUTE REMINDER WORKER ---
def telegram_reminder_worker():
    while True:
        try:
            time.sleep(60) 
            with state_lock:
                is_pending = state.get('pending_acknowledgement', False)
                system_active = state.get('reminder_system_active', True)
            if is_pending and system_active:
                execute_telegram_send("⚠️ මතක් කිරීම නැවැත්වීමට <b>/ok</b> විධානය ලබාදෙන්න. ⚠️")
        except: pass

# --- 📈 10. REAL-TIME MONITOR LOOP & ZONE FLIP HANDLING ---
def live_monitor_loop():
    while True:
        try:
            with state_lock: 
                active_keys = list(state['active_positions'].keys())
            
            for s in active_keys:
                with state_lock: 
                    pos = state['active_positions'].get(s)
                if not pos: continue
                side = pos['side']
                
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=2", timeout=10)
                    current_p = float(k_res2.json()[-1][4])
                    
                    # 💥 1H ZONE FLIP DETECTOR 💥
                    current_1h_zone = get_1h_trend_zone(s)
                    if current_1h_zone != pos.get("initial_1h_zone"):
                        price_diff_pct = abs(pos['entry_price'] - current_p) / pos['entry_price']
                        flip_loss = (pos['margin'] * 10) * price_diff_pct if ((side == "BUY" and current_p < pos['entry_price']) or (side == "SELL" and current_p > pos['entry_price'])) else 0.0
                        
                        with state_lock:
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + flip_loss
                            state['symbol_recovery_step'][s] = state['symbol_recovery_step'].get(s, 0) + 1 
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                            
                        execute_telegram_send(f"🔄 <b>1H ZONE FLIPPED: {s}</b>\nකලාපය මාරු විය! වත්මන් ට්‍රේඩ් එක වසා දමන ලදී. <b>නමුත් වහාම ට්‍රේඩ් එකක් නොගනී!</b>\n\nනැවතත් 5M Chart එකේ EMA Cross එක සහ Candle 20 දෙපැත්ත වදින තෙක් (Strict Fractal එකක් හැදෙන තෙක්) බොට් ඉවසනු ඇත. ⏳")
                        sync_save()
                        continue
                    
                    # TARGET HIT (WIN) 🟢
                    if (side == "BUY" and current_p >= pos['tp']) or (side == "SELL" and current_p <= pos['tp']):
                        with state_lock:
                            state['stats']['wins'] += 1
                            state['daily_stats']['wins'] += 1
                            state['stats']['won_trades'].append({"symbol": s, "max_step": pos["step"], "time": str(datetime.datetime.now())})
                            state['daily_stats']['won_trades'].append({"symbol": s, "max_step": pos["step"]})
                            state['symbol_recovery_step'][s] = 0
                            state['symbol_accumulated_loss'][s] = 0.0
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                        sync_save()
                        execute_telegram_send(f"✅ <b>RECOVERY TARGET HIT: {s}</b>\nනියමිත ඉලක්කය සපුරා සියලුම පාඩු පියවා අවසන් කරන ලදී! 🎉")
                    
                    # STOP LOSS HIT (LOSS) 🛑
                    elif (side == "BUY" and current_p <= pos['sl']) or (side == "SELL" and current_p >= pos['sl']):
                        price_diff_pct = abs(pos['entry_price'] - pos['sl']) / pos['entry_price']
                        trade_loss = (pos['margin'] * 10) * price_diff_pct
                        
                        with state_lock:
                            state['stats']['loss'] += 1
                            state['daily_stats']['loss'] += 1
                            next_step = pos['step'] + 1
                            current_total_loss = state['symbol_accumulated_loss'].get(s, 0.0) + trade_loss
                            
                            if next_step >= 4: 
                                if s not in state['block_list']: state['block_list'].append(s)
                                state['symbol_recovery_step'][s] = 0
                                state['symbol_accumulated_loss'][s] = 0.0
                                execute_telegram_send(f"❌ <b>TOTAL RECOVERY FAILED (Step 4 Failed): {s}</b>\nසියලුම පියවර 4ම අසාර්ථක විය! මෙම කාසිය බ්ලැක්ලිස්ට් (Blacklist) කරන ලදී. 🚫")
                            else:
                                state['symbol_recovery_step'][s] = next_step
                                state['symbol_accumulated_loss'][s] = current_total_loss
                                execute_telegram_send(f"⚠️ <b>STOP LOSS HIT (Step {pos['step']}/3): {s}</b>\nපාඩුව: ${round(trade_loss, 3)}. ඊළඟ 5M Fractal එකෙන් මෙය რიකවර් කිරීමට සැකසුම් සූදානම් කළා. ⏳")
                            
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                        sync_save()
                except: pass
                time.sleep(0.1)
            time.sleep(2)
        except: time.sleep(5)

# --- 11. DAILY REPORT ENGINE ---
def generate_report_text(ds, title_prefix="📅 TODAY'S"):
    return (f"📊 <b>{title_prefix} FRACTAL PERFORMANCE REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n\n"
            f"🟢 Wins Count : <b>{ds.get('wins', 0)} Trades</b>\n"
            f"🔴 Losses Count : <b>{ds.get('loss', 0)} Trades</b>\n\n"
            f"👑 Mr. RedBull Recovery Bot")

def cron_daily_report_worker():
    while True:
        try:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            if utc_now.hour == 12 and utc_now.minute == 59:
                today_str = str(datetime.date.today())
                with state_lock:
                    ds = state['daily_stats']
                    if ds.get('last_reset_date') != today_str or ds['wins'] > 0 or ds['loss'] > 0:
                        execute_telegram_send(generate_report_text(ds, title_prefix="✨ FINAL"))
                        state['daily_stats'] = {'wins': 0, 'loss': 0, 'won_trades': [], 'last_reset_date': today_str}
                sync_save(); time.sleep(60)
            time.sleep(30)
        except: time.sleep(10)

# --- 💬 12. TELEGRAM WEBHOOK MANAGER ---
@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json()
        if not update or "message" not in update: return "OK", 200
        msg_obj = update["message"]; chat_id = msg_obj.get("chat", {}).get("id"); raw_text = msg_obj.get("text", "")
        
        if str(chat_id).strip() == str(TELEGRAM_CHAT_ID).strip() and raw_text:
            tokens = str(raw_text).strip().split()
            cmd = tokens[0].lower().replace("/", "")
            
            if cmd == "ok":
                with state_lock: state['pending_acknowledgement'] = False
                sync_save()
                execute_telegram_send("👌 <b>[ACKNOWLEDGED]</b>\nසිග්නල් මතක් කිරීම් (Reminders) තාවකාලිකව නිහඬ කරන ලදී.")
            
            elif cmd == "set_margin" and len(tokens) > 1:
                try:
                    val = float(tokens[1])
                    with state_lock: state['base_margin'] = val
                    sync_save()
                    execute_telegram_send(f"💵 <b>[MARGIN UPDATED]</b>\nමීළඟ සියලුම අලුත් ට්‍රේඩ් සඳහා මූලික Margin එක සාර්ථකව වෙනස් කරන ලදී!\n• නව අගය: <b>${val}</b>")
                except: execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි දශම අගයක් ඇතුළත් කරන්න. උදා: <code>/set_margin 0.80</code>")

            elif cmd == "set_sl_pct" and len(tokens) > 1:
                try:
                    val = float(tokens[1])
                    with state_lock: state['margin_sl_pct'] = val
                    sync_save()
                    execute_telegram_send(f"🛡️ <b>[SL PERCENTAGE UPDATED]</b>\nට්‍රේඩ් එකක පාඩු සීමාව (Stop Loss) සාර්ථකව වෙනස් කරන ලදී!\n• නව සීමාව: යෙදවූ මුදලෙන් <b>{val}%</b> ක් ලොස් වන විට.")
                except: execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි අගයක් දෙන්න. උදා: <code>/set_sl_pct 50</code>")

            elif cmd == "set_fast_tp_pct" and len(tokens) > 1:
                try:
                    val = float(tokens[1])
                    with state_lock: state['fast_tp_pct'] = val
                    sync_save()
                    execute_telegram_send(f"🎯 <b>[FAST TP UPDATED]</b>\nකිසිදු පාඩුවක් නොමැතිව ගන්නා පළමු සාමාන්‍ය ට්‍රේඩ් එකේ (Step 0) ලාභ සීමාව වෙනස් කරන ලදී!\n• නව ඉලක්කය: <b>{val}%</b> ලාභ ලැබුණු විට.")
                except: execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි අගයක් දෙන්න. උදා: <code>/set_fast_tp_pct 25</code>")

            elif cmd == "set_max" and len(tokens) > 1:
                try:
                    val = int(tokens[1])
                    if val > 0:
                        with state_lock: state['max_signals'] = val
                        sync_save()
                        execute_telegram_send(f"🚀 <b>[MAX TRADES UPDATED]</b>\nබොට්ට එකවර ඇරඹිය හැකි උපරිම සජීවී ට්‍රේඩ් ගණන සාර්ථකව වෙනස් කරන ලදී!\n• නව උපරිම සීමාව: <b>{val}</b>")
                    else: raise ValueError
                except: execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි පූර්ණ සංඛ්‍යාවක් ඇතුළත් කරන්න. උදා: <code>/set_max 3</code>")

            elif cmd == "set_start_time" and len(tokens) > 1:
                try:
                    t_parts = tokens[1].split(":")
                    h, m = int(t_parts[0]), int(t_parts[1])
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        with state_lock:
                            state['start_hour'] = h
                            state['start_minute'] = m
                        sync_save()
                        execute_telegram_send(f"⏰ <b>[START TIME UPDATED]</b>\nබොට් වැඩ ආරම්භ කරන වේලාව සාර්ථකව වෙනස් කරන ලදී: <b>{str(h).zfill(2)}:{str(m).zfill(2)}</b>")
                    else: raise ValueError
                except: execute_telegram_send("❌ දෝෂයකි! නිවැරදි වේලාව ඇතුළත් කරන්න (HH:MM). උදා: <code>/set_start_time 12:30</code>")

            elif cmd == "set_end_time" and len(tokens) > 1:
                try:
                    t_parts = tokens[1].split(":")
                    h, m = int(t_parts[0]), int(t_parts[1])
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        with state_lock:
                            state['end_hour'] = h
                            state['end_minute'] = m
                        sync_save()
                        execute_telegram_send(f"⏰ <b>[END TIME UPDATED]</b>\nබොට් වැඩ අවසන් කරන වේලාව සාර්ථකව වෙනස් කරන ලදී: <b>{str(h).zfill(2)}:{str(m).zfill(2)}</b>")
                    else: raise ValueError
                except: execute_telegram_send("❌ දෝෂයකි! නිවැරදි වේලාව ඇතුළත් කරන්න (HH:MM). උදා: <code>/set_end_time 23:59</code>")
            
            elif cmd == "reminder_on":
                with state_lock: state['reminder_system_active'] = True
                sync_save()
                execute_telegram_send("🔔 <b>[REMINDER SYSTEM ON]</b>\nසිග්නල් එකක් ආ විට ඔබ /ok ලබාදෙන තෙක් මිනිත්තුවෙන් මිනිත්තුවට මතක් කිරීම් සිදු කරයි.")
            
            elif cmd == "reminder_off":
                with state_lock: 
                    state['reminder_system_active'] = False
                    state['pending_acknowledgement'] = False
                sync_save()
                execute_telegram_send("🔕 <b>[REMINDER SYSTEM OFF]</b>\nකරදරාකාරී මිනිත්තුවේ සිහිගැන්වීම් පණිවිඩ සම්පූර්ණයෙන්ම අක්‍රීය කරන ලදී.")
            
            elif cmd == "rec_only_on":
                with state_lock: state['recovery_only_mode'] = True
                sync_save()
                execute_telegram_send("🛑 <b>[RECOVERY ONLY MODE ON]</b>\nඅලුත් Trades (Step 0) ගැනීම අත්හිටුවන ලදී. දැනට ලොස් එකේ ඇති කාසිවල Recovery සිග්නල් (Step 1, 2, 3) පමණක් ක්‍රියාත්මක වේ.")

            elif cmd == "rec_only_off":
                with state_lock: state['recovery_only_mode'] = False
                sync_save()
                execute_telegram_send("🟢 <b>[NORMAL MODE ACTIVE]</b>\nසාමාන්‍ය පරිදි අලුත් සිග්නල් සහ Recovery සිග්නල් යන දෙකම ක්‍රියාත්මක වේ.")

            elif cmd == "status":
                window_status = "ACTIVE 🟢" if is_ict_trading_window() else "SLEEP 💤"
                with state_lock:
                    st_h, st_m = str(state.get('start_hour', 12)).zfill(2), str(state.get('start_minute', 30)).zfill(2)
                    en_h, en_m = str(state.get('end_hour', 23)).zfill(2), str(state.get('end_minute', 59)).zfill(2)
                    rec_mode_str = 'ON (අලුත් සිග්නල් නැත)' if state.get('recovery_only_mode') else 'OFF (සාමාන්‍ය)'
                    
                    msg = (f"ℹ️ <b>[RED BULL MASTER STATUS REPORT]</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━\n\n"
                           f"▶️ ස්කෑනර් එන්ජිම: <b>{'සක්‍රීයයි (ON)' if state.get('is_scanning') else 'අක්‍රීයයි (OFF)'}</b>\n"
                           f"🔄 Recovery Only Mode: <b>{rec_mode_str}</b>\n"
                           f"⏸️ PRADANA VIRAMA: <b>{'PAUSED' if state.get('is_paused') else 'RUNNING'}</b>\n"
                           f"🔥 සජීවී ට්‍රේඩ් ගණන: <b>{len(state['active_positions'])} / {state.get('max_signals')}</b>\n"
                           f"📢 මතක් කිරීමේ පද්ධතිය: <b>{'සක්‍රීයයි 🔔' if state.get('reminder_system_active') else 'නිහඬයි 🔕'}</b>\n"
                           f" ⏱️ ICT WINDOW STATUS : <b>{window_status}</b>\n"
                           f"⏰ සක්‍රීය කාල රාමුව: <b>දවල් {st_h}:{st_m} සිට රාත්‍රී {en_h}:{en_m} දක්වා පමණි.</b>\n"
                           f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                           f"🛡️ SL: <b>{state.get('margin_sl_pct', 50.0)}%</b>\n"
                           f"🎯 TP: <b>{state.get('fast_tp_pct', 25.0)}%</b>")
                execute_telegram_send(msg)
            
            elif cmd == "balance":
                with state_lock:
                    init = state.get('manual_initial_balance', 100.0)
                    profit = (state['stats']['wins'] * 0.20) - (state['stats']['loss'] * 0.40)
                execute_telegram_send(f"💰 <b>[ACCOUNT BALANCE]</b>\n\n• ආරම්භක ගිණුම් ශේෂය: <b>${init}</b>\n• දැනට උපයා ඇති දළ ශේෂය: <b>${round(init + profit, 2)} USDT</b>")
            
            elif cmd == "t_s":
                with state_lock:
                    w_lines = [f"• {t['symbol']} (Step {t['max_step']})" for t in state['stats'].get('won_trades', [])[-5:]]
                    l_text = ", ".join(state['stats'].get('lost_trades', [])) if state['stats'].get('lost_trades') else "කිසිවක් නැත"
                w_joined = "\n".join(w_lines) if w_lines else 'කිසිවක් නැත'
                execute_telegram_send(f"📊 <b>[HISTORY STATISTICS]</b>\n\n<b>අවසන් වරට දිනූ ට්‍රේඩ් 5:</b>\n━━━━━━━━━━━━━━━━━\n{w_joined}\n\n<b>බ්ලැක්ලිස්ට් (🚫) වූ කාසි:</b>\n<code>{l_text}</code>")
            
            elif cmd == "report":
                with state_lock: ds = state.get('daily_stats', {})
                execute_telegram_send(generate_report_text(ds, title_prefix="📊 CURRENT"))
            
            elif cmd == "pause":
                with state_lock: state['is_scanning'] = False
                sync_save()
                execute_telegram_send("⏸️ <b>[SCANNER PAUSED]</b>\nඅලුත් සිග්නල් සෙවීම තාවකාලිකව නවත්වන ලදී. දැනට දිවෙන ට්‍රේඩ්ස් වලට බලපෑමක් නැත.")
            
            elif cmd == "resume":
                with state_lock: state['is_scanning'] = True
                sync_save()
                execute_telegram_send("▶️ <b>[SCANNER RESUMED]</b>\nස්කෑනර් එන්ජිම නැවත පණ ගන්වන ලදී. අලුත් ට්‍රේඩ් සෙවීම ආරම්භ වේ.")
            
            elif cmd == "block_list":
                with state_lock: bl = ", ".join(state.get('block_list', [])) if state.get('block_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🚫 <b>[BLOCKED COINS]</b>\nපියවර 4ම අසාර්ථක වී බොට් විසින් ට්‍රේඩ් කිරීම තහනම් කර ඇති කාසි:\n<code>{bl}</code>")
            
            elif cmd == "block_add" and len(tokens) > 1:
                coin = tokens[1].upper()
                with state_lock:
                    if coin not in state['block_list']: state['block_list'].append(coin)
                sync_save()
                execute_telegram_send(f"🚫 <code>{coin}</code> කාසිය සාර්ථකව බ්ලැක්ලිස්ට් එකට එකතු කරන ලදී. බොට් මෙය තවදුරටත් ස්කෑන් නොකරයි.")
            
            elif cmd == "block_remove" and len(tokens) > 1:
                coin = tokens[1].upper()
                with state_lock:
                    if coin in state['block_list']: state['block_list'].remove(coin)
                sync_save()
                execute_telegram_send(f"✅ <code>{coin}</code> කාසිය බ්ලැක්ලිස්ට් එකෙන් ඉවත් කරන ලදී. බොට්ට නැවත එය ට්‍රේඩ් කිරීමට අවසර ඇත.")
            
            elif cmd == "alarm_on":
                with state_lock: state['alarm_active'] = True
                sync_save()
                execute_telegram_send("🔊 <b>[ALARM ON]</b>\nඅලුත් සිග්නල් එකක් ඇතුල් වන විට ශබ්දයක් සහිතව ඇලර්ට් පණිවිඩ පැමිණේ.")
            
            elif cmd == "alarm_off":
                with state_lock: state['alarm_active'] = False
                sync_save()
                execute_telegram_send("🔇 <b>[ALARM MUTED]</b>\nසිග්නල් ඇලර්ට් ශබ්දයන් නිහඬ කරන ලදී.")
            
            elif cmd == "active_trades":
                with state_lock: ak = list(state['active_positions'].keys())
                lines = []
                for k in ak:
                    pos = state['active_positions'][k]
                    lines.append(f"• <code>{pos['symbol']}</code> ({pos['side']}) | <b>පියවර (Step): {pos['step']}</b>")
                execute_telegram_send(f"🔥 <b>[LIVE ACTIVE TRADES]</b>\nමේ මොහොතේ සජීවීව මාකට් එකේ විවෘතව පවතින ට්‍රේඩ් ලැයිස්තුව:\n" + ("\n".join(lines) if lines else "සජීවී ට්‍රේඩ් කිසිවක් නොමැත."))
            
            elif cmd == "master_pause":
                with state_lock: state['is_paused'] = True
                sync_save()
                execute_telegram_send("🛑 <b>[MASTER PAUSED COMPLETED]</b>\nහදිසි අවස්ථාවකි! බොට්ගේ සම්පූර්ණ ක්‍රියාකාරීත්වයම (Master Stop) අත්හිටුවන ලදී.")

            elif cmd == "master_resume":
                with state_lock: state['is_paused'] = False
                sync_save()
                execute_telegram_send("🟢 <b>[MASTER RESUMED ACTIVATED]</b>\nබොට්ව නැවත සාමාන්‍ය ක්‍රියාකාරී තත්ත්වයට පත් කරන ලදී.")
                
            elif cmd == "clear_stats":
                with state_lock:
                    state['stats'] = {'wins': 0, 'loss': 0, 'total_pnl': 0.0, 'won_trades': [], 'lost_trades': []}
                    state['daily_stats'] = {'wins': 0, 'loss': 0, 'won_trades': [], 'last_reset_date': str(datetime.date.today())}
                sync_save()
                execute_telegram_send("♻️ <b>[STATISTICS RESET]</b>\nබොට්ගේ පැරණි දිනුම්/පැරදුම් ඉතිහාස දත්ත සියල්ල සාර්ථකව මකා දමන ලදී!")

            elif cmd == "recovery_status":
                with state_lock:
                    recovery_steps = dict(state.get('symbol_recovery_step', {}))
                    accumulated_losses = dict(state.get('symbol_accumulated_loss', {}))
                lines = []
                for coin, step in recovery_steps.items():
                    if step > 0:
                        loss_amt = accumulated_losses.get(coin, 0.0)
                        lines.append(f"• <code>{coin}</code> ➡️ <b>පියවර: {step}/3</b> | පියවිය යුතු පාඩුව: <code>${round(loss_amt, 4)}</code>")
                if lines:
                    report_msg = "⏳ <b>[PENDING RECOVERY LIST]</b>\nකලින් ට්‍රේඩ් පැරදී, දැනට පාඩුව පියවා ගැනීමට බලාපොරොත්තුවෙන් සිටින කාසි:\n" + "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n" + "\n".join(lines)
                else:
                    report_msg = "✅ <b>සියලුම කාසි රිකවර් වී ඇත!</b>\nමේ මොහොතේ ලොස් එකේ පවතින (Pending Recovery) කිසිදු කාසියක් නොමැත."
                execute_telegram_send(report_msg)
            
            elif cmd in ["menu", "help"]:
                menu_msg = (
                    f"👑 <b>RED-BULL LOSS RECOVERY MASTER PANEL</b> 👑\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"⚙️ <b>1. නව පාලක විධානයන් (Dynamic Settings)</b>\n"
                    f"• <code>/set_margin [අගය]</code> - ට්‍රේඩ් මුදල වෙනස් කරයි\n• <code>/set_sl_pct [අගය]</code> - SL පාඩු සීමාව වෙනස් කරයි\n• <code>/set_fast_tp_pct [අගය]</code> - ලාභ සීමාව වෙනස් කරයි\n"
                    f"• <code>/set_max [ගණන]</code> - උපරිම සජීවී ට්‍රේඩ් ගණන වෙනස් කරයි\n"
                    f"• <code>/set_start_time [HH:MM]</code> - වැඩ අරඹන වේලාව වෙනස් කරයි\n• <code>/set_end_time [HH:MM]</code> - වැඩ අවසන් කරන වේලාව වෙනස් කරයි\n\n"
                    f"🛡️ <b>2. බොට් සහ ස්කෑනර් පාලනය (Bot Controls)</b>\n"
                    f"• <code>/pause</code> | <code>/resume</code> - ස්කෑනර් නවත්වයි / පණගන්වයි\n"
                    f"• <code>/rec_only_on</code> | <code>/rec_only_off</code> - Recovery Mode (On / Off)\n"
                    f"• <code>/master_pause</code> | <code>/master_resume</code> - බොට් මුළුමනින්ම නවත්වයි / පණගන්වයි\n• <code>/ok</code> - මිනිත්තුවේ ඇලර්ට් නිහඬ කරයි\n\n"
                    f"📊 <b>3. ගිණුම් තත්ත්ව සහ වාර්තා (Status & Reports)</b>\n"
                    f"• <code>/status</code> - වත්මන් තත්ත්ව වාර්තාව\n• <code>/active_trades</code> - දැනට දිවෙන සජීවී ට්‍රේඩ් ලැයිස්තුව\n• <code>/recovery_status</code> - පාඩු පියවීමට ඇති කාසි ලැයිස්තුව\n• <code>/balance</code> - ගිණුමේ වත්මන් දළ ශේෂය\n• <code>/report</code> - අද දවසේ මුළු වාර්තාව\n• <code>/t_s</code> - මුළු ඉතිහාස වාර්තාව\n• <code>/clear_stats</code> - වාර්තා මකා දමයි\n\n"
                    f"🚫 <b>4. ආරක්ෂක සහ පද්ධති සැකසුම් (Safety & System)</b>\n"
                    f"• <code>/block_list</code> - තහනම් කළ කාසි ලැයිස්තුව\n• <code>/block_add [COIN]</code> - කාසි තහනම් කරයි\n• <code>/block_remove [COIN]</code> - තහනම ඉවත් කරයි\n• <code>/reminder_on</code> | <code>/reminder_off</code> - මිනිත්තුවේ සිහිගැන්වීම ක්‍රියාත්මක/අක්‍රීය කරයි\n• <code>/alarm_on</code> | <code>/alarm_off</code> - ඇලර්ට් ශබ්ද ක්‍රියාත්මක/අක්‍රීය කරයි\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
                execute_telegram_send(menu_msg)
    except Exception as e: print(f"Webhook Main Error: {e}")
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "Fractal Strategy Live Recovery Bot Active!", 200

if __name__ == '__main__':
    with state_lock:
        state['pending_acknowledgement'] = False
    sync_save()

    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    threading.Thread(target=telegram_reminder_worker, daemon=True).start() 
    
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
