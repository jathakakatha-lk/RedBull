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

# --- 1. CONFIGURATIONS & INITIALIZATION ---
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 8080))

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
        
        'first_win_list': [],           
        'shared_loss_buffer': 0.0,       
        'shared_loss_splits': 0,         
        
        'base_margin': 0.80,            
        'margin_sl_pct': 27.0,          
        'fast_tp_pct': 30.0,            
        'leverage': 10,                 
        
        'sig_start_hour': 12, 'sig_start_minute': 30,  
        'sig_end_hour': 23, 'sig_end_minute': 59,      
        
        'rec_start_hour': 8, 'rec_start_minute': 0,    
        'rec_end_hour': 23, 'rec_end_minute': 59,      
        
        'coin_stats': {},
        'scanned_coins_count': 0  
    }
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f: 
                loaded_state = json.load(f)
                for k, v in default_state.items():
                    if k not in loaded_state: loaded_state[k] = v
                if 'coin_stats' not in loaded_state: loaded_state['coin_stats'] = {}
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

# --- 3. TELEGRAM MESSENGER CORE ---
def execute_telegram_send(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": str(TELEGRAM_CHAT_ID).strip(), "text": msg, "parse_mode": "HTML"}
    for attempt in range(3):
        try:
            res = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code == 200: return True
            elif res.status_code == 429:
                retry_after = res.json().get('parameters', {}).get('retry_after', 5)
                time.sleep(retry_after)
        except:
            time.sleep(2)
    return False

# --- 4. TIME WINDOW CHECKERS ---
def is_signal_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('sig_start_hour', 12) * 60) + state.get('sig_start_minute', 30)
            end_time = (state.get('sig_end_hour', 23) * 60) + state.get('sig_end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

def is_recovery_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('rec_start_hour', 8) * 60) + state.get('rec_start_minute', 0)
            end_time = (state.get('rec_end_hour', 23) * 60) + state.get('rec_end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

# --- 5. MARKET TREND & ZONE ANALYZER ---
TREND_CACHE = {}
CACHE_DURATION_SEC = 900  

def get_1h_trend_zone(symbol):
    global TREND_CACHE
    now = time.time()
    if symbol in TREND_CACHE:
        cache_time, cached_zone = TREND_CACHE[symbol]
        if now - cache_time < CACHE_DURATION_SEC: return cached_zone  
    try:
        res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=1000", timeout=15)
        df_1h = pd.DataFrame(res.json(), columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        closes = df_1h['close'].astype(float)
        if len(closes) < 505: return "BUY_ZONE" 
        
        ema_80_series = closes.ewm(span=80, adjust=False).mean()
        ema_160_series = closes.ewm(span=160, adjust=False).mean()
        ema_500_series = closes.ewm(span=500, adjust=False).mean()
        
        current_zone = "BUY_ZONE"
        for idx in range(500, len(closes) - 1):
            prev_80, prev_160 = ema_80_series.iloc[idx], ema_160_series.iloc[idx]
            curr_80, curr_160, curr_500 = ema_80_series.iloc[idx + 1], ema_160_series.iloc[idx + 1], ema_500_series.iloc[idx + 1]
            if prev_80 < prev_160 and curr_80 >= curr_160:
                if curr_80 < curr_500: current_zone = "BUY_ZONE"
            elif prev_80 > prev_160 and curr_80 <= curr_160:
                if curr_80 > curr_500: current_zone = "SELL_ZONE"
        TREND_CACHE[symbol] = (now, current_zone)
        return current_zone
    except:
        if symbol in TREND_CACHE: return TREND_CACHE[symbol][1]
        return "BUY_ZONE"

# --- 6. 5M FRACTAL & INDICATOR ALIGNMENT ---
def find_strict_20_bar_fractal(df, side):
    highs = df['high'].astype(float).tolist()
    lows = df['low'].astype(float).tolist()
    length = len(df)
    if length < 42: return None
    i = length - 21 
    if side == "BUY": 
        current_low = lows[i]
        if all(current_low < lows[i - j] for j in range(1, 21)) and all(current_low < lows[i + j] for j in range(1, 21)): return current_low
    elif side == "SELL": 
        current_high = highs[i]
        if all(current_high > highs[i - j] for j in range(1, 21)) and all(current_high > highs[i + j] for j in range(1, 21)): return current_high
    return None

def is_flat_line_coin(df):
    if len(df) < 30: return True
    highs = df['high'].astype(float).iloc[-20:]
    lows = df['low'].astype(float).iloc[-20:]
    closes = df['close'].astype(float).iloc[-20:]
    avg_candle_range = (highs - lows).mean()
    current_price = float(closes.iloc[-1])
    if current_price == 0: return True
    if (avg_candle_range / current_price) * 100 < 0.015: return True
    if len(set(df['close'].astype(float).iloc[-15:].tolist())) <= 3: return True
    return False

def check_5m_indicator_alignment(df, zone):
    if len(df) < 510: return "NONE"
    closes = df['close'].astype(float)
    ema_60 = closes.ewm(span=60, adjust=False).mean().iloc[-1]
    ema_80 = closes.ewm(span=80, adjust=False).mean().iloc[-1]
    ema_500 = closes.ewm(span=500, adjust=False).mean().iloc[-1]
    if zone == "BUY_ZONE" and (ema_500 > ema_80) and (ema_80 > ema_60):
        if find_strict_20_bar_fractal(df, "BUY"): return "BUY"
    elif zone == "SELL_ZONE" and (ema_500 < ema_80) and (ema_80 < ema_60):
        if find_strict_20_bar_fractal(df, "SELL"): return "SELL"
    return "NONE"

# --- 7. BACKGROUND MARKET SCANNER (24/7) ---
def scan_markets():
    while True:
        try:
            with state_lock:
                is_scanning = state.get('is_scanning', True)
                bot_paused = state.get('is_paused', False)
                active_positions = dict(state['active_positions'])
                max_signals = state.get('max_signals', 3)
                current_margin = state.get('base_margin', 0.80)
                leverage = state.get('leverage', 10)
            position_size = current_margin * leverage 
            
            if is_scanning and not bot_paused:
                res = requests.get("https://fapi.binance.com/v1/ticker/24hr", timeout=15)
                symbols = [t['symbol'] for t in res.json() if t['symbol'].endswith("USDT") and float(t.get('lastPrice', 0)) > 0 and position_size >= 5.0]
                
                with state_lock:
                    state['scanned_coins_count'] = len(symbols)
                
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
                        if (zone_status == "SELL_ZONE" and signal_type == "SELL") or (zone_status == "BUY_ZONE" and signal_type == "BUY"):
                            if s in state.get('first_win_list', []):
                                if is_signal_window() and is_recovery_window():
                                    if coin_step > 0 or (len([p for p in active_positions.values() if p['symbol'] in state['first_win_list']]) < max_signals):
                                        execute_trade = True
                            else:
                                execute_trade = True
                                
                        if execute_trade:
                            execute_new_recovery_trade(s, signal_type, float(df['close'].iloc[-1]))
                    except: pass
            time.sleep(5)
        except: time.sleep(15)

# --- 8. RECOVERY ORDER EXECUTION ENGINE ---
def execute_new_recovery_trade(s, side, current_p):
    with state_lock:
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 27.0)
        leverage = state.get('leverage', 10)
        is_verified_coin = s in state.get('first_win_list', [])
        
        if s not in state['coin_stats']: state['coin_stats'][s] = {"run_trade": 0, "profit": 0}
        if step == 0: state['coin_stats'][s]["run_trade"] += 1
        coin_info = state['coin_stats'][s]
        
        if step == 0 and state.get('shared_loss_splits', 0) > 0:
            split_amount = state['shared_loss_buffer'] / state['shared_loss_splits']
            accumulated_loss += split_amount
            state['shared_loss_splits'] -= 1
            state['shared_loss_buffer'] -= split_amount
            
    position_size = current_margin * leverage 
    coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
    
    if side == "BUY":
        initial_sl = current_p * (1.0 - coin_sl_move_pct)
        if step == 0:
            required_move_pct = ((current_margin * (state.get('fast_tp_pct', 30.0) / 100.0)) + accumulated_loss) / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
        else:
            required_move_pct = (accumulated_loss + (position_size * 0.0008) + 0.15) / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
    else:
        initial_sl = current_p * (1.0 + coin_sl_move_pct)
        if step == 0:
            required_move_pct = ((current_margin * (state.get('fast_tp_pct', 30.0) / 100.0)) + accumulated_loss) / position_size
            initial_tp = current_p * (1.0 - required_move_pct)
        else:
            required_move_pct = (accumulated_loss + (position_size * 0.0008) + 0.15) / position_size
            initial_tp = current_p * (1.0 - required_move_pct)
            
    with state_lock:
        state['active_positions'][s] = {
            "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
            "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
            "initial_1h_zone": get_1h_trend_zone(s) 
        }
        
    if is_verified_coin:
        with state_lock:
            state['signal_count'] += 1
            sig_id = state['signal_count']
            if state.get('alarm_active', True): state['last_alarm_symbol'] = f"{s} ({side} Step {step})"
            if state.get('reminder_system_active', True): state['pending_acknowledgement'] = True
            
        msg = (f"🔔 <b>NEW SIGNAL #{sig_id:02d}</b> 🚨\n\n"
               f"📍 Symbol: <b>{s}</b> | Side: <b>{side}</b>\n"
               f"📈 Recovery Step: <b>{step + 1}/3</b> (Total 4 Steps)\n"
               f"💵 Base Margin: <b>${current_margin} ({leverage}x)</b>\n"
               f"🛡️ Protection SL: <b>{sl_margin_pct}% (${round(current_margin * (sl_margin_pct/100.0), 2)})</b>\n"
               f"📊 Accumulated Loss: <b>${round(accumulated_loss, 4)}</b>\n"
               f"⚡ <b>RUN TRADE {coin_info['run_trade']} | PROFIT {coin_info['profit']}</b>\n\n"
               f"🎯 Target TP Price: <code>{round(initial_tp, 5)}</code>\n"
               f"🛑 Stop Loss Price: <code>{round(initial_sl, 5)}</code>\n\n"
               f"Mr. RedBull LOSS RECOVERY MASTER👑")
        execute_telegram_send(msg)
        
    sync_save()

# --- 9. LIVE POSITION MONITOR LOOP ---
def live_monitor_loop():
    while True:
        try:
            with state_lock: active_keys = list(state['active_positions'].keys())
            for s in active_keys:
                with state_lock: pos = state['active_positions'].get(s)
                if not pos: continue
                side = pos['side']
                is_verified = s in state.get('first_win_list', [])
                
                if is_verified and not is_recovery_window():
                    time.sleep(1); continue
                    
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=2", timeout=10)
                    current_p = float(k_res2.json()[-1][4])
                    
                    if get_1h_trend_zone(s) != pos.get("initial_1h_zone"):
                        flip_loss = (pos['margin'] * state.get('leverage', 10)) * (abs(pos['entry_price'] - current_p) / pos['entry_price']) if ((side == "BUY" and current_p < pos['entry_price']) or (side == "SELL" and current_p > pos['entry_price'])) else 0.0
                        with state_lock:
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + flip_loss
                            state['symbol_recovery_step'][s] = state['symbol_recovery_step'].get(s, 0) + 1 
                            if s in state['active_positions']: del state['active_positions'][s]
                        if is_verified:
                            execute_telegram_send(f"🔄 <b>1H ZONE FLIPPED: {s}</b>\nකලාපය මාරු විය! වත්මන් ට්‍රේඩ් එක වසා දැමුවා. ⏳")
                        sync_save(); continue
                        
                    if (side == "BUY" and current_p >= pos['tp']) or (side == "SELL" and current_p <= pos['tp']):
                        with state_lock:
                            state['stats']['wins'] += 1; state['daily_stats']['wins'] += 1
                            if s not in state['first_win_list']: state['first_win_list'].append(s)
                            if s in state['coin_stats']: state['coin_stats'][s]["profit"] += 1
                            state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                        if is_verified:
                            execute_telegram_send(f"✅ <b>RECOVERY TARGET HIT: {s}</b>\nනියමිත ඉලක්කය සපුරා සියලුම පාඩු පියවා අවසන් කරන ලදී! 🎉")
                            
                    elif (side == "BUY" and current_p <= pos['sl']) or (side == "SELL" and current_p >= pos['sl']):
                        trade_loss = (pos['margin'] * state.get('leverage', 10)) * (abs(pos['entry_price'] - pos['sl']) / pos['entry_price'])
                        with state_lock:
                            state['stats']['loss'] += 1; state['daily_stats']['loss'] += 1
                            next_step = pos['step'] + 1
                            current_total_loss = state['symbol_accumulated_loss'].get(s, 0.0) + trade_loss
                            
                            if next_step >= 4: 
                                if s not in state['block_list']: state['block_list'].append(s)
                                if s in state.get('first_win_list', []): state['first_win_list'].remove(s)
                                state['shared_loss_buffer'] += current_total_loss; state['shared_loss_splits'] = 8
                                state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                                if is_verified:
                                    execute_telegram_send(f"❌ <b>TOTAL RECOVERY FAILED: {s}</b>\nපියවර 4ම අසාර්ථක විය! මෙම කාසිය බ්ලැක්ලිස්ට් කරන ලදී. 🚫")
                            else:
                                state['symbol_recovery_step'][s] = next_step; state['symbol_accumulated_loss'][s] = current_total_loss
                                if is_verified:
                                    execute_telegram_send(f"⚠️ <b>STOP LOSS HIT (Step {pos['step'] + 1}/3): {s}</b>\nඊළඟ 5M Fractal එකෙන් රිකවර් කිරීමට සැකසුම් සූදානම්. ⏳")
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                except: pass
                time.sleep(0.1)
            time.sleep(2)
        except: time.sleep(5)

# --- 10. CRON WORKERS & DAILY REPORTING ---
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

def generate_report_text(ds, title_prefix="📅 TODAY'S"):
    return (f"📊 <b>{title_prefix} PERFORMANCE REPORT</b>\n━━━━━━━━━━━━━━━━━━━\n\n🟢 Wins: <b>{ds.get('wins', 0)}</b>\n🔴 Loss: <b>{ds.get('loss', 0)}</b>\n\nMr. MASTER👑")

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

# --- 11. TELEGRAM WEBHOOK MANAGER (COMMANDS) ---
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
                sync_save(); execute_telegram_send("👌 <b>[ACKNOWLEDGED]</b>"); return "OK", 200
            
            elif cmd == "block_list":
                with state_lock: bl = ", ".join(state.get('block_list', [])) if state.get('block_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🚫 <b>[BLACKLISTED COINS]</b>\n<code>{bl}</code>"); return "OK", 200

            elif cmd == "add_block" and len(tokens) > 1:
                coin_to_add = tokens[1].upper()
                with state_lock:
                    if coin_to_add not in state['block_list']: state['block_list'].append(coin_to_add)
                    if coin_to_add in state.get('first_win_list', []): state['first_win_list'].remove(coin_to_add)
                sync_save(); execute_telegram_send(f"🚫 <code>{coin_to_add}</code> Blacklist කරන ලදී."); return "OK", 200

            elif cmd == "remove_block" and len(tokens) > 1:
                coin_to_rem = tokens[1].upper()
                with state_lock:
                    if coin_to_rem in state['block_list']: state['block_list'].remove(coin_to_rem)
                sync_save(); execute_telegram_send(f"🟢 <code>{coin_to_rem}</code> Blacklist එකෙන් ඉවත් කළා."); return "OK", 200

            elif cmd == "first_win_list":
                with state_lock:
                    lines = []
                    for s in state.get('first_win_list', []):
                        stats = state['coin_stats'].get(s, {"run_trade": 0, "profit": 0})
                        lines.append(f"• <code>{s}</code> | RUN: <b>{stats['run_trade']}</b> | PROFIT: <b>{stats['profit']}</b>")
                    fwl = "\n".join(lines) if lines else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🥇 <b>[FIRST WIN LIST COINS]</b>\n\n{fwl}"); return "OK", 200

            elif cmd == "add_first" and len(tokens) > 1:
                coin_to_add = tokens[1].upper()
                with state_lock:
                    if coin_to_add not in state['first_win_list']: state['first_win_list'].append(coin_to_add)
                    if coin_to_add in state.get('block_list', []): state['block_list'].remove(coin_to_add)
                    if coin_to_add not in state['coin_stats']: state['coin_stats'][coin_to_add] = {"run_trade": 0, "profit": 0}
                sync_save(); execute_telegram_send(f"🥇 <code>{coin_to_add}</code> First Win List එකට එකතු කළා."); return "OK", 200

            elif cmd == "remove_first" and len(tokens) > 1:
                coin_to_rem = tokens[1].upper()
                with state_lock:
                    if coin_to_rem in state['first_win_list']: state['first_win_list'].remove(coin_to_rem)
                sync_save(); execute_telegram_send(f"❌ <code>{coin_to_rem}</code> First Win List එකෙන් ඉවත් කළා."); return "OK", 200
            
            elif cmd == "set_times" and len(tokens) > 4:
                try:
                    sig_start = tokens[1].split(":")
                    sig_end = tokens[2].split(":")
                    rec_start = tokens[3].split(":")
                    rec_end = tokens[4].split(":")
                    with state_lock:
                        state['sig_start_hour'], state['sig_start_minute'] = int(sig_start[0]), int(sig_start[1])
                        state['sig_end_hour'], state['sig_end_minute'] = int(sig_end[0]), int(sig_end[1])
                        state['rec_start_hour'], state['rec_start_minute'] = int(rec_start[0]), int(rec_start[1])
                        state['rec_end_hour'], state['rec_end_minute'] = int(rec_end[0]), int(rec_end[1])
                    sync_save()
                    msg = (f"⏰ <b>[TIMERS UPDATED SUCCESSFULLY]</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                           f"🔍 1. First Win Scanner: <b>24 Hours Active 🔄</b>\n"
                           f"📢 2. Trade Signal Window: <b>{tokens[1]} - {tokens[2]}</b>\n"
                           f"🔄 3. Recovery Trade Window: <b>{tokens[3]} - {tokens[4]}</b>")
                    execute_telegram_send(msg)
                except Exception as e:
                    execute_telegram_send(f"❌ Time format එක වැරදියි. උදා: `/set_times 12:30 23:59 08:00 23:59` ලෙස ඇතුලත් කරන්න.")
                return "OK", 200
            
            elif cmd == "set_margin" and len(tokens) > 1:
                try:
                    with state_lock: state['base_margin'] = float(tokens[1])
                    sync_save(); execute_telegram_send(f"💵 Margin එක ${tokens[1]} ලෙස වෙනස් කළා.")
                except: pass
                return "OK", 200

            elif cmd == "set_leverage" and len(tokens) > 1:
                try:
                    lev_val = int(tokens[1])
                    with state_lock: state['leverage'] = lev_val
                    sync_save(); execute_telegram_send(f"⚙️ Leverage එක <b>{lev_val}x</b> ලෙස වෙනස් කරන ලදී.")
                except: pass
                return "OK", 200

            elif cmd == "status":
                sig_window = "ACTIVE 🟢" if is_signal_window() else "SLEEP 💤"
                rec_window = "ACTIVE 🟢" if is_recovery_window() else "SLEEP 💤"
                with state_lock:
                    msg = (f"ℹ️ <b>[RED BULL MASTER STATUS REPORT]</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                           f"▶️ ස්කෑනර් එන්ට්‍රීම: <b>{'සක්‍රීයයි (ON)' if state.get('is_scanning') else 'අක්‍රීයයි (OFF)'}</b>\n"
                           f"⏱️ SIGNAL WINDOW: <b>{sig_window} ({state.get('sig_start_hour',12):02d}:{state.get('sig_start_minute',30):02d} - {state.get('sig_end_hour',23):02d}:{state.get('sig_end_minute',59):02d})</b>\n"
                           f"⏱️ RECOVERY WINDOW: <b>{rec_window} ({state.get('rec_start_hour',8):02d}:{state.get('rec_start_minute',0):02d} - {state.get('rec_end_hour',23):02d}:{state.get('rec_end_minute',59):02d})</b>\n"
                           f"🔍 SCANNER STATUS: <b>Total Scanned Coins: {state.get('scanned_coins_count', 0)} 🔄</b>\n\n"
                           f"🔥 Verified සජීවී ට්‍රේඩ්: <b>{len([p for p in state['active_positions'].values() if p['symbol'] in state['first_win_list']])} / {state.get('max_signals')}</b>\n"
                           f"💵 මූලික මාජින්: <b>${state.get('base_margin', 0.80)}</b> | Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                           f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b> | TP: <b>{state.get('fast_tp_pct', 30.0)}%</b>\n"
                           f"🥇 First Win Coins: <b>{len(state.get('first_win_list', []))}</b> | 🚫 Blacklist: <b>{len(state.get('block_list', []))}</b>")
                execute_telegram_send(msg)
                return "OK", 200
            
            elif cmd == "pause":
                with state_lock: state['is_scanning'] = False
                sync_save(); execute_telegram_send("⏸️ ස්කෑනරය නැවැත්තුවා."); return "OK", 200
            
            elif cmd == "resume":
                with state_lock: state['is_scanning'] = True
                sync_save(); execute_telegram_send("▶️ ස්කෑනරය ක්‍රියාත්මක කළා."); return "OK", 200

            elif cmd in ["menu", "help"]:
                menu_msg = (
                    f"👑 <b>RED-BULL LOSS RECOVERY MASTER PANEL</b> 👑\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"⚙️ <b>කාල රාමු වෙනස් කිරීමේ විධානය (New Timer Command)</b>\n"
                    f"• <code>/set_times [Sig_Start] [Sig_End] [Rec_Start] [Rec_End]</code>\n"
                    f"• උදා: <code>/set_times 12:30 23:59 08:00 23:59</code>\n\n"
                    f"📊 <b>සෙසු විධානයන් (Other Commands)</b>\n"
                    f"• <code>/status</code> - වත්මන් තත්ත්ව වාර්තාව සහ වේලාවන්\n"
                    f"• <code>/first_win_list</code> - ලැයිස්තුව සහ කාසිවල සාර්ථකත්වය\n"
                    f"• <code>/add_first [COINNAME]</code> - Manual කාසි ඇතුලත් කිරීමට\n"
                    f"• <code>/pause</code> | <code>/resume</code> - බොට් ක්‍රියාත්මක කිරීම/නැවතීම"
                )
                execute_telegram_send(menu_msg)
                return "OK", 200
    except: pass
    return "OK", 200

# --- 12. FLASK SERVER & APPLICATION START ---
@app.route('/', methods=['GET'])
def health(): return "Live Recovery Bot Active With Background Filter!", 200

if __name__ == '__main__':
    with state_lock: state['pending_acknowledgement'] = False
    sync_save()
    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    threading.Thread(target=telegram_reminder_worker, daemon=True).start() 
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
