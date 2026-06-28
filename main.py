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
        'symbol_last_win_zone': {},     
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
    except: return True

def is_first_win_scan_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        current_hour = tz_now.hour
        if 0 <= current_hour < 8: 
            return True
        return False
    except: return False

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

def scan_markets():
    while True:
        try:
            trading_active = is_ict_trading_window()
            fw_scan_active = is_first_win_scan_window()
            
            if not trading_active and not fw_scan_active:
                time.sleep(60); continue
                
            with state_lock:
                is_scanning = state.get('is_scanning', True)
                bot_paused = state.get('is_paused', False)
                active_positions = dict(state['active_positions'])
                max_signals = state.get('max_signals', 3)
                current_margin = state.get('base_margin', 0.80)
                leverage = state.get('leverage', 10)
                first_win_list_coins = list(state.get('first_win_list', []))
                
            position_size = current_margin * leverage 
            if is_scanning and not bot_paused:
                res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
                symbols = [t['symbol'] for t in res.json() if t['symbol'].endswith("USDT") and float(t.get('lastPrice', 0)) > 0 and position_size >= 5.0]
                for s in symbols:
                    if s in state.get('block_list', []): continue
                    if s in active_positions: continue
                    
                    if trading_active and (s not in first_win_list_coins):
                        continue
                        
                    zone_status = get_1h_trend_zone(s)
                    
                    with state_lock: 
                        coin_step = state['symbol_recovery_step'].get(s, 0)
                        last_win_zone = state.get('symbol_last_win_zone', {}).get(s, "NONE")
                    
                    if coin_step == 0 and last_win_zone == zone_status:
                        continue
                        
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
                                if coin_step > 0 or (len([p for p in active_positions.values() if p['symbol'] in state['first_win_list']]) < max_signals):
                                    execute_trade = True
                            else:
                                if fw_scan_active:
                                    execute_trade = True
                                
                        if execute_trade:
                            execute_new_recovery_trade(s, signal_type, float(df['close'].iloc[-1]))
                    except: pass
            time.sleep(5)
        except: time.sleep(15)

def execute_new_recovery_trade(s, side, current_p):
    trading_active = is_ict_trading_window()
    
    with state_lock:
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 27.0)
        leverage = state.get('leverage', 10)
        is_verified_coin = s in state.get('first_win_list', [])
        
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
        
    if is_verified_coin and trading_active:
        with state_lock:
            state['signal_count'] += 1
            sig_id = state['signal_count']
            if state.get('alarm_active', True): state['last_alarm_symbol'] = f"{s} ({side} Step {step})"
            if state.get('reminder_system_active', True): state['pending_acknowledgement'] = True
            
        msg = (f"🔔 <b>NEW SIGNAL #{sig_id}</b> 🚨\n\n"
               f"📍 Symbol: <code>{s}</code> | Side: <b>{side}</b>\n"
               f"💵 Base Margin: <b>${current_margin} ({leverage}x)</b>\n"
               f"🎯 Target TP Price: <code>{round(initial_tp, 5)}</code>\n"
               f"🛑 <code>{round(initial_sl, 5)}</code> :Stop Loss Price\n\n"
               f"📈 Recovery Step: <b>{step}/3</b>\n"
               f"🛡️ Protection SL: <b>{sl_margin_pct}% (${round(current_margin * (sl_margin_pct/100.0), 3)})</b>\n"
               f"📊 Accumulated Loss: <b>${round(accumulated_loss, 4)}</b>\n\n"
               f"Mr. MASTER👑")
        execute_telegram_send(msg)
        
    sync_save()

def telegram_reminder_worker():
    while True:
        try:
            time.sleep(60) 
            if is_ict_trading_window():
                with state_lock:
                    is_pending = state.get('pending_acknowledgement', False)
                    system_active = state.get('reminder_system_active', True)
                if is_pending and system_active: 
                    execute_telegram_send("⚠️ මතක් කිරීම නැවැත්වීමට <b>/ok</b> විධානය ලබාදෙන්න. ⚠️")
        except: pass

def live_monitor_loop():
    while True:
        try:
            with state_lock: active_keys = list(state['active_positions'].keys())
            for s in active_keys:
                with state_lock: pos = state['active_positions'].get(s)
                if not pos: continue
                side = pos['side']
                is_verified = s in state.get('first_win_list', [])
                trading_active = is_ict_trading_window()
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=2", timeout=10)
                    current_p = float(k_res2.json()[-1][4])
                    
                    if get_1h_trend_zone(s) != pos.get("initial_1h_zone"):
                        flip_loss = (pos['margin'] * state.get('leverage', 10)) * (abs(pos['entry_price'] - current_p) / pos['entry_price']) if ((side == "BUY" and current_p < pos['entry_price']) or (side == "SELL" and current_p > pos['entry_price'])) else 0.0
                        with state_lock:
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + flip_loss
                            state['symbol_recovery_step'][s] = state['symbol_recovery_step'].get(s, 0) + 1 
                            if s in state['active_positions']: del state['active_positions'][s]
                        if is_verified and trading_active:
                            execute_telegram_send(f"🔄 <b>1H ZONE FLIPPED: {s}</b>\nකලාපය මාරු විය! වත්මන් ට්‍රේඩ් එක වසා දැමුවා. ⏳")
                        sync_save(); continue
                        
                    if (side == "BUY" and current_p >= pos['tp']) or (side == "SELL" and current_p <= pos['tp']):
                        with state_lock:
                            state['stats']['wins'] += 1
                            
                            if is_verified and trading_active:
                                state['daily_stats']['wins'] += 1
                                
                            if s not in state['first_win_list']: 
                                state['first_win_list'].append(s)
                            
                            if 'symbol_last_win_zone' not in state:
                                state['symbol_last_win_zone'] = {}
                            state['symbol_last_win_zone'][s] = pos.get("initial_1h_zone", "NONE")
                            
                            state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                        if is_verified and trading_active:
                            execute_telegram_send(f"✅ <b>RECOVERY TARGET HIT: {s}</b>\nනියමිත ඉලක්කය සපුරා සියලුම පාඩු පියවා අවසන් කරන ලදී! 🎉")
                            
                    elif (side == "BUY" and current_p <= pos['sl']) or (side == "SELL" and current_p >= pos['sl']):
                        trade_loss = (pos['margin'] * state.get('leverage', 10)) * (abs(pos['entry_price'] - pos['sl']) / pos['entry_price'])
                        with state_lock:
                            state['stats']['loss'] += 1
                            next_step = pos['step'] + 1
                            current_total_loss = state['symbol_accumulated_loss'].get(s, 0.0) + trade_loss
                            
                            if next_step >= 4: 
                                if s not in state['block_list']: state['block_list'].append(s)
                                if s in state.get('first_win_list', []): state['first_win_list'].remove(s)
                                state['shared_loss_buffer'] += current_total_loss; state['shared_loss_splits'] = 8
                                state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                                
                                if is_verified and trading_active:
                                    state['daily_stats']['loss'] += 1
                                    
                                if is_verified and trading_active:
                                    execute_telegram_send(f"❌ <b>TOTAL RECOVERY FAILED: {s}</b>\nපියවර 4ම අසාර්ථක විය! මෙම කාසිය බ්ලැක්ලිස්ට් කරන ලදී. 🚫")
                            else:
                                state['symbol_recovery_step'][s] = next_step; state['symbol_accumulated_loss'][s] = current_total_loss
                                if is_verified and trading_active:
                                    execute_telegram_send(f"⚠️ <b>STOP LOSS HIT (Step {pos['step']}/3): {s}</b>\nඊළඟ 5M Fractal එකෙන් රිකවර් කිරීමට සැකසුම් සූදානම්. ⏳")
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                except: pass
                time.sleep(0.1)
            time.sleep(2)
        except: time.sleep(5)

def generate_report_text(ds, title_prefix="📅 TODAY'S"):
    return (f"📊 <b>{title_prefix} PERFORMANCE REPORT</b>\n━━━━━━━━━━━━━━━━━━━\n\n🟢 Wins (Real Signals): <b>{ds.get('wins', 0)}</b>\n🔴 Loss (Blacklisted): <b>{ds.get('loss', 0)}</b>\n\nMr. MASTER👑")

def cron_daily_report_worker():
    while True:
        try:
            # ශ්‍රී ලංකා (Asia/Colombo) වත්මන් වේලාව ලබා ගැනීම
            tz = pytz.timezone(BOT_TIMEZONE)
            colombo_now = datetime.datetime.now(tz)
            
            # රාත්‍රී 11:59 (23:59) ද යන්න පරීක්ෂා කිරීම
            if colombo_now.hour == 23 and colombo_now.minute == 59: 
                today_str = str(datetime.date.today())
                with state_lock:
                    ds = state['daily_stats']
                    execute_telegram_send(generate_report_text(ds, title_prefix="✨ FINAL DAILY"))
                    
                    # [MODIFIED] දෛනිකව first_win_list එක රීසෙට් වන කේත කොටස ඉවත් කරන ලදී. (ලැයිස්තුව ආරක්ෂිතයි)
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
                execute_telegram_send("👌 <b>[ACKNOWLEDGED]</b>\nමතක් කිරීම් සාර්ථකව නිහඬ කරන ලදී.")
                return "OK", 200
            
            elif cmd == "block_list":
                with state_lock: bl = ", ".join(state.get('block_list', [])) if state.get('block_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🚫 <b>[BLACKLISTED COINS]</b>\n<code>{bl}</code>")
                return "OK", 200

            elif cmd == "add_block" and len(tokens) > 1:
                coin_to_add = tokens[1].upper()
                with state_lock:
                    if coin_to_add not in state['block_list']: state['block_list'].append(coin_to_add)
                    if coin_to_add in state.get('first_win_list', []): state['first_win_list'].remove(coin_to_add)
                sync_save()
                execute_telegram_send(f"🚫 <code>{coin_to_add}</code> කාසිය සාර්ථකව තහනම් ලැයිස්තුවට (Blacklist) එකතු කරන ලදී.")
                return "OK", 200

            elif cmd == "remove_block" and len(tokens) > 1:
                coin_to_rem = tokens[1].upper()
                with state_lock:
                    if coin_to_rem in state['block_list']: state['block_list'].remove(coin_to_rem)
                sync_save()
                execute_telegram_send(f"🟢 <code>{coin_to_rem}</code> කාසිය තහනම් ලැයිස්තුවෙන් සාර්ථකව ඉවත් කරන ලදී.")
                return "OK", 200

            elif cmd == "first_win_list":
                with state_lock: fwl = ", ".join(state.get('first_win_list', [])) if state.get('first_win_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🟢 <b>[FIRST WIN LIST]</b>\n<code>{fwl}</code>")
                return "OK", 200

            elif cmd == "add_first" and len(tokens) > 1:
                coin_to_add = tokens[1].upper()
                with state_lock:
                    if coin_to_add not in state['first_win_list']: state['first_win_list'].append(coin_to_add)
                    if coin_to_add in state.get('block_list', []): state['block_list'].remove(coin_to_add)
                sync_save()
                execute_telegram_send(f"🥇 <code>{coin_to_add}</code> කාසිය Manual ක්‍රමයට <b>First Win List</b> එකට එකතු කරන ලදී. මින් ඉදිරියට මෙහි සිග්නල් ට්‍රේඩින් වේලාව තුළ ලැබෙනු ඇත!")
                return "OK", 200

            elif cmd == "remove_first" and len(tokens) > 1:
                coin_to_rem = tokens[1].upper()
                with state_lock:
                    if coin_to_rem in state['first_win_list']: state['first_win_list'].remove(coin_to_rem)
                sync_save()
                execute_telegram_send(f"❌ <code>{coin_to_rem}</code> කාසිය <b>First Win List</b> එකෙන් ඉවත් කරන ලදී.")
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

            elif cmd == "set_time" and len(tokens) > 2:
                try:
                    start_t = tokens[1].split(":")
                    end_t = tokens[2].split(":")
                    with state_lock:
                        state['start_hour'] = int(start_t[0])
                        state['start_minute'] = int(start_t[1])
                        state['end_hour'] = int(end_t[0])
                        state['end_minute'] = int(end_t[1])
                    sync_save()
                    execute_telegram_send(f"⏰ <b>[TIME SCHEDULE UPDATED]</b>\nවැඩ කරන වේලාව: <b>{tokens[1]} සිට {tokens[2]} දක්වා</b> ලෙස සකස් කළා.")
                except: pass
                return "OK", 200

            elif cmd == "reminder_on":
                with state_lock: state['reminder_system_active'] = True
                sync_save(); execute_telegram_send("🔔 විනාඩියේ සිහිගැන්වීමේ පද්ධතිය <b>සක්‍රීය (ON)</b> කරන ලදී.")
                return "OK", 200

            elif cmd == "reminder_off":
                with state_lock: state['reminder_system_active'] = False; state['pending_acknowledgement'] = False
                sync_save(); execute_telegram_send("🔕 විනාඩියේ සිහිගැන්වීමේ පද්ධතිය <b>අක්‍රීය (OFF)</b> කරන ලදී.")
                return "OK", 200
            
            elif cmd == "set_sl_pct" and len(tokens) > 1:
                try:
                    val = float(tokens[1])
                    with state_lock: 
                        state['margin_sl_pct'] = val
                        # ⭐ [MODIFIED] ලැයිස්තු Clear වන කේත පේළි ඉවත් කර ආරක්ෂා කරන ලදී.
                    sync_save()
                    execute_telegram_send(f"🛡️ <b>[SL UPDATED]</b>\n• නව SL: <b>{val}%</b>\n💡 සියලුම පවතින Lists සාර්ථකව ආරක්ෂා කරන ලදී.")
                except: pass
                return "OK", 200
            
            elif cmd == "set_fast_tp_pct" and len(tokens) > 1:
                try:
                    val = float(tokens[1])
                    with state_lock: 
                        state['fast_tp_pct'] = val
                        # ⭐ [MODIFIED] ලැයිස්තු Clear වන කේත පේළි ඉවත් කර ආරක්ෂා කරන ලදී.
                    sync_save()
                    execute_telegram_send(f"🎯 <b>[TP UPDATED]</b>\n• නව TP: <b>{val}%</b>\n💡 සියලුම පවතින Lists සාර්ථකව ආරක්ෂා කරන ලදී.")
                except: pass
                return "OK", 200
            
            elif cmd == "set_max" and len(tokens) > 1:
                try:
                    with state_lock: state['max_signals'] = int(tokens[1])
                    sync_save(); execute_telegram_send(f"🚀 උපරිම සජීවී ට්‍රේඩ් ගණන {tokens[1]} කළා.")
                except: pass
                return "OK", 200
            
            elif cmd == "status":
                window_status = "ACTIVE 🟢" if is_ict_trading_window() else ("SCANNING NIGHT 💤" if is_first_win_scan_window() else "OFFLINE 🔴")
                with state_lock:
                    rem_system = "සක්‍රීයයි 🔔" if state.get('reminder_system_active', True) else "අක්‍රීයයි 🔕"
                    
                    all_pos = state['active_positions'].values()
                    fw_list = state.get('first_win_list', [])
                    
                    verified_count = len([p for p in all_pos if p['symbol'] in fw_list])
                    bg_testing_count = len([p for p in all_pos if p['symbol'] not in fw_list])
                    
                    msg = (f"ℹ️ <b>[RED BULL MASTER STATUS REPORT]</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                           f"▶️ ස්කෑනර් එන්ට්‍රීම: <b>{'සක්‍රීයයි (ON)' if state.get('is_scanning') else 'අක්‍රීයයි (OFF)'}</b>\n"
                           f"🔥 Verified සජීවී ට්‍රේඩ් ගණන: <b>{verified_count} / {state.get('max_signals')}</b>\n"
                           f"🧪 Background Testing Trades: <b>{bg_testing_count}</b>\n"
                           f"📢 මතක් කිරීමේ පද්ධතිය: <b>{rem_system}</b>\n"
                           f"⏱️ BOT WINDOW STATUS : <b>{window_status}</b>\n"
                           f"⏰ සිග්නල් දෙන කාලය: <b>දවල් {state.get('start_hour',12)}:{state.get('start_minute',30)} සිට රාත්‍රී {state.get('end_hour',23)}:{state.get('end_minute',59)} දක්වා.</b>\n"
                           f"🌙 AXIS ස්කෑන් කාලය: <b>රාත්‍රී 00:00 සිට උදේ 08:00 දක්වා.</b>\n"
                           f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                           f"⚙️ වත්මන් Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                           f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b>\n"
                           f"🎯 TP: <b>{state.get('fast_tp_pct', 30.0)}%</b>\n"
                           f"🥇 First Win Coins ගණන: <b>{len(fw_list)}</b>\n"
                           f"🚫 Blacklist Coins ගණන: <b>{len(state.get('block_list', []))}</b>")
                execute_telegram_send(msg)
                return "OK", 200
            
            elif cmd == "pause":
                with state_lock: state['is_scanning'] = False
                sync_save(); execute_telegram_send("⏸️ ස්කෑනරය නැවැත්තුවා.")
                return "OK", 200
            
            elif cmd == "resume":
                with state_lock: state['is_scanning'] = True
                sync_save(); execute_telegram_send("▶️ ස්කෑනරය ක්‍රියාත්මක කළා.")
                return "OK", 200

            elif cmd in ["menu", "help"]:
                menu_msg = (
                    f"👑 <b>RED-BULL LOSS RECOVERY MASTER PANEL</b> 👑\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📊 <b>1. තොරතුරු ලබාගැනීම (Info)</b>\n"
                    f"• <code>/status</code> - වත්මන් තත්ත්ව වාර්තාව\n"
                    f"• <code>/block_list</code> - තහනම් කළ කාසි ලැයිස්තුව\n"
                    f"• <code>/first_win_list</code> - First Win ලැබූ කාසි\n\n"
                    f"🥇 <b>2. කාසි Manual කළමනාකරණය</b>\n"
                    f"• <code>/add_first [COINNAME]</code> -> First Win ลැයිස්තුවට දැමීමට\n"
                    f"• <code>/remove_first [COINNAME]</code> -> First Win ලැයිස්තුවෙන් ඉවත් කිරීමට\n"
                    f"• <code>/add_block [COINNAME]</code> -> Blacklist ලැයිස්තුවට දැමීමට\n"
                    f"• <code>/remove_block [COINNAME]</code> -> Blacklist ලැයිස්තුවෙන් ඉවත් කිරීමට\n\n"
                    f"⚙️ <b>3. සැකසුම් වෙනස් කිරීම (Settings)</b>\n"
                    f"• <code>/set_margin [අගය]</code> - Margin වෙනස් කිරීමට\n"
                    f"• <code>/set_leverage [ගණන]</code> - Leverage වෙනස් කිරීමට\n"
                    f"• <code>/set_time [HH:MM] [HH:MM]</code> - වේලාව වෙනස් කිරීමට\n"
                    f"• <code>/set_sl_pct [අගය]</code> - SL වෙනස් කිරීමට\n"
                    f"• <code>/set_fast_tp_pct [අගය]</code> - TP වෙනස් කිරීමට\n"
                    f"• <code>/set_max [ගණන]</code> - උපරිම සිග්නල් ගණන\n\n"
                    f"🛡️ <b>4. බොට් පාලනය (Bot Controls)</b>\n"
                    f"• <code>/pause</code> - ස්කෑනරය නැවතීමට\n"
                    f"• <code>/resume</code> - ස්කෑනරය පණගැන්වීමට\n"
                    f"• <code>/reminder_on</code> - සිහිගැන්වීම සක්‍රීය කිරීමට\n"
                    f"• <code>/reminder_off</code> - සිහිගැන්වීම අක්‍රීය කිරීමට\n"
                    f"• <code>/ok</code> - මතක් කිරීම් නිහඬ කිරීමට\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
                execute_telegram_send(menu_msg)
                return "OK", 200
    except: pass
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "Live Recovery Bot Active With Corrected Performance Metrics!", 200

if __name__ == '__main__':
    with state_lock: state['pending_acknowledgement'] = False
    sync_save()
    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    threading.Thread(target=telegram_reminder_worker, daemon=True).start() 
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
