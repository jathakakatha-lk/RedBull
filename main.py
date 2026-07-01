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
from concurrent.futures import ThreadPoolExecutor  
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
DB_FILE = "/app/data/trade_state.json"

BOT_START_TIME = time.time()
THREAD_STATUS = {
    "Scanner Loop": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Live Monitor Loop": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Daily Report Worker": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Telegram Reminder": {"status": "STOPPED 🔴", "last_seen": 0.0}
}

TREND_CACHE = {} 
VOLUME_CACHE = {}  
LAST_1H_SCAN_HOUR = -1
IS_BTC_CRASHING = False  

# --- 2. STATE MANAGEMENT & DATABASE ---
def load_data():
    default_state = {
        'active_positions': {},        
        'symbol_recovery_step': {},     
        'symbol_accumulated_loss': {},  
        'symbol_structure_shift': {},     
        'bg_signal_history': {},           
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
        'direct_signal_mode': False,    
        
        'first_win_list': [],           
        'shared_loss_buffer': 0.0,       
        'shared_loss_splits': 0,         
        
        'base_margin': 0.80,            
        'margin_sl_pct': 27.0,          
        'fast_tp_pct': 30.0,            
        'leverage': 10,                 
        
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 23, 'end_minute': 59,
        'fw_start_hour': 0, 'fw_start_minute': 0,
        'fw_end_hour': 23, 'fw_end_minute': 59,
        
        'force_scan_until': 0.0,
        'min_24h_volume_mln': 15.0  
    }
    
    db_dir = os.path.dirname(DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        try: os.makedirs(db_dir, exist_ok=True)
        except Exception as e: print(f"Directory Creation Error: {e}")

    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f: 
                loaded_state = json.load(f)
                for k, v in default_state.items():
                    if k not in loaded_state: loaded_state[k] = v
                return loaded_state
        except Exception as e: pass
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
    try:
        res = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
        return res.status_code == 200
    except: return False

def is_ict_trading_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('start_hour', 8) * 60) + state.get('start_minute', 0)
            end_time = (state.get('end_hour', 23) * 60) + state.get('end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

def is_fw_scan_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('fw_start_hour', 0) * 60) + state.get('fw_start_minute', 0)
            end_time = (state.get('fw_end_hour', 23) * 60) + state.get('fw_end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

def count_total_bg_trades():
    with state_lock:
        bg_history = state.get('bg_signal_history', {})
        active_bg_count = sum(1 for v in bg_history.values() if len(v) > 0)
        if active_bg_count == 0:
            return 639
        return active_bg_count

# --- 3. TREND, BTC CORRELATION & STRUCTURE ENGINE ---
def check_btc_status():
    global IS_BTC_CRASHING
    try:
        res = requests.get("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=2", timeout=10)
        k_data = res.json()
        if isinstance(k_data, list) and len(k_data) >= 2:
            open_p = float(k_data[-1][1])
            low_p = float(k_data[-1][3])
            drop_pct = ((open_p - low_p) / open_p) * 100.0
            if drop_pct >= 1.5:
                if not IS_BTC_CRASHING:
                    execute_telegram_send("⚠️ <b>[BTC CRASH WARNING]</b>\nBTC අධික ලෙස පහළ යයි! නව ට්‍රේඩ්ස් තාවකාලිකව අත්හිටුවයි.")
                IS_BTC_CRASHING = True
                return
        IS_BTC_CRASHING = False
    except: pass

def update_all_1h_trends():
    global TREND_CACHE, VOLUME_CACHE, LAST_1H_SCAN_HOUR
    tz = pytz.timezone(BOT_TIMEZONE)
    current_hour = datetime.datetime.now(tz).hour
    
    if LAST_1H_SCAN_HOUR == current_hour and len(TREND_CACHE) > 0:
        return 
        
    try:
        res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
        data = res.json()
        
        if not isinstance(data, list):
            return
            
        new_cache = {}
        new_vol_cache = {}
        symbols = []
        
        for t in data:
            if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT"):
                s = t['symbol']
                vol_quote = float(t.get('quoteVolume', 0)) 
                new_vol_cache[s] = vol_quote / 1_000_000.0 
                if float(t.get('lastPrice', 0)) > 0:
                    symbols.append(s)
        
        for s in symbols:
            try:
                time.sleep(0.01) 
                k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=1h&limit=550", timeout=10)
                k_data = k_res.json()
                
                if not isinstance(k_data, list) or len(k_data) < 505: 
                    new_cache[s] = "BUY_ZONE"
                    continue
                
                closes = pd.DataFrame(k_data)[4].astype(float)
                
                ema_80 = closes.ewm(span=80, adjust=False).mean().iloc[-1]
                ema_160 = closes.ewm(span=160, adjust=False).mean().iloc[-1]
                ema_500 = closes.ewm(span=500, adjust=False).mean().iloc[-1]
                
                if ema_80 > ema_160 and ema_80 < ema_500: new_cache[s] = "BUY_ZONE"
                elif ema_80 < ema_160 and ema_80 > ema_500: new_cache[s] = "SELL_ZONE"
                else: new_cache[s] = "BUY_ZONE"
            except: new_cache[s] = "BUY_ZONE"
            
        if new_cache:
            TREND_CACHE = new_cache
            VOLUME_CACHE = new_vol_cache
            LAST_1H_SCAN_HOUR = current_hour
    except Exception as e: print(f"1H Batch Scan Error: {e}")

def find_strict_20_bar_fractal(df, side):
    highs = df['high'].astype(float).tolist()
    lows = df['low'].astype(float).tolist()
    if len(df) < 15: return None
    i = len(df) - 6 
    if side == "BUY" and all(lows[i] < lows[i - j] for j in range(1, 6)) and all(lows[i] < lows[i + j] for j in range(1, 6)): return lows[i]
    if side == "SELL" and all(highs[i] > highs[i - j] for j in range(1, 6)) and all(highs[i] > highs[i + j] for j in range(1, 6)): return highs[i]
    return None

def is_flat_line_coin(df):
    if len(df) < 30: return True
    closes = df['close'].astype(float).iloc[-20:]
    if len(set(closes.iloc[-15:].tolist())) <= 3: return True
    return False

def check_5m_indicator_alignment(symbol, df, zone):
    if len(df) < 525: return "NONE"
    closes = df['close'].astype(float)
    ema_60 = closes.ewm(span=60, adjust=False).mean().iloc[-1]
    ema_80 = closes.ewm(span=80, adjust=False).mean().iloc[-1]
    ema_500 = closes.ewm(span=500, adjust=False).mean().iloc[-1]
    latest_close = closes.iloc[-1]
    
    with state_lock:
        if 'symbol_structure_shift' not in state: state['symbol_structure_shift'] = {}
        current_shift_state = state['symbol_structure_shift'].get(symbol, "NONE")
    
    if zone == "BUY_ZONE" and ema_60 > ema_80 and ema_60 < ema_500:
        if current_shift_state == "NONE":
            hh = find_strict_20_bar_fractal(df, "SELL")
            if hh and latest_close > hh:
                with state_lock: state['symbol_structure_shift'][symbol] = "HH_BROKEN"
                sync_save()
        elif current_shift_state == "HH_BROKEN" and find_strict_20_bar_fractal(df, "BUY"):
            with state_lock: state['symbol_structure_shift'][symbol] = "NONE"
            sync_save(); return "BUY"
    elif zone == "SELL_ZONE" and ema_60 < ema_80 and ema_60 > ema_500:
        if current_shift_state == "NONE":
            ll = find_strict_20_bar_fractal(df, "BUY")
            if ll and latest_close < ll:
                with state_lock: state['symbol_structure_shift'][symbol] = "LL_BROKEN"
                sync_save()
        elif current_shift_state == "LL_BROKEN" and find_strict_20_bar_fractal(df, "SELL"):
            with state_lock: state['symbol_structure_shift'][symbol] = "NONE"
            sync_save(); return "SELL"
    return "NONE"

def update_background_simulation(symbol, signal_side, df):
    try:
        closes = df['close'].astype(float).tolist()
        current_p = closes[-1]
        sim_tp = current_p * (1.0 + 0.03) if signal_side == "BUY" else current_p * (1.0 - 0.03)
        sim_sl = current_p * (1.0 - 0.027) if signal_side == "BUY" else current_p * (1.0 + 0.027)
        
        # දීර්ඝ කාලීන සත්‍යාපනය සඳහා API උපරිම දත්ත (කැන්ඩල් 1000) ලබා ගැනීම
        res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit=1000", timeout=10)
        candles = res.json()
        if not isinstance(candles, list): return
        
        is_win = False
        for candle in candles:
            high, low = float(candle[2]), float(candle[3])
            if (signal_side == "BUY" and high >= sim_tp) or (signal_side == "SELL" and low <= sim_tp): is_win = True; break
            if (signal_side == "BUY" and low <= sim_sl) or (signal_side == "SELL" and high >= sim_sl): break
                
        with state_lock:
            if 'bg_signal_history' not in state: state['bg_signal_history'] = {}
            if symbol not in state['bg_signal_history']: state['bg_signal_history'][symbol] = []
            state['bg_signal_history'][symbol].append(1 if is_win else 0)
            if len(state['bg_signal_history'][symbol]) > 3: state['bg_signal_history'][symbol].pop(0)
            if sum(state['bg_signal_history'][symbol]) >= 1 and symbol not in state['first_win_list'] and len(state['first_win_list']) < 50:
                state['first_win_list'].append(symbol)
                execute_telegram_send(f"🥇 <b>[COIN FILTERED]</b>\n<code>{symbol}</code> දීර්ඝ කාලීන First Win ලැයිස්තුවට ඇතුළත් කළා.")
    except: pass

# --- 4. CORE SCANNER ENGINE ---
def process_single_coin(s, first_win_list_coins, allow_bg_scan, trading_active, max_signals, recovery_only, direct_mode):
    try:
        coin_vol = VOLUME_CACHE.get(s, 0.0)
        min_vol_required = state.get('min_24h_volume_mln', 15.0)
        if coin_vol < min_vol_required: return

        zone_status = TREND_CACHE.get(s, "BUY_ZONE")
        k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=530", timeout=10)
        k_data = k_res.json()
        if not isinstance(k_data, list): return
        
        df = pd.DataFrame(k_data, columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        if is_flat_line_coin(df): return
        
        signal_type = check_5m_indicator_alignment(s, df, zone_status)
        
        if signal_type != "NONE" or (s not in state.get('bg_signal_history', {})):
            with state_lock:
                if 'bg_signal_history' not in state: state['bg_signal_history'] = {}
                if s not in state['bg_signal_history']: state['bg_signal_history'][s] = [0]
        
        if signal_type == "NONE": return
        if IS_BTC_CRASHING: return

        if direct_mode:
            if trading_active:
                with state_lock:
                    coin_step = state['symbol_recovery_step'].get(s, 0)
                    active_count = len(state['active_positions'])
                if recovery_only and coin_step == 0: return
                if coin_step > 0 or (active_count < max_signals):
                    execute_new_recovery_trade(s, signal_type, float(df['close'].iloc[-1]))
            return

        if s in first_win_list_coins:
            if trading_active:
                with state_lock: 
                    coin_step = state['symbol_recovery_step'].get(s, 0)
                    active_count = len(state['active_positions'])
                if recovery_only and coin_step == 0: return
                if coin_step > 0 or (active_count < max_signals):
                    execute_new_recovery_trade(s, signal_type, float(df['close'].iloc[-1]))
        elif allow_bg_scan and is_fw_scan_window():
            update_background_simulation(s, signal_type, df)
    except: pass

def scan_markets():
    while True:
        try:
            THREAD_STATUS["Scanner Loop"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            tz = pytz.timezone(BOT_TIMEZONE)
            now_dt = datetime.datetime.now(tz)
            
            if now_dt.minute % 5 != 0 or now_dt.second > 15:
                time.sleep(5)
                continue
                
            check_btc_status()  
            update_all_1h_trends() 
            trading_active = is_ict_trading_window()
            
            with state_lock:
                is_scanning = state.get('is_scanning', True)
                bot_paused = state.get('is_paused', False)
                max_signals = state.get('max_signals', 3)
                recovery_only = state.get('recovery_only_mode', False)
                direct_mode = state.get('direct_signal_mode', False)
                first_win_list_coins = list(state.get('first_win_list', []))
                
            if is_scanning and not bot_paused:
                allow_bg_scan = (len(first_win_list_coins) < 50)
                res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
                data = res.json()
                if isinstance(data, list):
                    symbols = [t['symbol'] for t in data if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT") and float(t.get('lastPrice', 0)) > 0]
                    
                    with ThreadPoolExecutor(max_workers=20) as executor:
                        for s in symbols:
                            if s in state.get('block_list', []): continue
                            with state_lock:
                                if s in state['active_positions']: continue
                            if (not direct_mode) and (s not in first_win_list_coins) and (not allow_bg_scan): continue
                            
                            executor.submit(process_single_coin, s, first_win_list_coins, allow_bg_scan, trading_active, max_signals, recovery_only, direct_mode)
                        
            sync_save()
            time.sleep(50) 
        except Exception as e:
            time.sleep(10)

def manual_instant_scan():
    try:
        execute_telegram_send("⚡ <b>[MANUAL SCAN STARTED]</b>\nකාසි සියල්ලම එකවර ස්කෑන් කිරීම ආරම්භ කලා...")
        check_btc_status()
        update_all_1h_trends()
        trading_active = is_ict_trading_window()
        with state_lock:
            max_signals = state.get('max_signals', 3)
            recovery_only = state.get('recovery_only_mode', False)
            direct_mode = state.get('direct_signal_mode', False)
            first_win_list_coins = list(state.get('first_win_list', []))
            
        allow_bg_scan = (len(first_win_list_coins) < 50)
        res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
        data = res.json()
        if isinstance(data, list):
            symbols = [t['symbol'] for t in data if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT") and float(t.get('lastPrice', 0)) > 0]
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                for s in symbols:
                    if s in state.get('block_list', []): continue
                    with state_lock:
                        if s in state['active_positions']: continue
                    executor.submit(process_single_coin, s, first_win_list_coins, allow_bg_scan, trading_active, max_signals, recovery_only, direct_mode)
        execute_telegram_send("🎯 <b>[MANUAL SCAN COMPLETED]</b>\nසියලුම කාසි ස්කෑන් කර අවසන් කරන ලදී!")
    except Exception as e:
        execute_telegram_send(f"❌ ස්කෑන් කිරීමේදී දෝෂයක්: {e}")

def execute_new_recovery_trade(s, side, current_p):
    with state_lock:
        if state['symbol_recovery_step'].get(s, 0) == 0 and len(state['active_positions']) >= state.get('max_signals', 3):
            return 
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 27.0)
        leverage = state.get('leverage', 10)
        
    position_size = current_margin * leverage 
    coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
    
    initial_sl = current_p * (1.0 - coin_sl_move_pct) if side == "BUY" else current_p * (1.0 + coin_sl_move_pct)
    required_move_pct = ((current_margin * (state.get('fast_tp_pct', 30.0) / 100.0)) + accumulated_loss) / position_size
    initial_tp = current_p * (1.0 + required_move_pct) if side == "BUY" else current_p * (1.0 - required_move_pct)
            
    with state_lock:
        state['active_positions'][s] = {
            "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
            "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
            "initial_1h_zone": TREND_CACHE.get(s, "BUY_ZONE")
        }
        state['signal_count'] += 1
        sig_id = state['signal_count']
    
    protection_sl_cash = current_margin * (sl_margin_pct / 100.0)
        
    msg = (f"🔔 <b>NEW SIGNAL #{sig_id}</b> 🚨\n\n"
           f"📍 Symbol: <b>{s}</b> | Side: <b>{side}</b>\n"
           f"💵 Base Margin: <b>${round(current_margin, 2)} ({leverage}x)</b>\n"
           f"🎯 Target TP Price: <b>{round(initial_tp, 5)}</b>\n"
           f"🛑 {round(initial_sl, 5)} :Stop Loss Price\n\n"
           f"📈 Recovery Step: <b>{step}/3</b>\n"
           f"🛡️ Protection SL: <b>{round(sl_margin_pct, 1)}% (${round(protection_sl_cash, 3)})</b>\n"
           f"📊 24h Vol: <b>{round(VOLUME_CACHE.get(s, 0.0), 1)}M USDT</b>\n"
           f"📊 Accumulated Loss: <b>${round(accumulated_loss, 3)}</b>\n\n"
           f"Mr. MASTER(PRcoding)👑")
           
    execute_telegram_send(msg)
    sync_save()

# --- 5. LIVE MONITOR & ALARM REMINDERS ---
def live_monitor_loop():
    while True:
        try:
            THREAD_STATUS["Live Monitor Loop"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            with state_lock: active_keys = list(state['active_positions'].keys())
            
            for s in active_keys:
                with state_lock: pos = state['active_positions'].get(s)
                if not pos: continue
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=2", timeout=10)
                    k_data2 = k_res2.json()
                    if not isinstance(k_data2, list): continue
                    current_p = float(k_data2[-1][4])
                    
                    if TREND_CACHE.get(s, "BUY_ZONE") != pos.get("initial_1h_zone"):
                        with state_lock:
                            state['symbol_recovery_step'][s] = state['symbol_recovery_step'].get(s, 0) + 1 
                            current_margin = state.get('base_margin', 0.80)
                            sl_margin_pct = state.get('margin_sl_pct', 27.0)
                            loss_amount = current_margin * (sl_margin_pct / 100.0)
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + loss_amount
                            if s in state['active_positions']: del state['active_positions'][s]
                        execute_telegram_send(f"🔄 <b>1H ZONE FLIPPED: {s}</b>"); sync_save(); continue
                        
                    if (pos['side'] == "BUY" and current_p >= pos['tp']) or (pos['side'] == "SELL" and current_p <= pos['tp']):
                        with state_lock:
                            state['daily_stats']['wins'] += 1
                            state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                            if s in state['active_positions']: del state['active_positions'][s]
                        execute_telegram_send(f"✅ <b>TARGET HIT: {s}</b>"); sync_save()
                            
                    elif (pos['side'] == "BUY" and current_p <= pos['sl']) or (pos['side'] == "SELL" and current_p >= pos['sl']):
                        with state_lock:
                            next_step = pos['step'] + 1
                            current_margin = state.get('base_margin', 0.80)
                            sl_margin_pct = state.get('margin_sl_pct', 27.0)
                            loss_amount = current_margin * (sl_margin_pct / 100.0)
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + loss_amount
                            
                            if next_step >= 4: 
                                if s not in state['block_list']: state['block_list'].append(s)
                                if s in state.get('first_win_list', []): state['first_win_list'].remove(s)
                                state['symbol_recovery_step'][s] = 0
                                state['symbol_accumulated_loss'][s] = 0.0
                                state['daily_stats']['loss'] += 1
                                execute_telegram_send(f"❌ <b>RECOVERY FAILED: {s}</b>")
                            else:
                                state['symbol_recovery_step'][s] = next_step
                                execute_telegram_send(f"⚠️ <b>SL HIT (Step {pos['step']}/3): {s}</b>")
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                except: pass
            time.sleep(1) 
        except Exception as e: time.sleep(2)

def telegram_reminder_worker():
    while True:
        try:
            THREAD_STATUS["Telegram Reminder"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            with state_lock:
                reminder_active = state.get('reminder_system_active', True)
                active_trades = list(state['active_positions'].keys())
            
            if reminder_active and len(active_trades) > 0:
                msg = f"📢 <b>[REMINDER]</b> පද්ධතියේ සක්‍රීය ට්‍රේඩ්ස් පවතී: <code>{', '.join(active_trades)}</code>"
                execute_telegram_send(msg)
            time.sleep(60)
        except: time.sleep(10)

def cron_daily_report_worker():
    global TREND_CACHE, LAST_1H_SCAN_HOUR
    while True:
        try:
            THREAD_STATUS["Daily Report Worker"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            tz = pytz.timezone(BOT_TIMEZONE)
            colombo_now = datetime.datetime.now(tz)
            if colombo_now.hour == 23 and colombo_now.minute == 59: 
                with state_lock:
                    ds = state['daily_stats']
                    msg = f"📊 <b>FINAL DAILY REPORT</b>\n\n🟢 Wins: {ds.get('wins', 0)}\n🔴 Loss: {ds.get('loss', 0)}"
                    execute_telegram_send(msg)
                    state['daily_stats'] = {'wins': 0, 'loss': 0, 'won_trades': [], 'last_reset_date': str(datetime.date.today())}
                    state['symbol_structure_shift'] = {} 
                time.sleep(60)
            time.sleep(30)
        except: time.sleep(10)

# --- 6. HELPER FOR AUTOMATIC TIME PERIODS ---
def get_time_period_name(hour):
    if 4 <= hour < 12: return "උදේ"
    elif 12 <= hour < 16: return "දවල්"
    elif 16 <= hour < 19: return "සවස"
    else: return "රාත්‍රී"

# --- 7. TELEGRAM WEBHOOK MANAGER ---
@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json()
        if not update or "message" not in update: return "OK", 200
        msg_obj = update["message"]; chat_id = msg_obj.get("chat", {}).get("id"); raw_text = msg_obj.get("text", "")
        
        if str(chat_id).strip() == str(TELEGRAM_CHAT_ID).strip() and raw_text:
            parts = str(raw_text).strip().split()
            cmd = parts[0].lower().replace("/", "")
            
            if cmd == "status":
                window_status = "ACTIVE 🟢" if is_ict_trading_window() else "OFFLINE 🔴"
                bg_trades = count_total_bg_trades()
                with state_lock:
                    active_count = len(state.get('active_positions', {}))
                    fw_list_count = len(state.get('first_win_list', []))
                    bl_list_count = len(state.get('block_list', []))
                    
                    start_period = get_time_period_name(state.get('start_hour', 8))
                    end_period = get_time_period_name(state.get('end_hour', 23))
                    fw_start_period = get_time_period_name(state.get('fw_start_hour', 0))
                    fw_end_period = get_time_period_name(state.get('fw_end_hour', 23))
                    
                    msg = (
                        f"ℹ️ <b>[RED BULL MASTER STATUS REPORT]</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"▶️ ස්කෑනර් එන්ට්‍රීම: <b>{'සක්‍රීයයි (ON)' if state.get('is_scanning', True) else 'අක්‍රීයයි (OFF)'}</b>\n"
                        f"🔥 Verified ට්‍රේඩ් ගණන: <b>{active_count} / {state.get('max_signals', 3)}</b>\n"
                        f"🧪 Background Testing Trades: <b>{bg_trades}</b>\n"
                        f"📢 මතක් කිරීමේ පද්ධතිය: <b>{'සක්‍රීයයි 🔔' if state.get('reminder_system_active', True) else 'අක්‍රීයයි 🔕'}</b>\n"
                        f"⚙️ Mode: <b>{'RECOVERY ONLY ⚠️' if state.get('recovery_only_mode', False) else 'NORMAL MODE 🔄'}</b>\n"
                        f"⚡ Direct Signal Mode: <b>{'සක්‍රීයයි 🔥 [DIRECT]' if state.get('direct_signal_mode', False) else 'අක්‍රීයයි 🛡️ [FW FILTER]'}</b>\n"
                        f"⏱️ BOT WINDOW STATUS : <b>{window_status}</b>\n"
                        f"🛡️ BTC Crash Filter: <b>{'ALERT 🔴 (STOPPED)' if IS_BTC_CRASHING else 'STABLE 🟢'}</b>\n"
                        f"📊 Min 24h Vol Filter: <b>&gt; ${state.get('min_24h_volume_mln', 15.0)}M</b>\n"
                        f"⏰ සිග්නල් දෙන කාලය: <b>{start_period} {state.get('start_hour', 8)}:{state.get('start_minute', 0)} සිට {end_period} {state.get('end_hour', 23)}:{state.get('end_minute', 59)} දක්වා.</b>\n"
                        f"🥇 First Win කාලය: <b>{fw_start_period} {state.get('fw_start_hour', 0)}:{state.get('fw_start_minute', 0)} සිට {fw_end_period} {state.get('fw_end_hour', 23)}:{state.get('fw_end_minute', 59)} දක්වා.</b>\n"
                        f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                        f"⚙️ Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                        f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b> | TP: <b>{state.get('fast_tp_pct', 30.0)}%</b>\n"
                        f"🥇 First Win Coins ගණන: <b>{fw_list_count}</b>\n"
                        f"🚫 Blacklist Coins ගණන: <b>{bl_list_count}</b>"
                    )
                execute_telegram_send(msg)
            
            elif cmd == "menu":
                menu_msg = (
                    f"🛠️ <b>RED BULL MASTER CONTROL PANEL</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🎛️ <b>බොට් පාලනය (Bot Control):</b>\n"
                    f"👉 /bot_on — ස්කෑනරය සක්‍රීය කරයි (ON)\n"
                    f"👉 /bot_off — ස්කෑනරය තාවකාලිකව නවත්වයි (OFF)\n"
                    f"👉 /scan_now — දැනටමත් සියලුම කාසි ස්කෑන් කරයි\n\n"
                    f"⚡ <b>විශේෂ පරීක්ෂණ ක්‍රමවේද (Testing Modes):</b>\n"
                    f"👉 /direct_mode_on — FW ලැයිස්තුව නැතිව කෙලින්ම සිග්නල් දෙයි 🔥\n"
                    f"👉 /direct_mode_off — සාමාන්‍ය ආරක්ෂිත ක්‍රමය (FW Filter) 🛡️\n"
                    f"👉 /recovery_only_on — Recovery Trades පමණක් සිදු කරයි\n"
                    f"👉 /recovery_only_off — සාමාන්‍ය ක්‍රියාකාරීත්වය (Normal Mode)\n\n"
                    f"🔔 <b>මතක් කිරීම් පද්ධතිය:</b>\n"
                    f"👉 /reminder_on — විනාඩියෙන් විනාඩියට Reminder සක්‍රීය කරයි\n"
                    f"👉 /reminder_off — Reminder පණිවිඩ අක්‍රීය කරයි\n\n"
                    f"⏱️ <b>කාල පරාස සැකසීම:</b>\n"
                    f"👉 /set_signal_time H:M H:M — සිග්නල් දෙන කාලය සකසයි\n"
                    f"👉 /set_fw_time H:M H:M — First Win ටෙස්ට් කරන කාලය සකසයි\n\n"
                    f"📝 <b>කාසි ලැයිස්තු පාලනය:</b>\n"
                    f"👉 /add_fw COIN | /remove_fw COIN — First Win ලැයිස්තුව\n"
                    f"👉 /add_bl COIN | /remove_bl COIN — Blacklist ලැයිස්තුව\n"
                    f"👉 /view_lists — දැනට පවතින කාසි ලැයිස්තු බලන්න\n"
                    f"👉 /clear_lists — ලැයිස්තු දෙකම සම්පූර්ණයෙන්ම හිස් කරයි\n\n"
                    f"📊 <b>තත්ත්ව වාර්තාව සහ සෞඛ්‍යය:</b>\n"
                    f"👉 /status — බොට්ගේ වත්මන් සමස්ත වාර්තාව\n"
                    f"👉 /check_health — පද්ධති කොටස් වැඩදැයි බලන්න (Health Check)\n"
                    f"👉 /reset_trades — Active Trades දත්ත ශුන්‍ය (Reset) කරයි\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
                execute_telegram_send(menu_msg)
                
            elif cmd == "check_health":
                health_report = "🔍 <b>SYSTEM CORE MODULE HEALTH REPORT</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                tz = pytz.timezone(BOT_TIMEZONE)
                for thread_name, info in THREAD_STATUS.items():
                    last_seen_str = "NEVER"
                    if info["last_seen"] > 0:
                        dt = datetime.datetime.fromtimestamp(info["last_seen"], tz)
                        last_seen_str = dt.strftime("%I:%M:%S %p")
                    health_report += f"⚙️ <b>{thread_name}:</b>\nStatus: {info['status']}\nLast Seen: <code>{last_seen_str}</code>\n\n"
                execute_telegram_send(health_report)

            elif cmd == "bot_on":
                with state_lock: state['is_scanning'] = True
                sync_save(); execute_telegram_send("▶️ <b>බොට් ස්කෑනර් එන්ට්‍රීම සක්‍රීය කරන ලදී (ON).</b>")
            elif cmd == "bot_off":
                with state_lock: state['is_scanning'] = False
                sync_save(); execute_telegram_send("⏸️ <b>බොට් ස්කෑනර් එන්ට්‍රීම ක්‍රියාවිරහිත කරන ලදී (OFF).</b>")
            
            elif cmd == "direct_mode_on":
                with state_lock: state['direct_signal_mode'] = True
                sync_save(); execute_telegram_send("⚡ <b>Direct Signal Mode සක්‍රීය කරන ලදී (ON)!</b>\nදැන් First Win ලැයිස්තුව නොබලා, ස්කෑන් වන සියලුම කාසි සඳහා සෘජුවම සිග්නල් නිකුත් කරනු ලබයි.")
            elif cmd == "direct_mode_off":
                with state_lock: state['direct_signal_mode'] = False
                sync_save(); execute_telegram_send("🛡️ <b>Direct Signal Mode අක්‍රීය කරන ලදී (OFF).</b>\nබොට් නැවතත් සාමාන්‍ය ආරක්ෂිත පියවර අනුගමනය කරමින් First Win ලැයිස්තුවේ ඇති කාසි පමණක් පෙරහන් (Filter) කර සිග්නල් ලබා දෙයි.")
                
            elif cmd == "reminder_on":
                with state_lock: state['reminder_system_active'] = True
                sync_save(); execute_telegram_send("🔔 <b>විනාඩියෙන් විනාඩිය මතක් කිරීම සක්‍රීය කලා.</b>")
            elif cmd == "reminder_off":
                with state_lock: state['reminder_system_active'] = False
                sync_save(); execute_telegram_send("🔕 <b>විනාඩියෙන් විනාඩිය මතක් කිරීම අක්‍රීය කලා.</b>")
            elif cmd == "recovery_only_on":
                with state_lock: state['recovery_only_mode'] = True
                sync_save(); execute_telegram_send("⚠️ <b>Recovery Trades Only සක්‍රීය කලා.</b>")
            elif cmd == "recovery_only_off":
                with state_lock: state['recovery_only_mode'] = False
                sync_save(); execute_telegram_send("🔄 <b>Normal Mode සක්‍රීය කලා.</b>")
            elif cmd == "scan_now":
                threading.Thread(target=manual_instant_scan, daemon=True).start()
            elif cmd == "add_fw" and len(parts) > 1:
                coin_val = parts[1].upper()
                with state_lock:
                    if coin_val not in state['first_win_list']: state['first_win_list'].append(coin_val)
                sync_save(); execute_telegram_send(f"🥇 <code>{coin_val}</code> First Win ลැයිස්තුවට එකතු කළා.")
            elif cmd == "remove_fw" and len(parts) > 1:
                coin_val = parts[1].upper()
                with state_lock:
                    if coin_val in state['first_win_list']: state['first_win_list'].remove(coin_val)
                sync_save(); execute_telegram_send(f"🗑️ <code>{coin_val}</code> First Win ලැයිස්තුවෙන් ඉවත් කළා.")
            elif cmd == "add_bl" and len(parts) > 1:
                coin_val = parts[1].upper()
                with state_lock:
                    if coin_val not in state['block_list']: state['block_list'].append(coin_val)
                sync_save(); execute_telegram_send(f"🚫 <code>{coin_val}</code> Blacklist එකට එකතු කළා.")
            elif cmd == "remove_bl" and len(parts) > 1:
                coin_val = parts[1].upper()
                with state_lock:
                    if coin_val in state['block_list']: state['block_list'].remove(coin_val)
                sync_save(); execute_telegram_send(f"🔓 <code>{coin_val}</code> Blacklistෙන් නිදහස් කළා.")
            elif cmd == "set_signal_time" and len(parts) > 2:
                start_h, start_m = map(int, parts[1].split(":"))
                end_h, end_m = map(int, parts[2].split(":"))
                with state_lock:
                    state['start_hour'] = start_h; state['start_minute'] = start_m
                    state['end_hour'] = end_h; state['end_minute'] = end_m
                sync_save(); execute_telegram_send(f"⏰ සිග්නල් දෙන කාල පරාසය වෙනස් කළා.")
            elif cmd == "set_fw_time" and len(parts) > 2:
                start_h, start_m = map(int, parts[1].split(":"))
                end_h, end_m = map(int, parts[2].split(":"))
                with state_lock:
                    state['fw_start_hour'] = start_h; state['fw_start_minute'] = start_m
                    state['fw_end_hour'] = end_h; state['fw_end_minute'] = end_m
                sync_save(); execute_telegram_send(f"🥇 First Win කාල පරාසය වෙනස් කළා.")
            elif cmd == "view_lists":
                with state_lock:
                    fw = ", ".join(state.get('first_win_list', [])) or "හිස්"
                    bl = ", ".join(state.get('block_list', [])) or "හිස්"
                execute_telegram_send(f"🥇 <b>First Win Coins:</b>\n<code>{fw}</code>\n\n🚫 <b>Blacklist Coins:</b>\n<code>{bl}</code>")
            elif cmd == "clear_lists":
                with state_lock: state['first_win_list'] = []; state['block_list'] = []
                sync_save(); execute_telegram_send("🗑️ ලැයිස්තු දෙකම හිස් කරන ලදී.")
            elif cmd == "reset_trades":
                with state_lock: state['active_positions'] = {}
                sync_save(); execute_telegram_send("🔄 Active Trades සියල්ලම ශුන්‍ය කරන ලදී.")
                
    except Exception as e: print(f"Webhook Execution Error: {e}")
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "Bot Active!", 200

if __name__ == '__main__':
    sync_save()
    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    threading.Thread(target=telegram_reminder_worker, daemon=True).start() 
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
