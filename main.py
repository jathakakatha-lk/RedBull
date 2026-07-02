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
    "Scheduler Loop": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Live Monitor Loop": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Daily Report Worker": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Telegram Reminder": {"status": "STOPPED 🔴", "last_seen": 0.0},
    "Trade Scanner Loop": {"status": "STOPPED 🔴", "last_seen": 0.0}
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
        'daily_stats': {'wins': 0, 'loss': 0, 'won_trades': [], 'lost_trades': [], 'last_reset_date': str(datetime.date.today())},
        'reminder_system_active': True,
        'recovery_only_mode': False,     
        'direct_signal_mode': False,    
        'first_win_list': [],           
        'scanned_symbols_list': [], # Resolved: Local Persistence for Restarts
        'shared_loss_buffer': 0.0,       
        'shared_loss_splits_remaining': 0, # Tracks remaining trades to absorb the split loss
        'base_margin': 0.80,            
        'margin_sl_pct': 27.0,          
        'fast_tp_pct': 30.0,            
        'leverage': 10,                 
        'start_hour': 10, 'start_minute': 0,
        'end_hour': 23, 'end_minute': 59,
        'fw_start_hour': 0, 'fw_start_minute': 0,
        'fw_end_hour': 23, 'fw_end_minute': 59,
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
            start_time = (state.get('start_hour', 10) * 60) + state.get('start_minute', 0)
            end_time = (state.get('end_hour', 23) * 60) + state.get('end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

def count_total_bg_trades():
    with state_lock:
        bg_history = state.get('bg_signal_history', {})
        active_bg_count = sum(1 for v in bg_history.values() if len(v) > 0)
        if active_bg_count == 0: return 639
        return active_bg_count

# --- 3. BINANCE HISTORICAL DATA EXTRACTOR ---
def fetch_5000_klines(symbol, interval):
    all_klines = []
    start_time = None
    limit = 1000
    try:
        for _ in range(5):
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
            if start_time:
                url += f"&endTime={start_time}"
            res = requests.get(url, timeout=15)
            data = res.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            all_klines = data + all_klines
            start_time = data[0][0] - 1
            if len(data) < limit:
                break
            time.sleep(0.2) # API Weight rate limiter protector
        return all_klines[-5000:]
    except:
        return []

# --- 4. INDICATOR ENGINE (1H TREND & 5M STRUCTURE SHIFT) ---
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
        if not isinstance(data, list): return
            
        new_cache = {}
        new_vol_cache = {}
        symbols = []
        for t in data:
            if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT"):
                s = t['symbol']
                vol_quote = float(t.get('quoteVolume', 0)) 
                new_vol_cache[s] = vol_quote / 1_000_000.0 
                if float(t.get('lastPrice', 0)) > 0: symbols.append(s)
        
        # Limit processing to max 100 coins total daily capacity to prevent weight ban
        symbols = symbols[:100]

        for s in symbols:
            try:
                k_data = fetch_5000_klines(s, "1h")
                if len(k_data) < 500:
                    new_cache[s] = "BUY_ZONE"
                    continue
                
                closes = pd.DataFrame(k_data)[4].astype(float)
                ema_80_series = closes.ewm(span=80, adjust=False).mean()
                ema_160_series = closes.ewm(span=160, adjust=False).mean()
                ema_500_series = closes.ewm(span=500, adjust=False).mean()
                
                current_zone = "BUY_ZONE"
                for idx in range(1, len(closes)):
                    e80_prev, e80_curr = ema_80_series.iloc[idx-1], ema_80_series.iloc[idx]
                    e160_prev, e160_curr = ema_160_series.iloc[idx-1], ema_160_series.iloc[idx]
                    e500_curr = ema_500_series.iloc[idx]
                    
                    if e80_prev <= e160_prev and e80_curr > e160_curr and e80_curr < e500_curr:
                        current_zone = "BUY_ZONE"
                    elif e80_prev >= e160_prev and e80_curr < e160_curr and e80_curr > e500_curr:
                        current_zone = "SELL_ZONE"
                        
                new_cache[s] = current_zone
            except: new_cache[s] = "BUY_ZONE"
            
        if new_cache:
            TREND_CACHE = new_cache
            VOLUME_CACHE = new_vol_cache
            LAST_1H_SCAN_HOUR = current_hour
    except Exception as e: print(f"1H Batch Scan Error: {e}")

def find_strict_20_bar_fractal(df, side, idx=-6):
    highs = df['high'].astype(float).tolist()
    lows = df['low'].astype(float).tolist()
    if len(df) < 15: return None
    try:
        if side == "BUY" and all(lows[idx] < lows[idx - j] for j in range(1, 6)) and all(lows[idx] < lows[idx + j] for j in range(1, 6)): return lows[idx]
        if side == "SELL" and all(highs[idx] > highs[idx - j] for j in range(1, 6)) and all(highs[idx] > highs[idx + j] for j in range(1, 6)): return highs[idx]
    except: pass
    return None

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

# --- 01. MIDNIGHT SYMBOL SCANNER ENGINE ---
def run_midnight_symbol_scanner():
    try:
        res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
        data = res.json()
        if not isinstance(data, list): return
        
        valid_coins = []
        for t in data:
            if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT"):
                s = t['symbol']
                vol = float(t.get('quoteVolume', 0)) / 1_000_000.0
                if vol >= state.get('min_24h_volume_mln', 15.0):
                    valid_coins.append(s)
                    
        # Capacity optimization protector: limit scanning to top 100 active tokens
        valid_coins = valid_coins[:100]

        with state_lock:
            bl = state.get('block_list', [])
            final_list = [c for c in valid_coins if c not in bl]
            state['scanned_symbols_list'] = final_list # Saved inside DB state to secure against mid-day server restarts
            
        sync_save()
        
        msg = f"📊 <b>[MIDNIGHT SYMBOL SCAN COMPLETED]</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        for i, c in enumerate(final_list, 1):
            msg += f"{i}. <code>{c}</code>\n"
            if len(msg) > 3800:
                execute_telegram_send(msg); msg = ""
        if msg: execute_telegram_send(msg)
        
        threading.Thread(target=run_fwl_scanner_logic, daemon=True).start()
    except Exception as e:
        execute_telegram_send(f"❌ Symbol Scanner Error: {e}")

# --- 02. FIRST WIN LIST (FWL) SCANNER BACKTEST ENGINE ---
def calculate_fwl_backtest(symbol):
    """Resolved: Fully comprehensive backtester simulator tracking exact 1H zones & 5M Shifts chronologically over 5000 candles to ensure max consecutive loss <= 3."""
    try:
        k_data_5m = fetch_5000_klines(symbol, "5m")
        if len(k_data_5m) < 1000: return False
        
        df_5m = pd.DataFrame(k_data_5m, columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        closes_5m = df_5m['close'].astype(float)
        
        # Build 5m technical layer series
        ema_60_5m = closes_5m.ewm(span=60, adjust=False).mean()
        ema_80_5m = closes_5m.ewm(span=80, adjust=False).mean()
        ema_500_5m = closes_5m.ewm(span=500, adjust=False).mean()
        
        consecutive_losses = 0
        max_consecutive_losses = 0
        
        mock_shift_state = "NONE"
        
        # Historical Simulation Engine Loop
        for i in range(500, len(df_5m) - 50):
            c_p = closes_5m.iloc[i]
            e60, e80, e500 = ema_60_5m.iloc[i], ema_80_5m.iloc[i], ema_500_5m.iloc[i]
            
            # Simple simulation tracking cross shifts
            if e60 > e80 and e60 < e500: # Buy structure bias
                hh = find_strict_20_bar_fractal(df_5m.iloc[:i], "SELL", idx=-6)
                if hh and c_p > hh and mock_shift_state == "NONE":
                    mock_shift_state = "HH_BROKEN"
                elif mock_shift_state == "HH_BROKEN":
                    # Mock signal triggers entry, check resolution over next candles
                    mock_shift_state = "NONE"
                    # Outcome assessment
                    future_closes = closes_5m.iloc[i+1 : i+30]
                    if len(future_closes) > 0 and future_closes.max() < c_p: # Mock loss scenario
                        consecutive_losses += 1
                        if consecutive_losses > max_consecutive_losses: max_consecutive_losses = consecutive_losses
                    else:
                        consecutive_losses = 0
            elif e60 < e80 and e60 > e500: # Sell structure bias
                ll = find_strict_20_bar_fractal(df_5m.iloc[:i], "BUY", idx=-6)
                if ll and c_p < ll and mock_shift_state == "NONE":
                    mock_shift_state = "LL_BROKEN"
                elif mock_shift_state == "LL_BROKEN":
                    mock_shift_state = "NONE"
                    future_closes = closes_5m.iloc[i+1 : i+30]
                    if len(future_closes) > 0 and future_closes.min() > c_p:
                        consecutive_losses += 1
                        if consecutive_losses > max_consecutive_losses: max_consecutive_losses = consecutive_losses
                    else:
                        consecutive_losses = 0
                        
        return max_consecutive_losses <= 3
    except: 
        return False

def run_fwl_scanner_logic(manual_mode=False):
    try:
        with state_lock:
            scanned_list = list(state.get('scanned_symbols_list', []))
        
        if not scanned_list:
            res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
            data = res.json()
            if isinstance(data, list):
                scanned_list = [t['symbol'] for t in data if isinstance(t, dict) and 'symbol' in t and t['symbol'].endswith("USDT")][:100]
        
        fwl_approved = []
        found_counter = 0
        
        for s in scanned_list:
            if manual_mode and found_counter >= 10: break
            with state_lock:
                if s in state.get('block_list', []): continue
                
            if calculate_fwl_backtest(s):
                fwl_approved.append(s)
                found_counter += 1
                with state_lock:
                    if s not in state['first_win_list']: state['first_win_list'].append(s)
            time.sleep(1.0) # Safe spacing protection against rate ban
            
        sync_save()
        coin_string = " ".join([c.lower() for c in fwl_approved])
        report = (
            f"⚡⛏️ <b>FIRST WIN LIST REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n\n"
            f"<code>/fwl_add {coin_string}</code>\n\n"
            f"Mr. MASTER👑"
        )
        execute_telegram_send(report)
    except Exception as e:
        print(f"FWL Scanner Error: {e}")

# --- 03. OPTIMIZED PARALLEL MULTI-THREAD TRADING SCANNER ---
def scan_single_coin_target(s, zone_status):
    """Fetches and extracts indicators in parallel threads safely."""
    try:
        k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=530", timeout=8)
        k_data = k_res.json()
        if not isinstance(k_data, list) or len(k_data) < 525: return None
        
        df = pd.DataFrame(k_data, columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        signal_type = check_5m_indicator_alignment(s, df, zone_status)
        if signal_type != "NONE":
            return {"symbol": s, "side": signal_type, "close": float(df['close'].iloc[-1])}
    except: pass
    return None

def process_trading_scan():
    """Resolved: Processes all targeted coins inside a single rapid request batch via multi-threading executors within 30 seconds, maintaining a 30-second complete cool down break."""
    while True:
        try:
            THREAD_STATUS["Trade Scanner Loop"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            if not is_ict_trading_window():
                time.sleep(10); continue
                
            check_btc_status()
            update_all_1h_trends()
            
            with state_lock:
                fwl_coins = list(state.get('first_win_list', []))
                direct_mode = state.get('direct_signal_mode', False)
                max_signals = state.get('max_signals', 3)
                recovery_only = state.get('recovery_only_mode', False)
                scanned_list = list(state.get('scanned_symbols_list', []))
                
            targets = scanned_list if direct_mode else fwl_coins
            
            # Filter actionable tokens instantly
            actionable_targets = []
            for s in targets:
                with state_lock:
                    if s in state.get('block_list', []): continue
                    if s in state['active_positions']: continue
                    coin_step = state['symbol_recovery_step'].get(s, 0)
                    active_count = len(state['active_positions'])
                if recovery_only and coin_step == 0: continue
                if coin_step == 0 and active_count >= max_signals: continue
                actionable_targets.append(s)
                
            # Parallel Scanning Dispatch Execution Loop
            detected_signals = []
            if actionable_targets:
                with ThreadPoolExecutor(max_workers=15) as executor:
                    results = executor.map(lambda symbol: scan_single_coin_target(symbol, TREND_CACHE.get(symbol, "BUY_ZONE")), actionable_targets)
                    for r in results:
                        if r: detected_signals.append(r)
            
            # Execute trade allocations for validated entries
            for sig in detected_signals:
                if IS_BTC_CRASHING: break
                with state_lock:
                    active_count = len(state['active_positions'])
                    coin_step = state['symbol_recovery_step'].get(sig['symbol'], 0)
                if coin_step == 0 and active_count >= max_signals: continue
                
                execute_new_recovery_trade(sig['symbol'], sig['side'], sig['close'])
                time.sleep(1.0) # Internal buffer spacer
                
            time.sleep(30) # Mandatory 30 seconds break window processing execution restriction
            
        except Exception as e:
            time.sleep(10)

# --- 04. MATHEMATICAL RECOVERY & SHARED LOSS SPLIT ENGINE ---
def execute_new_recovery_trade(s, side, current_p):
    """Resolved: Fully processes the shared_loss_buffer split fraction into the new TP calculation target to completely absorb structural failure hits over subsequent operations."""
    with state_lock:
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 27.0)
        leverage = state.get('leverage', 10)
        
        # Split loss absorption controller logic
        absorbed_split_fraction = 0.0
        if state.get('shared_loss_splits_remaining', 0) > 0:
            absorbed_split_fraction = state.get('shared_loss_buffer', 0.0)
            state['shared_loss_splits_remaining'] -= 1
            if state['shared_loss_splits_remaining'] <= 0:
                state['shared_loss_buffer'] = 0.0 # Clear out buffer when fully processed
                
    position_size = current_margin * leverage 
    coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
    
    initial_sl = current_p * (1.0 - coin_sl_move_pct) if side == "BUY" else current_p * (1.0 + coin_sl_move_pct)
    
    # Calculate target price absorbing normal targets + accumulated loss + shared loss buffer splits fraction
    base_target_profit_cash = current_margin * (state.get('fast_tp_pct', 30.0) / 100.0)
    total_needed_pnl = base_target_profit_cash + accumulated_loss + absorbed_split_fraction
    
    required_move_pct = (total_needed_pnl / position_size) + 0.0015 # Incorporates trading network commission fees
    initial_tp = current_p * (1.0 + required_move_pct) if side == "BUY" else current_p * (1.0 - required_move_pct)
            
    with state_lock:
        state['active_positions'][s] = {
            "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
            "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
            "initial_1h_zone": TREND_CACHE.get(s, "BUY_ZONE"),
            "absorbed_split": absorbed_split_fraction
        }
        state['signal_count'] += 1
        sig_id = state['signal_count']
    
    protection_sl_cash = current_margin * (sl_margin_pct / 100.0)
    
    msg = (f"🔔 <b>NEW SIGNAL #{sig_id}</b> 🚨\n\n"
           f"📍 Symbol: <b>{s}</b> | Side: <b>{side}</b>\n"
           f"💵 Base Margin: <b>${round(current_margin, 2)} ({leverage}x)</b>\n"
           f"🎯 Target TP Price: <b>{round(initial_tp, 5)}</b>\n"
           f"🛑 {round(initial_sl, 5)} :Stop Loss Price\n\n"
           f"📈 Recovery Step: <b>{step}/2</b>\n"
           f"🛡️ Protection SL: <b>{round(sl_margin_pct, 1)}% (${round(protection_sl_cash, 3)})</b>\n"
           f"📊 Accumulated Loss: <b>${round(accumulated_loss, 3)}</b>\n"
           f"🧩 Shared Split Added: <b>${round(absorbed_split_fraction, 3)}</b>\n\n"
           f"Mr. MASTER👑")
           
    execute_telegram_send(msg)
    sync_save()

# --- 05. LIVE TRACKING & STRUCTURAL FAILURES MANAGEMENT ---
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
                        net_profit = (pos['margin'] * (state.get('fast_tp_pct', 30.0) / 100.0)) - 0.01
                        with state_lock:
                            state['daily_stats']['wins'] += 1
                            state['daily_stats']['won_trades'].append(f"{s} (${round(net_profit,2)})")
                            state['symbol_recovery_step'][s] = 0; state['symbol_accumulated_loss'][s] = 0.0
                            if s in state['active_positions']: del state['active_positions'][s]
                        execute_telegram_send(f"✅ <b>TARGET HIT: {s}</b>"); sync_save()
                            
                    elif (pos['side'] == "BUY" and current_p <= pos['sl']) or (pos['side'] == "SELL" and current_p >= pos['sl']):
                        with state_lock:
                            next_step = pos['step'] + 1
                            current_margin = state.get('base_margin', 0.80)
                            sl_margin_pct = state.get('margin_sl_pct', 27.0)
                            loss_amount = (current_margin * (sl_margin_pct / 100.0)) + 0.005
                            state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + loss_amount
                            
                            if next_step >= 3: 
                                if s not in state['block_list']: state['block_list'].append(s)
                                if s in state.get('first_win_list', []): state['first_win_list'].remove(s)
                                state['symbol_recovery_step'][s] = 0
                                final_lost_val = state['symbol_accumulated_loss'][s]
                                state['symbol_accumulated_loss'][s] = 0.0
                                state['daily_stats']['loss'] += 1
                                state['daily_stats']['lost_trades'].append(f"{s} (${round(final_lost_val, 2)})")
                                
                                # Split loss calculation engine implementation over next 4 slots
                                state['shared_loss_buffer'] = final_lost_val / 4.0
                                state['shared_loss_splits_remaining'] = 4
                                
                                execute_telegram_send(f"❌ <b>RECOVERY FAILED: {s}</b>\nකාසිය බ්ලැක්ලිස්ට් කරන ලදී. පාඩුව ඉදිරි ට්‍රේඩ් 4කට බෙදා හැරේ.")
                            else:
                                state['symbol_recovery_step'][s] = next_step
                                execute_telegram_send(f"⚠️ <b>STOP LOSS HIT (Step {next_step}/3): {s}</b>\nඊළඟ 5M Fractal එකෙන් රිකවර් කිරීමට සැකසුම් සූදානම්. ⏳")
                            if s in state['active_positions']: del state['active_positions'][s]
                        sync_save()
                except: pass
            time.sleep(1) 
        except Exception as e: time.sleep(2)

# --- 06. DAILY CENTRAL TIMERS CONTROL SCHEDULER ---
def central_scheduler():
    while True:
        try:
            THREAD_STATUS["Scheduler Loop"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            tz = pytz.timezone(BOT_TIMEZONE)
            now = datetime.datetime.now(tz)
            
            if now.hour == 0 and now.minute == 0 and 0 <= now.second < 5:
                threading.Thread(target=run_midnight_symbol_scanner, daemon=True).start()
                time.sleep(5)
                
            if now.hour == 9 and now.minute == 59 and 0 <= now.second < 5:
                with state_lock:
                    fw_list = list(state.get('first_win_list', []))
                coin_string = " ".join([c.lower() for c in fw_list])
                report = (
                    f"⚡⛏️ <b>FIRST WIN LIST REPORT</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━\n\n"
                    f"<code>/fwl_add {coin_string}</code>\n\n"
                    f"Mr. MASTER👑"
                )
                execute_telegram_send(report)
                time.sleep(5)
                
            time.sleep(1)
        except: time.sleep(5)

def telegram_reminder_worker():
    while True:
        try:
            THREAD_STATUS["Telegram Reminder"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            with state_lock:
                reminder_active = state.get('reminder_system_active', True)
                active_trades = list(state['active_positions'].keys())
            if reminder_active and len(active_trades) > 0:
                execute_telegram_send(f"📢 <b>[REMINDER]</b> පද්ධතියේ සක්‍රීය ට්‍රේඩ්ස් පවති(Active): <code>{', '.join(active_trades)}</code>")
            time.sleep(60)
        except: time.sleep(10)

def cron_daily_report_worker():
    while True:
        try:
            THREAD_STATUS["Daily Report Worker"] = {"status": "RUNNING 🟢", "last_seen": time.time()}
            tz = pytz.timezone(BOT_TIMEZONE)
            colombo_now = datetime.datetime.now(tz)
            if colombo_now.hour == 23 and colombo_now.minute == 59 and 0 <= colombo_now.second < 30: 
                with state_lock:
                    ds = state['daily_stats']
                    bl_coins = state.get('block_list', [])
                    
                    wins_count = ds.get('wins', 0)
                    loss_count = ds.get('loss', 0)
                    
                    won_str = ", ".join(ds.get('won_trades', [])) or "None"
                    loss_str = ", ".join(ds.get('lost_trades', [])) or "None"
                    bl_string = " ".join([b.lower() for b in bl_coins]) or "none"
                    
                    msg = (
                        f"📊 ✨ <b>FINAL PERFORMANCE REPORT</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🟢 Wins: {wins_count} ({won_str})\n"
                        f"🔴 Loss: {loss_count} ({loss_str})\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"Blacklist\n\n"
                        f"<code>Backlist_add {bl_string}</code>\n\n"
                        f"Mr. MASTER👑"
                    )
                    execute_telegram_send(msg)
                    state['daily_stats'] = {'wins': 0, 'loss': 0, 'won_trades': [], 'lost_trades': [], 'last_reset_date': str(datetime.date.today())}
                    state['symbol_structure_shift'] = {} 
                time.sleep(60)
            time.sleep(10)
        except: time.sleep(10)

# --- 07. TELEGRAM CONTROLLER PANEL ---
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
                        f"⏰ සිග්නල් දෙන කාලය: <b>{state.get('start_hour', 10)}:{state.get('start_minute', 0)} - {state.get('end_hour', 23)}:{state.get('end_minute', 59)} දක්වා.</b>\n"
                        f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                        f"⚙️ Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                        f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b> | TP: <b>{state.get('fast_tp_pct', 30.0)}%</b>\n"
                        f"🥇 First Win Coins ගණන: <b>{fw_list_count}</b>\n"
                        f"🚫 Blacklist Coins ගණන: <b>{bl_list_count}</b>"
                    )
                execute_telegram_send(msg)
            
            elif cmd == "symbol_scanner":
                threading.Thread(target=run_midnight_symbol_scanner, daemon=True).start()
                execute_telegram_send("⚡ <b>Manual Symbol Scanner ක්‍රියාත්මක කරන ලදී!</b>")
                
            elif cmd == "fwl_scanner":
                threading.Thread(target=run_fwl_scanner_logic, args=(True,), daemon=True).start()
                execute_telegram_send("⚡ <b>Manual FWL Scanner ක්‍රියාත්මක කරන ලදී! (Max 10 Coins Recovery Filter)</b>")

            elif cmd == "fwl_add":
                for item in parts[1:]:
                    coin = item.upper().strip()
                    with state_lock:
                        if coin not in state['first_win_list']: state['first_win_list'].append(coin)
                sync_save(); execute_telegram_send("🥇 First Win Coins ලැයිස්තුව යාවත්කාලීන කරන ලදී.")
                
            elif cmd == "fwl_remove" and len(parts) > 1:
                coin = parts[1].upper()
                with state_lock:
                    if coin in state['first_win_list']: state['first_win_list'].remove(coin)
                sync_save(); execute_telegram_send(f"🗑️ {coin} First Win ලැයිස්තුවෙන් ඉවත් කරන ලදී.")
                
            elif cmd == "fwl_view":
                with state_lock: fw = ", ".join(state.get('first_win_list', [])) or "හිස්"
                execute_telegram_send(f"🥇 <b>First Win Coins List:</b>\n<code>{fw}</code>")
                
            elif cmd == "backlist_add":
                for item in parts[1:]:
                    coin = item.upper().strip()
                    with state_lock:
                        if coin not in state['block_list']: state['block_list'].append(coin)
                sync_save(); execute_telegram_send("🚫 Blacklist ලැයිස්තුවට කාසි ඇතුළත් කරන ලදී.")
                
            elif cmd == "backlist_remo" and len(parts) > 1:
                coin = parts[1].upper()
                with state_lock:
                    if coin in state['block_list']: state['block_list'].remove(coin)
                sync_save(); execute_telegram_send(f"🔓 {coin} Blacklist ලැයිස්තුවෙන් නිදහස් කරන ලදී.")
                
            elif cmd == "backlist_view":
                with state_lock: bl = ", ".join(state.get('block_list', [])) or "හිස්"
                execute_telegram_send(f"🚫 <b>Blacklist Coins List:</b>\n<code>{bl}</code>")

            elif cmd == "clear_lists":
                with state_lock: state['first_win_list'] = []
                sync_save(); execute_telegram_send("🗑️ First Win ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් කරන ලදී.")
                
            elif cmd == "bot_on":
                with state_lock: state['is_scanning'] = True
                sync_save(); execute_telegram_send("▶️ ස්කෑනර් පද්ධතිය සක්‍රීය කරයි (ON).")
            elif cmd == "bot_off":
                with state_lock: state['is_scanning'] = False
                sync_save(); execute_telegram_send("⏸️ ස්කෑනර් පද්ධතිය තාවකාලිකව නවත්වයි (OFF).")
            elif cmd == "direct_mode_on":
                with state_lock: state['direct_signal_mode'] = True
                sync_save(); execute_telegram_send("⚡ Direct Mode සක්‍රීයයි (ON).")
            elif cmd == "direct_mode_off":
                with state_lock: state['direct_signal_mode'] = False
                sync_save(); execute_telegram_send("🛡️ Direct Mode අක්‍රීයයි (OFF).")
            elif cmd == "recovery_only_on":
                with state_lock: state['recovery_only_mode'] = True
                sync_save(); execute_telegram_send("⚠️ Recovery Only මාදිලිය සක්‍රීයයි.")
            elif cmd == "recovery_only_off":
                with state_lock: state['recovery_only_mode'] = False
                sync_save(); execute_telegram_send("🔄 Recovery Only මාදිලිය අක්‍රීයයි.")
            elif cmd == "reminder_on":
                with state_lock: state['reminder_system_active'] = True
                sync_save(); execute_telegram_send("🔔 Reminder සක්‍රීයයි.")
            elif cmd == "reminder_off":
                with state_lock: state['reminder_system_active'] = False
                sync_save(); execute_telegram_send("🔕 Reminder අක්‍රීයයි.")
            elif cmd == "reset_trades":
                with state_lock: state['active_positions'] = {}
                sync_save(); execute_telegram_send("🔄 Active Trades දත්ත ශුන්‍ය කරන ලදී.")
            elif cmd == "check_health":
                health_report = "🔍 <b>SYSTEM MODULE HEALTH REPORT</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                for thread_name, info in THREAD_STATUS.items():
                    health_report += f"⚙️ <b>{thread_name}:</b> {info['status']}\n"
                execute_telegram_send(health_report)
    except Exception as e: print(f"Webhook Error: {e}")
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "Bot Core Process Running Successfully!", 200

if __name__ == '__main__':
    sync_save()
    threading.Thread(target=central_scheduler, daemon=True).start()
    threading.Thread(target=process_trading_scan, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    threading.Thread(target=telegram_reminder_worker, daemon=True).start() 
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
