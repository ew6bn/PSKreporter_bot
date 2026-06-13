# -*- coding: utf-8 -*-
import asyncio
import json
import os
import sys
import io
import threading
import time
import re
import html
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramForbiddenError
import paho.mqtt.client as mqtt

# ===== FIX WINDOWS ENCODING =====
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
# ================================

# ========== SETTINGS ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    print("BOT_TOKEN environment variable not set!")
    sys.exit(1)

MQTT_BROKER = "mqtt.pskreporter.info"
MQTT_PORT = 1883
MQTT_TOPIC = "pskr/filter/v2/#"
MQTT_KEEPALIVE = 60

HTTP_API_URL = "https://pskreporter.info/cgi-bin/pskdata.pl"

USERS_CONFIG_FILE = "pskreporter_users.json"

MQTT_UPDATE_INTERVALS = {
    "30 sec": 30,
    "1 min": 60,
    "2 min": 120,
    "5 min": 300,
    "10 min": 600,
    "15 min": 900
}

HTTP_SEARCH_INTERVALS = {
    "15 min": 15 * 60,
    "30 min": 30 * 60,
    "1 hour": 60 * 60,
    "2 hours": 2 * 60 * 60,
    "3 hours": 3 * 60 * 60,
    "6 hours": 6 * 60 * 60,
    "12 hours": 12 * 60 * 60,
    "24 hours": 24 * 60 * 60
}

BANDS = ["160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "VHF+"]

def freq_to_band(freq_mhz: float) -> str:
    if 1.8 <= freq_mhz <= 2.0: return "160m"
    if 3.5 <= freq_mhz <= 4.0: return "80m"
    if 5.3 <= freq_mhz <= 5.5: return "60m"
    if 7.0 <= freq_mhz <= 7.3: return "40m"
    if 10.1 <= freq_mhz <= 10.15: return "30m"
    if 14.0 <= freq_mhz <= 14.35: return "20m"
    if 18.068 <= freq_mhz <= 18.168: return "17m"
    if 21.0 <= freq_mhz <= 21.45: return "15m"
    if 24.89 <= freq_mhz <= 24.99: return "12m"
    if 28.0 <= freq_mhz <= 29.7: return "10m"
    if 50.0 <= freq_mhz <= 54.0: return "6m"
    if 144.0 <= freq_mhz <= 148.0: return "VHF+"
    if 430.0 <= freq_mhz <= 440.0: return "VHF+"
    return "?"

users_config = {}
spot_storage = {}
spot_storage_lock = threading.Lock()
mqtt_client = None
waiting_for_callsign = {}
auto_update_tasks = {}

# ========== CONFIG ==========
def load_users_config():
    global users_config
    if os.path.exists(USERS_CONFIG_FILE):
        try:
            with open(USERS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                users_config = json.load(f)
        except:
            users_config = {}

def save_users_config():
    try:
        with open(USERS_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(users_config, f, ensure_ascii=False, indent=2)
    except:
        pass

def get_user_config(user_id):
    uid = str(user_id)
    if uid not in users_config:
        users_config[uid] = {
            "callsign": None,
            "mqtt_interval": "1 min",
            "http_interval": "1 hour",
            "data_source": "mqtt",
            "active": True,
            "http_bands": ["all"]
        }
    return users_config[uid]

def save_user_callsign(user_id, callsign):
    get_user_config(user_id)["callsign"] = callsign
    save_users_config()

def get_user_callsign(user_id):
    return get_user_config(user_id).get("callsign")

def get_user_mqtt_interval(user_id):
    return get_user_config(user_id).get("mqtt_interval", "1 min")

def get_user_mqtt_interval_seconds(user_id):
    interval_str = get_user_mqtt_interval(user_id)
    return MQTT_UPDATE_INTERVALS.get(interval_str, 60)

def save_user_mqtt_interval(user_id, interval_str):
    get_user_config(user_id)["mqtt_interval"] = interval_str
    save_users_config()

def get_user_http_interval(user_id):
    return get_user_config(user_id).get("http_interval", "1 hour")

def get_user_http_interval_seconds(user_id):
    interval_str = get_user_http_interval(user_id)
    return HTTP_SEARCH_INTERVALS.get(interval_str, 3600)

def save_user_http_interval(user_id, interval_str):
    get_user_config(user_id)["http_interval"] = interval_str
    save_users_config()

def get_user_data_source(user_id):
    return get_user_config(user_id).get("data_source", "mqtt")

def save_user_data_source(user_id, source):
    get_user_config(user_id)["data_source"] = source
    save_users_config()

def is_user_active(user_id):
    return get_user_config(user_id).get("active", True)

def set_user_active(user_id, active):
    get_user_config(user_id)["active"] = active
    save_users_config()

def remove_user(user_id):
    uid = str(user_id)
    if uid in users_config:
        del users_config[uid]
        save_users_config()

def get_user_http_bands(user_id):
    return get_user_config(user_id).get("http_bands", ["all"])

def save_user_http_bands(user_id, bands):
    get_user_config(user_id)["http_bands"] = bands
    save_users_config()

def get_current_interval_for_button(user_id):
    source = get_user_data_source(user_id)
    if source == "mqtt":
        return get_user_mqtt_interval(user_id)
    else:
        return get_user_http_interval(user_id)

def get_bands_display(user_id):
    bands = get_user_http_bands(user_id)
    if "all" in bands:
        return "All"
    if not bands:
        return "None"
    return ", ".join(bands)

def get_interval_keyboard_for_source(source):
    if source == "mqtt":
        intervals = list(MQTT_UPDATE_INTERVALS.keys())
    else:
        intervals = list(HTTP_SEARCH_INTERVALS.keys())
    keyboard = []
    for i in range(0, len(intervals), 2):
        row = []
        for j in range(i, min(i+2, len(intervals))):
            interval = intervals[j]
            row.append(InlineKeyboardButton(text=interval, callback_data=f"interval_{source}_{interval}"))
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="❌ Cancel", callback_data="interval_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_bands_keyboard(user_id):
    selected = get_user_http_bands(user_id)
    keyboard = []
    # Две колонки для выбора диапазонов
    for i in range(0, len(BANDS), 2):
        row = []
        band1 = BANDS[i]
        check1 = "✅" if band1 in selected else ""
        row.append(InlineKeyboardButton(text=f"{check1}{band1}", callback_data=f"band_{band1}"))
        if i + 1 < len(BANDS):
            band2 = BANDS[i+1]
            check2 = "✅" if band2 in selected else ""
            row.append(InlineKeyboardButton(text=f"{check2}{band2}", callback_data=f"band_{band2}"))
        keyboard.append(row)
    # Одна строка с кнопками Clear, Cancel, All, Apply
    keyboard.append([
        InlineKeyboardButton(text="❌ Clear", callback_data="band_clear"),
        InlineKeyboardButton(text="❌ Cancel", callback_data="band_cancel"),
        InlineKeyboardButton(text="🌍 All" if "all" in selected else "📻 All", callback_data="band_all"),
        InlineKeyboardButton(text="✅ Apply", callback_data="band_apply")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== MQTT STORAGE ==========
def add_spot_to_storage(spot):
    callsign = spot.get('sc', '').upper()
    if not callsign:
        return
    spot_data = {
        't': spot.get('t', time.time()),
        'freq_hz': spot.get('f', 0),
        'rx': spot.get('rc', '?'),
        'mode': spot.get('md', '?'),
        'snr': spot.get('rp', '?')
    }
    with spot_storage_lock:
        if callsign not in spot_storage:
            spot_storage[callsign] = []
        spot_storage[callsign].append(spot_data)
        if len(spot_storage[callsign]) > 500:
            spot_storage[callsign] = spot_storage[callsign][-500:]

def get_all_spots(callsign):
    if not callsign:
        return []
    callsign = callsign.upper()
    with spot_storage_lock:
        if callsign not in spot_storage:
            return []
        return spot_storage[callsign].copy()

def clear_old_spots():
    cutoff = time.time() - 7200
    with spot_storage_lock:
        for callsign in list(spot_storage.keys()):
            spot_storage[callsign] = [s for s in spot_storage[callsign] if s.get('t', 0) >= cutoff]
            if not spot_storage[callsign]:
                del spot_storage[callsign]

# ========== HTTP ADIF PARSER ==========
async def fetch_spots_http(callsign: str, days_back: int = 2) -> list:
    params = {
        'adif': 1,
        'senderCallsign': callsign.upper(),
        'days': days_back
    }
    headers = {
        'User-Agent': 'PSKReporterBot/1.0',
        'Cache-Control': 'no-cache'
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(HTTP_API_URL, params=params, headers=headers, timeout=15) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
                return parse_adif_full(text)
    except Exception as e:
        print(f"HTTP fetch error: {e}")
        return []

def parse_adif_full(adif_text: str) -> list:
    spots = []
    raw_records = re.split(r'<eor>', adif_text, flags=re.IGNORECASE)
    for raw in raw_records:
        if not raw.strip():
            continue
        fields = {}
        tag_pattern = r'<([A-Za-z_]+):(\d+)(:[^>]*)?>([^<]*)'
        for match in re.finditer(tag_pattern, raw):
            tag = match.group(1).upper()
            length = int(match.group(2))
            value = match.group(4)[:length].strip()
            fields[tag] = value
        if not (fields.get('OPERATOR') or fields.get('STATION_CALLSIGN') or fields.get('CALL')):
            continue
        spot = adif_to_spot_dict(fields)
        if spot:
            spots.append(spot)
    return spots

def adif_to_spot_dict(fields: dict) -> dict:
    qso_date = fields.get('QSO_DATE', '')
    time_on = fields.get('TIME_ON', '')
    timestamp = time.time()
    if qso_date and time_on:
        try:
            dt_str = f"{qso_date[:4]}-{qso_date[4:6]}-{qso_date[6:8]} {time_on[:2]}:{time_on[2:4]}:{time_on[4:6]}"
            dt_utc = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            timestamp = dt_utc.timestamp()
        except:
            pass

    freq_str = fields.get('FREQ', '0')
    receiver = fields.get('OPERATOR', fields.get('STATION_CALLSIGN', fields.get('CALL', '?')))
    mode = fields.get('MODE', '?')
    snr_raw = fields.get('APP_PSKREP_SNR', '')
    try:
        snr = int(snr_raw)
    except:
        snr = None
    dist_raw = fields.get('DISTANCE', '0')
    try:
        distance = float(dist_raw)
    except:
        distance = 0.0

    try:
        freq_mhz = float(freq_str)
    except:
        freq_mhz = 0.0
    band = freq_to_band(freq_mhz)

    return {
        't': timestamp,
        'freq_str': freq_str,
        'mode': mode,
        'rx': receiver,
        'snr': snr,
        'distance': distance,
        'band': band
    }

# ========== MQTT CLIENT ==========
class PSKReporterMQTTClient:
    def __init__(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.connected = False

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.connected = True
            client.subscribe(MQTT_TOPIC)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            spot = {
                't': payload.get('t'),
                'sc': payload.get('sc'),
                'rc': payload.get('rc'),
                'f': payload.get('f'),
                'md': payload.get('md'),
                'rp': payload.get('rp'),
            }
            if spot['sc'] and spot['rc']:
                add_spot_to_storage(spot)
        except:
            pass

    def start(self):
        def loop():
            while True:
                try:
                    self.client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
                    self.client.loop_forever()
                    break
                except:
                    time.sleep(5)
        threading.Thread(target=loop, daemon=True).start()
        time.sleep(2)

# ========== SEARCH & FORMATTING ==========
async def search_spots(user_id, callsign=None, interval_seconds=None):
    if callsign is None:
        callsign = get_user_callsign(user_id)
    if not callsign:
        return None, "❌ No callsign set. Use 📝 Set Callsign button"

    source = get_user_data_source(user_id)
    if interval_seconds is None:
        interval_seconds = get_user_http_interval_seconds(user_id)

    now_utc = time.time()
    cutoff_utc = now_utc - interval_seconds

    if source == "http":
        spots = await fetch_spots_http(callsign, 2)
        if not spots:
            return [], f"📡 {callsign}\nNo spots found in the last {get_user_http_interval(user_id)} (HTTP ADIF)\n\nCheck your band or time interval selection!"
        filtered = [s for s in spots if s.get('t', 0) >= cutoff_utc]
        selected_bands = get_user_http_bands(user_id)
        if "all" not in selected_bands:
            filtered = [s for s in filtered if s.get('band', '?') in selected_bands]
            if not filtered:
                interval_human = get_user_http_interval(user_id)
                bands_str = ", ".join(selected_bands)
                return [], f"📡 {callsign}\nNo spots found in selected bands {bands_str} or last {interval_human}.\n\nCheck your band or time interval selection!"
        if not filtered:
            interval_human = get_user_http_interval(user_id)
            return [], f"📡 {callsign}\nNo spots found in the last {interval_human} (HTTP ADIF)\n\nCheck your band or time interval selection!"
        best_by_rx = {}
        for s in filtered:
            rx = s.get('rx', '?')
            dist = s.get('distance', 0)
            if rx not in best_by_rx or dist > best_by_rx[rx]['distance']:
                best_by_rx[rx] = s
        unique = list(best_by_rx.values())
        unique.sort(key=lambda x: x.get('distance', 0), reverse=True)
        interval_human = get_user_http_interval(user_id)
        return unique, format_spots_http(callsign, unique, interval_human, len(filtered), len(unique))
    else:
        spots = get_all_spots(callsign)
        filtered = [s for s in spots if s.get('t', 0) >= cutoff_utc]
        filtered.sort(key=lambda x: x.get('t', 0), reverse=True)
        return filtered, format_spots_mqtt(callsign, filtered)

def format_spots_mqtt(callsign, spots):
    if not spots:
        return f"📡 {callsign}\nNo spots found"

    lines = [f"📡 {callsign}  {len(spots)} spots"]
    by_date = defaultdict(list)
    for s in spots:
        dt = datetime.fromtimestamp(s.get('t', 0), tz=timezone.utc)
        date_key = dt.strftime('%Y-%m-%d')
        by_date[date_key].append((dt, s))

    for date_key in sorted(by_date.keys(), reverse=True):
        dt_first = datetime.strptime(date_key, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        date_str = dt_first.strftime('%d %B %Y')
        lines.append("")
        lines.append(date_str)
        for dt, s in by_date[date_key]:
            t_str = dt.strftime('%H:%M')
            freq_hz = s.get('freq_hz', 0)
            if freq_hz:
                freq_mhz = freq_hz / 1_000_000
                freq_str = f"{freq_mhz:.6f}".rstrip('0').rstrip('.')
            else:
                freq_str = '?'
            rx = html.escape(s.get('rx', '?'))
            mode = s.get('mode', '?')
            snr = s.get('snr', '?')
            lines.append(f"{t_str} {freq_str} <b>{rx}</b> {mode} {snr}dB")
    return "\n".join(lines)

def format_spots_http(callsign, spots, interval_human, total_raw, total_unique):
    if not spots:
        return f"📡 {callsign}\nNo spots found in last {interval_human} (HTTP)"

    lines = [f"📡 {callsign}  {len(spots)} unique (of {total_raw})  [{interval_human}]"]
    by_date = defaultdict(list)
    for s in spots:
        dt = datetime.fromtimestamp(s.get('t', 0), tz=timezone.utc)
        date_key = dt.strftime('%Y-%m-%d')
        by_date[date_key].append((dt, s))

    for date_key in sorted(by_date.keys(), reverse=True):
        dt_first = datetime.strptime(date_key, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        date_str = dt_first.strftime('%d %B %Y')
        lines.append("")
        lines.append(date_str)
        for dt, s in by_date[date_key]:
            t_str = dt.strftime('%H:%M')
            freq = s.get('freq_str', '?')
            rx = html.escape(s.get('rx', '?'))
            mode = s.get('mode', '?')
            snr = s.get('snr')
            snr_str = f"{snr}dB" if snr is not None else "?dB"
            dist = s.get('distance', 0)
            dist_str = f"{dist:.0f}km" if dist > 0 else ""
            parts = [t_str, freq, f"<b>{rx}</b>", mode, snr_str]
            if dist_str:
                parts.append(dist_str)
            lines.append(' '.join(parts))
    return "\n".join(lines)

def split_long_message(text, max_len=4096):
    if len(text) <= max_len:
        return [text]
    lines = text.split('\n')
    parts = []
    cur = []
    for line in lines:
        if len('\n'.join(cur + [line])) + 1 > max_len:
            parts.append('\n'.join(cur))
            cur = [line]
        else:
            cur.append(line)
    if cur:
        parts.append('\n'.join(cur))
    return parts

# ========== KEYBOARDS ==========
def get_main_keyboard(user_id):
    active = is_user_active(user_id)
    callsign = get_user_callsign(user_id)
    source = get_user_data_source(user_id)
    if source == "mqtt":
        source_text = "MQTT (current spots)"
    else:
        source_text = "HTTP"
    callsign_text = f"📝 {callsign}" if callsign else "📝 Set Callsign"
    interval_display = get_current_interval_for_button(user_id)

    if source == "http":
        bands_display = get_bands_display(user_id)
        bands_text = f"🎚️ Bands: {bands_display}"
    else:
        bands_text = None

    buttons = [
        [KeyboardButton(text="🔍 SEARCH NOW")],
        [KeyboardButton(text=callsign_text), KeyboardButton(text=f"⏱ {interval_display}")],
        [KeyboardButton(text=f"📡 {source_text}")]
    ]
    if bands_text:
        buttons[2].append(KeyboardButton(text=bands_text))
    buttons.append([KeyboardButton(text="🛑 STOP" if active else "▶️ START"), KeyboardButton(text="❓ Help")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_source_keyboard(user_id):
    current = get_user_data_source(user_id)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'✅ ' if current == 'mqtt' else ''}MQTT (current spots)", callback_data="source_mqtt"),
         InlineKeyboardButton(text=f"{'✅ ' if current == 'http' else ''}HTTP (history)", callback_data="source_http")],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="source_cancel")]
    ])

# ========== AUTO UPDATE ==========
async def auto_update_for_user(user_id, bot_instance):
    while True:
        if not is_user_active(user_id):
            await asyncio.sleep(5)
            continue
        source = get_user_data_source(user_id)
        if source != "mqtt":
            await asyncio.sleep(10)
            continue
        update_interval_sec = get_user_mqtt_interval_seconds(user_id)
        search_interval_sec = get_user_http_interval_seconds(user_id)
        callsign = get_user_callsign(user_id)
        if callsign:
            spots = get_all_spots(callsign)
            if spots:
                cutoff_utc = time.time() - search_interval_sec
                filtered = [s for s in spots if s.get('t', 0) >= cutoff_utc]
                if filtered:
                    filtered.sort(key=lambda x: x.get('t', 0), reverse=True)
                    text = format_spots_mqtt(callsign, filtered)
                    for part in split_long_message(text):
                        try:
                            await bot_instance.send_message(user_id, part, parse_mode='HTML')
                        except TelegramForbiddenError:
                            remove_user(user_id)
                            return
                        except:
                            pass
        await asyncio.sleep(update_interval_sec)

def start_auto_update(user_id, bot_instance):
    if user_id in auto_update_tasks:
        try:
            auto_update_tasks[user_id].cancel()
        except:
            pass
        del auto_update_tasks[user_id]
    task = asyncio.create_task(auto_update_for_user(user_id, bot_instance))
    auto_update_tasks[user_id] = task

def stop_auto_update(user_id):
    if user_id in auto_update_tasks:
        try:
            auto_update_tasks[user_id].cancel()
        except:
            pass
        del auto_update_tasks[user_id]

# ========== HANDLERS ==========
def register_handlers(dp, bot_instance):
    @dp.message(Command("start"))
    async def cmd_start(m: types.Message):
        uid = m.from_user.id
        if get_user_data_source(uid) == "mqtt":
            start_auto_update(uid, bot_instance)
        callsign = get_user_callsign(uid)
        source = get_user_data_source(uid)
        interval_display = get_current_interval_for_button(uid)
        bands_display = get_bands_display(uid) if source == "http" else None
        text = f"🤖 PSKReporter Bot\nCallsign: {callsign or 'Not set'}\nSource: {'MQTT (current spots)' if source == 'mqtt' else 'HTTP'}\nStatus: {'ACTIVE' if is_user_active(uid) else 'STOPPED'}\nInterval: {interval_display}"
        if bands_display:
            text += f"\nBands: {bands_display}"
        await m.answer(text, reply_markup=get_main_keyboard(uid))

    @dp.message(Command("stop"))
    async def cmd_stop(m: types.Message):
        uid = m.from_user.id
        set_user_active(uid, False)
        stop_auto_update(uid)
        await m.answer("🛑 STOPPED", reply_markup=get_main_keyboard(uid))

    @dp.message(Command("start_auto"))
    async def cmd_start_auto(m: types.Message):
        uid = m.from_user.id
        set_user_active(uid, True)
        if get_user_data_source(uid) == "mqtt":
            start_auto_update(uid, bot_instance)
        await m.answer("▶️ RESUMED", reply_markup=get_main_keyboard(uid))

    @dp.message(Command("search"))
    async def cmd_search(m: types.Message):
        uid = m.from_user.id
        callsign = get_user_callsign(uid)
        if not callsign:
            await m.answer("❌ Set callsign first!\nUse 📝 Set Callsign button")
            return
        msg = await m.answer("🔍 Searching...")
        interval_sec = get_user_http_interval_seconds(uid)
        spots, text = await search_spots(uid, callsign, interval_sec)
        await msg.delete()
        for part in split_long_message(text):
            await m.answer(part, parse_mode='HTML' if '<b>' in part else None)

    @dp.message(Command("help"))
    async def cmd_help(m: types.Message):
        await m.answer(
            "📖 Help\n\n"
            "🔍 SEARCH NOW - manual search\n"
            "📝 Set Callsign - set callsign\n"
            "⏱ - set interval (MQTT: auto‑update, HTTP: search depth)\n"
            "🎚️ Bands (HTTP only) - select bands for HTTP mode\n"
            "📡 - switch source (MQTT/HTTP)\n"
            "🛑 STOP / ▶️ START - pause/resume auto-updates (MQTT only)"
        )

    @dp.message(Command("cancel"))
    async def cmd_cancel(m: types.Message):
        uid = m.from_user.id
        if uid in waiting_for_callsign:
            del waiting_for_callsign[uid]
            await m.answer("❌ Cancelled", reply_markup=get_main_keyboard(uid))

    @dp.message(lambda m: m.text == "🔍 SEARCH NOW")
    async def btn_search(m: types.Message):
        await cmd_search(m)

    @dp.message(lambda m: m.text == "📝 Set Callsign" or (m.text and m.text.startswith("📝 ") and m.text != "📝 Set Callsign"))
    async def btn_set_callsign(m: types.Message):
        waiting_for_callsign[m.from_user.id] = True
        await m.answer("📝 Send callsign\nExample: W0DKA\n\n/cancel - cancel")

    @dp.message(lambda m: m.text and m.text.startswith("⏱ "))
    async def btn_interval(m: types.Message):
        uid = m.from_user.id
        source = get_user_data_source(uid)
        await m.answer(f"⏱ Select interval for {'MQTT' if source == 'mqtt' else 'HTTP'}:",
                       reply_markup=get_interval_keyboard_for_source(source))

    @dp.message(lambda m: m.text and m.text.startswith("🎚️ Bands"))
    async def btn_bands(m: types.Message):
        uid = m.from_user.id
        source = get_user_data_source(uid)
        if source != "http":
            await m.answer("⚠️ Band filtering only available in HTTP mode. Switch to HTTP first (📡 button).")
            return
        await m.answer(f"🎚️ Select bands for HTTP mode (current: {get_bands_display(uid)})",
                       reply_markup=get_bands_keyboard(uid))

    @dp.message(lambda m: m.text and m.text.startswith("📡 "))
    async def btn_source(m: types.Message):
        uid = m.from_user.id
        await m.answer("📡 Select source:", reply_markup=get_source_keyboard(uid))

    @dp.message(lambda m: m.text == "🛑 STOP")
    async def btn_stop(m: types.Message):
        await cmd_stop(m)

    @dp.message(lambda m: m.text == "▶️ START")
    async def btn_start(m: types.Message):
        await cmd_start_auto(m)

    @dp.message(lambda m: m.text == "❓ Help")
    async def btn_help(m: types.Message):
        await cmd_help(m)

    @dp.message()
    async def handle_text(m: types.Message):
        uid = m.from_user.id
        if uid in waiting_for_callsign:
            callsign = m.text.strip().upper()
            if 3 <= len(callsign) <= 10 and callsign[0].isalpha():
                save_user_callsign(uid, callsign)
                del waiting_for_callsign[uid]
                await m.answer(f"✅ Callsign {callsign} saved!", reply_markup=get_main_keyboard(uid))
            else:
                await m.answer("❌ Invalid callsign (3-10 chars, starts with letter)")
            return
        await m.answer("Use buttons:", reply_markup=get_main_keyboard(uid))

    @dp.callback_query()
    async def handle_callbacks(cb: CallbackQuery):
        uid = cb.from_user.id
        data = cb.data
        if data == "interval_cancel":
            await cb.message.edit_text("❌ Cancelled")
        elif data.startswith("interval_"):
            parts = data.split('_')
            if len(parts) >= 3:
                source = parts[1]
                interval_str = '_'.join(parts[2:])
                if source == "mqtt":
                    if interval_str in MQTT_UPDATE_INTERVALS:
                        save_user_mqtt_interval(uid, interval_str)
                        if is_user_active(uid) and get_user_data_source(uid) == "mqtt":
                            start_auto_update(uid, bot_instance)
                        await cb.message.edit_text(f"✅ MQTT interval set to {interval_str}")
                        await cb.message.answer("Settings updated", reply_markup=get_main_keyboard(uid))
                elif source == "http":
                    if interval_str in HTTP_SEARCH_INTERVALS:
                        save_user_http_interval(uid, interval_str)
                        await cb.message.edit_text(f"✅ HTTP interval set to {interval_str}")
                        await cb.message.answer("Settings updated", reply_markup=get_main_keyboard(uid))
        elif data.startswith("band_"):
            band = data[5:]
            if get_user_data_source(uid) != "http":
                await cb.answer("Band selection only in HTTP mode", show_alert=True)
                return
            bands = get_user_http_bands(uid)
            if band == "all":
                save_user_http_bands(uid, ["all"])
                await cb.message.edit_text("🎚️ All bands selected", reply_markup=get_bands_keyboard(uid))
            elif band == "clear":
                save_user_http_bands(uid, [])
                await cb.message.edit_text("🎚️ Cleared", reply_markup=get_bands_keyboard(uid))
            elif band == "apply":
                await cb.message.delete()
                await cb.message.answer(f"✅ Bands saved: {get_bands_display(uid)}", reply_markup=get_main_keyboard(uid))
            elif band == "cancel":
                await cb.message.edit_text("❌ Cancelled")
            elif band in BANDS:
                if "all" in bands:
                    bands = []
                if band in bands:
                    bands.remove(band)
                else:
                    bands.append(band)
                save_user_http_bands(uid, bands)
                await cb.message.edit_text(f"🎚️ Current: {get_bands_display(uid)}", reply_markup=get_bands_keyboard(uid))
        elif data == "source_cancel":
            await cb.message.edit_text("❌ Cancelled")
        elif data == "source_mqtt":
            save_user_data_source(uid, "mqtt")
            if is_user_active(uid):
                start_auto_update(uid, bot_instance)
            await cb.message.delete()
            await cb.message.answer("✅ Source: MQTT (current spots)", reply_markup=get_main_keyboard(uid))
        elif data == "source_http":
            save_user_data_source(uid, "http")
            stop_auto_update(uid)
            await cb.message.delete()
            await cb.message.answer("✅ Source: HTTP", reply_markup=get_main_keyboard(uid))
        await cb.answer()

# ========== MAIN ==========
async def main():
    global mqtt_client
    async def cleanup_task():
        while True:
            await asyncio.sleep(300)
            clear_old_spots()

    mqtt_client = PSKReporterMQTTClient()
    mqtt_client.start()
    load_users_config()

    bot_instance = Bot(token=BOT_TOKEN)
    dp_instance = Dispatcher()
    register_handlers(dp_instance, bot_instance)

    asyncio.create_task(cleanup_task())
    for uid_str, cfg in users_config.items():
        if cfg.get("active", True) and cfg.get("data_source", "mqtt") == "mqtt":
            start_auto_update(int(uid_str), bot_instance)

    print("Bot started! Final version with polished messages.")
    await dp_instance.start_polling(bot_instance)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
    except Exception as e:
        print(f"Fatal error: {e}")