"""
Bot B - Volume Profile Circular Mean Reversion (v1-bybit-xrp-profile)
entry: standing post-only limit at strong volume-profile support/resistance zones
dca: only at the next meaningful volume zone, with circular sizing
release: reduce recent size after a small favorable reaction
avg rescue: if DCA count >= 2, standing reduce-only limit near avg profit
invalidation: reduce the latest layer if the zone fails or gives no reaction
fail: market reduce after the full ladder is used and the last layer is badly wrong

Bybit XRP/USDT perpetual | hedge mode
"""
import ccxt
import json
import math
import os
import sys
import time
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

SYMBOL = "XRP/USDT:USDT"
TF = "5m"
LEVERAGE = 20
CHECK_SEC = 0.5
FETCH_INTERVAL = 45
HTF_FETCH_INTERVAL = 180

BB_LEN = 20
BB_ENTRY_STD = 3.0
BB_MGMT_STD = 2.0

SIDE_MARGIN_CAP = 0.50

DCA_COOL_SECS = 30 * 60
MARKET_REDUCE_GUARD_SECS = 3
LIMIT_REDUCE_SLIPPAGE_PCT = 0.0015
FAIL_LIMIT_REDUCE_SLIPPAGE_PCT = 0.0040
ENTRY_REPRICE_MIN_DELTA = 0.0005
BALANCE_REBASE_THRESHOLD = 0.25
SEED_ACCEPT_RATIO = 0.85
DCA_STEP_PCTS = [0.008, 0.018]
DCA_ADD_MULTIPLIERS = [2.0, 1.333333]
DCA_MAX_ZONE_DIST_PCTS = {1: 0.0500, 2: 0.0800}
STALE_LAST_FILL_MAX_DEV_PCT = 0.0300
ZONE_RESTORE_MAX_DEV_PCT = 0.0200
QUICK_RELEASE_MAX_DIST_PCT = 0.0600

SIGMA2_TP_RATIO = 0.35
SIGMA3_TP_RATIO = 0.35
STRUCTURE_TP_RATIOS = {2: 0.25, 3: 0.30, 4: 0.40, 5: 0.50}
STAGE_TP_RATIOS = {1: 0.25, 2: 0.45, 3: 0.65}
SIGMA2_TP_COOL_BARS = 6
SIGMA3_TP_COOL_BARS = 6
AVG_RESCUE_TP_RATIO = 0.50
AVG_RESCUE_DCA_COUNT = 2
AVG_RESCUE_MIN_USED_RATIO = 0.30
AVG_RESCUE_PROFIT_PCT = 0.0010
AVG_RESCUE_TP_COOL_BARS = 6
MIN_PROFIT_PCT_ENTRY_LIMIT = 0.0
MIN_PROFIT_PCT_TP_LIMIT = 0.0005
MIN_PROFIT_PCT_STRUCTURE_TP = 0.0020

FAIL_BARS = 5
FAIL_MOVE_BB = 1.00
FAIL_MOVE_PCT = 0.025
FAIL_ACTIVE_RATIO = 0.90

PROFILE_LOOKBACK_BARS = 360
PROFILE_WINDOWS = {"M5": 288, "M15": 192, "H1": 168, "H4": 120}
PROFILE_FETCH_LIMITS = {"M5": 360, "M15": 220, "H1": 200, "H4": 160}
PROFILE_TF_WEIGHTS = {"M5": 1.0, "M15": 1.25, "H1": 1.70, "H4": 2.25}
PROFILE_BIN_PCT = 0.0005
PROFILE_TOP_NODE_RATIO = 0.08
PROFILE_MIN_NODE_SHARE = 0.008
PROFILE_MAX_ZONES = 28
HORIZONTAL_ZONE_PCT = 0.0012
HORIZONTAL_MIN_TOUCHES = 2
TRENDLINE_ZONE_PCT = 0.0015
TRENDLINE_MAX_DIST_PCT = 0.0400
TRENDLINE_MIN_SEP_BARS = 12
TRENDLINE_MIN_TOUCHES = 3
ZONE_MERGE_GAP_BINS = 2
ZONE_TOUCH_PCT = 0.0008
ENTRY_MIN_ZONE_TIER = 2
DCA_MIN_ZONE_TIER = 2
MIN_ENTRY_ZONE_DIST_PCT = 0.0035
MAX_DIRECT_ZONE_WIDTH_PCT = 0.0150
BROAD_EDGE_ZONE_MIN_TIER = 4
BROAD_EDGE_ZONE_MAX_WIDTH_PCT = 0.0350
BROAD_EDGE_ZONE_PAD_PCT = 0.0010
MIN_HEDGE_ENTRY_SPREAD_PCT = 0.0030
TIER_TARGET_RATIOS = {1: 0.05, 2: 0.35, 3: 0.60, 4: 0.85, 5: 1.00}
REGIME_PROBE_RATIO = 0.142857
KUMO_FAR_NOW_PCT = 0.006
KUMO_FAR_RECENT_PCT = 0.012
KUMO_MEMORY_BARS = 24
QUICK_RELEASE_PCT = 0.0020
QUICK_RELEASE_RATIO = 0.50
QUICK_RELEASE_ENABLED = False
QUICK_RELEASE_MIN_DCA_COUNT = 1
INVALID_BARS = 2
INVALID_CUT_RATIO = 0.50
ZONE_BREAK_BUFFER_PCT = 0.0025

MIN_QTY = 0.01
MIN_VAL = 5.0
QTY_TOL = MIN_QTY / 2
SYMBOL_MIN_QTY = MIN_QTY

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bbot_state_bybit_xrp_v1_profile.json")
RUN_LOCK_FILE = os.path.join(BASE_DIR, "bot_b.lock")
STATE_FILE_FALLBACKS = []
ENTRY_SLOTS = ["long_entry", "short_entry"]
TPAVG_SLOTS = ["long_tpavg", "short_tpavg"]
TP2_SLOTS = ["long_tp2", "short_tp2"]
TP3_SLOTS = ["long_tp3", "short_tp3"]
ORDER_SLOTS = ENTRY_SLOTS + TPAVG_SLOTS + TP2_SLOTS + TP3_SLOTS
FINAL_STATES = ("closed", "canceled", "expired", "rejected")

ENTRY_TARGET_RATIOS = [0.142857, 0.428571, 1.00]
MAX_DCA_COUNT = len(ENTRY_TARGET_RATIOS) - 1

ex = ccxt.bybit(
    {
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_SECRET_KEY"),
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
)

LAST_MARKET_REDUCE_TS = {"LONG": 0.0, "SHORT": 0.0}
RUN_LOCK_HANDLE = None


def trunc(x, d):
    return math.trunc(x * 10**d) / 10**d


def ts():
    return time.time()


def get_price():
    try:
        return float(ex.fetch_ticker(SYMBOL)["last"])
    except Exception:
        return None


def normalize_qty(qty, round_up=False):
    global SYMBOL_MIN_QTY
    if qty <= 0:
        return 0.0
    try:
        precise = float(ex.amount_to_precision(SYMBOL, qty))
    except Exception:
        precise = trunc(qty, 3)
    min_qty = max(MIN_QTY, SYMBOL_MIN_QTY)
    if precise >= min_qty:
        return precise
    if round_up and qty >= QTY_TOL:
        return min_qty
    return 0.0


def configure_symbol_rules():
    global SYMBOL_MIN_QTY
    ex.load_markets()
    market = ex.market(SYMBOL)
    min_qty = market.get("limits", {}).get("amount", {}).get("min")
    if min_qty:
        SYMBOL_MIN_QTY = float(min_qty)
    return market


def get_bal():
    try:
        b = ex.fetch_balance({"type": "swap"})
        if "USDT" in b and isinstance(b["USDT"], dict):
            for key in ["total", "free", "equity"]:
                value = float(b["USDT"].get(key, 0) or 0)
                if value > 0:
                    return value
        if "total" in b and isinstance(b["total"], dict):
            value = float(b["total"].get("USDT", 0) or 0)
            if value > 0:
                return value
        if "free" in b and isinstance(b["free"], dict):
            value = float(b["free"].get("USDT", 0) or 0)
            if value > 0:
                return value
        info = b.get("info", {})
        if isinstance(info, dict):
            data = info.get("result", info.get("data", info))
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        for key in ["equity", "walletBalance", "totalAvailableBalance", "balance", "availableMargin"]:
                            value = float(item.get(key, 0) or 0)
                            if value > 0:
                                return value
            elif isinstance(data, dict):
                for key in ["equity", "walletBalance", "totalAvailableBalance", "balance", "availableMargin"]:
                    value = float(data.get(key, 0) or 0)
                    if value > 0:
                        return value
                balance_list = data.get("list", [])
                if isinstance(balance_list, list):
                    for account in balance_list:
                        if not isinstance(account, dict):
                            continue
                        for key in ["totalEquity", "totalWalletBalance", "totalAvailableBalance"]:
                            value = float(account.get(key, 0) or 0)
                            if value > 0:
                                return value
                        coin_list = account.get("coin", [])
                        if isinstance(coin_list, list):
                            for coin in coin_list:
                                if not isinstance(coin, dict):
                                    continue
                                if str(coin.get("coin", "")).upper() != "USDT":
                                    continue
                                for key in ["equity", "walletBalance", "availableToWithdraw", "availableToBorrow"]:
                                    value = float(coin.get(key, 0) or 0)
                                    if value > 0:
                                        return value
        return 0.0
    except Exception as e:
        print(f"[B] BAL:{e}")
        return 0.0


def parse_exchange_ts(value):
    try:
        if value is None:
            return 0.0
        numeric = float(value)
        if numeric <= 0:
            return 0.0
        if numeric > 1_000_000_000_000:
            return numeric / 1000.0
        if numeric > 1_000_000_000:
            return numeric
        return 0.0
    except Exception:
        return 0.0


def position_update_ts(position):
    candidates = [
        position.get("timestamp"),
    ]
    info = position.get("info") or {}
    if isinstance(info, dict):
        candidates.extend(
            [
                info.get("updatedTime"),
                info.get("updatedAt"),
                info.get("createdTime"),
                info.get("createdAt"),
            ]
        )
    for value in candidates:
        parsed = parse_exchange_ts(value)
        if parsed > 0:
            return parsed
    return 0.0


def get_positions():
    long_pos, short_pos = None, None
    try:
        for position in ex.fetch_positions([SYMBOL]):
            if position.get("symbol") != SYMBOL:
                continue
            qty = float(position.get("contracts") or 0)
            if qty <= 0:
                continue
            side = str(position.get("side", "")).lower()
            info = {
                "qty": qty,
                "entry": float(position.get("entryPrice") or 0),
                "updated_ts": position_update_ts(position),
            }
            if side == "long":
                long_pos = info
            elif side == "short":
                short_pos = info
    except Exception:
        pass
    return long_pos, short_pos


def fetch_position_qty(side):
    long_pos, short_pos = get_positions()
    snap = long_pos if side == "LONG" else short_pos
    return snap["qty"] if snap else 0.0


def position_idx(side):
    return 1 if side == "LONG" else 2


def limit_open(side, qty, price):
    try:
        order_side = "buy" if side == "LONG" else "sell"
        return ex.create_order(
            SYMBOL,
            "limit",
            order_side,
            qty,
            price,
            params={"positionIdx": position_idx(side), "postOnly": True},
        )
    except Exception as e:
        print(f"[B] LMT_O:{e}")
        return None


def limit_close(side, qty, price):
    try:
        order_side = "sell" if side == "LONG" else "buy"
        return ex.create_order(
            SYMBOL,
            "limit",
            order_side,
            qty,
            price,
            params={"positionIdx": position_idx(side), "reduceOnly": True},
        )
    except Exception as e:
        print(f"[B] LMT_C:{e}")
        return None


def mkt_close(side, qty):
    try:
        order_side = "sell" if side == "LONG" else "buy"
        return ex.create_order(
            SYMBOL,
            "market",
            order_side,
            qty,
            None,
            params={"positionIdx": position_idx(side), "reduceOnly": True},
        )
    except Exception as e:
        print(f"[B] MKT_C:{e}")
        return None


def wait_order_not_open(oid, attempts=8, delay=0.25):
    for _ in range(attempts):
        still_open = is_order_still_open(oid)
        if still_open is False:
            return True
        if still_open is None:
            return False
        time.sleep(delay)
    return False


def cancel_order(oid, verify=False):
    if not oid:
        return True
    try:
        ex.cancel_order(oid, SYMBOL)
    except Exception as e:
        if verify:
            still_open = is_order_still_open(oid)
            if still_open is False:
                return True
            print(f"[B] CANCEL_WAIT {oid} err={e}")
            return False
        return True
    if verify and not wait_order_not_open(oid):
        print(f"[B] CANCEL_WAIT {oid} still_open")
        return False
    return True


def cancel_all():
    try:
        for order in ex.fetch_open_orders(SYMBOL):
            try:
                ex.cancel_order(order["id"], SYMBOL)
            except Exception:
                pass
    except Exception:
        pass


def is_order_still_open(oid):
    try:
        for order in ex.fetch_open_orders(SYMBOL):
            if order.get("id") == oid:
                return True
        return False
    except Exception as e:
        print(f"[B] ORD_SYNC:{e}")
        return None


def order_reduce_only(order):
    info = order.get("info") or {}
    values = [order.get("reduceOnly")]
    if isinstance(info, dict):
        values.extend([info.get("reduceOnly"), info.get("reduce_only")])
    for value in values:
        if value is True or str(value).lower() == "true":
            return True
    return False


def order_position_idx_value(order):
    info = order.get("info") or {}
    for value in [order.get("positionIdx"), info.get("positionIdx") if isinstance(info, dict) else None]:
        try:
            parsed = int(value)
            if parsed in (1, 2):
                return parsed
        except Exception:
            continue
    return None


def fetch_open_entry_orders(side):
    want_side = "buy" if side == "LONG" else "sell"
    want_idx = position_idx(side)
    try:
        orders = []
        for order in ex.fetch_open_orders(SYMBOL):
            if str(order.get("side", "")).lower() != want_side:
                continue
            if order_reduce_only(order):
                continue
            order_idx = order_position_idx_value(order)
            if order_idx is not None and order_idx != want_idx:
                continue
            orders.append(order)
        return orders
    except Exception as e:
        print(f"[B] OPEN_ENTRY_SYNC {side}:{e}")
        return None


def fetch_open_reduce_orders(side):
    want_side = "sell" if side == "LONG" else "buy"
    want_idx = position_idx(side)
    try:
        orders = []
        for order in ex.fetch_open_orders(SYMBOL):
            if str(order.get("side", "")).lower() != want_side:
                continue
            order_idx = order_position_idx_value(order)
            if not order_reduce_only(order) and order_idx != want_idx:
                continue
            if order_idx is not None and order_idx != want_idx:
                continue
            orders.append(order)
        return orders
    except Exception as e:
        print(f"[B] OPEN_REDUCE_SYNC {side}:{e}")
        return None


def cancel_untracked_entry_orders(side, keep_oid=None):
    orders = fetch_open_entry_orders(side)
    if orders is None:
        return None
    stale = [order for order in orders if order.get("id") != keep_oid]
    if not stale:
        return 0
    failed = 0
    for order in stale:
        oid = order.get("id")
        if oid and not cancel_order(oid, verify=True):
            failed += 1
    print(f"[B] ORPHAN_ENTRY_CANCEL {side} count={len(stale)} failed={failed}")
    return len(stale)


def cancel_untracked_reduce_orders(side, keep_oids=None):
    keep = {oid for oid in (keep_oids or []) if oid}
    orders = fetch_open_reduce_orders(side)
    if orders is None:
        return None
    stale = [order for order in orders if order.get("id") not in keep]
    if not stale:
        return 0
    failed = 0
    for order in stale:
        oid = order.get("id")
        if oid and not cancel_order(oid, verify=True):
            failed += 1
    print(f"[B] ORPHAN_REDUCE_CANCEL {side} count={len(stale)} failed={failed}")
    return len(stale)


def check_fill(oid):
    try:
        order = ex.fetch_order(oid, SYMBOL)
        return (
            float(order.get("filled", 0) or 0),
            float(order.get("average") or order.get("price") or 0),
            order.get("status", ""),
            float(order.get("remaining", 0) or 0),
        )
    except Exception:
        return 0, 0, "error", None


def fetch_candles(n=200):
    try:
        return ex.fetch_ohlcv(SYMBOL, TF, limit=n)
    except Exception:
        return []


def fetch_candles_tf(tf, n=200):
    try:
        return ex.fetch_ohlcv(SYMBOL, tf, limit=n)
    except Exception as e:
        print(f"[B] OHLCV {tf}:{e}")
        return []


def calc_bb(candles, std_mult):
    closed = candles[:-1]
    if len(closed) < BB_LEN:
        return None
    closes = [c[4] for c in closed[-BB_LEN:]]
    sma = sum(closes) / BB_LEN
    variance = sum((c - sma) ** 2 for c in closes) / BB_LEN
    std = variance**0.5
    return {
        "sma": round(sma, 4),
        "upper": round(sma + std * std_mult, 4),
        "lower": round(sma - std * std_mult, 4),
        "width": round(2 * std * std_mult, 4),
    }


def candle_closed(candles):
    return candles[:-1] if candles else []


def profile_bin_size(price):
    return max(0.0001, round(price * PROFILE_BIN_PCT, 4))


def bin_key(price, bin_size):
    if bin_size <= 0:
        return 0
    return int(math.floor(price / bin_size))


def build_histogram(candles, bin_size):
    hist = {}
    for candle in candles:
        high = float(candle[2] or 0)
        low = float(candle[3] or 0)
        close = float(candle[4] or 0)
        volume = float(candle[5] or 0)
        if high <= 0 or low <= 0 or close <= 0 or volume <= 0:
            continue
        if high < low:
            high, low = low, high
        start = bin_key(low, bin_size)
        end = bin_key(high, bin_size)
        span = max(1, end - start + 1)
        if span > 50:
            mid = bin_key(close, bin_size)
            start = max(start, mid - 25)
            end = min(end, mid + 25)
            span = max(1, end - start + 1)
        share = volume / span
        for key in range(start, end + 1):
            hist[key] = hist.get(key, 0.0) + share
    return hist


def median(values):
    values = sorted(v for v in values if v > 0)
    if not values:
        return 0.0
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2


def extract_profile_nodes(name, candles, bin_size):
    hist = build_histogram(candles, bin_size)
    if len(hist) < 8:
        return [], None
    total_volume = sum(hist.values())
    med = median(hist.values())
    if total_volume <= 0 or med <= 0:
        return [], None
    sorted_bins = sorted(hist.items(), key=lambda item: item[1], reverse=True)
    poc_key, poc_vol = sorted_bins[0]
    top_count = max(3, int(len(sorted_bins) * PROFILE_TOP_NODE_RATIO))
    selected = {
        key
        for key, vol in sorted_bins[:top_count]
        if vol / total_volume >= PROFILE_MIN_NODE_SHARE or vol >= med * 2.0
    }
    if not selected:
        selected = {poc_key}
    zones = []
    keys = sorted(selected)
    group = []
    prev = None
    for key in keys:
        if prev is None or key <= prev + 1 + ZONE_MERGE_GAP_BINS:
            group.append(key)
        else:
            zones.append(make_profile_zone(name, group, hist, total_volume, med, bin_size, poc_key))
            group = [key]
        prev = key
    if group:
        zones.append(make_profile_zone(name, group, hist, total_volume, med, bin_size, poc_key))
    zones = [zone for zone in zones if zone]
    zones.sort(key=lambda zone: zone["strength"], reverse=True)
    poc = {
        "price": round((poc_key + 0.5) * bin_size, 4),
        "volume": poc_vol,
        "profile": name,
    }
    return zones[:PROFILE_MAX_ZONES], poc


def make_profile_zone(name, keys, hist, total_volume, med, bin_size, poc_key):
    if not keys:
        return None
    lower = min(keys) * bin_size
    upper = (max(keys) + 1) * bin_size
    volume = sum(hist.get(key, 0.0) for key in keys)
    if total_volume <= 0:
        return None
    rel = volume / max(med * len(keys), 1e-12)
    share = volume / total_volume
    center = (lower + upper) / 2
    return {
        "id": f"{name}:{min(keys)}:{max(keys)}",
        "lower": round(lower, 4),
        "upper": round(upper, 4),
        "center": round(center, 4),
        "volume": volume,
        "share": share,
        "rel": rel,
        "strength": share * 100 + rel,
        "profiles": [name],
        "poc_touch": min(keys) <= poc_key <= max(keys),
        "tier": 1,
    }


def profile_has_tf(profiles, tf):
    return tf in profiles or any(str(profile).endswith("_" + tf) for profile in profiles)


def profile_has_kind(profiles, kind):
    prefix = kind + "_"
    return any(str(profile).startswith(prefix) for profile in profiles)


def find_swing_points(candles, left=3, right=3):
    points = []
    if len(candles) < left + right + 1:
        return points
    for idx in range(left, len(candles) - right):
        window = candles[idx - left : idx + right + 1]
        high = float(candles[idx][2] or 0)
        low = float(candles[idx][3] or 0)
        if high > 0 and high >= max(float(row[2] or 0) for row in window):
            points.append({"idx": idx, "price": high, "kind": "high"})
        if low > 0 and low <= min(float(row[3] or 0) for row in window):
            points.append({"idx": idx, "price": low, "kind": "low"})
    return points


def structure_zone_width(price, bin_size, pct):
    return max(bin_size * 3, price * pct)


def make_structure_zone(name, label, center, width, strength, touches, profiles):
    lower = max(0.0, center - width)
    upper = center + width
    return {
        "id": f"{label}_{name}:{lower:.4f}:{upper:.4f}",
        "lower": round(lower, 4),
        "upper": round(upper, 4),
        "center": round(center, 4),
        "volume": strength,
        "share": 0.0,
        "rel": float(touches),
        "strength": strength,
        "profiles": profiles,
        "poc_touch": False,
        "tier": 1,
    }


def extract_horizontal_structure_zones(name, candles, price, bin_size):
    closed = candle_closed(candles)
    if len(closed) < 40:
        return []
    points = find_swing_points(closed, left=3, right=3)[-120:]
    if not points:
        return []

    tolerance = structure_zone_width(price, bin_size, HORIZONTAL_ZONE_PCT)
    groups = []
    for point in sorted(points, key=lambda item: item["price"]):
        placed = False
        for group in groups:
            if abs(point["price"] - group["center"]) <= tolerance:
                group["points"].append(point)
                group["center"] = sum(item["price"] for item in group["points"]) / len(group["points"])
                placed = True
                break
        if not placed:
            groups.append({"center": point["price"], "points": [point]})

    zones = []
    min_touches = HORIZONTAL_MIN_TOUCHES + (1 if name == "M5" else 0)
    tf_weight = PROFILE_TF_WEIGHTS.get(name, 1.0)
    for group in groups:
        touches = len(group["points"])
        if touches < min_touches:
            continue
        prices = [point["price"] for point in group["points"]]
        latest_idx = max(point["idx"] for point in group["points"])
        recency = 1 + latest_idx / max(len(closed), 1)
        width = max(tolerance, (max(prices) - min(prices)) / 2 + bin_size)
        strength = (touches * 2.0 + recency) * tf_weight
        zones.append(
            make_structure_zone(
                name,
                "SR",
                sum(prices) / touches,
                width,
                strength,
                touches,
                [f"SR_{name}"],
            )
        )
    zones.sort(key=lambda zone: zone["strength"], reverse=True)
    return zones[:PROFILE_MAX_ZONES]


def extract_trendline_structure_zones(name, candles, price, bin_size):
    closed = candle_closed(candles)
    if len(closed) < 60:
        return []
    points = find_swing_points(closed, left=3, right=3)
    current_idx = len(closed) - 1
    tolerance = structure_zone_width(price, bin_size, TRENDLINE_ZONE_PCT)
    min_touches = 2 if name in ("H1", "H4") else TRENDLINE_MIN_TOUCHES
    tf_weight = PROFILE_TF_WEIGHTS.get(name, 1.0)
    zones = []

    for kind in ("low", "high"):
        pivots = [point for point in points if point["kind"] == kind][-16:]
        candidates = []
        for left_idx in range(len(pivots)):
            for right_idx in range(left_idx + 1, len(pivots)):
                p1 = pivots[left_idx]
                p2 = pivots[right_idx]
                span = p2["idx"] - p1["idx"]
                if span < TRENDLINE_MIN_SEP_BARS:
                    continue
                slope = (p2["price"] - p1["price"]) / span
                projected = p1["price"] + slope * (current_idx - p1["idx"])
                if projected <= 0 or abs(projected - price) / price > TRENDLINE_MAX_DIST_PCT:
                    continue

                touches = 0
                total_error = 0.0
                for pivot in pivots[left_idx:]:
                    line_px = p1["price"] + slope * (pivot["idx"] - p1["idx"])
                    error = abs(pivot["price"] - line_px)
                    if error <= tolerance:
                        touches += 1
                        total_error += error
                if touches < min_touches:
                    continue
                avg_error = total_error / max(touches, 1)
                strength = (touches * 3.0 + (1 - min(avg_error / tolerance, 1.0))) * tf_weight
                candidates.append((strength, touches, projected))
        candidates.sort(key=lambda item: item[0], reverse=True)
        for strength, touches, projected in candidates[:2]:
            zones.append(
                make_structure_zone(
                    name,
                    "TL",
                    projected,
                    tolerance,
                    strength,
                    touches,
                    [f"TL_{name}"],
                )
            )
    zones.sort(key=lambda zone: zone["strength"], reverse=True)
    return zones[:4]


def zones_overlap(a, b, price):
    gap = price * ZONE_TOUCH_PCT
    return not (a["upper"] + gap < b["lower"] or b["upper"] + gap < a["lower"])


def merge_profile_zones(zones, price):
    merged = []
    for zone in sorted(zones, key=lambda item: item["center"]):
        found = None
        for existing in merged:
            if zones_overlap(existing, zone, price):
                found = existing
                break
        if found is None:
            merged.append(dict(zone))
            continue
        total_volume = found["volume"] + zone["volume"]
        found["lower"] = round(min(found["lower"], zone["lower"]), 4)
        found["upper"] = round(max(found["upper"], zone["upper"]), 4)
        found["center"] = round(
            ((found["center"] * found["volume"]) + (zone["center"] * zone["volume"])) / total_volume,
            4,
        )
        found["volume"] = total_volume
        found["share"] = max(found["share"], zone["share"])
        found["rel"] = max(found["rel"], zone["rel"])
        found["strength"] += zone["strength"]
        found["profiles"] = sorted(set(found["profiles"] + zone["profiles"]))
        found["poc_touch"] = found["poc_touch"] or zone["poc_touch"]
        found["id"] = "+".join(found["profiles"]) + f":{found['lower']:.4f}:{found['upper']:.4f}"
    for zone in merged:
        zone["tier"] = profile_zone_tier(zone)
    merged.sort(key=lambda item: item["strength"], reverse=True)
    return merged[:PROFILE_MAX_ZONES]


def profile_zone_tier(zone):
    profiles = set(zone.get("profiles", []))
    overlap = len(profiles)
    share = float(zone.get("share", 0) or 0)
    rel = float(zone.get("rel", 0) or 0)
    poc = bool(zone.get("poc_touch"))
    has_m5 = profile_has_tf(profiles, "M5")
    has_m15 = profile_has_tf(profiles, "M15")
    has_h1 = profile_has_tf(profiles, "H1")
    has_h4 = profile_has_tf(profiles, "H4")
    has_sr = profile_has_kind(profiles, "SR")
    has_tl = profile_has_kind(profiles, "TL")
    has_structure = has_sr or has_tl

    if has_structure and has_h4 and (has_h1 or has_m15) and rel >= 2.0:
        return 5
    if has_h4 and has_h1 and (overlap >= 3 or poc or rel >= 2.0 or share >= 0.010):
        return 5
    if has_structure and (has_h1 or has_h4) and (has_m15 or has_m5 or rel >= 3.0):
        return 4
    if (has_h4 and (has_h1 or has_m15 or poc)) or (has_h1 and has_m15 and (poc or rel >= 1.7 or share >= 0.008)):
        return 4
    if has_structure and (has_m15 or has_h1 or has_h4):
        return 3
    if has_h1 or (has_m15 and has_m5) or (has_m15 and (poc or rel >= 1.8 or share >= 0.010)):
        return 3
    if has_structure and has_m5 and rel >= 3.0:
        return 2
    if has_m15 or has_h4 or overlap >= 2:
        return 2
    if has_m5 and (poc and rel >= 2.5 and share >= 0.015):
        return 2
    return 1


def build_volume_profile(c5, price, c15=None, c1h=None, c4h=None):
    bin_size = profile_bin_size(price)
    all_zones = []
    pocs = {}
    sources = [
        ("M5", c5, PROFILE_WINDOWS["M5"]),
        ("M15", c15 or [], PROFILE_WINDOWS["M15"]),
        ("H1", c1h or [], PROFILE_WINDOWS["H1"]),
        ("H4", c4h or [], PROFILE_WINDOWS["H4"]),
    ]
    for name, candles, bars in sources:
        closed = candle_closed(candles)
        if len(closed) < 30:
            continue
        window = closed[-bars:] if len(closed) >= bars else closed
        zones, poc = extract_profile_nodes(name, window, bin_size)
        weight = PROFILE_TF_WEIGHTS.get(name, 1.0)
        for zone in zones:
            zone["strength"] *= weight
            zone["tf_weight"] = weight
        all_zones.extend(zones)
        structure_source = candles[-(bars + 1) :] if len(candles) > bars + 1 else candles
        all_zones.extend(extract_horizontal_structure_zones(name, structure_source, price, bin_size))
        all_zones.extend(extract_trendline_structure_zones(name, structure_source, price, bin_size))
        if poc:
            poc["weight"] = weight
            pocs[name] = poc
    return {"zones": merge_profile_zones(all_zones, price), "pocs": pocs, "bin_size": bin_size}


def ichimoku_cloud_at(candles, idx):
    if idx < 51:
        return None
    rows = candles
    high9 = max(row[2] for row in rows[idx - 8 : idx + 1])
    low9 = min(row[3] for row in rows[idx - 8 : idx + 1])
    high26 = max(row[2] for row in rows[idx - 25 : idx + 1])
    low26 = min(row[3] for row in rows[idx - 25 : idx + 1])
    high52 = max(row[2] for row in rows[idx - 51 : idx + 1])
    low52 = min(row[3] for row in rows[idx - 51 : idx + 1])
    tenkan = (high9 + low9) / 2
    kijun = (high26 + low26) / 2
    span_a = (tenkan + kijun) / 2
    span_b = (high52 + low52) / 2
    return min(span_a, span_b), max(span_a, span_b)


def kumo_distance(close, cloud):
    if not cloud or close <= 0:
        return 0.0
    low, high = cloud
    if close > high:
        return (close - high) / close
    if close < low:
        return -(low - close) / close
    return 0.0


def classify_kumo_regime(c15, c1h, price, c4h=None):
    closed15 = candle_closed(c15)
    closed1h = candle_closed(c1h)
    closed4h = candle_closed(c4h) if c4h else []
    if len(closed15) < 80:
        return {"name": "UNKNOWN", "dist15": 0.0, "recent15": 0.0, "dist1h": 0.0, "dist4h": 0.0}
    idx15 = len(closed15) - 1
    cloud15 = ichimoku_cloud_at(closed15, idx15)
    dist15 = kumo_distance(price, cloud15)
    recent = 0.0
    start = max(51, idx15 - KUMO_MEMORY_BARS + 1)
    for idx in range(start, idx15 + 1):
        cloud = ichimoku_cloud_at(closed15, idx)
        dist = kumo_distance(closed15[idx][4], cloud)
        if abs(dist) > abs(recent):
            recent = dist
    dist1h = 0.0
    if len(closed1h) >= 80:
        idx1h = len(closed1h) - 1
        dist1h = kumo_distance(price, ichimoku_cloud_at(closed1h, idx1h))
    dist4h = 0.0
    if len(closed4h) >= 80:
        idx4h = len(closed4h) - 1
        dist4h = kumo_distance(price, ichimoku_cloud_at(closed4h, idx4h))
    if dist15 >= KUMO_FAR_NOW_PCT or dist1h >= KUMO_FAR_NOW_PCT or dist4h >= KUMO_FAR_NOW_PCT:
        name = "STRONG_UP"
    elif dist15 <= -KUMO_FAR_NOW_PCT or dist1h <= -KUMO_FAR_NOW_PCT or dist4h <= -KUMO_FAR_NOW_PCT:
        name = "STRONG_DOWN"
    elif recent >= KUMO_FAR_RECENT_PCT:
        name = "RECENT_UP_EXCURSION"
    elif recent <= -KUMO_FAR_RECENT_PCT:
        name = "RECENT_DOWN_EXCURSION"
    else:
        name = "RANGE"
    return {"name": name, "dist15": dist15, "recent15": recent, "dist1h": dist1h, "dist4h": dist4h}


def regime_against_side(side, regime):
    name = regime.get("name", "UNKNOWN")
    if side == "LONG":
        return name in ("STRONG_DOWN", "RECENT_DOWN_EXCURSION")
    return name in ("STRONG_UP", "RECENT_UP_EXCURSION")


def zone_allowed_target_ratio(zone, side, regime):
    tier = int(zone.get("tier", 0) or 0)
    allowed = TIER_TARGET_RATIOS.get(tier, 0.0)
    if regime.get("name", "UNKNOWN") == "UNKNOWN":
        return min(allowed, REGIME_PROBE_RATIO)
    if regime_against_side(side, regime):
        allowed = min(allowed, REGIME_PROBE_RATIO)
    return allowed


def select_profile_zone(side, profile, price, has_position):
    min_tier = DCA_MIN_ZONE_TIER if has_position else ENTRY_MIN_ZONE_TIER
    if side == "LONG":
        return nearest_support_zone(profile, price, min_tier, MIN_ENTRY_ZONE_DIST_PCT)
    return nearest_resistance_zone(profile, price, min_tier, MIN_ENTRY_ZONE_DIST_PCT)


def direct_profile_zones(profile, min_tier=2):
    return [zone for zone in profile.get("zones", []) if int(zone.get("tier", 0) or 0) >= min_tier and is_direct_zone(zone)]


def broad_edge_zone(zone, role, price):
    tier = int(zone.get("tier", 0) or 0)
    if tier < BROAD_EDGE_ZONE_MIN_TIER or is_direct_zone(zone):
        return None
    if zone_width_pct(zone) > BROAD_EDGE_ZONE_MAX_WIDTH_PCT:
        return None

    lower = float(zone.get("lower", 0) or 0)
    upper = float(zone.get("upper", 0) or 0)
    if lower <= 0 or upper <= 0 or price <= 0:
        return None

    if role == "support":
        if lower <= price <= upper:
            edge_px = lower
        elif upper < price:
            edge_px = upper
        else:
            return None
        pad = max(price * BROAD_EDGE_ZONE_PAD_PCT, profile_bin_size(price) * 2)
        edge_lower = max(0.0, edge_px - pad)
        edge_upper = edge_px
    else:
        if lower <= price <= upper:
            edge_px = upper
        elif lower > price:
            edge_px = lower
        else:
            return None
        pad = max(price * BROAD_EDGE_ZONE_PAD_PCT, profile_bin_size(price) * 2)
        edge_lower = edge_px
        edge_upper = edge_px + pad

    out = dict(zone)
    out["id"] = f"{zone.get('id', 'zone')}:edge:{role}:{edge_px:.4f}"
    out["lower"] = round(edge_lower, 4)
    out["upper"] = round(edge_upper, 4)
    out["center"] = round((edge_lower + edge_upper) / 2, 4)
    out["profiles"] = list(zone.get("profiles", [])) + [f"EDGE_{role.upper()}"]
    return out


def support_zone_candidates(profile, price, min_tier=2):
    candidates = direct_profile_zones(profile, min_tier)
    for zone in profile.get("zones", []):
        edge = broad_edge_zone(zone, "support", price)
        if edge and int(edge.get("tier", 0) or 0) >= min_tier:
            candidates.append(edge)
    return candidates


def resistance_zone_candidates(profile, price, min_tier=2):
    candidates = direct_profile_zones(profile, min_tier)
    for zone in profile.get("zones", []):
        edge = broad_edge_zone(zone, "resistance", price)
        if edge and int(edge.get("tier", 0) or 0) >= min_tier:
            candidates.append(edge)
    return candidates


def nearest_support_zone(profile, price, min_tier=2, min_dist_pct=0.0):
    candidates = [
        zone
        for zone in support_zone_candidates(profile, price, min_tier)
        if zone["center"] < price and zone["upper"] <= price * (1 - min_dist_pct)
    ]
    candidates.sort(key=lambda zone: (price - zone["upper"], -zone["tier"], -zone["strength"]))
    if not candidates:
        return None
    return candidates[0]


def nearest_resistance_zone(profile, price, min_tier=2, min_dist_pct=0.0):
    candidates = [
        zone
        for zone in resistance_zone_candidates(profile, price, min_tier)
        if zone["center"] > price and zone["lower"] >= price * (1 + min_dist_pct)
    ]
    candidates.sort(key=lambda zone: (zone["lower"] - price, -zone["tier"], -zone["strength"]))
    if not candidates:
        return None
    return candidates[0]


def take_profit_zone(side, profile, price):
    if side == "LONG":
        return nearest_resistance_zone(profile, price, 2, 0.0)
    return nearest_support_zone(profile, price, 2, 0.0)


def zone_role_for_entry(side):
    return "support" if side == "LONG" else "resistance"


def zone_role_for_tp(side):
    return "resistance" if side == "LONG" else "support"


def entry_price_from_zone(side, zone, price, bin_size):
    tick_gap = max(0.0001, bin_size)
    if side == "LONG":
        target = min(float(zone["upper"]), price - tick_gap)
    else:
        target = max(float(zone["lower"]), price + tick_gap)
    return round(max(target, 0.0), 4)


def nearest_opposite_zone(side, profile, price):
    return take_profit_zone(side, profile, price)


def structure_tp_ratio(side, bal, ref_price, opposite_zone):
    stage = position_stage_from_used(side, bal, ref_price)
    stage_ratio = STAGE_TP_RATIOS.get(stage, 0.25)
    if not opposite_zone:
        return stage_ratio, stage, 0
    tier = int(opposite_zone.get("tier", 0) or 0)
    tier_ratio = STRUCTURE_TP_RATIOS.get(tier, 0.25)
    return max(stage_ratio, tier_ratio), stage, tier


def profile_poc_target(side, profile, current_price):
    pocs = profile.get("pocs", {})
    ordered = [pocs.get("H4"), pocs.get("H1"), pocs.get("M15"), pocs.get("M5")]
    for poc in ordered:
        if not poc:
            continue
        px = round(float(poc.get("price", 0) or 0), 4)
        if is_resting_tp_price(side, px, current_price):
            return px
    return 0.0


def active_entry_order_should_stay(side, price):
    slot = sk(side) + "_entry"
    oid = ORD.get(slot)
    order_px = float(ORD.get(slot + "_px", 0) or 0)
    tier = int(ORD.get(slot + "_zone_tier", 0) or 0)
    if not oid or order_px <= 0 or tier <= 0 or price <= 0:
        return False
    return order_px < price if side == "LONG" else order_px > price


def default_side():
    return {
        "layers": [],
        "entry_count": 0,
        "last_entry_bar": 99,
        "last_entry_ts": 0,
        "last_fill_entry": 0.0,
        "dca_ref_avg": 0.0,
        "basis_cap_margin": 0.0,
        "basis_balance": 0.0,
        "last_avg_tp_bar": 99,
        "last_sigma2_tp_bar": 99,
        "last_sigma3_tp_bar": 99,
        "last_entry_zone_id": "",
        "last_entry_zone_lower": 0.0,
        "last_entry_zone_upper": 0.0,
        "last_entry_zone_center": 0.0,
        "last_entry_zone_tier": 0,
        "zone_restore_warned": False,
        "last_reaction_action": "",
        "invalid_bars": 0,
        "last_invalid_check_candle": 0,
        "ztp_done": False,
        "used_tp_zones": [],
    }


S = {
    "long": default_side(),
    "short": default_side(),
    "long_last_close_ts": 0,
    "short_last_close_ts": 0,
    "nt": 0,
    "nw": 0,
    "nl": 0,
    "day": "",
    "real_pnl": 0.0,
}
SC = {"sig_seen": 0, "sent": 0, "filled": 0}
SKIP = {}
ORD = {}
for slot in ORDER_SLOTS:
    ORD[slot] = None
    ORD[slot + "_px"] = 0.0
    ORD[slot + "_qty"] = 0.0
    ORD[slot + "_fill"] = 0.0


def gs(side):
    return S["long"] if side == "LONG" else S["short"]


def sk(side):
    return "long" if side == "LONG" else "short"


def get_layers(side):
    return gs(side)["layers"]


def total_qty(side):
    return sum(layer["qty"] for layer in get_layers(side))


def avg_entry(side):
    layers = get_layers(side)
    total = sum(layer["qty"] for layer in layers)
    return sum(layer["qty"] * layer["entry"] for layer in layers) / total if total > 0 else 0


def last_entry(side):
    layers = get_layers(side)
    return layers[-1]["entry"] if layers else 0


def last_fill_entry(side):
    state = gs(side)
    anchor = float(state.get("last_fill_entry", 0) or 0)
    if anchor > 0:
        return anchor
    return last_entry(side)


def dca_ref_avg(side):
    state = gs(side)
    ref_avg = float(state.get("dca_ref_avg", 0) or 0)
    if ref_avg > 0:
        return ref_avg
    return avg_entry(side)


def reset_slot(slot, do_cancel=False, verify=False):
    if do_cancel and ORD.get(slot):
        if not cancel_order(ORD[slot], verify=verify):
            return False
    ORD[slot] = None
    ORD[slot + "_px"] = 0.0
    ORD[slot + "_qty"] = 0.0
    ORD[slot + "_fill"] = 0.0
    ORD.pop(slot + "_zone_id", None)
    ORD.pop(slot + "_zone_lower", None)
    ORD.pop(slot + "_zone_upper", None)
    ORD.pop(slot + "_zone_center", None)
    ORD.pop(slot + "_zone_tier", None)
    return True


def set_slot(slot, oid, price, qty):
    ORD[slot] = oid
    ORD[slot + "_px"] = round(float(price), 4)
    ORD[slot + "_qty"] = trunc(float(qty), 3)
    ORD[slot + "_fill"] = 0.0


def set_entry_slot_zone(slot, zone):
    ORD[slot + "_zone_id"] = zone.get("id", "")
    ORD[slot + "_zone_lower"] = float(zone.get("lower", 0) or 0)
    ORD[slot + "_zone_upper"] = float(zone.get("upper", 0) or 0)
    ORD[slot + "_zone_center"] = float(zone.get("center", 0) or 0)
    ORD[slot + "_zone_tier"] = int(zone.get("tier", 0) or 0)


def entry_slot_zone_snapshot(slot):
    lower = float(ORD.get(slot + "_zone_lower", 0) or 0)
    upper = float(ORD.get(slot + "_zone_upper", 0) or 0)
    center = float(ORD.get(slot + "_zone_center", 0) or 0)
    if lower <= 0 or upper <= 0:
        return None
    if center <= 0:
        center = (lower + upper) / 2
    return {
        "id": ORD.get(slot + "_zone_id", ""),
        "lower": lower,
        "upper": upper,
        "center": center,
        "tier": int(ORD.get(slot + "_zone_tier", 0) or 0),
    }


def normalized_zone_snapshot(zone):
    if not zone:
        return None
    lower = float(zone.get("lower", 0) or 0)
    upper = float(zone.get("upper", 0) or 0)
    center = float(zone.get("center", 0) or 0)
    if lower <= 0 or upper <= 0:
        return None
    if center <= 0:
        center = (lower + upper) / 2
    return {
        "id": zone.get("id", ""),
        "lower": round(lower, 4),
        "upper": round(upper, 4),
        "center": round(center, 4),
        "tier": int(zone.get("tier", 0) or 0),
    }


def set_tp_slot_zone(slot, zone):
    set_entry_slot_zone(slot, zone)


def tp_slot_zone_snapshot(slot):
    return entry_slot_zone_snapshot(slot)


def zones_same_area(a, b, price=0.0):
    a = normalized_zone_snapshot(a)
    b = normalized_zone_snapshot(b)
    if not a or not b:
        return False
    ref = price if price and price > 0 else max(a["center"], b["center"], 0.0)
    tolerance = max(profile_bin_size(ref) * 4, ref * 0.0015 if ref > 0 else 0.0005)
    overlap = min(a["upper"], b["upper"]) - max(a["lower"], b["lower"])
    if overlap >= -tolerance:
        return True
    center_dist = abs(a["center"] - b["center"])
    width = max(a["upper"] - a["lower"], b["upper"] - b["lower"], tolerance)
    return center_dist <= width + tolerance


def tp_zone_already_used(side, zone, price=0.0):
    for used in gs(side).get("used_tp_zones", []) or []:
        if zones_same_area(used, zone, price):
            return True
    return False


def mark_tp_zone_used(side, zone):
    snap = normalized_zone_snapshot(zone)
    if not snap:
        return False
    state = gs(side)
    used = [item for item in (state.get("used_tp_zones", []) or []) if normalized_zone_snapshot(item)]
    for item in used:
        if zones_same_area(item, snap, snap["center"]):
            state["used_tp_zones"] = used[-12:]
            return False
    used.append(snap)
    state["used_tp_zones"] = used[-12:]
    print(f"[B] ZONE_TP_DONE {side} T{snap['tier']}[{snap['lower']:,.4f}-{snap['upper']:,.4f}]")
    return True


def apply_entry_zone_state(side, zone):
    if not zone:
        return False
    lower = float(zone.get("lower", 0) or 0)
    upper = float(zone.get("upper", 0) or 0)
    center = float(zone.get("center", 0) or 0)
    if lower <= 0 or upper <= 0:
        return False
    if center <= 0:
        center = (lower + upper) / 2
    state = gs(side)
    state["last_entry_zone_id"] = zone.get("id", "")
    state["last_entry_zone_lower"] = round(lower, 4)
    state["last_entry_zone_upper"] = round(upper, 4)
    state["last_entry_zone_center"] = round(center, 4)
    state["last_entry_zone_tier"] = int(zone.get("tier", 0) or 0)
    state["zone_restore_warned"] = False
    return True


def clear_entry_zone_state(side):
    state = gs(side)
    state["last_entry_zone_id"] = ""
    state["last_entry_zone_lower"] = 0.0
    state["last_entry_zone_upper"] = 0.0
    state["last_entry_zone_center"] = 0.0
    state["last_entry_zone_tier"] = 0


def zone_distance_pct_to_price(zone, price):
    if not zone or price <= 0:
        return float("inf")
    lower = float(zone.get("lower", 0) or 0)
    upper = float(zone.get("upper", 0) or 0)
    if lower <= 0 or upper <= 0:
        return float("inf")
    if lower <= price <= upper:
        return 0.0
    if price < lower:
        return (lower - price) / price
    return (price - upper) / price


def zone_width_pct(zone):
    if not zone:
        return float("inf")
    lower = float(zone.get("lower", 0) or 0)
    upper = float(zone.get("upper", 0) or 0)
    if lower <= 0 or upper <= 0:
        return float("inf")
    center = (lower + upper) / 2
    return (upper - lower) / center if center > 0 else float("inf")


def is_direct_zone(zone):
    return zone_width_pct(zone) <= MAX_DIRECT_ZONE_WIDTH_PCT


def entry_price_distance_pct(side, target_px, price):
    if target_px <= 0 or price <= 0:
        return float("inf")
    if side == "LONG":
        return max(0.0, (price - target_px) / price)
    return max(0.0, (target_px - price) / price)


def dca_max_zone_dist_pct(side):
    next_dca = min(MAX_DCA_COUNT, max(1, dca_count(side) + 1))
    return DCA_MAX_ZONE_DIST_PCTS.get(next_dca, DCA_MAX_ZONE_DIST_PCTS[max(DCA_MAX_ZONE_DIST_PCTS)])


def normalize_last_fill_anchor(side, reason=""):
    state = gs(side)
    total = total_qty(side)
    entry = avg_entry(side)
    if total < SYMBOL_MIN_QTY or entry <= 0:
        state["last_fill_entry"] = 0.0
        return False
    anchor = float(state.get("last_fill_entry", 0) or 0)
    layers = get_layers(side)
    layer_entry = last_entry(side) or entry
    expected = layer_entry if len(layers) > 1 else entry
    if anchor <= 0 or abs(anchor - expected) / expected > STALE_LAST_FILL_MAX_DEV_PCT:
        state["last_fill_entry"] = round(expected, 4)
        print(
            f"[B] ANCHOR_REBASE {side} {anchor:.4f}->{expected:.4f} "
            f"avg=${entry:,.4f} reason={reason}"
        )
        return True
    return False


def find_zone_near_anchor(profile, anchor, min_tier=2):
    if anchor <= 0 or not profile:
        return None
    candidates = []
    for zone in profile.get("zones", []):
        if int(zone.get("tier", 0) or 0) < min_tier:
            continue
        dist = zone_distance_pct_to_price(zone, anchor)
        if dist <= ZONE_RESTORE_MAX_DEV_PCT:
            candidates.append((dist, -int(zone.get("tier", 0) or 0), -float(zone.get("strength", 0) or 0), zone))
    candidates.sort(key=lambda item: item[:3])
    return candidates[0][3] if candidates else None


def ensure_entry_zone_state(side, profile, reason=""):
    if total_qty(side) < SYMBOL_MIN_QTY:
        clear_entry_zone_state(side)
        return False
    state = gs(side)
    lower = float(state.get("last_entry_zone_lower", 0) or 0)
    upper = float(state.get("last_entry_zone_upper", 0) or 0)
    if lower > 0 and upper > 0:
        return False
    anchor = last_fill_entry(side) or avg_entry(side)
    zone = find_zone_near_anchor(profile, anchor, min_tier=2)
    if zone and apply_entry_zone_state(side, zone):
        state["last_reaction_action"] = ""
        state["invalid_bars"] = 0
        state["last_invalid_check_candle"] = 0
        print(
            f"[B] ZONE_RESTORE {side} T{zone.get('tier', 0)} "
            f"[{zone['lower']:,.4f}-{zone['upper']:,.4f}] anchor=${anchor:,.4f} reason={reason}"
        )
        return True
    if not state.get("zone_restore_warned", False):
        print(f"[B] ZONE_MISSING {side} anchor=${anchor:,.4f} reason={reason}")
        state["zone_restore_warned"] = True
    return False


def slot_matches(slot, price, qty):
    if not ORD.get(slot):
        return False
    return abs((ORD.get(slot + "_px", 0) or 0) - price) < 0.0001 and abs((ORD.get(slot + "_qty", 0) or 0) - qty) < QTY_TOL


def cancel_tp(side):
    side_key = sk(side)
    reset_slot(side_key + "_tpavg", do_cancel=True)
    reset_slot(side_key + "_tp2", do_cancel=True)
    reset_slot(side_key + "_tp3", do_cancel=True)


def save():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as handle:
            json.dump(S, handle, indent=2)
    except Exception:
        pass


def load():
    global S
    try:
        load_path = STATE_FILE
        if not os.path.exists(load_path):
            for fallback in STATE_FILE_FALLBACKS:
                if os.path.exists(fallback):
                    load_path = fallback
                    print(f"[B] MIGRATE_STATE from {fallback}")
                    break
        if os.path.exists(load_path):
            with open(load_path, "r", encoding="utf-8") as handle:
                loaded = json.load(handle)
            for key, value in loaded.items():
                if key in S:
                    S[key] = value
            for side in ["LONG", "SHORT"]:
                state = gs(side)
                state.setdefault("entry_count", 0)
                state.setdefault("last_entry_bar", 99)
                state.setdefault("last_entry_ts", 0)
                state.setdefault("last_fill_entry", 0.0)
                state.setdefault("dca_ref_avg", 0.0)
                state.setdefault("basis_cap_margin", 0.0)
                state.setdefault("basis_balance", 0.0)
                state.setdefault("last_avg_tp_bar", 99)
                state.setdefault("last_sigma2_tp_bar", 99)
                state.setdefault("last_sigma3_tp_bar", 99)
                state.setdefault("last_entry_zone_id", "")
                state.setdefault("last_entry_zone_lower", 0.0)
                state.setdefault("last_entry_zone_upper", 0.0)
                state.setdefault("last_entry_zone_center", 0.0)
                state.setdefault("last_entry_zone_tier", 0)
                state.setdefault("zone_restore_warned", False)
                state.setdefault("last_reaction_action", "")
                state.setdefault("invalid_bars", 0)
                state.setdefault("last_invalid_check_candle", 0)
                state.setdefault("ztp_done", False)
                state.setdefault("used_tp_zones", [])
                state["entry_count"] = min(max(0, int(state.get("entry_count", 0) or 0)), len(ENTRY_TARGET_RATIOS))
                for layer in state.get("layers", []):
                    layer.setdefault("src", None)
                normalize_last_fill_anchor(side, "load")
                print(f"[B] LOAD {side} qty={total_qty(side):.3f}")
    except Exception:
        pass


def rec(act, side, qty, price, pnl_pct, detail=""):
    S["nt"] += 1
    if pnl_pct > 0:
        S["nw"] += 1
    elif pnl_pct < 0:
        S["nl"] += 1
    S["real_pnl"] += pnl_pct
    win_rate = S["nw"] / S["nt"] * 100 if S["nt"] > 0 else 0
    print(f"[B] #{S['nt']} {act} {side} {qty:.3f}@${price:,.4f} {pnl_pct*100:+.2f}% WR{win_rate:.0f}% {detail}")
    save()


def do_skip(reason):
    SKIP[reason] = SKIP.get(reason, 0) + 1


def side_cap_margin(bal):
    return bal * SIDE_MARGIN_CAP


def basis_side_cap_margin(side, bal=None):
    basis = float(gs(side).get("basis_cap_margin", 0) or 0)
    if basis > 0:
        return basis
    if bal is None:
        bal = get_bal()
    return side_cap_margin(bal) if bal > 0 else 0.0


def refresh_balance_basis(side, bal, ref_price, force=False):
    state = gs(side)
    if bal <= 0:
        return False
    old_balance = float(state.get("basis_balance", 0) or 0)
    if old_balance <= 0:
        state["basis_balance"] = round(bal, 8)
        state["basis_cap_margin"] = round(side_cap_margin(bal), 8)
        if total_qty(side) > 0 and ref_price > 0:
            state["entry_count"] = infer_entry_count_from_position(side, total_qty(side), ref_price)
            if state["entry_count"] <= 1:
                state["dca_ref_avg"] = round(avg_entry(side), 4)
        return True
    changed = abs(bal - old_balance) / old_balance if old_balance > 0 else 0.0
    if not force and changed < BALANCE_REBASE_THRESHOLD:
        return False

    old_cap = float(state.get("basis_cap_margin", 0) or side_cap_margin(old_balance))
    state["basis_balance"] = round(bal, 8)
    state["basis_cap_margin"] = round(side_cap_margin(bal), 8)
    if total_qty(side) > 0 and ref_price > 0:
        state["entry_count"] = infer_entry_count_from_position(side, total_qty(side), ref_price)
        if state["entry_count"] <= 1:
            # A deposit can shrink the live position below the old DCA ladder.
            # Treat it as a fresh seed, not as a 4x+ DCA rescue candidate.
            state["dca_ref_avg"] = round(avg_entry(side), 4)
    print(
        f"[B] BAL_REBASE {side} bal ${old_balance:.2f}->${bal:.2f} "
        f"cap ${old_cap:.2f}->${state['basis_cap_margin']:.2f} "
        f"dca={dca_count(side)}/{MAX_DCA_COUNT}"
    )
    return True


def refresh_all_balance_basis(bal, ref_price):
    changed = False
    for side in ["LONG", "SHORT"]:
        changed = refresh_balance_basis(side, bal, ref_price) or changed
    if changed:
        S["_entry_refresh"] = True
        save()
    return changed


def used_margin(side, ref_price):
    qty = total_qty(side)
    return qty * ref_price / LEVERAGE if qty > 0 and ref_price > 0 else 0.0


def used_ratio(side, bal, ref_price):
    cap = side_cap_margin(bal)
    if cap <= 0:
        return 0.0
    return used_margin(side, ref_price) / cap


def entry_count(side):
    return int(gs(side).get("entry_count", 0) or 0)


def dca_count(side):
    return max(0, entry_count(side) - 1)


def seed_sized(side, bal, ref_price):
    if total_qty(side) <= 0:
        return False
    seed_ratio = ENTRY_TARGET_RATIOS[0]
    return used_ratio(side, bal, ref_price) + 1e-9 >= seed_ratio * SEED_ACCEPT_RATIO


def min_effective_topup_ratio(bal, ref_price):
    cap = side_cap_margin(bal)
    if cap <= 0 or ref_price <= 0:
        return float("inf")
    min_notional = max(MIN_VAL, SYMBOL_MIN_QTY * ref_price)
    return min_notional / (cap * LEVERAGE)


def next_target_ratio(side, bal, ref_price):
    ratio = used_ratio(side, bal, ref_price)
    total = total_qty(side)
    seed_ratio = ENTRY_TARGET_RATIOS[0]
    if total <= 0 or ratio <= 0:
        return seed_ratio

    if ratio + 1e-9 < seed_ratio:
        if ratio + 1e-9 < seed_ratio * SEED_ACCEPT_RATIO:
            return seed_ratio
        count = 1
    else:
        count = infer_entry_count_from_position(side, total, ref_price)
    dca_used = max(0, count - 1)
    if dca_used >= MAX_DCA_COUNT:
        return None
    multiplier = DCA_ADD_MULTIPLIERS[min(dca_used, len(DCA_ADD_MULTIPLIERS) - 1)]
    target_ratio = ratio * (1 + multiplier)
    return min(target_ratio, ENTRY_TARGET_RATIOS[-1])

def infer_entry_count_from_ratio(ratio):
    if ratio <= 0:
        return 0
    if ratio + 1e-9 < ENTRY_TARGET_RATIOS[0]:
        if ratio + 1e-9 >= ENTRY_TARGET_RATIOS[0] * SEED_ACCEPT_RATIO:
            return 1
        return 0
    count = 1
    for idx in range(1, len(ENTRY_TARGET_RATIOS)):
        midpoint = (ENTRY_TARGET_RATIOS[idx - 1] + ENTRY_TARGET_RATIOS[idx]) / 2
        if ratio >= midpoint:
            count = idx + 1
    return min(count, len(ENTRY_TARGET_RATIOS))


def position_stage_from_ratio(ratio):
    return infer_entry_count_from_ratio(ratio)


def position_stage_from_used(side, bal, ref_price):
    return position_stage_from_ratio(used_ratio(side, bal, ref_price))


def infer_entry_count_from_position(side, qty, ref_price):
    bal = get_bal()
    basis_cap = basis_side_cap_margin(side, bal)
    if basis_cap <= 0 or qty <= 0 or ref_price <= 0:
        return 1 if qty > 0 else 0
    ratio = (qty * ref_price / LEVERAGE) / basis_cap
    count = infer_entry_count_from_ratio(ratio)
    if count == 0:
        gap = ENTRY_TARGET_RATIOS[0] - ratio
        if gap >= 0 and gap + 1e-9 < min_effective_topup_ratio(bal, ref_price):
            # If the remaining deficit is too small to place a meaningful reseed
            # order, treat the position as already seeded and prepare the first
            # real DCA step instead of stalling below the first target forever.
            return 1
    return count


def calc_topup_qty(side, bal, target_price):
    target_ratio = next_target_ratio(side, bal, target_price)
    if target_ratio is None:
        return 0.0, None
    return calc_topup_qty_to_ratio(side, bal, target_price, target_ratio), target_ratio


def calc_topup_qty_to_ratio(side, bal, target_price, target_ratio):
    cap_margin = side_cap_margin(bal)
    target_margin = cap_margin * target_ratio
    current_margin = used_margin(side, target_price)
    add_margin = max(0.0, target_margin - current_margin)
    return trunc(add_margin * LEVERAGE / target_price, 3) if target_price > 0 else 0.0


def ceil_qty_3(qty):
    return math.ceil(max(0.0, qty) * 1000) / 1000


def min_tradable_qty_for_price(price):
    min_qty = max(MIN_QTY, SYMBOL_MIN_QTY)
    if price and price > 0:
        min_qty = max(min_qty, ceil_qty_3((MIN_VAL / price) * 1.001))
    return min_qty


def calc_fractional_close_qty(total_qty_value, ratio, price=0.0):
    raw_qty = trunc(total_qty_value * ratio, 3)
    close_qty = normalize_qty(raw_qty, round_up=False)
    min_trade_qty = min_tradable_qty_for_price(price)
    if close_qty >= min_trade_qty and (price <= 0 or close_qty * price >= MIN_VAL):
        return close_qty

    total_qty_norm = normalize_qty(total_qty_value, round_up=False)
    total_notional = total_qty_norm * price if price > 0 else 0.0
    if price > 0 and total_notional > 0 and total_notional < MIN_VAL and total_qty_norm >= SYMBOL_MIN_QTY:
        # Reduce-only close attempts are still useful for dust-sized leftovers.
        return total_qty_norm
    if total_qty_norm >= min_trade_qty:
        return normalize_qty(min_trade_qty, round_up=True)
    if total_qty_value >= SYMBOL_MIN_QTY and total_qty_value <= (SYMBOL_MIN_QTY / ratio) + QTY_TOL:
        return normalize_qty(total_qty_value, round_up=False)
    return 0.0


def add_layer(side, qty, entry, src=None):
    qty = normalize_qty(qty, round_up=False)
    if qty < QTY_TOL:
        return
    state = gs(side)
    if float(state.get("basis_cap_margin", 0) or 0) <= 0:
        bal = get_bal()
        if bal > 0:
            state["basis_cap_margin"] = round(side_cap_margin(bal), 8)
            state["basis_balance"] = round(bal, 8)
    merged = False
    if src:
        for layer in state["layers"]:
            if layer.get("src") == src:
                new_qty = trunc(layer["qty"] + qty, 3)
                if new_qty >= QTY_TOL:
                    layer["entry"] = round(((layer["qty"] * layer["entry"]) + (qty * entry)) / new_qty, 4)
                    layer["qty"] = new_qty
                    layer["ts"] = ts()
                merged = True
                break
    if not merged:
        state["layers"].append({"qty": qty, "entry": round(entry, 4), "ts": ts(), "src": src})
        state["entry_count"] = min(state.get("entry_count", 0) + 1, len(ENTRY_TARGET_RATIOS))
    state["last_entry_bar"] = 0
    state["last_entry_ts"] = ts()
    state["last_fill_entry"] = round(entry, 4)
    state["dca_ref_avg"] = round(avg_entry(side), 4)
    state["last_reaction_action"] = ""
    state["invalid_bars"] = 0
    state["last_invalid_check_candle"] = 0
    state["ztp_done"] = False
    state["used_tp_zones"] = []
    cancel_tp(side)
    SC["filled"] += 1
    print(f"[B] FILL {side} +{qty:.3f}@${entry:,.4f} q={total_qty(side):.3f}")


def reduce_qty(side, qty):
    layers = get_layers(side)
    remain = trunc(qty, 3)
    for layer in reversed(layers):
        if remain <= 0:
            break
        take = min(layer["qty"], remain)
        layer["qty"] = trunc(layer["qty"] - take, 3)
        remain = trunc(remain - take, 3)
    gs(side)["layers"] = [layer for layer in layers if layer["qty"] >= QTY_TOL]


def rebase_side_anchor(side):
    total = trunc(total_qty(side), 3)
    entry = avg_entry(side)
    if total < SYMBOL_MIN_QTY or entry <= 0:
        return
    state = gs(side)
    state["layers"] = [{"qty": total, "entry": round(entry, 4), "ts": ts(), "src": "rebased"}]
    bal_now = get_bal()
    refresh_balance_basis(side, bal_now, entry)
    new_count = infer_entry_count_from_position(side, total, entry)
    state["entry_count"] = new_count
    state["last_fill_entry"] = round(entry, 4)
    state["dca_ref_avg"] = round(entry, 4)
    zone = {
        "lower": float(state.get("last_entry_zone_lower", 0) or 0),
        "upper": float(state.get("last_entry_zone_upper", 0) or 0),
        "center": float(state.get("last_entry_zone_center", 0) or 0),
        "tier": int(state.get("last_entry_zone_tier", 0) or 0),
    }
    if zone_distance_pct_to_price(zone, entry) > ZONE_RESTORE_MAX_DEV_PCT:
        clear_entry_zone_state(side)
    print(f"[B] REBASE {side} q={total:.3f} avg=${entry:,.4f} dca={max(0, new_count - 1)}/{MAX_DCA_COUNT} ref=${entry:,.4f}")


def clear_side(side):
    side_key = sk(side)
    S[side_key] = default_side()
    S[side_key + "_last_close_ts"] = ts()
    reset_slot(side_key + "_entry", do_cancel=True)
    reset_slot(side_key + "_tpavg", do_cancel=True)
    reset_slot(side_key + "_tp2", do_cancel=True)
    reset_slot(side_key + "_tp3", do_cancel=True)


def sync_pos():
    sync_tp_side = S.pop("_sync_tp_cooldown_side", None)
    sync_tp_key = S.pop("_sync_tp_cooldown_key", None)
    sync_tp_fill_px = float(S.pop("_sync_tp_fill_px", 0.0) or 0.0)
    sync_tp_tag = S.pop("_sync_tp_fill_tag", "TP_SYNC")
    sync_tp_detail = S.pop("_sync_tp_fill_detail", "exchange position sync TP")
    sync_tp_zone = S.pop("_sync_tp_zone", None)
    long_pos, short_pos = get_positions()
    for side, exchange_pos in [("LONG", long_pos), ("SHORT", short_pos)]:
        side_key = sk(side)
        internal_qty = total_qty(side)
        if exchange_pos:
            if abs(internal_qty - exchange_pos["qty"]) > 0.005:
                print(f"[B] SYNC {side} x={exchange_pos['qty']:.3f} i={internal_qty:.3f}")
                prev_state = gs(side)
                prev_entry_count = int(prev_state.get("entry_count", 0) or 0)
                prev_last_entry_bar = prev_state.get("last_entry_bar", 99)
                prev_last_entry_ts = prev_state.get("last_entry_ts", 0)
                prev_last_fill_entry = prev_state.get("last_fill_entry", 0.0)
                prev_basis_cap_margin = prev_state.get("basis_cap_margin", 0.0)
                prev_basis_balance = prev_state.get("basis_balance", 0.0)
                prev_avg_tp_bar = prev_state.get("last_avg_tp_bar", 99)
                prev_sigma2_tp_bar = prev_state.get("last_sigma2_tp_bar", 99)
                prev_sigma3_tp_bar = prev_state.get("last_sigma3_tp_bar", 99)
                prev_zone_id = prev_state.get("last_entry_zone_id", "")
                prev_zone_lower = prev_state.get("last_entry_zone_lower", 0.0)
                prev_zone_upper = prev_state.get("last_entry_zone_upper", 0.0)
                prev_zone_center = prev_state.get("last_entry_zone_center", 0.0)
                prev_zone_tier = prev_state.get("last_entry_zone_tier", 0)
                prev_reaction_action = prev_state.get("last_reaction_action", "")
                prev_invalid_bars = prev_state.get("invalid_bars", 0)
                prev_invalid_check_candle = prev_state.get("last_invalid_check_candle", 0)
                prev_ztp_done = bool(prev_state.get("ztp_done", False))
                prev_used_tp_zones = list(prev_state.get("used_tp_zones", []) or [])
                qty_increased = exchange_pos["qty"] > internal_qty + 0.005
                qty_decreased = exchange_pos["qty"] < internal_qty - 0.005
                tracked_entry_order = bool(ORD.get(side_key + "_entry"))
                actual_new_fill = qty_increased and (internal_qty > 0.005 or tracked_entry_order)
                fill_delta = trunc(max(0.0, exchange_pos["qty"] - internal_qty), 3) if actual_new_fill else 0.0
                fill_zone = entry_slot_zone_snapshot(side_key + "_entry") if actual_new_fill else None
                fill_px = float(ORD.get(side_key + "_entry_px", 0) or 0) if actual_new_fill else 0.0
                exchange_updated_ts = float(exchange_pos.get("updated_ts", 0) or 0)
                cooldown_source = "none"
                next_last_entry_ts = 0.0
                if actual_new_fill:
                    next_last_entry_ts = ts()
                    cooldown_source = "fill"
                elif prev_last_entry_ts > 0:
                    next_last_entry_ts = prev_last_entry_ts
                    cooldown_source = "state"
                elif exchange_updated_ts > 0:
                    next_last_entry_ts = exchange_updated_ts
                    cooldown_source = "exchange"
                cooldown_armed = next_last_entry_ts > 0 and ts() - next_last_entry_ts < DCA_COOL_SECS
                sync_tp_filled = qty_decreased and sync_tp_side == side
                if sync_tp_filled:
                    closed_qty = trunc(max(0.0, internal_qty - exchange_pos["qty"]), 3)
                    fill_price = sync_tp_fill_px if sync_tp_fill_px > 0 else get_price() or 0.0
                    prev_entry = avg_entry(side)
                    bal_for_rec = get_bal() or 1.0
                    if closed_qty >= SYMBOL_MIN_QTY and fill_price > 0 and prev_entry > 0:
                        pnl = (
                            closed_qty * (fill_price - prev_entry) / bal_for_rec
                            if side == "LONG"
                            else closed_qty * (prev_entry - fill_price) / bal_for_rec
                        )
                        rec(sync_tp_tag, side, closed_qty, fill_price, pnl, sync_tp_detail)
                next_avg_tp_bar = 0 if sync_tp_filled and sync_tp_key == "last_avg_tp_bar" else prev_avg_tp_bar
                next_sigma2_tp_bar = 0 if sync_tp_filled and sync_tp_key == "last_sigma2_tp_bar" else prev_sigma2_tp_bar
                next_sigma3_tp_bar = 0 if sync_tp_filled and sync_tp_key == "last_sigma3_tp_bar" else prev_sigma3_tp_bar
                clear_side(side)
                state = gs(side)
                state["used_tp_zones"] = prev_used_tp_zones
                state["layers"].append(
                    {
                        "qty": trunc(exchange_pos["qty"], 3),
                        "entry": round(exchange_pos["entry"], 4),
                        "ts": ts(),
                        "src": None,
                    }
                )
                bal_now = get_bal()
                state["basis_cap_margin"] = round(float(prev_basis_cap_margin or side_cap_margin(bal_now or 0)), 8)
                state["basis_balance"] = round(float(prev_basis_balance or bal_now or 0), 8)
                refresh_balance_basis(side, bal_now, exchange_pos["entry"])
                inferred_entry_count = infer_entry_count_from_position(side, exchange_pos["qty"], exchange_pos["entry"])
                if actual_new_fill:
                    state["entry_count"] = min(max(prev_entry_count + 1, inferred_entry_count), len(ENTRY_TARGET_RATIOS))
                elif qty_decreased:
                    state["entry_count"] = inferred_entry_count
                else:
                    state["entry_count"] = min(max(prev_entry_count, inferred_entry_count), len(ENTRY_TARGET_RATIOS))
                state["last_entry_bar"] = 0 if cooldown_armed else prev_last_entry_bar
                state["last_entry_ts"] = next_last_entry_ts
                if actual_new_fill and fill_px > 0:
                    state["last_fill_entry"] = round(fill_px, 4)
                elif qty_decreased:
                    state["last_fill_entry"] = round(float(exchange_pos["entry"]), 4)
                else:
                    state["last_fill_entry"] = round(float(prev_last_fill_entry or exchange_pos["entry"]), 4)
                state["dca_ref_avg"] = round(float(exchange_pos["entry"]), 4)
                # Preserve TP cooldowns across exchange syncs so partial fills do not
                # immediately re-arm fresh 2?/3? TP orders.
                state["last_avg_tp_bar"] = next_avg_tp_bar
                state["last_sigma2_tp_bar"] = next_sigma2_tp_bar
                state["last_sigma3_tp_bar"] = next_sigma3_tp_bar
                if fill_zone:
                    apply_entry_zone_state(side, fill_zone)
                else:
                    prev_zone = {
                        "id": prev_zone_id,
                        "lower": prev_zone_lower,
                        "upper": prev_zone_upper,
                        "center": prev_zone_center,
                        "tier": prev_zone_tier,
                    }
                    if zone_distance_pct_to_price(prev_zone, state["last_fill_entry"]) <= ZONE_RESTORE_MAX_DEV_PCT:
                        apply_entry_zone_state(side, prev_zone)
                    else:
                        clear_entry_zone_state(side)
                if actual_new_fill:
                    state["last_reaction_action"] = ""
                    state["invalid_bars"] = 0
                    state["last_invalid_check_candle"] = 0
                    state["ztp_done"] = False
                    state["used_tp_zones"] = []
                else:
                    state["last_reaction_action"] = prev_reaction_action
                    state["invalid_bars"] = prev_invalid_bars
                    state["last_invalid_check_candle"] = prev_invalid_check_candle
                    state["ztp_done"] = prev_ztp_done or (sync_tp_filled and sync_tp_key == "last_sigma3_tp_bar")
                    state["used_tp_zones"] = prev_used_tp_zones
                    if sync_tp_filled and sync_tp_key == "last_sigma3_tp_bar":
                        mark_tp_zone_used(side, sync_tp_zone)
                if not actual_new_fill and normalize_last_fill_anchor(side, "sync"):
                    state["last_reaction_action"] = ""
                    state["invalid_bars"] = 0
                    state["last_invalid_check_candle"] = 0
                print(
                    f"[B] SYNC_COOLDOWN {side} armed={cooldown_armed} "
                    f"src={cooldown_source} age={int(ts() - state['last_entry_ts']) if state['last_entry_ts'] > 0 else -1}s"
                )
                if sync_tp_filled and sync_tp_key:
                    print(
                        f"[B] SYNC_TP_COOLDOWN {side} "
                        f"key={sync_tp_key}"
                    )
                S["_entry_refresh"] = True
                save()
        elif internal_qty > 0:
            price = get_price() or 0
            bal = get_bal() or 1
            if price > 0:
                entry = avg_entry(side)
                pnl = internal_qty * (price - entry) / bal if side == "LONG" else internal_qty * (entry - price) / bal
                rec("EXT", side, internal_qty, price, pnl)
            clear_side(side)


def is_limit_tp_profitable(side, price):
    entry = avg_entry(side)
    if entry <= 0 or price <= 0:
        return False
    pnl_pct = (price - entry) / entry if side == "LONG" else (entry - price) / entry
    return pnl_pct >= MIN_PROFIT_PCT_TP_LIMIT


def is_limit_tp_profitable_at(side, price, min_profit_pct):
    entry = avg_entry(side)
    if entry <= 0 or price <= 0:
        return False
    pnl_pct = (price - entry) / entry if side == "LONG" else (entry - price) / entry
    return pnl_pct >= min_profit_pct


def is_resting_tp_price(side, target_price, current_price):
    if target_price <= 0 or current_price <= 0:
        return False
    return target_price > current_price if side == "LONG" else target_price < current_price


def resting_tp_price(side, target_price, current_price, bin_size):
    if target_price <= 0 or current_price <= 0:
        return target_price
    if is_resting_tp_price(side, target_price, current_price):
        return round(target_price, 4)
    tick_gap = max(0.0001, bin_size)
    if side == "LONG":
        return round(current_price + tick_gap, 4)
    return round(current_price - tick_gap, 4)


def execute_market_reduce(side, requested_qty, price, bal, tag, detail):
    now = ts()
    guard_age = now - LAST_MARKET_REDUCE_TS.get(side, 0.0)
    if guard_age < MARKET_REDUCE_GUARD_SECS:
        print(f"[B] MKT_GUARD {side} skip req={requested_qty:.3f} age={guard_age:.2f}s tag={tag}")
        return 0.0

    before_qty = fetch_position_qty(side)
    local_qty = total_qty(side)
    close_qty = normalize_qty(min(requested_qty, before_qty, local_qty), round_up=False)
    if close_qty < SYMBOL_MIN_QTY:
        return 0.0

    entry = avg_entry(side)
    order = mkt_close(side, close_qty)
    if not order:
        return 0.0

    LAST_MARKET_REDUCE_TS[side] = now
    oid = order.get("id")
    print(f"[B] MKT_SENT {tag} {side} req={close_qty:.3f} oid={oid}")
    fill_px = price
    time.sleep(0.8)
    if oid:
        filled, avg, status, remaining = check_fill(oid)
        if avg > 0:
            fill_px = avg
        if status == "error":
            filled = 0
        del remaining
    after_qty = fetch_position_qty(side)
    closed_qty = trunc(max(0.0, before_qty - after_qty), 3)
    if closed_qty < SYMBOL_MIN_QTY and oid:
        filled, avg, status, remaining = check_fill(oid)
        if avg > 0:
            fill_px = avg
        if filled >= SYMBOL_MIN_QTY:
            closed_qty = normalize_qty(min(close_qty, filled), round_up=False)
        del status, remaining

    if closed_qty < SYMBOL_MIN_QTY:
        print(f"[B] MKT_WARN {side} requested={close_qty:.3f} but no confirmed reduction")
        sync_pos()
        return 0.0

    print(f"[B] MKT_DONE {tag} {side} closed={closed_qty:.3f} oid={oid}")
    pnl = closed_qty * (fill_px - entry) / bal if side == "LONG" else closed_qty * (entry - fill_px) / bal
    rec(tag, side, closed_qty, fill_px, pnl, detail)
    reduce_qty(side, closed_qty)
    if total_qty(side) < SYMBOL_MIN_QTY:
        clear_side(side)
    else:
        rebase_side_anchor(side)
        S["_entry_refresh"] = True
    save()
    return closed_qty


def execute_limit_reduce(side, requested_qty, price, bal, tag, detail, slippage_pct=LIMIT_REDUCE_SLIPPAGE_PCT):
    now = ts()
    guard_age = now - LAST_MARKET_REDUCE_TS.get(side, 0.0)
    if guard_age < MARKET_REDUCE_GUARD_SECS:
        print(f"[B] LIM_GUARD {side} skip req={requested_qty:.3f} age={guard_age:.2f}s tag={tag}")
        return 0.0

    before_qty = fetch_position_qty(side)
    local_qty = total_qty(side)
    close_qty = normalize_qty(min(requested_qty, before_qty, local_qty), round_up=False)
    if close_qty < SYMBOL_MIN_QTY:
        return 0.0

    entry = avg_entry(side)
    if side == "LONG":
        limit_px = round(price * (1 - slippage_pct), 4)
    else:
        limit_px = round(price * (1 + slippage_pct), 4)
    if limit_px <= 0:
        return 0.0

    order = limit_close(side, close_qty, limit_px)
    if not order:
        return 0.0

    LAST_MARKET_REDUCE_TS[side] = now
    oid = order.get("id")
    print(f"[B] LIM_SENT {tag} {side} req={close_qty:.3f}@${limit_px:,.4f} oid={oid}")
    fill_px = price
    time.sleep(0.9)
    if oid:
        filled, avg, status, remaining = check_fill(oid)
        if avg > 0:
            fill_px = avg
        del status, remaining
    after_qty = fetch_position_qty(side)
    closed_qty = trunc(max(0.0, before_qty - after_qty), 3)
    if closed_qty < SYMBOL_MIN_QTY and oid:
        filled, avg, status, remaining = check_fill(oid)
        if avg > 0:
            fill_px = avg
        if filled >= SYMBOL_MIN_QTY:
            closed_qty = normalize_qty(min(close_qty, filled), round_up=False)
        del status, remaining

    if closed_qty < SYMBOL_MIN_QTY:
        if oid:
            cancel_order(oid, verify=True)
        print(f"[B] LIM_WARN {side} requested={close_qty:.3f} but no confirmed reduction")
        sync_pos()
        return 0.0

    if oid:
        cancel_order(oid, verify=False)

    print(f"[B] LIM_DONE {tag} {side} closed={closed_qty:.3f} oid={oid}")
    pnl = closed_qty * (fill_px - entry) / bal if side == "LONG" else closed_qty * (entry - fill_px) / bal
    rec(tag, side, closed_qty, fill_px, pnl, detail)
    reduce_qty(side, closed_qty)
    if total_qty(side) < SYMBOL_MIN_QTY:
        clear_side(side)
    else:
        rebase_side_anchor(side)
        S["_entry_refresh"] = True
    save()
    return closed_qty


def force_opposite_tp3_on_entry(entry_side, trigger_price):
    opposite_side = "SHORT" if entry_side == "LONG" else "LONG"
    total = total_qty(opposite_side)
    if total < SYMBOL_MIN_QTY:
        return None

    ref_price = trigger_price if trigger_price and trigger_price > 0 else (get_price() or 0)
    if ref_price <= 0:
        return None
    if not is_limit_tp_profitable(opposite_side, ref_price):
        return None

    side_key = sk(opposite_side)
    avg_rescue_reserve = ORD.get(side_key + "_tpavg_qty", 0) if ORD.get(side_key + "_tpavg") else 0.0
    sigma2_reserve = ORD.get(side_key + "_tp2_qty", 0) if ORD.get(side_key + "_tp2") else 0.0
    close_base = trunc(max(0.0, total - avg_rescue_reserve - sigma2_reserve), 3)
    close_qty = calc_fractional_close_qty(close_base, SIGMA3_TP_RATIO, ref_price)
    if close_qty < SYMBOL_MIN_QTY:
        return None

    slot = side_key + "_tp3"
    target_px = round(ref_price, 4)
    if slot_matches(slot, target_px, close_qty):
        return ORD.get(slot)

    reset_slot(slot, do_cancel=True)
    time.sleep(0.15)
    order = limit_close(opposite_side, close_qty, target_px)
    if order:
        set_slot(slot, order.get("id"), target_px, close_qty)
        print(
            f"[B] TP3_FORCE {opposite_side} {close_qty:.3f}@${target_px:,.4f} "
            f"by {entry_side} fill"
        )
        save()
    return order


def remember_entry_zone(side, slot, fill_price):
    state = gs(side)
    zone = entry_slot_zone_snapshot(slot)
    if not zone:
        zone = {
            "id": "fill_fallback",
            "lower": round(fill_price, 4),
            "upper": round(fill_price, 4),
            "center": round(fill_price, 4),
            "tier": 0,
        }
    apply_entry_zone_state(side, zone)
    state["last_reaction_action"] = ""
    state["invalid_bars"] = 0
    state["last_invalid_check_candle"] = 0


def rearm_tp_after_boot_cancel():
    changed = False
    for side in ["LONG", "SHORT"]:
        if total_qty(side) < SYMBOL_MIN_QTY:
            continue
        state = gs(side)
        for key in ["last_avg_tp_bar", "last_sigma2_tp_bar", "last_sigma3_tp_bar"]:
            if int(state.get(key, 99) or 99) < 99:
                state[key] = 99
                changed = True
    if changed:
        save()


def manage_entries(profile, regime, bal, price):
    refresh_all_balance_basis(bal, price)
    for side in ["LONG", "SHORT"]:
        side_key = sk(side)
        slot = side_key + "_entry"
        tracked_oid = ORD.get(slot)
        orphan_count = cancel_untracked_entry_orders(side, keep_oid=tracked_oid)
        if orphan_count is None:
            do_skip(side_key + "_open_sync")
            continue
        if orphan_count > 0:
            do_skip(side_key + "_orphan_entry")
            continue

        current_used = used_ratio(side, bal, price)
        seed_sized_position = seed_sized(side, bal, price)
        zone = select_profile_zone(side, profile, price, seed_sized_position)
        if not zone:
            do_skip(side_key + "_no_zone")
            if active_entry_order_should_stay(side, price):
                do_skip(side_key + "_keep_live_entry")
                continue
            if not reset_slot(slot, do_cancel=True, verify=True):
                do_skip(side_key + "_cancel_wait")
            continue
        target_px = entry_price_from_zone(side, zone, price, profile.get("bin_size", profile_bin_size(price)))
        if target_px <= 0:
            do_skip(side_key + "_bad_zone_px")
            if not reset_slot(slot, do_cancel=True, verify=True):
                do_skip(side_key + "_cancel_wait")
            continue
        if seed_sized_position:
            zone_dist = entry_price_distance_pct(side, target_px, price)
            max_zone_dist = dca_max_zone_dist_pct(side)
            if zone_dist > max_zone_dist:
                do_skip(side_key + "_zone_far")
                live_px = float(ORD.get(slot + "_px", 0) or 0)
                if active_entry_order_should_stay(side, price) and entry_price_distance_pct(side, live_px, price) <= max_zone_dist:
                    do_skip(side_key + "_keep_live_entry")
                    continue
                if not reset_slot(slot, do_cancel=True, verify=True):
                    do_skip(side_key + "_cancel_wait")
                continue

        opposite_side = "SHORT" if side == "LONG" else "LONG"
        opposite_slot = sk(opposite_side) + "_entry"
        opposite_px = float(ORD.get(opposite_slot + "_px", 0) or 0)
        if opposite_px > 0 and price > 0 and abs(target_px - opposite_px) / price < MIN_HEDGE_ENTRY_SPREAD_PCT:
            opposite_tier = int(ORD.get(opposite_slot + "_zone_tier", 0) or 0)
            current_tier = int(zone.get("tier", 0) or 0)
            if current_tier > opposite_tier:
                if not reset_slot(opposite_slot, do_cancel=True, verify=True):
                    do_skip(sk(opposite_side) + "_cancel_wait")
                    continue
                print(
                    f"[B] HEDGE_SPREAD_REPLACE {side} tier={current_tier} "
                    f"opp={opposite_side} tier={opposite_tier} gap={abs(target_px - opposite_px) / price:.4f}"
                )
            else:
                do_skip(side_key + "_hedge_near")
                if not reset_slot(slot, do_cancel=True, verify=True):
                    do_skip(side_key + "_cancel_wait")
                continue

        current_used = used_ratio(side, bal, price if price > 0 else target_px)
        should_cancel = ts() - S.get(side_key + "_last_close_ts", 0) < 60
        if not should_cancel and seed_sized_position and regime_against_side(side, regime):
            should_cancel = True
            do_skip(side_key + "_regime_block")
        target_ratio = next_target_ratio(side, bal, target_px)
        reseed_mode = total_qty(side) > 0 and target_ratio == ENTRY_TARGET_RATIOS[0] and current_used + 1e-9 < ENTRY_TARGET_RATIOS[0]
        seed_promoted = total_qty(side) > 0 and not reseed_mode and current_used + 1e-9 < ENTRY_TARGET_RATIOS[0]
        zone_cap = zone_allowed_target_ratio(zone, side, regime)
        if target_ratio is not None and target_ratio > zone_cap + 1e-9:
            if current_used + 1e-9 < zone_cap:
                target_ratio = zone_cap
                do_skip(side_key + f"_tier{zone.get('tier', 0)}_cap_trim")
            else:
                should_cancel = True
                do_skip(side_key + f"_tier{zone.get('tier', 0)}_cap")
        if target_ratio is None:
            should_cancel = True
            if total_qty(side) > 0 and current_used + 1e-9 < ENTRY_TARGET_RATIOS[0]:
                do_skip(side_key + "_micro_reseed")
            else:
                do_skip(side_key + "_full")

        if not should_cancel and total_qty(side) > 0:
            state = gs(side)
            entry_time_cooldown = ts() - state.get("last_entry_ts", 0) < DCA_COOL_SECS
            if entry_time_cooldown:
                should_cancel = True
                do_skip(side_key + "_cooldown")
            elif seed_sized_position and not reseed_mode:
                step_count = infer_entry_count_from_position(side, total_qty(side), price if price > 0 else avg_entry(side))
                dist_idx = min(len(DCA_STEP_PCTS) - 1, max(0, step_count - 1))
                ref_avg = dca_ref_avg(side)
                min_dist_pct = DCA_STEP_PCTS[dist_idx]
                if side == "LONG":
                    if target_px >= ref_avg:
                        should_cancel = True
                        do_skip(side_key + "_wrong_side")
                    elif target_px > ref_avg * (1 - min_dist_pct):
                        should_cancel = True
                        do_skip(side_key + "_nearby")
                else:
                    if target_px <= ref_avg:
                        should_cancel = True
                        do_skip(side_key + "_wrong_side")
                    elif target_px < ref_avg * (1 + min_dist_pct):
                        should_cancel = True
                        do_skip(side_key + "_nearby")
            elif not seed_sized_position:
                # A dust-sized remaining position is treated as a fresh seed.
                # The live support/resistance distance already controls whether
                # the order is meaningful, so the old average should not block it.
                pass

        if should_cancel:
            if not reset_slot(slot, do_cancel=True, verify=True):
                do_skip(side_key + "_cancel_wait")
            continue

        if target_ratio is None:
            if not reset_slot(slot, do_cancel=True, verify=True):
                do_skip(side_key + "_cancel_wait")
            continue
        entry_qty = calc_topup_qty_to_ratio(side, bal, target_px, target_ratio)
        entry_qty = normalize_qty(entry_qty, round_up=(entry_count(side) == 0))
        if entry_qty < SYMBOL_MIN_QTY or entry_qty * target_px < MIN_VAL:
            do_skip(side_key + "_small")
            if not reset_slot(slot, do_cancel=True, verify=True):
                do_skip(side_key + "_cancel_wait")
            continue
        if ORD.get(slot):
            current_px = ORD.get(slot + "_px", 0) or 0
            current_qty = ORD.get(slot + "_qty", 0) or 0
            if abs(current_px - target_px) <= ENTRY_REPRICE_MIN_DELTA and abs(current_qty - entry_qty) < QTY_TOL:
                continue
        if slot_matches(slot, target_px, entry_qty):
            continue

        if not reset_slot(slot, do_cancel=True, verify=True):
            do_skip(side_key + "_cancel_wait")
            continue
        time.sleep(0.25)
        open_entries = fetch_open_entry_orders(side)
        if open_entries is None:
            do_skip(side_key + "_open_sync")
            continue
        if open_entries:
            do_skip(side_key + "_open_exists")
            continue
        order = limit_open(side, entry_qty, target_px)
        if order:
            set_slot(slot, order.get("id"), target_px, entry_qty)
            set_entry_slot_zone(slot, zone)
            SC["sent"] += 1
            print(
                f"[B] ENTRY_STAND {side} step={infer_entry_count_from_position(side, total_qty(side), target_px)+1}/{len(ENTRY_TARGET_RATIOS)} "
                f"tgt={target_ratio:.2f} used={used_ratio(side, bal, target_px):.3f} tier={zone.get('tier', 0)} "
                f"regime={regime.get('name')} reseed={reseed_mode} promoted={seed_promoted} bal=${bal:.2f} "
                f"{zone_role_for_entry(side)}[{zone['lower']:,.4f}-{zone['upper']:,.4f}] "
                f"ref=${dca_ref_avg(side):,.4f} {entry_qty:.3f}@${target_px:,.4f}"
            )


def check_entry_fills(bal):
    for side in ["LONG", "SHORT"]:
        slot = sk(side) + "_entry"
        oid = ORD.get(slot)
        if not oid:
            continue
        filled, avg, status, remaining = check_fill(oid)
        if status == "error":
            still_open = is_order_still_open(oid)
            if still_open is None or still_open:
                continue
            # Bybit can briefly fail fetch_order() right after a resting entry/DCA
            # fills. If the order also disappeared from open orders, resync first so
            # the fill is counted and the same-side 1h cooldown is armed.
            print(f"[B] ENTRY_LOST_SYNC {side} oid={oid}")
            sync_pos()
            reset_slot(slot)
            save()
            continue
        prev = float(ORD.get(slot + "_fill", 0) or 0)
        delta = trunc(max(0, filled - prev), 3)
        ORD[slot + "_fill"] = filled
        if delta >= QTY_TOL:
            fill_price = avg or ORD.get(slot + "_px", 0)
            add_layer(side, delta, fill_price, oid)
            remember_entry_zone(side, slot, fill_price)
            cancel_rest = status not in FINAL_STATES and (remaining is None or remaining >= QTY_TOL)
            if reset_slot(slot, do_cancel=cancel_rest, verify=cancel_rest):
                S["_entry_refresh"] = True
            else:
                do_skip(sk(side) + "_cancel_wait")
            save()
            continue
        requested_qty = ORD.get(slot + "_qty", 0)
        if status in FINAL_STATES or (remaining is not None and remaining < QTY_TOL and filled > 0) or (requested_qty > 0 and filled + QTY_TOL >= requested_qty):
            reset_slot(slot)


def manage_take_profits(profile, regime, bal, current_price):
    refresh_all_balance_basis(bal, current_price)
    for side in ["LONG", "SHORT"]:
        state = gs(side)
        side_key = sk(side)
        avg_slot = side_key + "_tpavg"
        sigma2_slot = side_key + "_tp2"
        sigma3_slot = side_key + "_tp3"
        total = trunc(total_qty(side), 3)
        entry = avg_entry(side)
        if total < SYMBOL_MIN_QTY or entry <= 0:
            cancel_tp(side)
            cancel_untracked_reduce_orders(side, keep_oids=[])
            continue
        normalize_last_fill_anchor(side, "tp")

        reduce_slots = [avg_slot, sigma2_slot, sigma3_slot]
        orphan_reduce_count = cancel_untracked_reduce_orders(side, keep_oids=[ORD.get(slot) for slot in reduce_slots])
        if orphan_reduce_count is None:
            do_skip(side_key + "_reduce_sync")
            continue
        if orphan_reduce_count > 0:
            do_skip(side_key + "_orphan_reduce")
            continue

        bin_size = float((profile or {}).get("bin_size") or profile_bin_size(current_price))
        last_fill = last_fill_entry(side) or entry
        raw_quick_level = round(last_fill * (1 + QUICK_RELEASE_PCT), 4) if side == "LONG" else round(last_fill * (1 - QUICK_RELEASE_PCT), 4)
        quick_release_far = current_price > 0 and abs(raw_quick_level - current_price) / current_price > QUICK_RELEASE_MAX_DIST_PCT
        tp_zone = take_profit_zone(side, profile, current_price)
        poc_level = profile_poc_target(side, profile, current_price)
        if tp_zone:
            zone_level = round(tp_zone["lower"], 4) if side == "LONG" else round(tp_zone["upper"], 4)
        else:
            zone_level = 0.0
        sigma2_level = resting_tp_price(side, raw_quick_level, current_price, bin_size)
        raw_sigma3_level = zone_level if zone_level > 0 else poc_level
        sigma3_level = resting_tp_price(side, raw_sigma3_level, current_price, bin_size) if raw_sigma3_level > 0 else 0.0
        raw_avg_level = round(entry * (1 + AVG_RESCUE_PROFIT_PCT), 4) if side == "LONG" else round(entry * (1 - AVG_RESCUE_PROFIT_PCT), 4)
        avg_level = resting_tp_price(side, raw_avg_level, current_price, bin_size)

        avg_ready = state.get("last_avg_tp_bar", 99) >= AVG_RESCUE_TP_COOL_BARS
        avg_rescue_live = False
        avg_rescue_qty = 0.0
        if (
            avg_ready
            and dca_count(side) >= AVG_RESCUE_DCA_COUNT
            and used_ratio(side, bal, current_price) >= AVG_RESCUE_MIN_USED_RATIO
            and is_resting_tp_price(side, avg_level, current_price)
            and is_limit_tp_profitable(side, avg_level)
        ):
            avg_rescue_qty = calc_fractional_close_qty(total, AVG_RESCUE_TP_RATIO, avg_level)
            if avg_rescue_qty >= SYMBOL_MIN_QTY:
                avg_rescue_live = True
                if not slot_matches(avg_slot, avg_level, avg_rescue_qty):
                    reset_slot(avg_slot, do_cancel=True)
                    time.sleep(0.15)
                    order = limit_close(side, avg_rescue_qty, avg_level)
                    if order:
                        set_slot(avg_slot, order.get("id"), avg_level, avg_rescue_qty)
                        print(f"[B] AVG_TP_STAND {side} {avg_rescue_qty:.3f}@${avg_level:,.4f}")
                    else:
                        reset_slot(avg_slot)
                        avg_rescue_live = False
                        avg_rescue_qty = 0.0
        if not avg_rescue_live:
            reset_slot(avg_slot, do_cancel=True)

        sigma2_ready = state.get("last_sigma2_tp_bar", 99) >= SIGMA2_TP_COOL_BARS
        quick_release_ready = QUICK_RELEASE_ENABLED and dca_count(side) >= QUICK_RELEASE_MIN_DCA_COUNT
        profitable2 = is_limit_tp_profitable_at(side, sigma2_level, QUICK_RELEASE_PCT)
        sigma2_base = trunc(max(0.0, total - (avg_rescue_qty if avg_rescue_live else 0.0)), 3)
        sigma2_qty = 0.0
        sigma2_live = False
        if (
            sigma2_ready
            and quick_release_ready
            and sigma2_base >= SYMBOL_MIN_QTY
            and not quick_release_far
            and is_resting_tp_price(side, sigma2_level, current_price)
            and profitable2
        ):
            sigma2_qty = calc_fractional_close_qty(sigma2_base, SIGMA2_TP_RATIO, sigma2_level)
            if sigma2_qty >= SYMBOL_MIN_QTY:
                sigma2_live = True
                if not slot_matches(sigma2_slot, sigma2_level, sigma2_qty):
                    reset_slot(sigma2_slot, do_cancel=True)
                    time.sleep(0.15)
                    order = limit_close(side, sigma2_qty, sigma2_level)
                    if order:
                        set_slot(sigma2_slot, order.get("id"), sigma2_level, sigma2_qty)
                        print(f"[B] QUICK_REL_STAND {side} {sigma2_qty:.3f}@${sigma2_level:,.4f}")
                    else:
                        reset_slot(sigma2_slot)
                        sigma2_live = False
                        sigma2_qty = 0.0
        elif quick_release_far:
            do_skip(side_key + "_quick_far")
        if not sigma2_live:
            reset_slot(sigma2_slot, do_cancel=True)

        if sigma2_live and sigma3_level > 0 and abs(sigma3_level - sigma2_level) < 0.0001:
            if not reset_slot(sigma2_slot, do_cancel=True, verify=True):
                do_skip(side_key + "_quick_merge_cancel_wait")
                continue
            sigma2_live = False
            sigma2_qty = 0.0
            do_skip(side_key + "_quick_merged_zone")

        sigma3_ready = state.get("last_sigma3_tp_bar", 99) >= SIGMA3_TP_COOL_BARS
        profitable = is_limit_tp_profitable_at(side, sigma3_level, MIN_PROFIT_PCT_STRUCTURE_TP)
        sigma3_base = trunc(max(0.0, total - (avg_rescue_qty if avg_rescue_live else 0.0) - (sigma2_qty if sigma2_live else 0.0)), 3)
        sigma3_ratio, pos_stage, tp_tier = structure_tp_ratio(side, bal, current_price, tp_zone)
        sigma3_qty = 0.0
        sigma3_live = False
        same_zone_order_live = False
        if tp_zone and ORD.get(sigma3_slot):
            live_zone = tp_slot_zone_snapshot(sigma3_slot)
            live_qty = float(ORD.get(sigma3_slot + "_qty", 0) or 0)
            if live_zone and zones_same_area(live_zone, tp_zone, current_price) and live_qty <= sigma3_base + QTY_TOL:
                same_zone_order_live = True
                sigma3_live = True
        zone_tp_used = bool(tp_zone and tp_zone_already_used(side, tp_zone, current_price))
        if zone_tp_used and not same_zone_order_live:
            do_skip(side_key + "_zone_tp_done")
        elif (
            sigma3_ready
            and sigma3_base >= SYMBOL_MIN_QTY
            and sigma3_level > 0
            and is_resting_tp_price(side, sigma3_level, current_price)
            and profitable
        ):
            sigma3_qty = calc_fractional_close_qty(sigma3_base, sigma3_ratio, sigma3_level)
            if sigma3_qty >= SYMBOL_MIN_QTY:
                sigma3_live = True
                if not slot_matches(sigma3_slot, sigma3_level, sigma3_qty):
                    reset_slot(sigma3_slot, do_cancel=True)
                    time.sleep(0.15)
                    order = limit_close(side, sigma3_qty, sigma3_level)
                    if order:
                        set_slot(sigma3_slot, order.get("id"), sigma3_level, sigma3_qty)
                        label = "ZONE_TP" if zone_level > 0 else "POC_TP"
                        role = zone_role_for_tp(side) if zone_level > 0 else "poc"
                        if label == "ZONE_TP" and tp_zone:
                            set_tp_slot_zone(sigma3_slot, tp_zone)
                        print(f"[B] {label}_STAND {side} {role} stage={pos_stage} tier={tp_tier} ratio={sigma3_ratio:.2f} {sigma3_qty:.3f}@${sigma3_level:,.4f}")
                    else:
                        reset_slot(sigma3_slot)
                        sigma3_live = False
        if not sigma3_live:
            reset_slot(sigma3_slot, do_cancel=True)


def process_tp_fill(side, slot, tag, detail, cooldown_key, bal):
    oid = ORD.get(slot)
    if not oid:
        return

    filled, avg, status, remaining = check_fill(oid)
    if status == "error":
        still_open = is_order_still_open(oid)
        if still_open is None or still_open:
            return
        # Bybit can briefly fail fetch_order() right after a limit TP fully fills.
        # Treat a disappeared TP order as filled for cooldown purposes, then resync
        # the real position size from the exchange before allowing any new TP.
        gs(side)[cooldown_key] = 0
        S["_sync_tp_cooldown_side"] = side
        S["_sync_tp_cooldown_key"] = cooldown_key
        S["_sync_tp_fill_px"] = float(ORD.get(slot + "_px", 0) or 0)
        S["_sync_tp_fill_tag"] = tag
        S["_sync_tp_fill_detail"] = detail
        if tag == "ZTP":
            S["_sync_tp_zone"] = tp_slot_zone_snapshot(slot)
        reset_slot(slot)
        sync_pos()
        save()
        return

    prev = float(ORD.get(slot + "_fill", 0) or 0)
    delta = trunc(max(0, filled - prev), 3)
    ORD[slot + "_fill"] = filled
    if delta >= QTY_TOL:
        fill_price = avg or ORD.get(slot + "_px", 0)
        entry = avg_entry(side)
        pnl = delta * (fill_price - entry) / bal if side == "LONG" else delta * (entry - fill_price) / bal
        rec(tag, side, delta, fill_price, pnl, detail)
        reduce_qty(side, delta)
        if tag == "ZTP":
            mark_tp_zone_used(side, tp_slot_zone_snapshot(slot))
            gs(side)["ztp_done"] = True
        gs(side)[cooldown_key] = 0
        reset_slot(slot, do_cancel=True)
        if total_qty(side) < SYMBOL_MIN_QTY:
            clear_side(side)
        else:
            rebase_side_anchor(side)
            S["_entry_refresh"] = True
        save()
        return

    requested_qty = ORD.get(slot + "_qty", 0)
    if status in FINAL_STATES or (remaining is not None and remaining < QTY_TOL and filled > 0) or (requested_qty > 0 and filled + QTY_TOL >= requested_qty):
        # If Bybit reports the TP order as final before filled qty is visible,
        # sync before clearing the slot so sync_pos() can start the TP cooldown
        # when the exchange position already decreased.
        if status == "closed" or filled >= QTY_TOL:
            gs(side)[cooldown_key] = 0
            S["_sync_tp_cooldown_side"] = side
            S["_sync_tp_cooldown_key"] = cooldown_key
            S["_sync_tp_fill_px"] = float(avg or ORD.get(slot + "_px", 0) or 0)
            S["_sync_tp_fill_tag"] = tag
            S["_sync_tp_fill_detail"] = detail
            if tag == "ZTP":
                S["_sync_tp_zone"] = tp_slot_zone_snapshot(slot)
            sync_pos()
        reset_slot(slot)


def check_tp_fills(bal):
    for side in ["LONG", "SHORT"]:
        process_tp_fill(side, sk(side) + "_tpavg", "AVGS", "50% avg rescue TP after 3x DCA", "last_avg_tp_bar", bal)
        process_tp_fill(side, sk(side) + "_tp2", "REL", "quick release after favorable reaction", "last_sigma2_tp_bar", bal)
        process_tp_fill(side, sk(side) + "_tp3", "ZTP", "volume-profile support/resistance zone TP", "last_sigma3_tp_bar", bal)


def latest_layer_qty(side):
    layers = get_layers(side)
    if not layers:
        return 0.0
    return float(layers[-1].get("qty", 0) or 0)


def reduce_latest_layer(side, ratio, price, bal, tag, detail):
    qty = calc_fractional_close_qty(latest_layer_qty(side), ratio, price)
    if qty < SYMBOL_MIN_QTY:
        return 0.0
    cancel_tp(side)
    return execute_limit_reduce(side, qty, price, bal, tag, detail)


def manage_reaction_controls(bal, price, candles):
    closed = candle_closed(candles)
    if len(closed) < 3 or price <= 0:
        return
    last_candle = closed[-1]
    candle_ts = int(last_candle[0])
    close_px = float(last_candle[4] or 0)
    if close_px <= 0:
        return
    for side in ["LONG", "SHORT"]:
        state = gs(side)
        if total_qty(side) < SYMBOL_MIN_QTY:
            continue
        lower = float(state.get("last_entry_zone_lower", 0) or 0)
        upper = float(state.get("last_entry_zone_upper", 0) or 0)
        if lower <= 0 or upper <= 0:
            continue
        if int(state.get("last_invalid_check_candle", 0) or 0) != candle_ts:
            broken = close_px < lower * (1 - ZONE_BREAK_BUFFER_PCT) if side == "LONG" else close_px > upper * (1 + ZONE_BREAK_BUFFER_PCT)
            state["invalid_bars"] = int(state.get("invalid_bars", 0) or 0) + 1 if broken else 0
            state["last_invalid_check_candle"] = candle_ts

        action = state.get("last_reaction_action", "")
        if state.get("invalid_bars", 0) >= INVALID_BARS and action != "invalid_fail":
            if dca_count(side) < MAX_DCA_COUNT:
                do_skip(sk(side) + "_invalid_wait_final")
                continue
            before_qty = fetch_position_qty(side)
            if before_qty < SYMBOL_MIN_QTY:
                clear_side(side)
                save()
                continue
            cancel_tp(side)
            closed_qty = execute_limit_reduce(
                side,
                before_qty,
                price,
                bal,
                "FAIL",
                f"final zone invalid {lower:.4f}-{upper:.4f}",
                slippage_pct=FAIL_LIMIT_REDUCE_SLIPPAGE_PCT,
            )
            if closed_qty > 0:
                state["last_reaction_action"] = "invalid_fail"
                S["_entry_refresh"] = True
                save()
            continue


def is_final_active(side, bal):
    ref = avg_entry(side)
    if ref <= 0:
        return False
    return dca_count(side) >= MAX_DCA_COUNT and used_ratio(side, bal, ref) >= FAIL_ACTIVE_RATIO


def check_final(side, price, bal):
    total = total_qty(side)
    entry = avg_entry(side)
    state = gs(side)
    if total <= 0 or entry <= 0:
        return
    if not is_final_active(side, bal):
        return

    last_px = last_entry(side)
    fail_dist = price * FAIL_MOVE_PCT
    big_move = price < last_px - fail_dist if side == "LONG" else price > last_px + fail_dist
    if not big_move:
        return

    before_qty = fetch_position_qty(side)
    if before_qty < SYMBOL_MIN_QTY:
        clear_side(side)
        save()
        return
    cancel_tp(side)
    execute_limit_reduce(
        side,
        before_qty,
        price,
        bal,
        "FAIL",
        f"adverse bars={state.get('last_entry_bar', 0)}",
        slippage_pct=FAIL_LIMIT_REDUCE_SLIPPAGE_PCT,
    )


def tick_bars():
    for side in ["LONG", "SHORT"]:
        state = gs(side)
        if state.get("last_entry_bar", 99) < 99:
            state["last_entry_bar"] = state.get("last_entry_bar", 0) + 1
        if state.get("last_avg_tp_bar", 99) < 99:
            state["last_avg_tp_bar"] = state.get("last_avg_tp_bar", 0) + 1
        if state.get("last_sigma2_tp_bar", 99) < 99:
            state["last_sigma2_tp_bar"] = state.get("last_sigma2_tp_bar", 0) + 1
        if state.get("last_sigma3_tp_bar", 99) < 99:
            state["last_sigma3_tp_bar"] = state.get("last_sigma3_tp_bar", 0) + 1


def acquire_run_lock():
    global RUN_LOCK_HANDLE
    try:
        import fcntl
    except Exception:
        return True
    try:
        handle = open(RUN_LOCK_FILE, "w", encoding="utf-8")
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        RUN_LOCK_HANDLE = handle
        return True
    except BlockingIOError:
        print(f"[B] LOCKED another bot_b.py is already running lock={RUN_LOCK_FILE}")
        return False
    except Exception as e:
        print(f"[B] LOCK_WARN:{e}")
        return True


def run():
    if not acquire_run_lock():
        return
    load()
    bal = get_bal()
    if bal <= 0:
        try:
            print(f"[B] Bal=0 keys={list(ex.fetch_balance({'type': 'swap'}).keys())[:10]}")
        except Exception as e:
            print(f"[B] Bal=0 err:{e}")
        print("[B] Bal=0")
        return

    print(f"[B] VP circular v1-bybit-xrp-profile ${bal:.2f} | {LEVERAGE}x | side_cap={SIDE_MARGIN_CAP:.2f} | state={STATE_FILE}")
    try:
        market = configure_symbol_rules()
        print(f"[B] market_min_qty={SYMBOL_MIN_QTY:g} contractSize={market.get('contractSize')}")
        ex.set_position_mode(True, SYMBOL)
        print("[B] hedge_mode=True (requested)")
    except Exception as e:
        print(f"[B] pos_mode:{e}")
    try:
        ex.set_leverage(LEVERAGE, SYMBOL)
    except Exception as e:
        print(f"[B] lev:{e}")

    sync_pos()
    cancel_all()
    time.sleep(1)
    cancel_all()
    rearm_tp_after_boot_cancel()
    print("[B] All orders cleared")

    boot_price = get_price() or 0
    if boot_price > 0:
        max_side_qty = normalize_qty(side_cap_margin(bal) * LEVERAGE / boot_price, round_up=False)
        min_qty_for_tp2 = math.ceil(SYMBOL_MIN_QTY / SIGMA2_TP_RATIO)
        min_qty_for_tp3 = math.ceil(SYMBOL_MIN_QTY / SIGMA3_TP_RATIO)
        if max_side_qty + QTY_TOL < min_qty_for_tp2:
            print(
                f"[B] CONFIG_WARN max_side_qty={max_side_qty:.3f}, but TP2 {SIGMA2_TP_RATIO*100:.0f}% needs at least "
                f"{min_qty_for_tp2:.3f} tradable qty on {SYMBOL}. This symbol/account size cannot realize exact {SIGMA2_TP_RATIO*100:.0f}% TP."
            )
        if max_side_qty + QTY_TOL < min_qty_for_tp3:
            print(
                f"[B] CONFIG_WARN max_side_qty={max_side_qty:.3f}, but TP3 {SIGMA3_TP_RATIO*100:.0f}% needs at least "
                f"{min_qty_for_tp3:.3f} tradable qty on {SYMBOL}. This symbol/account size cannot realize exact {SIGMA3_TP_RATIO*100:.0f}% TP."
            )

    last_fetch_ts = 0.0
    last_htf_fetch_ts = 0.0
    last_candle_ts = 0
    last_sync_ts = 0.0
    last_hb_ts = 0.0
    last_stats_ts = 0.0
    c5 = []
    c15 = []
    c1h = []
    c4h = []
    profile = {"zones": [], "pocs": {}, "bin_size": 0.0}
    regime = {"name": "UNKNOWN", "dist15": 0.0, "recent15": 0.0, "dist1h": 0.0, "dist4h": 0.0}

    while True:
        try:
            now = ts()
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if today != S.get("day", ""):
                S["day"] = today
                S["real_pnl"] = 0.0
                save()
                SC.update({"sig_seen": 0, "sent": 0, "filled": 0})
                SKIP.clear()

            bal = get_bal()
            check_entry_fills(bal)
            check_tp_fills(bal)

            if now - last_fetch_ts > FETCH_INTERVAL:
                c5 = fetch_candles(PROFILE_FETCH_LIMITS["M5"])
                if now - last_htf_fetch_ts > HTF_FETCH_INTERVAL or not c15 or not c1h or not c4h:
                    c15 = fetch_candles_tf("15m", PROFILE_FETCH_LIMITS["M15"])
                    c1h = fetch_candles_tf("1h", PROFILE_FETCH_LIMITS["H1"])
                    c4h = fetch_candles_tf("4h", PROFILE_FETCH_LIMITS["H4"])
                    last_htf_fetch_ts = now
                if c5:
                    closed5 = candle_closed(c5)
                    candle_ts = closed5[-1][0] if closed5 else 0
                    new_closed_candle = candle_ts != last_candle_ts
                    if new_closed_candle and last_candle_ts > 0:
                        tick_bars()
                    if new_closed_candle or not profile.get("zones"):
                        last_candle_ts = candle_ts
                        bal = get_bal()
                        price = get_price() or 0
                        ref_profile_price = float(closed5[-1][4] or price) if closed5 else price
                        if price > 0 and ref_profile_price > 0:
                            profile = build_volume_profile(c5, ref_profile_price, c15, c1h, c4h)
                            regime = classify_kumo_regime(c15, c1h, price, c4h)
                            restored_zone = False
                            for side in ["LONG", "SHORT"]:
                                restored_zone = ensure_entry_zone_state(side, profile, "profile") or restored_zone
                            if restored_zone:
                                save()
                            manage_entries(profile, regime, bal, price)
                            manage_take_profits(profile, regime, bal, price)
                            manage_reaction_controls(bal, price, c5)
                last_fetch_ts = now

            if not c5 or not profile.get("zones"):
                time.sleep(CHECK_SEC)
                continue

            price = get_price()
            if not price:
                time.sleep(CHECK_SEC)
                continue

            if S.get("_entry_refresh") and profile.get("zones"):
                manage_take_profits(profile, regime, bal, price)
                manage_reaction_controls(bal, price, c5)
                S["_entry_refresh"] = False

            if now - last_sync_ts > 30:
                sync_pos()
                last_sync_ts = now

            for side in ["LONG", "SHORT"]:
                check_final(side, price, bal)

            if now - last_hb_ts > 300:
                for side in ["LONG", "SHORT"]:
                    side_key = sk(side)
                    total = total_qty(side)
                    if total > 0:
                        entry = avg_entry(side)
                        state = gs(side)
                        upnl = total * (price - entry) if side == "LONG" else total * (entry - price)
                        used = used_ratio(side, bal, price)
                        margin_pct = used * 100
                        print(
                            f"[B] {side} q={total:.3f} avg=${entry:,.4f} ${upnl:.2f} "
                            f"used={used:.3f} sideM={margin_pct:.1f}% "
                            f"dca={dca_count(side)}/{MAX_DCA_COUNT} "
                            f"3s_cd={state.get('last_sigma3_tp_bar', 99)}"
                        )
                    entry_px = ORD.get(side_key + "_entry_px", 0)
                    avg_px = ORD.get(side_key + "_tpavg_px", 0)
                    tp2_px = ORD.get(side_key + "_tp2_px", 0)
                    tp3_px = ORD.get(side_key + "_tp3_px", 0)
                    parts = []
                    if entry_px > 0:
                        parts.append(f"entry@{entry_px:,.4f}")
                    if avg_px > 0:
                        parts.append(f"avg_tp@{avg_px:,.4f}")
                    if tp2_px > 0:
                        parts.append(f"tp2@{tp2_px:,.4f}")
                    if tp3_px > 0:
                        parts.append(f"tp3@{tp3_px:,.4f}")
                    if parts:
                        print(f"[B] ORD_{side} {' | '.join(parts)}")
                top_zones = " ".join(
                    f"T{zone['tier']}[{zone['lower']:.4f}-{zone['upper']:.4f}]"
                    for zone in profile.get("zones", [])[:6]
                )
                print(
                    f"[B] ${price:,.4f} regime={regime.get('name')} "
                    f"k15={regime.get('dist15', 0)*100:+.2f}% "
                    f"k1h={regime.get('dist1h', 0)*100:+.2f}% "
                    f"k4h={regime.get('dist4h', 0)*100:+.2f}% zones: {top_zones}"
                )
                last_hb_ts = now

            if now - last_stats_ts > 1800:
                top = sorted(SKIP.items(), key=lambda item: -item[1])[:8]
                skip_text = " ".join(f"{key}={value}" for key, value in top) if top else "none"
                print(f"[B] STATS sent={SC['sent']} fill={SC['filled']} skips: {skip_text}")
                last_stats_ts = now

            time.sleep(CHECK_SEC)
        except Exception as e:
            print(f"[B] ERR:{e}")
            traceback.print_exc()
            time.sleep(10)


if __name__ == "__main__":
    run()
