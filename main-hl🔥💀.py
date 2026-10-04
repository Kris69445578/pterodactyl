import websocket
import json
import re
import time
import ssl
import threading
import queue
import os
import signal
import sys
import requests
import statistics   # used only by the new multi-stage signal validation logic

# Ensure every signal/order/result line is visible immediately when the bot is
# run under systemd, Docker, a hosting panel, or another non-interactive stdout.
try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
except (AttributeError, OSError):
    pass
import html as html_lib
import random
from collections import deque
from colorama import Fore, Style, init
from datetime import datetime, timedelta

init(autoreset=True)

# ╔══════════════════════════════════════════════════════════════╗
# ║                     CONFIG — COMMON                          ║
# ╚══════════════════════════════════════════════════════════════╝

MAX_SLOTS                = 9
CURRENCY                 = "USD"
DEFAULT_BASE_STAKE       = 1.0
DAILY_TP_TARGET          = 1000.00
CONTRACT_DURATION        = 5    # HIGHER/LOWER duration in ticks (unchanged)
CONTRACT_DURATION_UNIT   = "t"
CONTRACT_DURATION_RISE_FALL = 1 # RISE/FALL minimum duration in ticks

CONTRACT_MODE_RISE_FALL    = "RISE_FALL"
CONTRACT_MODE_HIGHER_LOWER = "HIGHER_LOWER"
DEFAULT_CONTRACT_MODE      = CONTRACT_MODE_HIGHER_LOWER  # preserves prior behaviour for existing slots

# When HIGHER/LOWER loses, users may optionally switch that symbol to
# RISE/FALL recovery until the recovery trade wins. Existing slots keep the
# original martingale-only behaviour.
HIGHER_LOWER_RECOVERY_MARTINGALE = "MARTINGALE"
HIGHER_LOWER_RECOVERY_RISE_FALL  = "RISE_FALL"
DEFAULT_HIGHER_LOWER_RECOVERY    = HIGHER_LOWER_RECOVERY_MARTINGALE

TRADE_MODE_REVERSAL = "REVERSAL"  # 5 consecutive down ticks → trade HIGHER (and vice versa) — current/original behaviour
TRADE_MODE_NORMAL   = "NORMAL"    # 5 consecutive down ticks → trade LOWER  (and vice versa) — trend-following
DEFAULT_TRADE_MODE  = TRADE_MODE_REVERSAL  # preserves prior behaviour for existing slots

DEFAULT_VIRTUAL_MODE        = False  # existing slots default to real trading — unchanged behaviour
DEFAULT_VIRTUAL_LOSS_LIMIT  = 0      # number of virtual losses before shifting to real trading
MAX_CONCURRENT_CONTRACTS = 5
ACCESS_DURATION_HOURS    = 168
MARTINGALE               = 11
MARTINGALE_MODE_IMMEDIATE = "immediate"
MARTINGALE_MODE_SIGNAL    = "signal"

CANDLE_INTERVAL_SECONDS  = 60
TICK_PCT_WINDOW          = 10    # how many recent ticks to evaluate for the % gate
TICK_PCT_THRESHOLD       = 70    # % dominance required to fire a (contrarian) signal (was 90 — moderately loosened)
TICK_DEBUG_PRINT         = False # set True to print the Higher/Lower/Equal window debug block
SHOW_CANDLE_LOGS         = False # set True to print "🕯 CANDLE CLOSED" lines to console
PORTFOLIO_RECONCILE_SECONDS = 20 # how often to cross-check Deriv's open-contract list against local tracking
SKIP_IF_REMAINING_SECS   = 0
TREND_LOOKBACK_CANDLES   = 2

# ── Multi-stage reversal-signal validation (signal-generation logic only) ──
# A raw 5-tick same-direction run is treated as a CANDIDATE, not a trade.
# The candidate must survive momentum/exhaustion/reversal-confirmation
# analysis over the next few ticks before a signal is actually generated.
REVERSAL_BASELINE_WINDOW   = 40   # ticks used to build the adaptive "normal" tick-size baseline
REVERSAL_CONFIRM_MAX_TICKS = 2    # how many ticks to watch after the 5-tick run before giving up (was 5 — more chances to confirm)
REVERSAL_MIN_INDEPENDENT   = 8    # min. number of independent sub-conditions (of 9, incl. RSI/Bollinger/Stochastic) that must pass (was 7 — moderately loosened)
REVERSAL_CONFIDENCE_MIN    = 0.20 # min. weighted confidence score required to fire (was 0.70 — moderately loosened)
REVERSAL_HISTORY_MAXLEN    = 30   # how many past 5-tick sequences are remembered for history validation

# ── Tick "body" filter ──
# A tick only gets a directional vote in the dominance window if its move is
# meaningfully larger than this symbol's own recent noise level. Without
# this, five tiny/doji ticks in a row would count identically to five ticks
# of real, tradeable size — this makes "5 in a row" mean 5 REAL moves.
TICK_BODY_MIN_RATIO = 0.15   # |delta| must be >= this fraction of the adaptive baseline to count

# ── Professional confirmation indicators (stages 10-12) ──
# These run on the completed 1-min candle series and add classic technical-
# analysis confirmation to the raw tick-delta reversal math above. None of
# them can fire a signal alone — they only shift the weighted confidence
# score and independent-condition count in _evaluate_reversal().
RSI_PERIOD           = 5
RSI_OVERBOUGHT       = 100.0
RSI_OVERSOLD         = 0.0
BOLLINGER_PERIOD     = 20
BOLLINGER_STDDEV     = 2.0
STOCH_PERIOD         = 14
STOCH_SMOOTH         = 3
STOCH_OVERBOUGHT     = 80.0
STOCH_OVERSOLD       = 20.0

BARRIER_OFFSETS = {
    "1HZ10V":  0.48,  "1HZ15V":  0.88,  "1HZ25V":  95.0,  "1HZ30V":  1.07,
    "1HZ50V":  40.0,  "1HZ75V":  1.60,  "1HZ90V":  4.5,   "1HZ100V": 0.39,
    "R_10": 0.289,  "R_25": 0.383, "R_50": 0.028, "R_75": 19.377, "R_100": 0.35
}

VOLATILITIES = ["1HZ10V", "1HZ15V", "1HZ25V", "1HZ30V", "1HZ50V", "1HZ75V", "1HZ90V", "1HZ100V", "R_10", "R_25", "R_75", "R_100"]

TICK_STALL_SECONDS        = 75
WATCHDOG_INTERVAL         = 8
WATCHDOG_KICK_GAP         = 120
WS_RECONNECT_MIN          = 2
WS_RECONNECT_MAX          = 30
WS_RECONNECT_JITTER       = 0.4
TG_POLL_BACKOFF_MAX       = 30
WS_HEARTBEAT_INTERVAL     = 25
SHARED_WS_RESUB_DELAY     = 0.0
PENDING_CONTRACT_TIMEOUT  = 45
OPEN_CONTRACT_TIMEOUT     = 120
WATCHDOG_SLOT_RESTART_GAP = 30
COOLDOWN_TICKS            = 0
PROCESSED_RESULTS_MAX     = 2000  # cap on the sold-contract de-dupe cache per slot

# ╔══════════════════════════════════════════════════════════════╗
# ║              CONFIG — NEW DERIV API                          ║
# ╚══════════════════════════════════════════════════════════════╝

NEW_APP_ID       = "34gpR1PaFlQgwLV3TvZxc"
NEW_REST_BASE    = "https://api.derivws.com"
NEW_WS_PUBLIC    = "wss://api.derivws.com/trading/v1/options/ws/public"

# ╔══════════════════════════════════════════════════════════════╗
# ║              TELEGRAM CONFIG                                 ║
# ╚══════════════════════════════════════════════════════════════╝

TG_TOKEN       = "8425580686:AAHJ60Ur3fqnSCLIsIt9hPrKGKjJcBevyrc"
TG_API         = f"https://api.telegram.org/bot{TG_TOKEN}"
ADMIN_CHAT_ID  = 6113290006
ADMIN_USERNAME = "@jahimtony"

# ╔══════════════════════════════════════════════════════════════╗
# ║                     FILE PATHS                               ║
# ╚══════════════════════════════════════════════════════════════╝

try:
    DATA_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    DATA_DIR = os.getcwd()

os.makedirs(DATA_DIR, exist_ok=True)

SLOTS_FILE        = os.path.join(DATA_DIR, "jahim_slots.json")
ADMIN_FILE        = os.path.join(DATA_DIR, "jahim_admin.json")
CONFIG_FILE       = os.path.join(DATA_DIR, "jahim_config.json")
OAUTH_TOKENS_FILE = os.path.join(DATA_DIR, "jahim_oauth.json")

# ╔══════════════════════════════════════════════════════════════╗
# ║                     GLOBAL STATE                             ║
# ╚══════════════════════════════════════════════════════════════╝

authorized_ids   = {}
slots            = {}
slots_lock       = threading.Lock()
pending_command  = {}
pending_cmd_lock = threading.Lock()
_tg_offset       = 0

_shutdown_event        = threading.Event()
_shared_heartbeat_stop = threading.Event()

# ── Tick-ingestion decoupling ──
# The shared-tick-ws thread is the ONLY thread reading ticks for every
# symbol off the wire. Nothing that can block on network I/O (order
# placement, result-check requests, an immediate re-entry fired off a
# virtual-contract settlement) may run inline on that thread — a single
# slow socket write would stall candle updates and signal detection for
# every other symbol until it returns, which is exactly how ticks get
# "wasted" during a busy signal/recovery moment. So the ingestion thread
# only ever does fast, in-memory work (buffer append + CandleEngine.push_tick)
# and hands everything else off to this queue for a dedicated worker to
# process, in the same order the ticks arrived.
_tick_work_queue = queue.Queue()

# ── Async, debounced slot-state persistence ──
# save_slots() used to do a synchronous JSON write straight to disk on the
# calling thread. Several of its ~30 call sites are on hot paths (virtual
# contract settlement during a losing/recovery streak fires on the
# tick-ingestion path), so a slow disk write there directly delayed the
# next tick being read. save_slots() below now just flags that a save is
# due; a single background thread coalesces bursts and does the actual
# write off any latency-sensitive thread.
_save_pending      = threading.Event()
_save_pending_lock = threading.Lock()

_oauth_tokens        = {}
_oauth_tokens_lock   = threading.Lock()

_pending_martingale_modes: dict = {}
_pending_contract_modes:   dict = {}

# Cache: last barrier that landed in-range per symbol — reused as starting point
# next signal so we usually hit the target payout on the very first proposal.
_good_barrier_cache: dict = {}     # vol → float
_good_barrier_ts:    dict = {}     # vol → epoch seconds of last confirmed-good reading
_good_barrier_lock   = threading.Lock()

PAYOUT_TARGET_MIN  = 0.10  # 10% minimum profit per trade
PAYOUT_TARGET_MAX  = 0.11  # 10% — stop once payout is in range
PAYOUT_MAX_RETRIES = 8     # max bisection steps before accepting whatever Deriv offers
PAYOUT_PROBE_STEP  = 0.10  # 10% initial probe when one bound is still unknown
BARRIER_SEARCH_TIME_BUDGET = 0.90  # hard wall-clock cap (secs) for the whole barrier search — never cross ~1s

# Background barrier prober — keeps _good_barrier_cache warm WHILE the candle
# engine is still analysing ticks (i.e. before any signal has fired), instead
# of only searching for the payout-target barrier after a signal fires.
BARRIER_PROBE_INTERVAL = 0.8   # secs paced between each symbol's probe send (avoid API bursts)
BARRIER_PROBE_STAKE    = DEFAULT_BASE_STAKE
BARRIER_PROBE_DEBUG_PRINT = False  # set True to print background probe activity to console

# How fresh a cached barrier must be (secs since it was last confirmed in the
# target payout window) for the real trade path to trust it outright and buy
# straight off the first proposal — skipping the bisection-retry loop entirely.
# Kept a little above BARRIER_PROBE_INTERVAL so a barrier that was warmed on
# the last probing pass is still considered "live" when a signal fires.
BARRIER_CACHE_MAX_AGE = 10


def _get_barrier(vol: str) -> float:
    """Return the configured baseline barrier for a symbol."""
    return BARRIER_OFFSETS.get(vol, 0.20)


def _reduce_barrier(vol: str) -> float:
    """
    Called when Deriv rejects a proposal with a barrier-related
    ContractBuyValidationError. Was previously referenced but never
    defined — that caused a raw NameError inside the trade WS message
    handler every time this error occurred, which silently killed that
    on_message callback invocation, left the contract slot stuck in a
    half-open state, and swallowed the failed trade with no result and no
    retry. Now it backs the cached "good" barrier for this symbol halfway
    toward the safe default so the next proposal is less likely to hit the
    same rejection, and returns the adjusted value for logging.
    """
    default = _get_barrier(vol)
    with _good_barrier_lock:
        current = _good_barrier_cache.get(vol, default)
        new_barrier = round((current + default) / 2, 3)
        if abs(new_barrier - default) < 0.01:
            new_barrier = default
        _good_barrier_cache[vol] = new_barrier
        # Deriv just rejected the cached barrier — it can no longer be trusted
        # blind. Clear its timestamp so the next trade falls back to a
        # verified bisection search instead of buying off a stale value.
        _good_barrier_ts.pop(vol, None)
    return new_barrier


def _bisect_barrier(current_barrier: float, payout_too_low: bool,
                    barrier_too_big, barrier_too_small):
    """
    Binary-search the barrier into the [PAYOUT_TARGET_MIN, PAYOUT_TARGET_MAX] window.

    For CALL/HIGHER contracts, barrier = distance below spot:
      larger barrier  -> easier win -> LOWER  payout
      smaller barrier -> harder win -> HIGHER payout

      barrier_too_big   : tried; gave payout < MIN (too easy, payout too low)
      barrier_too_small : tried; gave payout > MAX (too hard, payout too high)

    Returns (new_barrier, updated_barrier_too_big, updated_barrier_too_small).
    """
    if payout_too_low:
        new_too_big   = current_barrier
        new_too_small = barrier_too_small
        if barrier_too_small is not None:
            new_b = round((new_too_small + new_too_big) / 2, 2)
        else:
            new_b = max(round(current_barrier * (1.0 - PAYOUT_PROBE_STEP), 2), 0.01)
    else:
        new_too_small = current_barrier
        new_too_big   = barrier_too_big
        if barrier_too_big is not None:
            new_b = round((new_too_small + new_too_big) / 2, 2)
        else:
            new_b = round(current_barrier * (1.0 + PAYOUT_PROBE_STEP), 2)
    return max(new_b, 0.01), new_too_big, new_too_small


# ╔══════════════════════════════════════════════════════════════╗
# ║    1-MIN CANDLE ENGINE  (NEW API — used for BOTH modes)      ║
# ╚══════════════════════════════════════════════════════════════╝
# Candle boundaries aligned to UTC wall-clock minutes:
#   candle_start = floor(epoch / 60) * 60
# Identical to Deriv's own 1-minute candle boundaries.

def _candle_bucket(epoch: float) -> float:
    return float(int(epoch) // CANDLE_INTERVAL_SECONDS * CANDLE_INTERVAL_SECONDS)


class CandleEngine:
    def __init__(self, vol: str):
        self.vol                = vol
        self.lock               = threading.Lock()
        self.candles: list      = []
        self.current_open       = None
        self.current_high       = None
        self.current_low        = None
        self.current_close      = None
        self.current_bucket     = 0.0
        self.tick_window         = deque(maxlen=TICK_PCT_WINDOW)  # rolling "up"/"down" tick history
        self.signal_fired       = False
        self.last_price         = None
        self.current_tick_count = 0          # tick counter for current candle
        self.last_signal_epoch  = 0.0        # epoch of last fired signal (cooldown)

        # ── Multi-stage reversal validation state ──
        self.recent_deltas    = deque(maxlen=REVERSAL_BASELINE_WINDOW)  # raw signed tick deltas, adaptive baseline
        self.pending_reversal = None                                   # in-progress candidate awaiting confirmation
        self.reversal_history = deque(maxlen=REVERSAL_HISTORY_MAXLEN)  # outcomes of past candidates (for stage 7)

    def _candle_color(self, open_: float, close: float) -> str:
        return "green" if close >= open_ else "red"

    def _trend_direction(self):
        if len(self.candles) < TREND_LOOKBACK_CANDLES:
            return None
        recent = self.candles[-TREND_LOOKBACK_CANDLES:]
        colors = [c["color"] for c in recent]
        if all(c == "green" for c in colors):
            return "green"
        if all(c == "red" for c in colors):
            return "red"
        return None

    def _seconds_remaining(self, epoch: float) -> float:
        if self.current_bucket == 0.0:
            return CANDLE_INTERVAL_SECONDS
        return max(0.0, self.current_bucket + CANDLE_INTERVAL_SECONDS - epoch)

    def _close_current_candle(self, close_price: float, bucket_start: float):
        if self.current_open is None:
            return
        completed = {
            "open":       self.current_open,
            "high":       self.current_high,
            "low":        self.current_low,
            "close":      close_price,
            "color":      self._candle_color(self.current_open, close_price),
            "ts_open":    self.current_bucket,
            "ts_close":   self.current_bucket + CANDLE_INTERVAL_SECONDS,
            "tick_count": self.current_tick_count,
        }
        self.candles.append(completed)
        if len(self.candles) > 200:
            self.candles = self.candles[-200:]
        if SHOW_CANDLE_LOGS:
            ts_str = datetime.utcfromtimestamp(self.current_bucket).strftime("%H:%M:%S")
            print(Fore.YELLOW + (
                f"🕯  {self.vol} CANDLE CLOSED | {ts_str} UTC | "
                f"O={completed['open']:.5f}  C={completed['close']:.5f}  "
                f"{'🟢' if completed['color']=='green' else '🔴'}{completed['color'].upper()} | "
                f"ticks={completed['tick_count']}"
            ))
        self.current_open       = None
        self.current_high       = None
        self.current_low        = None
        self.current_close      = None
        self.current_bucket     = 0.0
        self.signal_fired       = False
        self.current_tick_count = 0

    def push_tick(self, price: float, epoch: float):
        """Returns (direction, barrier) when signal fires, else (None, None)."""
        with self.lock:
            bucket = _candle_bucket(epoch)

            if self.current_open is None:
                self.current_open = self.current_high = self.current_low = self.current_close = price
                self.current_bucket = bucket
                self.last_price = price
                return None, None

            if bucket > self.current_bucket:
                close_price = self.current_close if self.current_close is not None else price
                self._close_current_candle(close_price, bucket)
                self.current_open = self.current_high = self.current_low = self.current_close = price
                self.current_bucket = bucket
                self.last_price = price
                return None, None

            if bucket < self.current_bucket:
                return None, None

            if price > self.current_high:
                self.current_high = price
            if price < self.current_low:
                self.current_low = price
            self.current_close = price
            self.current_tick_count += 1

            if self.last_price is None:
                self.last_price = price
                return None, None

            delta = price - self.last_price
            self.last_price = price

            # Record every raw tick movement (regardless of run/pending state)
            # so we always have an up-to-date, self-adjusting picture of what
            # a "normal" tick move looks like for this symbol right now.
            self.recent_deltas.append(delta)

            # ── Tick % confirmation (body-filtered) ──
            # Every consecutive comparison is recorded — Higher, Lower, AND
            # Equal — regardless of signal_fired/trend/cooldown state, so the
            # rolling window always reflects the true last-N *meaningful*
            # comparisons with no skipped or duplicated ticks.
            # deque(maxlen=TICK_PCT_WINDOW) automatically evicts the oldest
            # comparison once full.
            #
            # A tick only counts as a real "up"/"down" vote if it has a real
            # body — i.e. |delta| is at least TICK_BODY_MIN_RATIO of this
            # symbol's own recent tick-size baseline. Small/noise ticks that
            # don't clear that bar are classed as "equal" instead: they still
            # occupy a window slot (so the window stays a true rolling last-N
            # ticks), but they can never push pct_higher/pct_lower toward the
            # 100% dominance threshold the way a genuine directional tick can.
            body_baseline = self._adaptive_tick_baseline()
            has_body = (
                body_baseline is None
                or body_baseline <= 0
                or abs(delta) >= TICK_BODY_MIN_RATIO * body_baseline
            )

            if delta > 0 and has_body:
                self.tick_window.append("up")
            elif delta < 0 and has_body:
                self.tick_window.append("down")
            else:
                self.tick_window.append("equal")

            window_full = len(self.tick_window) == TICK_PCT_WINDOW
            higher = sum(1 for d in self.tick_window if d == "up")
            lower  = sum(1 for d in self.tick_window if d == "down")
            equal  = sum(1 for d in self.tick_window if d == "equal")
            total  = higher + lower + equal
            pct_higher = (higher / total * 100) if total else 0.0
            pct_lower  = (lower  / total * 100) if total else 0.0
            pct_equal  = (equal  / total * 100) if total else 0.0

            # ── Integrity checks — a signal may NEVER fire unless ALL pass ──
            checks_pass = (
                window_full
                and total == TICK_PCT_WINDOW
                and (higher + lower + equal) == total
                and abs((pct_higher + pct_lower + pct_equal) - 100.0) < 0.01
            )

            if window_full and TICK_DEBUG_PRINT:
                print(Fore.CYAN + (
                    f"[{self.vol}] Window: {TICK_PCT_WINDOW} comparisons\n"
                    f"Higher: {higher} ({pct_higher:.1f}%)\n"
                    f"Lower : {lower} ({pct_lower:.1f}%)\n"
                    f"Equal : {equal} ({pct_equal:.1f}%)\n"
                    f"Total : {total}\n"
                    f"Check : {'PASS' if checks_pass else 'FAIL'}"
                ))

            if self.signal_fired:
                self.pending_reversal = None
                return None, None

            trend = self._trend_direction()
            if trend is None:
                self.pending_reversal = None
                return None, None

            if self._seconds_remaining(epoch) < SKIP_IF_REMAINING_SECS:
                return None, None

            # ══════════════════════════════════════════════════════════
            # STAGE 1 (initial setup) is the rolling 5-tick dominance
            # check above (checks_pass / pct_higher / pct_lower). A pass
            # here is only a CANDIDATE — it is NOT sufficient on its own
            # to trade. It must clear stages 2-9 below first.
            # ══════════════════════════════════════════════════════════

            if self.pending_reversal is not None:
                # ── A 5-tick run was already detected — this tick is a
                # confirmation/continuation tick for that candidate. ──
                pending = self.pending_reversal
                pending["ticks_waited"] += 1
                pending["confirm_deltas"].append(delta)

                result = self._evaluate_reversal(pending)

                fire = (
                    result["reversal_confirmed"]
                    and not result["continuation_high"]
                    and result["independent_pass"] >= REVERSAL_MIN_INDEPENDENT
                    and result["confidence"] >= REVERSAL_CONFIDENCE_MIN
                )
                give_up = (not fire) and pending["ticks_waited"] >= REVERSAL_CONFIRM_MAX_TICKS

                if TICK_DEBUG_PRINT:
                    ind = result["indicators"]
                    rsi_str   = f"{ind['rsi']:.1f}" if ind["rsi"] is not None else "n/a"
                    stoch_str = f"{ind['stochastic']['k']:.1f}" if ind["stochastic"] else "n/a"
                    print(Fore.CYAN + (
                        f"[{self.vol}] validating {pending['original_direction']}-run reversal | "
                        f"tick {pending['ticks_waited']}/{REVERSAL_CONFIRM_MAX_TICKS} | "
                        f"confidence={result['confidence']:.2f} "
                        f"conditions={result['independent_pass']}/9 | "
                        f"RSI={rsi_str} Stoch%K={stoch_str} | "
                        f"reversal_confirmed={result['reversal_confirmed']} "
                        f"continuation_high={result['continuation_high']}"
                    ))

                if fire:
                    direction = "PUT" if pending["original_direction"] == "up" else "CALL"
                    self.signal_fired      = True
                    self.last_signal_epoch = epoch
                    barrier = _get_barrier(self.vol)
                    self.reversal_history.append({
                        "magnitude_ratio":  result["magnitude_ratio"],
                        "exhaustion_score": result["exhaustion_score"],
                        "reversed": True,
                    })
                    self.pending_reversal = None
                    ts_str = datetime.utcfromtimestamp(epoch).strftime("%H:%M:%S")
                    ind = result["indicators"]
                    rsi_str   = f"{ind['rsi']:.1f}" if ind["rsi"] is not None else "n/a"
                    boll_str  = f"{ind['bollinger']['upper']:.4f}/{ind['bollinger']['lower']:.4f}" if ind["bollinger"] else "n/a"
                    stoch_str = f"{ind['stochastic']['k']:.1f}" if ind["stochastic"] else "n/a"
                    print(Fore.GREEN + (
                        f"🎯SIGNAL | {self.vol} | {ts_str} UTC | "
                        f"trend={'🟢' if trend=='green' else '🔴'} | "
                        f"run={pending['original_direction']} confidence={result['confidence']:.2f} "
                        f"conditions={result['independent_pass']}/9 exhaustion={result['exhaustion_score']:.2f} | "
                        f"RSI={rsi_str} BB={boll_str} Stoch%K={stoch_str} | "
                        f"{'▲HIGHER' if direction=='CALL' else '▼LOWER'} | "
                        f"barrier={barrier:.4f}"
                    ))
                    return direction, barrier

                if give_up:
                    self.reversal_history.append({
                        "magnitude_ratio":  result["magnitude_ratio"],
                        "exhaustion_score": result["exhaustion_score"],
                        "reversed": False,
                    })
                    self.pending_reversal = None

                return None, None   # still validating (or just abandoned) — no signal yet

            # ── No candidate in progress — check whether a fresh 5-tick
            # dominant run has just formed. If so, open a validation
            # window instead of trading immediately. ──
            if not checks_pass:
                return None, None          # gate never opens on a failed check

            if pct_higher >= TICK_PCT_THRESHOLD:
                original_direction = "up"      # mostly higher ticks → watching for a LOWER reversal
            elif pct_lower >= TICK_PCT_THRESHOLD:
                original_direction = "down"    # mostly lower ticks  → watching for a HIGHER reversal
            else:
                return None, None              # dominance below target — no candidate

            five_tick_deltas = list(self.recent_deltas)[-TICK_PCT_WINDOW:]
            if len(five_tick_deltas) < TICK_PCT_WINDOW:
                return None, None              # not enough raw history yet to analyse the run

            self.pending_reversal = {
                "original_direction": original_direction,
                "five_tick_deltas":   five_tick_deltas,
                "confirm_deltas":     [],
                "ticks_waited":       0,
                "start_epoch":        epoch,
            }
            if TICK_DEBUG_PRINT:
                print(Fore.CYAN + (
                    f"[{self.vol}] 5-tick {original_direction} run detected — entering "
                    f"reversal validation window (max {REVERSAL_CONFIRM_MAX_TICKS} ticks, "
                    f"ticks_higher={pct_higher:.0f}% ticks_lower={pct_lower:.0f}%)"
                ))
            return None, None

    # ══════════════════════════════════════════════════════════════
    # Multi-stage reversal-signal validation helpers (stages 2-9).
    # A 5-tick same-direction run only becomes a trade if it survives
    # ALL of the checks below — not merely because 5 ticks occurred.
    # ══════════════════════════════════════════════════════════════

    def _adaptive_tick_baseline(self):
        """Stage 9 (adaptive filtering) — average absolute tick movement
        over recent history. Used instead of a fixed hard-coded number so
        'large' vs 'small' is always relative to this symbol's *current*
        behaviour rather than a guessed constant."""
        if len(self.recent_deltas) < 5:
            return None
        mags = [abs(d) for d in self.recent_deltas if d != 0]
        if not mags:
            return None
        return statistics.mean(mags)

    def _movement_strength(self, five_deltas, baseline):
        """Stage 2 — total movement, average movement, and whether the run
        is accelerating or weakening. Weak/insignificant runs score low."""
        mags = [abs(d) for d in five_deltas]
        if not mags:
            return 0.0, False
        avg_move = sum(mags) / len(mags)
        if baseline and baseline > 0:
            ratio = avg_move / baseline
            score = max(0.0, min(1.0, ratio / 1.5))
            significant = ratio >= 0.6
        else:
            # No baseline yet (too little history) — neither reward nor
            # penalise strongly; let the other stages carry more weight.
            score = 0.4
            significant = True
        return score, significant

    def _momentum_exhaustion(self, five_deltas):
        """Stage 3 — compare the most recent tick movements in the run
        against the earlier ones. A shrinking magnitude is evidence the
        directional push is running out of steam (reversal-prone)."""
        mags = [abs(d) for d in five_deltas]
        if len(mags) < 4:
            return 0.0
        early = statistics.mean(mags[:2])
        late  = statistics.mean(mags[-2:])
        if early <= 0:
            return 0.0
        weakening = (early - late) / early
        return max(0.0, min(1.0, weakening))

    def _reversal_confirmation(self, original_direction, confirm_deltas):
        """Stage 4 — has at least one tick AFTER the run actually moved
        against the original direction? The 5-tick run alone is never
        treated as sufficient confirmation."""
        if not confirm_deltas:
            return 0.0, False
        opposing = 0
        for d in confirm_deltas:
            if d == 0:
                continue
            moved_up = d > 0
            if (original_direction == "up" and not moved_up) or \
               (original_direction == "down" and moved_up):
                opposing += 1
        ratio = opposing / len(confirm_deltas)
        return ratio, opposing >= 1

    def _continuation_risk(self, original_direction, confirm_deltas):
        """Stage 5 — how strongly is price still pushing in the ORIGINAL
        direction after the run? High continuation risk should suppress
        a reversal signal even if other conditions look favourable."""
        if not confirm_deltas:
            return 0.5   # no evidence yet either way
        same = 0
        for d in confirm_deltas:
            if d == 0:
                continue
            moved_up = d > 0
            if (original_direction == "up" and moved_up) or \
               (original_direction == "down" and not moved_up):
                same += 1
        return same / len(confirm_deltas)

    def _tick_size_consistency(self, five_deltas, baseline):
        """Stage 6 — distinguish five large directional ticks from five
        tiny ones, scaled to this symbol's own recent tick size rather
        than a fixed threshold. Tiny/noise runs score low; a run that
        shows real, meaningful movement scores higher."""
        if baseline is None or baseline <= 0:
            return 0.5
        mags = [abs(d) for d in five_deltas]
        avg_move = statistics.mean(mags) if mags else 0.0
        ratio = avg_move / baseline
        if ratio < 0.4:
            return 0.15   # noise-level ticks — weak evidence of anything
        if ratio > 3.0:
            return 0.7    # unusually large spike — notable, but riskier
        return min(1.0, ratio / 1.4)

    def _closes(self, n=None):
        """Closed 1-min candle closes, oldest→newest. Excludes the still-
        forming candle — indicators are only computed off confirmed data."""
        closes = [c["close"] for c in self.candles]
        return closes[-n:] if n else closes

    def _rsi(self, period=RSI_PERIOD):
        """Classic Wilder-style RSI over closed candles. None until there's
        enough history to compute a real value."""
        closes = self._closes(period + 1)
        if len(closes) < period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            gains.append(max(change, 0.0))
            losses.append(max(-change, 0.0))
        avg_gain = statistics.mean(gains)
        avg_loss = statistics.mean(losses)
        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _bollinger(self, period=BOLLINGER_PERIOD, num_std=BOLLINGER_STDDEV):
        """Bollinger Bands (SMA ± num_std * population stdev) over closed
        candles. None until there's enough history."""
        closes = self._closes(period)
        if len(closes) < period:
            return None
        mean  = statistics.mean(closes)
        stdev = statistics.pstdev(closes)
        return {
            "mean":  mean,
            "upper": mean + num_std * stdev,
            "lower": mean - num_std * stdev,
            "stdev": stdev,
        }

    def _stochastic(self, period=STOCH_PERIOD, smooth=STOCH_SMOOTH):
        """%K (smoothed) / %D Stochastic Oscillator over closed candle
        highs/lows/closes. None until there's enough history."""
        if len(self.candles) < period + smooth:
            return None
        window_slice = self.candles[-(period + smooth):]
        k_values = []
        for i in range(smooth):
            window = window_slice[i:i + period]
            highs  = [c["high"] for c in window]
            lows   = [c["low"]  for c in window]
            close  = window[-1]["close"]
            hh, ll = max(highs), min(lows)
            k = 50.0 if hh == ll else (close - ll) / (hh - ll) * 100.0
            k_values.append(k)
        return {"k": k_values[-1], "d": statistics.mean(k_values)}

    def _rsi_score(self, original_direction):
        """Stage 10 — RSI overbought/oversold confirmation. An 'up' run
        (watching for a LOWER reversal) scores high when RSI is overbought;
        a 'down' run scores high when RSI is oversold."""
        rsi = self._rsi()
        if rsi is None:
            return 0.5, None
        if original_direction == "up":
            if rsi >= RSI_OVERBOUGHT:
                score = 0.6 + (rsi - RSI_OVERBOUGHT) / 30.0
            else:
                score = max(0.0, (rsi - 50.0) / (RSI_OVERBOUGHT - 50.0)) * 0.6
        else:
            if rsi <= RSI_OVERSOLD:
                score = 0.6 + (RSI_OVERSOLD - rsi) / 30.0
            else:
                score = max(0.0, (50.0 - rsi) / (50.0 - RSI_OVERSOLD)) * 0.6
        return max(0.0, min(1.0, score)), rsi

    def _bollinger_score(self, original_direction):
        """Stage 11 — Bollinger Band exhaustion confirmation. Scores high
        when price has pushed to or through the band on the side matching
        the original run (a classic mean-reversion exhaustion signal)."""
        bands = self._bollinger()
        if bands is None or self.current_close is None:
            return 0.5, None
        width = bands["upper"] - bands["lower"]
        if width <= 0:
            return 0.5, bands
        price = self.current_close
        if original_direction == "up":
            position = (price - bands["upper"]) / width
        else:
            position = (bands["lower"] - price) / width
        return max(0.0, min(1.0, 0.5 + position)), bands

    def _stochastic_score(self, original_direction):
        """Stage 12 — Stochastic Oscillator confirmation. Scores high when
        %K is in the overbought/oversold zone matching the original run,
        with a small bonus if %K has already crossed back through %D."""
        stoch = self._stochastic()
        if stoch is None:
            return 0.5, None
        k, d = stoch["k"], stoch["d"]
        if original_direction == "up":
            base  = max(0.0, min(1.0, (k - 50.0) / (STOCH_OVERBOUGHT - 50.0))) if k >= 50.0 else 0.0
            bonus = 0.15 if k < d else 0.0
        else:
            base  = max(0.0, min(1.0, (50.0 - k) / (50.0 - STOCH_OVERSOLD))) if k <= 50.0 else 0.0
            bonus = 0.15 if k > d else 0.0
        return max(0.0, min(1.0, base + bonus)), stoch

    def _recent_history_score(self, magnitude_ratio, exhaustion_score):
        """Stage 7 — have similar 5-tick sequences (by relative size and
        exhaustion) historically been followed by a reversal or a
        continuation? Falls back to a neutral score when there isn't
        enough history yet to compare against."""
        if not self.reversal_history:
            return 0.5
        similar = [
            h for h in self.reversal_history
            if abs(h["magnitude_ratio"] - magnitude_ratio) < 0.6
            and abs(h["exhaustion_score"] - exhaustion_score) < 0.35
        ]
        pool = similar if len(similar) >= 4 else list(self.reversal_history)
        if not pool:
            return 0.5
        return sum(1 for h in pool if h["reversed"]) / len(pool)

    def _evaluate_reversal(self, pending):
        """Stage 8 — combine every independent condition above into a
        single confidence score. A signal is only worth generating when
        MULTIPLE conditions agree, not because one condition fired."""
        five_deltas    = pending["five_tick_deltas"]
        confirm_deltas = pending["confirm_deltas"]
        baseline       = self._adaptive_tick_baseline()

        strength_score, significant = self._movement_strength(five_deltas, baseline)
        exhaustion_score            = self._momentum_exhaustion(five_deltas)
        reversal_ratio, reversal_confirmed = self._reversal_confirmation(
            pending["original_direction"], confirm_deltas
        )
        continuation_score = self._continuation_risk(pending["original_direction"], confirm_deltas)
        mags = [abs(d) for d in five_deltas]
        avg_move = statistics.mean(mags) if mags else 0.0
        magnitude_ratio  = (avg_move / baseline) if baseline else 1.0
        consistency_score = self._tick_size_consistency(five_deltas, baseline)
        history_score      = self._recent_history_score(magnitude_ratio, exhaustion_score)

        # ── Professional indicator confirmation (stages 10-12) ──
        rsi_score, rsi_val     = self._rsi_score(pending["original_direction"])
        boll_score, boll_val   = self._bollinger_score(pending["original_direction"])
        stoch_score, stoch_val = self._stochastic_score(pending["original_direction"])

        sub_scores = {
            "strength":     strength_score,
            "exhaustion":   exhaustion_score,
            "reversal":     reversal_ratio,
            "continuation": 1.0 - continuation_score,   # low continuation risk = high score
            "consistency":  consistency_score,
            "history":      history_score,
            "rsi":          rsi_score,
            "bollinger":    boll_score,
            "stochastic":   stoch_score,
        }
        weights = {
            "strength": 0.12, "exhaustion": 0.16, "reversal": 0.20,
            "continuation": 0.16, "consistency": 0.08, "history": 0.08,
            "rsi": 0.08, "bollinger": 0.06, "stochastic": 0.06,
        }
        confidence = sum(sub_scores[k] * weights[k] for k in weights)
        independent_pass = sum(1 for v in sub_scores.values() if v >= 0.5)

        return {
            "confidence":         confidence,
            "independent_pass":   independent_pass,
            "reversal_confirmed": reversal_confirmed,
            "continuation_high":  continuation_score >= 0.8,
            "significant":        significant,
            "magnitude_ratio":    magnitude_ratio,
            "exhaustion_score":   exhaustion_score,
            "sub_scores":         sub_scores,
            "indicators": {
                "rsi": rsi_val, "bollinger": boll_val, "stochastic": stoch_val,
            },
        }

    def _streak(self) -> int:
        """Count how many consecutive same-color candles are at the tail."""
        if not self.candles:
            return 0
        color = self.candles[-1]["color"]
        count = 0
        for c in reversed(self.candles):
            if c["color"] == color:
                count += 1
            else:
                break
        return count

    def status_str(self) -> str:
        with self.lock:
            trend     = self._trend_direction()
            now_epoch = time.time()
            remaining = self._seconds_remaining(now_epoch) if self.current_bucket else 0.0
            bucket_ts = (datetime.utcfromtimestamp(self.current_bucket).strftime("%H:%M")
                         if self.current_bucket else "—")
            streak    = self._streak()
            if self.tick_window:
                n = len(self.tick_window)
                up_count = sum(1 for d in self.tick_window if d == "up")
                dn_count = sum(1 for d in self.tick_window if d == "down")
                eq_count = n - up_count - dn_count
                tick_str = (
                    f" ticks[{n}/{TICK_PCT_WINDOW}] "
                    f"up={up_count/n*100:.0f}% dn={dn_count/n*100:.0f}% eq={eq_count/n*100:.0f}%"
                )
            else:
                tick_str = f" ticks[0/{TICK_PCT_WINDOW}]"
            return (
                f"candles={len(self.candles)} bucket={bucket_ts}UTC "
                f"trend={trend or 'none'} streak={streak}"
                f"{tick_str} "
                f"remaining={remaining:.0f}s fired={self.signal_fired}"
            )


# One candle engine per symbol — shared across all slots and both modes
candle_engines: dict = {v: CandleEngine(v) for v in VOLATILITIES}


# ╔══════════════════════════════════════════════════════════════╗
# ║              SHARED TICK WS STATE                            ║
# ╚══════════════════════════════════════════════════════════════╝

shared_last_prices    = {v: deque(maxlen=500) for v in VOLATILITIES}
shared_tick_ts_buffers = {v: deque(maxlen=500) for v in VOLATILITIES}

_shared_ws_obj       = None
_shared_ws_connected = False
_shared_ws_lock      = threading.Lock()
_shared_ws_close_lock = threading.Lock()
_shared_subscribed   = set()
_shared_subscribed_lock = threading.Lock()
_shared_history_loaded  = set()
_shared_history_lock    = threading.Lock()
_shared_last_tick_time  = time.time()
_shared_last_tick_lock  = threading.Lock()

# Per-symbol last-tick timestamps — the single global timestamp above only
# proves *some* symbol is alive; it stays fresh even if one specific symbol's
# subscription silently dies while the others keep ticking. That is exactly
# what let 1HZ100V go stale forever: the watchdog never noticed because the
# other four symbols kept refreshing the shared timestamp. Track each symbol
# separately so a single dead subscription can be detected and repaired.
_shared_symbol_last_tick      = {v: time.time() for v in VOLATILITIES}
_shared_symbol_last_tick_lock = threading.Lock()
_shared_tick_sequence         = {v: 0 for v in VOLATILITIES}
_shared_tick_sequence_lock    = threading.Lock()

_watchdog_last_kick   = {}
_watchdog_kick_lock   = threading.Lock()
_shared_ws_thread_ref = None


def _get_shared_last_tick_time():
    with _shared_last_tick_lock:
        return _shared_last_tick_time


def _set_shared_last_tick_time(t):
    global _shared_last_tick_time
    with _shared_last_tick_lock:
        _shared_last_tick_time = t


def _get_symbol_last_tick_time(vol):
    with _shared_symbol_last_tick_lock:
        return _shared_symbol_last_tick.get(vol, 0.0)


def _set_symbol_last_tick_time(vol, t):
    with _shared_symbol_last_tick_lock:
        _shared_symbol_last_tick[vol] = t


def _get_tick_sequence(vol):
    with _shared_tick_sequence_lock:
        return _shared_tick_sequence.get(vol, 0)


def _advance_tick_sequence(vol):
    with _shared_tick_sequence_lock:
        _shared_tick_sequence[vol] = _shared_tick_sequence.get(vol, 0) + 1
        return _shared_tick_sequence[vol]


# ╔══════════════════════════════════════════════════════════════╗
# ║              TOKEN VALIDITY  (NEW MODE ONLY)                 ║
# ╚══════════════════════════════════════════════════════════════╝

def _get_valid_access_token(chat_id):
    with _oauth_tokens_lock:
        tok = _oauth_tokens.get(chat_id)
    if not tok:
        return None, "No token on file — user must /login again"
    if time.time() < tok.get("expires_at", 0):
        return tok["access_token"], None
    # PATs are static and don't refresh — if we ever hit an expiry here, re-login is required.
    return None, "Token expired — user must /login again"


def _save_oauth_tokens():
    tmp = OAUTH_TOKENS_FILE + ".tmp"
    try:
        with _oauth_tokens_lock:
            data = {str(k): v for k, v in _oauth_tokens.items()}
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, OAUTH_TOKENS_FILE)
    except Exception as e:
        print(Fore.RED + f"❌ save_oauth_tokens error: {e}")


def _load_oauth_tokens():
    if not os.path.exists(OAUTH_TOKENS_FILE):
        return
    try:
        with open(OAUTH_TOKENS_FILE) as f:
            data = json.load(f)
        with _oauth_tokens_lock:
            for k, v in data.items():
                _oauth_tokens[int(k)] = v
        print(Fore.GREEN + f"✅ OAuth tokens loaded: {len(_oauth_tokens)} entries")
    except Exception as e:
        print(Fore.RED + f"❌ load_oauth_tokens error: {e}")


def _get_otp_ws_url(chat_id, timeout=15):
    """Fetch a per-connection authenticated WS URL via the NEW API's OTP endpoint."""
    access_token, err = _get_valid_access_token(chat_id)
    if err:
        return None, err
    s          = get_slot_by_chat(chat_id)
    account_id = s.get("account_id", "") if s else ""
    if not account_id:
        return None, "Missing account_id — please /login again."
    try:
        r = requests.post(
            f"{NEW_REST_BASE}/trading/v1/options/accounts/{account_id}/otp",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Deriv-App-ID":  str(NEW_APP_ID),
                "Content-Type":  "application/json",
            },
            timeout=timeout,
        )
        if r.status_code == 401:
            return None, "OTP: Unauthorized — try /login to re-authenticate."
        if not r.ok:
            try:
                err_body = r.json()
                errors   = err_body.get("errors", [])
                err_msg  = errors[0].get("message", r.text[:200]) if errors else r.text[:200]
            except Exception:
                err_msg = r.text[:200]
            return None, f"OTP HTTP {r.status_code}: {err_msg}"
        data   = r.json()
        ws_url = data.get("data", {}).get("url", "")
        if not ws_url:
            return None, f"OTP missing data.url: {data}"
        return ws_url, None
    except requests.exceptions.Timeout:
        return None, "OTP request timed out."
    except Exception as e:
        return None, f"OTP error: {e}"


# ╔══════════════════════════════════════════════════════════════╗
# ║              ACCOUNT FETCH  (NEW MODE ONLY)                  ║
# ╚══════════════════════════════════════════════════════════════╝

def _fetch_accounts(access_token, timeout=20):
    """
    Fetch accounts from the NEW Deriv REST API.
    Handles both `data` as a list directly OR `data` as a dict with `accounts`/`account_list`.
    """
    try:
        r = requests.get(
            f"{NEW_REST_BASE}/trading/v1/options/accounts",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Deriv-App-ID":  str(NEW_APP_ID),
                "Content-Type":  "application/json",
            },
            timeout=(8, timeout),
        )
        print(f"[fetch_accounts] status={r.status_code} body={r.text[:400]}")
        if r.status_code == 401:
            return False, "Unauthorized — token invalid or expired."
        if not r.ok:
            try:
                err = r.json().get("errors", [{}])[0].get("message", r.text[:200])
            except Exception:
                err = r.text[:200]
            return False, f"HTTP {r.status_code}: {err}"
        data = r.json()
        raw  = data.get("data", data)
        if isinstance(raw, list):
            accounts = raw
        elif isinstance(raw, dict):
            accounts = (
                raw.get("accounts") or raw.get("account_list") or raw.get("items")
                or ([raw] if any(k in raw for k in ("account_id", "loginid", "id")) else [])
            )
        else:
            accounts = []
        if not accounts:
            loginid  = data.get("loginid") or data.get("account_id") or "unknown"
            accounts = [{
                "account_id": loginid, "loginid": loginid,
                "currency":   data.get("currency", "USD"),
                "balance":    float(data.get("balance", 0) or 0),
                "is_virtual": bool(data.get("is_virtual", 0)),
            }]
        for a in accounts:
            if "account_id" not in a and "loginid" in a:
                a["account_id"] = a["loginid"]
        return True, accounts
    except requests.exceptions.Timeout:
        return False, "Request timed out."
    except Exception as e:
        return False, f"Request error: {e}"


def _build_account_choice_msg(username, real_list, demo_list):
    def _acct_lines(accounts):
        lines = []
        for i, a in enumerate(accounts, 1):
            aid = a.get("account_id") or a.get("loginid", "?")
            bal = float(a.get("balance", 0) or 0)
            cur = a.get("currency", "USD")
            lines.append(f"    {i}. <code>{aid}</code>  —  <b>{bal:,.2f} {cur}</b>")
        return "\n".join(lines)

    n_real = len(real_list)
    n_demo = len(demo_list)
    total  = n_real + n_demo
    msg    = f"✅  <b>Deriv Login Successful!</b>  Welcome, <b>{html_lib.escape(str(username))}</b>!\n\n"
    msg   += f"Found <b>{total} account{'s' if total != 1 else ''}</b>.\n\n"
    msg   += "Which account type do you want to trade with?\n\n"
    if real_list:
        msg += f"1️⃣  💳 <b>Real Account{'s' if n_real > 1 else ''}:</b>\n{_acct_lines(real_list)}\n\n"
    else:
        msg += "1️⃣  💳 <b>Real Account:</b>  <i>(none found)</i>\n\n"
    if demo_list:
        msg += f"2️⃣  🎮 <b>Demo Account{'s' if n_demo > 1 else ''}:</b>\n{_acct_lines(demo_list)}\n\n"
    else:
        msg += "2️⃣  🎮 <b>Demo Account:</b>  <i>(none found)</i>\n\n"
    msg += "Reply <b>1</b> for Real  or  <b>2</b> for Demo."
    return msg


def _finalize_oauth_login(chat_id, username, chosen_account, tokens):
    account_id = chosen_account.get("account_id") or chosen_account.get("loginid", "")
    balance    = float(chosen_account.get("balance", 0) or 0)
    currency   = chosen_account.get("currency", "USD")
    is_virtual = bool(chosen_account.get("is_virtual", False))
    acct_type  = "🎮 Demo" if is_virtual else "💳 Real"

    # Store account_id in oauth_tokens for OTP requests
    with _oauth_tokens_lock:
        if chat_id in _oauth_tokens:
            _oauth_tokens[chat_id]["account_id"] = account_id
        else:
            _oauth_tokens[chat_id] = {**tokens, "account_id": account_id}
    _save_oauth_tokens()

    existing = get_slot_by_chat(chat_id)
    if existing:
        existing["account_id"] = account_id
        existing["username"]   = username
        existing["deriv_mode"] = "new"
        save_slots()
        tg_send(
            f"✅  <b>Re-Login Successful!</b>\n\n"
            f"┌─────────────────────────\n"
            f"│  💼  <code>{account_id}</code>  {acct_type}\n"
            f"│  💰  Balance: <code>{balance:.2f} {currency}</code>\n"
            f"│  🔄  Reconnecting to Deriv…\n"
            f"└─────────────────────────",
            chat_id=chat_id
        )
        _safe_close_trade_ws(existing)
        return

    free_s = next_free_slot()
    if free_s is None:
        tg_send("❌  All slots are full. Contact admin.", chat_id=chat_id)
        return

    pending_mode = _pending_martingale_modes.pop(chat_id, MARTINGALE_MODE_IMMEDIATE)

    free_s["chat_id"]         = chat_id
    free_s["username"]        = username
    free_s["account_id"]      = account_id
    free_s["api_token"]       = "__oauth__"   # sentinel — actual auth via _oauth_tokens
    free_s["deriv_mode"]      = "new"
    free_s["registered_at"]   = datetime.now().isoformat()
    free_s["last_day"]        = datetime.now().date()
    free_s["martingale_mode"] = pending_mode
    free_s["setup_complete"]  = False  # onboarding wizard (contract/trade/virtual mode) not finished yet

    with free_s["martingale_lock"]:
        for v in VOLATILITIES:
            free_s["symbol_martingale"][v]["next_stake"] = free_s["base_stake"]

    save_slots()
    remaining = free_slot_count()

    # Ask which contract type this slot should trade before starting it.
    with pending_cmd_lock:
        pending_command[chat_id] = {
            "cmd": "choose_contract_type_on_login", "step": 1,
            "username": username, "slot_id": free_s["slot_id"],
            "account_id": account_id, "acct_type": acct_type,
            "balance": balance, "currency": currency,
            "pending_mode": pending_mode, "remaining": remaining,
        }
    tg_send(
        f"✅  <b>Account Connected!</b>\n\n"
        f"┌─────────────────────────\n"
        f"│  🎰  Slot <b>#{free_s['slot_id']}</b>  │  🆕 New API\n"
        f"│  💼  <code>{account_id}</code>  {acct_type}\n"
        f"│  💰  Balance: <b>{balance:,.2f} {currency}</b>\n"
        f"│  🔁  Martingale: <b>{'⚡ Immediate' if pending_mode == MARTINGALE_MODE_IMMEDIATE else '🔍 Signal'}</b>\n"
        f"│  🎫  Slots left: <b>{remaining}</b>\n"
        f"└─────────────────────────\n\n"
        f"Choose your <b>contract type</b>:\n\n"
        f"<b>1</b> — 📈 <b>RISE/FALL</b> — no barrier, 1-tick minimum\n"
        f"<b>2</b> — 🎯 <b>HIGHER/LOWER</b> — barrier-based, payout-targeted, 5-tick minimum\n\n"
        f"Send <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║                     SLOT HELPERS                             ║
# ╚══════════════════════════════════════════════════════════════╝

def _blank_symbol_stats():
    return {
        "total_trades": 0, "wins": 0, "losses": 0, "streak": 0,
        "double_losses": 0, "triple_losses": 0, "more_losses": 0,
        "higher_trades": 0, "lower_trades": 0, "trade_log": [],
    }


def _blank_symbol_martingale(base_stake):
    return {
        "next_stake": base_stake, "level": 0, "in_martingale": False,
        "recovery_direction": None, "recovery_barrier": None,
        "recovery_active": False, "recovery_contract_mode": None,
        "pending_recovery": False, "reentry_pending": False,
        "reentry_direction": None, "reentry_barrier": None,
    }


def _blank_symbol_virtual(active):
    return {"active": active, "wins": 0, "losses": 0}


def _reset_all_symbol_virtual(s, active):
    """Reset every symbol's virtual-trading state independently — used when
    virtual mode is (re)enabled or disabled for the whole slot (onboarding
    wizard, /setvirtual). Each symbol still earns/loses its way to real
    trading on its own after this; a bad run on one symbol never forces
    another symbol into real money (and vice versa)."""
    s["symbol_virtual"] = {v: _blank_symbol_virtual(active) for v in VOLATILITIES}


def _init_slot_runtime(s):
    s.setdefault("ws",               None)
    s.setdefault("ws_thread",        None)
    s.setdefault("ws_restart_flag",  False)
    s.setdefault("ws_connected",     False)
    s.setdefault("trading_active",   True)
    s.setdefault("tp_reached_today", False)
    s.setdefault("daily_profit",     0.0)
    s.setdefault("current_balance",  0.0)
    s.setdefault("last_day",         None)
    s.setdefault("last_tick_time",   time.time())
    s.setdefault("martingale_mode",  MARTINGALE_MODE_IMMEDIATE)
    s.setdefault("contract_mode",    DEFAULT_CONTRACT_MODE)  # existing slots default to HIGHER/LOWER — unchanged behaviour
    s.setdefault("higher_lower_recovery_mode", DEFAULT_HIGHER_LOWER_RECOVERY)
    s.setdefault("trade_mode",       DEFAULT_TRADE_MODE)     # existing slots default to REVERSAL — unchanged behaviour
    s.setdefault("virtual_mode",       DEFAULT_VIRTUAL_MODE)
    s.setdefault("virtual_loss_limit", DEFAULT_VIRTUAL_LOSS_LIMIT)

    # Per-symbol virtual-trading state: each of the 8 volatility symbols this
    # slot trades proves itself in virtual mode independently, and only that
    # ONE symbol shifts to real money once IT racks up virtual_loss_limit
    # losses of its own. Previously this was a single slot-wide counter, so
    # a loss streak on one symbol could silently flip every other symbol
    # (including ones doing fine in virtual) over to real trading too.
    if "symbol_virtual" not in s:
        # Migrate an old slot-wide virtual_active/wins/losses save (if any)
        # onto every symbol so upgraded slots don't lose their state.
        legacy_active = s.pop("virtual_active", False)
        legacy_wins   = s.pop("virtual_wins", 0)
        legacy_losses = s.pop("virtual_losses", 0)
        s["symbol_virtual"] = {
            v: {"active": legacy_active, "wins": legacy_wins, "losses": legacy_losses}
            for v in VOLATILITIES
        }
    else:
        for v in VOLATILITIES:
            if v not in s["symbol_virtual"]:
                s["symbol_virtual"][v] = _blank_symbol_virtual(False)
            else:
                sv = s["symbol_virtual"][v]
                sv.setdefault("active", False)
                sv.setdefault("wins", 0)
                sv.setdefault("losses", 0)
        s.pop("virtual_active", None)
        s.pop("virtual_wins", None)
        s.pop("virtual_losses", None)
    s.setdefault("deriv_mode",       "new")
    s.setdefault("account_id",       "")
    # Per-slot duration (ticks) and martingale multiplier — configurable at
    # login (and later via /setduration, /setmartingalevalue). None/missing
    # means this slot predates the setting, so it falls back to the
    # contract type's original default via _slot_duration_ticks().
    s.setdefault("duration_ticks",        None)
    s.setdefault("martingale_multiplier", MARTINGALE)
    # Default True so slots persisted before this flag existed (already fully
    # configured under the old code) keep auto-starting as before; only
    # brand-new slots created via _make_empty_slot start out False.
    s.setdefault("setup_complete",   True)

    s["contracts_open"]     = {v: None for v in VOLATILITIES}
    s["contract_cooldown"]  = {v: 0    for v in VOLATILITIES}
    s["contract_opened_at"] = {v: 0.0  for v in VOLATILITIES}

    base = s.get("base_stake", DEFAULT_BASE_STAKE)
    if "symbol_martingale" not in s:
        s["symbol_martingale"] = {v: _blank_symbol_martingale(base) for v in VOLATILITIES}
    else:
        for v in VOLATILITIES:
            if v not in s["symbol_martingale"]:
                s["symbol_martingale"][v] = _blank_symbol_martingale(base)
            else:
                sm = s["symbol_martingale"][v]
                sm.setdefault("recovery_direction", None)
                sm.setdefault("recovery_barrier",   None)
                sm.setdefault("recovery_active",    False)
                sm.setdefault("recovery_contract_mode", None)
                sm.setdefault("pending_recovery",   sm.get("in_martingale", False))
                sm.setdefault("reentry_pending",    False)
                sm.setdefault("reentry_direction",  None)
                sm.setdefault("reentry_barrier",    None)

    s["martingale_lock"]     = threading.Lock()
    s["pending_lock"]        = threading.Lock()
    s["trade_map_lock"]      = threading.Lock()
    s["ws_close_lock"]       = threading.Lock()
    s["contract_lock"]       = threading.Lock()
    s["trade_ws_close_lock"] = threading.Lock()

    if "daily_stats" not in s:
        s["daily_stats"] = {v: _blank_symbol_stats() for v in VOLATILITIES}

    s["trade_map"] = {}
    # Every buy request is kept with its complete metadata until Deriv sends
    # the buy response.  Some websocket responses have a missing/empty
    # echo_req, so mapping only by echo_req can leave an accepted contract
    # without a trade_map entry.  That is the main cause of a taken contract
    # never reaching the result printer.
    s["pending_buys"] = {}
    s["pending_buys_lock"] = threading.Lock()
    s["contract_poll_times"] = {}
    s["contract_poll_lock"] = threading.Lock()
    s["processed_contract_results"] = set()
    s["processed_contract_results_order"] = deque()
    s["processed_results_lock"] = threading.Lock()
    # contract_id -> {"vol":, "since": epoch} for confirmed contracts whose
    # trading *slot* was force-freed by _sweep_stuck_contracts because they
    # took unusually long to resolve. The trade_map entry is intentionally
    # KEPT (not deleted) when this happens, so the eventual WIN/LOSS still
    # reaches the console; this dict just lets the watchdog actively
    # re-poll / eventually give up (with a visible log line) instead of
    # waiting forever and leaking memory.
    s["orphaned_contracts"] = {}


def _make_empty_slot(slot_id):
    s = {
        "slot_id":          slot_id,
        "chat_id":          None,
        "username":         None,
        "api_token":        None,
        "account_id":       "",
        "base_stake":       DEFAULT_BASE_STAKE,
        "registered_at":    None,
        "martingale_mode":  MARTINGALE_MODE_IMMEDIATE,
        "contract_mode":    DEFAULT_CONTRACT_MODE,
        "higher_lower_recovery_mode": DEFAULT_HIGHER_LOWER_RECOVERY,
        "trade_mode":       DEFAULT_TRADE_MODE,
        "virtual_mode":       DEFAULT_VIRTUAL_MODE,
        "virtual_loss_limit": DEFAULT_VIRTUAL_LOSS_LIMIT,
        "deriv_mode":       "new",
        "setup_complete":   False,  # gate: don't auto-start trading until the onboarding wizard finishes
    }
    _init_slot_runtime(s)
    return s


def get_slot_by_chat(chat_id):
    for s in slots.values():
        if s["chat_id"] == chat_id:
            return s
    return None


def free_slot_count():
    return sum(1 for s in slots.values() if s["chat_id"] is None)


def next_free_slot():
    for i in range(1, MAX_SLOTS + 1):
        if slots[i]["chat_id"] is None:
            return slots[i]
    return None


# ╔══════════════════════════════════════════════════════════════╗
# ║              CONTRACT SLOT MANAGEMENT                        ║
# ╚══════════════════════════════════════════════════════════════╝

def _open_contract_slot(s, vol, contract_type, stake, barrier):
    """
    Atomically reserve the account's single contract slot.

    The lock is intentionally held while checking every symbol and reserving
    the new one. Signal callbacks can arrive concurrently for different
    symbols, so a separate open-count check followed by a reservation would
    allow two signals to pass the check at the same time.
    """
    with s["contract_lock"]:
        open_count = sum(
            1 for info in s["contracts_open"].values()
            if info is not None
        )
        if open_count >= MAX_CONCURRENT_CONTRACTS:
            return False

        s["contracts_open"][vol] = {
            "contract_type": contract_type,
            "stake": stake,
            "barrier": barrier,
            "pending": True,
        }
        s["contract_opened_at"][vol] = time.time()
    return True


def _close_contract_slot(s, vol, reason="normal"):
    with s["contract_lock"]:
        was_open = s["contracts_open"][vol] is not None
        s["contracts_open"][vol]     = None
        s["contract_opened_at"][vol] = 0.0
    if was_open and reason != "normal":
        print(Fore.YELLOW + f"⚠ Slot{s['slot_id']} {vol} contract slot force-cleared ({reason})")


ORPHAN_REPOLL_SECONDS = 20   # how often to actively re-ask Deriv about an orphaned contract
ORPHAN_HARD_TIMEOUT   = 900  # give up watching an orphaned contract after this long (15 min)


def _sweep_stuck_contracts(s):
    """
    Force-frees a slot's trading capacity when a contract is taking too
    long, WITHOUT discarding the contract's result tracking.

    Previous behaviour deleted the trade_map entry (and therefore any
    memory of the contract_id) the moment the timeout hit. That meant:
      - if the trade genuinely hadn't resolved yet, its WIN/LOSS push
        from Deriv had nowhere to land once it finally arrived, and
      - if a WS reconnect happened after the wipe, the on_open()
        resubscribe loop (which only resubscribes contract_ids still
        present in trade_map) would never resubscribe to it either.
    Net effect: some taken contracts silently never printed a result.

    Fix: only the PENDING (never-confirmed) case is safe to fully drop —
    there's no contract_id yet, so nothing to wait for. A CONFIRMED
    contract (has a contract_id) that's just slow is left tracked in
    trade_map; only the local "slot" bookkeeping is freed so a new trade
    can be taken. The orphan is then watched by
    _service_orphaned_contracts() until it resolves or is hard-timed-out.
    """
    now = time.time()
    cleared = []
    for vol in VOLATILITIES:
        info = s["contracts_open"].get(vol)
        if info is None:
            continue
        opened_at = s["contract_opened_at"].get(vol, 0)
        if opened_at == 0:
            _close_contract_slot(s, vol, "no-timestamp")
            s["contract_cooldown"][vol] = COOLDOWN_TICKS
            cleared.append(vol)
            continue
        age     = now - opened_at
        pending = info.get("pending")
        timeout = PENDING_CONTRACT_TIMEOUT if pending else OPEN_CONTRACT_TIMEOUT
        if age > timeout:
            cid = info.get("contract_id")
            _close_contract_slot(s, vol, f"timeout-{age:.0f}s")
            s["contract_cooldown"][vol] = COOLDOWN_TICKS
            if pending or not cid:
                # Never got a confirmed contract_id back — genuinely
                # nothing to keep waiting on.
                with s["trade_map_lock"]:
                    s["trade_map"].pop(f"PENDING_{vol}", None)
            else:
                cid_str = str(cid)
                with s["trade_map_lock"]:
                    still_tracked = cid_str in s["trade_map"]
                if still_tracked:
                    s["orphaned_contracts"].setdefault(cid_str, {
                        "vol": vol, "since": now, "last_repoll": 0.0,
                    })
                    print(Fore.YELLOW + (
                        f"⏳ Slot{s['slot_id']} {vol} contract_id={cid_str} still unresolved "
                        f"after {age:.0f}s — slot freed for new trades, still watching for its result"
                    ), flush=True)
            cleared.append(vol)
    if cleared:
        print(Fore.YELLOW + f"⚠ Slot{s['slot_id']} force-cleared stuck contract slot(s): {cleared}")


def _service_orphaned_contracts(s, ws_obj):
    """
    Called periodically from the watchdog. For every contract that
    _sweep_stuck_contracts freed the *slot* for but kept tracking:
      - every ORPHAN_REPOLL_SECONDS, actively re-request its status
        (in case the original push subscription silently died), and
      - if it's been unresolved for longer than ORPHAN_HARD_TIMEOUT,
        give up with a loud, explicit console line instead of just
        letting it vanish — so nothing is ever "taken but never shown",
        even in the worst case.
    """
    if not s["orphaned_contracts"]:
        return
    now = time.time()
    give_up = []
    for cid_str, meta in list(s["orphaned_contracts"].items()):
        with s["trade_map_lock"]:
            still_tracked = cid_str in s["trade_map"]
        if not still_tracked:
            # Result already arrived and was processed normally.
            s["orphaned_contracts"].pop(cid_str, None)
            continue

        age = now - meta["since"]
        if age > ORPHAN_HARD_TIMEOUT:
            give_up.append(cid_str)
            continue

        if now - meta.get("last_repoll", 0) >= ORPHAN_REPOLL_SECONDS and ws_obj is not None:
            meta["last_repoll"] = now
            try:
                ws_obj.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": int(cid_str),
                    "subscribe": 1,
                }))
            except Exception as e:
                print(Fore.RED + (
                    f"❌ Slot{s['slot_id']} [NEW] orphan re-poll error "
                    f"contract_id={cid_str}: {e}"
                ))

    for cid_str in give_up:
        meta = s["orphaned_contracts"].pop(cid_str, None)
        with s["trade_map_lock"]:
            info = s["trade_map"].pop(cid_str, None)
        vol = (meta or {}).get("vol") or (info or {}).get("vol") or "?"
        print(Fore.RED + (
            f"❓ RESULT NEVER CONFIRMED | Slot{s['slot_id']} | {vol} | "
            f"contract_id={cid_str} — gave up after {ORPHAN_HARD_TIMEOUT}s with no "
            f"resolution from Deriv. Check the account's statement/portfolio manually."
        ), flush=True)


def _clear_all_pending_contracts(s):
    """
    Called when the trade WS disconnects. Only wipes contracts that never
    got a confirmed contract_id back from Deriv (buy still in flight) —
    those are genuinely ambiguous. Contracts that already have a contract_id
    are LEFT tracked in contracts_open/trade_map so on_open() can
    re-subscribe to them after reconnect and still report WIN/LOSS to the
    console instead of the result silently vanishing while the trade
    resolves on Deriv's side.
    """
    cleared = []
    with s["contract_lock"]:
        for v in VOLATILITIES:
            info = s["contracts_open"].get(v)
            if info is not None and info.get("pending"):
                s["contracts_open"][v]     = None
                s["contract_opened_at"][v] = 0.0
                cleared.append(v)
    with s["trade_map_lock"]:
        for k in [k for k in list(s["trade_map"].keys()) if str(k).startswith("PENDING_")]:
            s["trade_map"].pop(k, None)
    if cleared:
        # Any virtual contract in flight for these symbols is now ambiguous too
        # (its reservation was just freed) — drop it rather than risk it
        # settling later against a slot a newer trade has since taken over.
        with _virtual_contracts_lock:
            for v in cleared:
                _virtual_contracts[v] = [
                    vc for vc in _virtual_contracts.get(v, []) if vc["slot_id"] != s["slot_id"]
                ]
        print(Fore.YELLOW + f"🧹 Slot{s['slot_id']} cleared {len(cleared)} unconfirmed pending contract(s) on disconnect: {cleared}")


# ╔══════════════════════════════════════════════════════════════╗
# ║              ACCESS EXPIRY                                   ║
# ╚══════════════════════════════════════════════════════════════╝

def _expiry_remaining_str(expiry_ts):
    if expiry_ts is None:
        return "♾️  Never (Admin)"
    remaining = expiry_ts - time.time()
    if remaining <= 0:
        return "⛔ EXPIRED"
    hours_left = int(remaining // 3600)
    mins_left  = int((remaining % 3600) // 60)
    if hours_left >= 24:
        days = hours_left // 24
        hrs  = hours_left % 24
        return f"⏳ {days}d {hrs}h {mins_left}m"
    elif hours_left > 0:
        return f"⏳ {hours_left}h {mins_left}m"
    else:
        return f"⏳ {mins_left}m"


def _is_expired(chat_id):
    expiry = authorized_ids.get(chat_id)
    if expiry is None:
        return False
    return time.time() > expiry


def _revoke_expired_users():
    expired = [cid for cid, exp in list(authorized_ids.items()) if exp is not None and time.time() > exp]
    for cid in expired:
        authorized_ids.pop(cid, None)
        with slots_lock:
            for s in slots.values():
                if s["chat_id"] == cid:
                    _safe_close_trade_ws(s)
                    slots[s["slot_id"]] = _make_empty_slot(s["slot_id"])
                    break
        tg_send(
            f"⛔  <b>Access Expired</b>\n\nYour access to <b>JAHIM BOT</b> has ended.\n\n"
            f"Contact {ADMIN_USERNAME} to renew.", chat_id=cid
        )
    if expired:
        save_admin()
        save_slots()


# ╔══════════════════════════════════════════════════════════════╗
# ║              PERSISTENCE                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def _serialise_slot(s):
    return {
        "slot_id":           s["slot_id"],
        "chat_id":           s["chat_id"],
        "username":          s["username"],
        "api_token":         s["api_token"],
        "account_id":        s.get("account_id", ""),
        "base_stake":        s["base_stake"],
        "martingale_multiplier": s.get("martingale_multiplier", MARTINGALE),
        "registered_at":     s["registered_at"],
        "trading_active":    s["trading_active"],
        "tp_reached_today":  s["tp_reached_today"],
        "daily_profit":      s["daily_profit"],
        "current_balance":   s["current_balance"],
        "last_day":          str(s["last_day"]) if s["last_day"] else None,
        "martingale_mode":   s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE),
        "contract_mode":     s.get("contract_mode", DEFAULT_CONTRACT_MODE),
        "higher_lower_recovery_mode": s.get(
            "higher_lower_recovery_mode", DEFAULT_HIGHER_LOWER_RECOVERY
        ),
        "trade_mode":        s.get("trade_mode", DEFAULT_TRADE_MODE),
        "virtual_mode":       s.get("virtual_mode", DEFAULT_VIRTUAL_MODE),
        "virtual_loss_limit": s.get("virtual_loss_limit", DEFAULT_VIRTUAL_LOSS_LIMIT),
        "symbol_virtual": {
            v: {
                "active": sv.get("active", False),
                "wins":   sv.get("wins", 0),
                "losses": sv.get("losses", 0),
            }
            for v, sv in s.get("symbol_virtual", {}).items()
        },
        "deriv_mode":        s.get("deriv_mode", "new"),
        "setup_complete":    s.get("setup_complete", True),
        "symbol_martingale": {
            v: {
                "next_stake":         sm["next_stake"],
                "level":              sm["level"],
                "in_martingale":      sm["in_martingale"],
                "recovery_direction": sm.get("recovery_direction"),
                "recovery_barrier":   sm.get("recovery_barrier"),
                "recovery_active":    sm.get("recovery_active", False),
                "recovery_contract_mode": sm.get("recovery_contract_mode"),
                "pending_recovery":   sm.get("pending_recovery", sm["in_martingale"]),
                "reentry_pending":    sm.get("reentry_pending", False),
                "reentry_direction":  sm.get("reentry_direction"),
                "reentry_barrier":    sm.get("reentry_barrier"),
            }
            for v, sm in s["symbol_martingale"].items()
        },
        "daily_stats": s["daily_stats"],
    }


def _save_slots_to_disk():
    """The real, synchronous writer. Only call this from the debounced
    saver thread or an explicit shutdown flush — never from a hot path."""
    tmp = SLOTS_FILE + ".tmp"
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        data = {str(i): _serialise_slot(slots[i]) for i in range(1, MAX_SLOTS + 1)}
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, SLOTS_FILE)
    except Exception as e:
        print(Fore.RED + f"❌ save_slots error: {e}")


def save_slots():
    """Fire-and-forget: flags a save as due and returns immediately.
    Safe to call from any thread, including the tick-ingestion path —
    the actual disk write happens on _slots_saver_loop's own thread."""
    _save_pending.set()


SLOTS_SAVE_DEBOUNCE_SECONDS = 0.25


def _slots_saver_loop():
    """Background thread: wakes as soon as a save is requested, waits a
    short debounce window to coalesce any further requests that land in
    the same burst (e.g. several virtual contracts settling back-to-back),
    then does one disk write covering all of them."""
    while not _shutdown_event.is_set():
        _save_pending.wait(timeout=1)
        if not _save_pending.is_set():
            continue
        _shutdown_event.wait(SLOTS_SAVE_DEBOUNCE_SECONDS)
        with _save_pending_lock:
            _save_pending.clear()
            _save_slots_to_disk()


def load_slots():
    global ADMIN_CHAT_ID
    for i in range(1, MAX_SLOTS + 1):
        slots[i] = _make_empty_slot(i)

    if not os.path.exists(SLOTS_FILE):
        print(Fore.YELLOW + "ℹ️  No slots file — all slots empty.")
        return

    try:
        with open(SLOTS_FILE) as f:
            data = json.load(f)
        today_str = str(datetime.now().date())

        for i in range(1, MAX_SLOTS + 1):
            saved = data.get(str(i))
            if not saved:
                continue
            s = slots[i]
            s["slot_id"]        = i
            s["chat_id"]        = saved.get("chat_id")
            s["username"]       = saved.get("username")
            s["api_token"]      = saved.get("api_token")
            s["account_id"]     = saved.get("account_id", "")
            s["base_stake"]     = float(saved.get("base_stake", DEFAULT_BASE_STAKE))
            s["martingale_multiplier"] = float(
                saved.get("martingale_multiplier", MARTINGALE)
            )
            s["registered_at"]  = saved.get("registered_at")
            s["martingale_mode"] = saved.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)
            s["contract_mode"]   = saved.get("contract_mode", DEFAULT_CONTRACT_MODE)
            s["higher_lower_recovery_mode"] = saved.get(
                "higher_lower_recovery_mode", DEFAULT_HIGHER_LOWER_RECOVERY
            )
            s["trade_mode"]      = saved.get("trade_mode", DEFAULT_TRADE_MODE)
            s["virtual_mode"]       = saved.get("virtual_mode", DEFAULT_VIRTUAL_MODE)
            s["virtual_loss_limit"] = saved.get("virtual_loss_limit", DEFAULT_VIRTUAL_LOSS_LIMIT)
            saved_sv = saved.get("symbol_virtual")
            if saved_sv:
                s["symbol_virtual"] = {
                    v: {
                        "active": bool(saved_sv.get(v, {}).get("active", False)),
                        "wins":   int(saved_sv.get(v, {}).get("wins", 0)),
                        "losses": int(saved_sv.get(v, {}).get("losses", 0)),
                    }
                    for v in VOLATILITIES
                }
            else:
                # Old save from before per-symbol tracking existed — migrate
                # the slot-wide values onto every symbol.
                legacy_active = saved.get("virtual_active", False)
                legacy_wins   = saved.get("virtual_wins", 0)
                legacy_losses = saved.get("virtual_losses", 0)
                s["symbol_virtual"] = {
                    v: {"active": legacy_active, "wins": legacy_wins, "losses": legacy_losses}
                    for v in VOLATILITIES
                }
            s["deriv_mode"]     = saved.get("deriv_mode", "new")
            # Slots saved before this flag existed had no onboarding gate at
            # all, so treat them as already configured (default True).
            s["setup_complete"] = saved.get("setup_complete", True)

            saved_day = saved.get("last_day")
            if saved_day == today_str:
                s["trading_active"]   = bool(saved.get("trading_active", True))
                s["tp_reached_today"] = bool(saved.get("tp_reached_today", False))
                s["daily_profit"]     = float(saved.get("daily_profit", 0.0))
                s["current_balance"]  = float(saved.get("current_balance", 0.0))
                s["last_day"]         = datetime.strptime(saved_day, "%Y-%m-%d").date()
                saved_stats = saved.get("daily_stats", {})
                for v in VOLATILITIES:
                    if v in saved_stats:
                        blank = _blank_symbol_stats()
                        blank.update(saved_stats[v])
                        s["daily_stats"][v] = blank
            else:
                s["last_day"]         = datetime.now().date()
                s["daily_profit"]     = 0.0
                s["trading_active"]   = True
                s["tp_reached_today"] = False

            saved_sm = saved.get("symbol_martingale", {})
            base = s["base_stake"]
            for v in VOLATILITIES:
                if v in saved_sm:
                    raw     = saved_sm[v]
                    in_mart = bool(raw.get("in_martingale", False))
                    s["symbol_martingale"][v] = {
                        "next_stake":         float(raw.get("next_stake", base)),
                        "level":              int(raw.get("level", 0)),
                        "in_martingale":      in_mart,
                        "recovery_direction": raw.get("recovery_direction"),
                        "recovery_barrier":   raw.get("recovery_barrier"),
                        "recovery_active":    bool(raw.get("recovery_active", False)),
                        "recovery_contract_mode": raw.get("recovery_contract_mode"),
                        "pending_recovery":   bool(raw.get("pending_recovery", in_mart)),
                        "reentry_pending":    bool(raw.get("reentry_pending", False)),
                        "reentry_direction":  raw.get("reentry_direction"),
                        "reentry_barrier":    raw.get("reentry_barrier"),
                    }
                else:
                    s["symbol_martingale"][v] = _blank_symbol_martingale(base)

        print(Fore.GREEN + "✅ Slots loaded from disk.")
    except Exception as e:
        print(Fore.RED + f"❌ load_slots error: {e} — starting fresh.")

    if os.path.exists(ADMIN_FILE):
        try:
            with open(ADMIN_FILE) as f:
                adata = json.load(f)
            ADMIN_CHAT_ID = adata.get("admin_chat_id", ADMIN_CHAT_ID)
            raw_ids = adata.get("authorized_ids", {})
            if isinstance(raw_ids, list):
                for cid in raw_ids:
                    authorized_ids[int(cid)] = None
            elif isinstance(raw_ids, dict):
                for cid_str, exp in raw_ids.items():
                    authorized_ids[int(cid_str)] = exp
            print(Fore.GREEN + f"👑 Admin: {ADMIN_CHAT_ID}  Authorized: {sorted(authorized_ids.keys())}")
        except Exception as e:
            print(Fore.RED + f"❌ load_admin error: {e}")


def save_admin():
    tmp = ADMIN_FILE + ".tmp"
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(tmp, "w") as f:
            json.dump({
                "admin_chat_id":  ADMIN_CHAT_ID,
                "authorized_ids": {str(k): v for k, v in authorized_ids.items()},
            }, f, indent=2)
        os.replace(tmp, ADMIN_FILE)
    except Exception as e:
        print(Fore.RED + f"❌ save_admin error: {e}")


# ╔══════════════════════════════════════════════════════════════╗
# ║              TELEGRAM HELPERS                                ║
# ╚══════════════════════════════════════════════════════════════╝

def tg_send(text, parse_mode="HTML", chat_id=None, retries=5):
    if not chat_id:
        return
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                f"{TG_API}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": parse_mode},
                timeout=15,
            )
            if r.ok:
                return
            if r.status_code == 429:
                retry_after = r.json().get("parameters", {}).get("retry_after", delay)
                time.sleep(retry_after)
                continue
        except Exception:
            pass
        if attempt < retries:
            time.sleep(min(delay, 30))
            delay = min(delay * 2, 30)


def tg_send_document(filepath, caption="", chat_id=None, retries=4):
    if not chat_id:
        return
    delay = 2
    for attempt in range(1, retries + 1):
        try:
            with open(filepath, "rb") as f:
                r = requests.post(
                    f"{TG_API}/sendDocument",
                    data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                    files={"document": f},
                    timeout=45,
                )
            if r.ok:
                return
        except Exception:
            pass
        if attempt < retries:
            time.sleep(min(delay, 30))
            delay = min(delay * 2, 30)


def tg_send_html_as_file(html_content, filename, caption="", chat_id=None):
    tmp = f"/tmp/{filename}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(html_content)
        tg_send_document(tmp, caption=caption, chat_id=chat_id)
    except Exception as e:
        print(Fore.RED + f"❌ TG html-as-file error: {e}")
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass


def _ws_heartbeat(ws_obj, stop_event, label, interval=30):
    while not stop_event.wait(interval):
        if stop_event.is_set():
            break
        try:
            ws_obj.send(json.dumps({"ping": 1}))
        except Exception:
            break


# ╔══════════════════════════════════════════════════════════════╗
# ║              DAILY STATS                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def reset_daily_stats(s):
    s["daily_stats"] = {v: _blank_symbol_stats() for v in VOLATILITIES}


def record_trade_result(s, vol, contract_type, barrier, stake, profit, result_str, pattern="—", is_reentry=False, is_virtual=False):
    st = s["daily_stats"][vol]
    st["trade_log"].append({
        "time":          datetime.now().strftime("%H:%M:%S"),
        "contract_type": contract_type,
        "barrier":       barrier,
        "stake":         stake,
        "result":        result_str,
        "profit":        profit,
        "pattern":       pattern,
        "is_reentry":    is_reentry,
        "is_virtual":    is_virtual,
    })
    st["total_trades"] += 1
    if contract_type == "CALL":
        st["higher_trades"] = st.get("higher_trades", 0) + 1
    else:
        st["lower_trades"] = st.get("lower_trades", 0) + 1

    if result_str == "WIN":
        st["wins"] += 1
        streak = st["streak"]
        if streak == 2:
            st["double_losses"] += 1
        elif streak == 3:
            st["triple_losses"] += 1
        elif streak >= 4:
            st["more_losses"] += 1
        st["streak"] = 0
    else:
        st["losses"] += 1
        st["streak"] += 1
    save_slots()


def _check_for_new_day(s):
    if s.get("last_day") is None:
        s["last_day"] = datetime.now().date()
        return
    today = datetime.now().date()
    if today != s["last_day"]:
        s["last_day"]         = today
        s["daily_profit"]     = 0.0
        s["trading_active"]   = True
        s["tp_reached_today"] = False
        reset_daily_stats(s)
        for v in VOLATILITIES:
            _close_contract_slot(s, v, "day-rollover")
            s["contract_cooldown"][v] = 0
        with _virtual_contracts_lock:
            for v in VOLATILITIES:
                _virtual_contracts[v] = [
                    vc for vc in _virtual_contracts.get(v, []) if vc["slot_id"] != s["slot_id"]
                ]
        save_slots()


# ╔══════════════════════════════════════════════════════════════╗
# ║              PER-SYMBOL MARTINGALE                           ║
# ╚══════════════════════════════════════════════════════════════╝

def _slot_duration_ticks(s):
    """This slot's configured contract duration in ticks, chosen at login
    (or via /setduration). Falls back to the contract type's original
    default for slots saved before this was configurable, or if the saved
    value is somehow invalid."""
    default = (CONTRACT_DURATION_RISE_FALL
               if s.get("contract_mode", DEFAULT_CONTRACT_MODE) == CONTRACT_MODE_RISE_FALL
               else CONTRACT_DURATION)
    val = s.get("duration_ticks")
    try:
        val = int(val)
    except (TypeError, ValueError):
        return default
    return val if val > 0 else default


def _slot_martingale_multiplier(s):
    """This slot's configured martingale multiplier, chosen at login (or
    via /setmartingalevalue). Falls back to the global MARTINGALE default
    for slots saved before this was configurable, or an invalid value."""
    val = s.get("martingale_multiplier", MARTINGALE)
    try:
        val = float(val)
    except (TypeError, ValueError):
        return MARTINGALE
    return val if val > 1 else MARTINGALE


def on_symbol_win(s, vol):
    sm = s["symbol_martingale"][vol]
    prev_level = sm["level"]
    sm.update({
        "next_stake": s["base_stake"], "level": 0, "in_martingale": False,
        "recovery_direction": None, "recovery_barrier": None, "pending_recovery": False,
        "recovery_active": False, "recovery_contract_mode": None,
        "reentry_pending": False, "reentry_direction": None, "reentry_barrier": None,
    })
    if prev_level > 0:
        print(Fore.GREEN + f"   ✅ {vol} Martingale RESET L{prev_level} → base ${s['base_stake']:.2f}")


def _reset_all_martingale_to_base(s):
    """Reset every symbol's martingale ladder to base stake — used when a
    slot shifts from virtual to real trading, so real money never inherits
    an already-escalated virtual-phase stake."""
    with s["martingale_lock"]:
        for v in VOLATILITIES:
            sm = s["symbol_martingale"][v]
            sm.update({
                "next_stake": s["base_stake"], "level": 0, "in_martingale": False,
                "recovery_direction": None, "recovery_barrier": None, "pending_recovery": False,
                "recovery_active": False, "recovery_contract_mode": None,
                "reentry_pending": False, "reentry_direction": None, "reentry_barrier": None,
            })


def on_symbol_loss(
    s, vol, lost_direction, lost_barrier,
    contract_mode=None, is_recovery_trade=False,
):
    sm   = s["symbol_martingale"][vol]
    mode = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)

    # A HIGHER/LOWER loss can optionally start a separate RISE/FALL
    # recovery cycle. The cycle owns the symbol until a RISE/FALL win;
    # losses inside it continue increasing the same martingale stake.
    use_rise_fall_recovery = (
        s.get("contract_mode", DEFAULT_CONTRACT_MODE) == CONTRACT_MODE_HIGHER_LOWER
        and s.get(
            "higher_lower_recovery_mode", DEFAULT_HIGHER_LOWER_RECOVERY
        ) == HIGHER_LOWER_RECOVERY_RISE_FALL
        and (
            contract_mode == CONTRACT_MODE_HIGHER_LOWER
            or is_recovery_trade
            # Reconciled contracts may have minimal metadata after a
            # reconnect; an already-active recovery cycle still identifies
            # that result as part of RISE/FALL recovery.
            or (contract_mode is None and sm.get("recovery_active", False))
        )
    )

    sm["level"]             += 1
    sm["in_martingale"]      = True
    if use_rise_fall_recovery:
        sm["recovery_active"]        = True
        sm["recovery_contract_mode"] = CONTRACT_MODE_RISE_FALL
        # RISE/FALL recovery trade matches the losing HIGHER/LOWER side
        # directly: LOWER loss -> FALL, HIGHER loss -> RISE (CALL/PUT
        # already mean the same "up"/"down" side in both contract modes).
        # That direction is held fixed for every trade in the recovery
        # cycle — no alternating — while martingale increases the stake,
        # until a win closes the cycle.
        is_recovery_result = (
            is_recovery_trade
            or (contract_mode is None and sm.get("recovery_active", False))
        )
        if not (is_recovery_result and sm.get("recovery_direction")):
            sm["recovery_direction"] = lost_direction
        sm["recovery_barrier"] = 0.0
    else:
        sm["recovery_active"]        = False
        sm["recovery_contract_mode"] = None
        sm["recovery_direction"]     = lost_direction
        sm["recovery_barrier"]       = lost_barrier
    sm["pending_recovery"]   = True
    new_stake                = round(sm["next_stake"] * _slot_martingale_multiplier(s), 2)
    sm["next_stake"]         = new_stake
    if mode == MARTINGALE_MODE_IMMEDIATE:
        sm["reentry_pending"]   = True
        sm["reentry_direction"] = (
            sm["recovery_direction"] if sm.get("recovery_active") else lost_direction
        )
        sm["reentry_barrier"]   = (
            0.0 if sm.get("recovery_active") else lost_barrier
        )
        recovery_label = " | 📈 RISE/FALL RECOVERY" if sm.get("recovery_active") else ""
        print(Fore.RED + f"   ❌ {vol} Martingale L{sm['level']} → ${new_stake:.2f} | ⚡IMMEDIATE RE-ENTRY queued{recovery_label}")
    else:
        sm["reentry_pending"]   = False
        sm["reentry_direction"] = None
        sm["reentry_barrier"]   = None
        recovery_label = " | 📈 RISE/FALL RECOVERY" if sm.get("recovery_active") else ""
        print(Fore.RED + f"   ❌ {vol} Martingale L{sm['level']} → ${new_stake:.2f} | 🔍WAITING for next signal{recovery_label}")
    save_slots()
    return new_stake


def _recover_trade_info_from_open_slot(s, contract_id):
    """Recover minimal trade metadata if a result arrives after a callback/map race."""
    contract_id = str(contract_id)
    for vol in VOLATILITIES:
        with s["contract_lock"]:
            slot_info = s["contracts_open"].get(vol)
            if not slot_info:
                continue
            if str(slot_info.get("contract_id", "")) != contract_id:
                continue
            return {
                "vol": vol,
                "stake": float(slot_info.get("stake", 0.0)),
                "contract_type": slot_info.get("contract_type", "CALL"),
                "barrier": float(slot_info.get("barrier", 0.0)),
                "pattern": "RECOVERED_FROM_OPEN_SLOT",
                "is_reentry": False,
            }
    return None


def _normalise_contract_type(contract_type):
    """Convert Deriv/API names to the internal CALL/PUT representation."""
    value = str(contract_type or "").upper()
    if value in ("HIGHER", "CALL", "RISE"):
        return "CALL"
    if value in ("LOWER", "PUT", "FALL"):
        return "PUT"
    return "CALL"


def _remember_pending_buy(s, req_id, proposal_id, info):
    """Store a buy with enough metadata to recover a response without echo_req."""
    with s["pending_buys_lock"]:
        s["pending_buys"][str(req_id)] = {
            "proposal_id": str(proposal_id),
            "info": dict(info),
            "sent_at": time.time(),
        }


def _take_pending_buy(s, req_id=None, proposal_id=None):
    """
    Pop the matching pending buy.

    Deriv normally returns echo_req.req_id, but the fallback by proposal id
    and then by the only live pending buy protects against incomplete echoes.
    """
    with s["pending_buys_lock"]:
        candidate = None
        if req_id is not None:
            candidate = s["pending_buys"].pop(str(req_id), None)
        if candidate is None and proposal_id is not None:
            for key, item in list(s["pending_buys"].items()):
                if item.get("proposal_id") == str(proposal_id):
                    candidate = s["pending_buys"].pop(key)
                    break
        if candidate is None and len(s["pending_buys"]) == 1:
            key = next(iter(s["pending_buys"]))
            candidate = s["pending_buys"].pop(key)
        return candidate


def _request_portfolio_reconciliation(s, ws_obj):
    """
    Ask Deriv directly for the account's full list of currently-open
    contracts. This is the safety net for the "bot opens a trade but never
    shows the result" bug: if a disconnect/race causes a buy to succeed on
    Deriv's side while the local trade_map never learns the contract_id (or
    the follow-up subscribe silently fails), the trade becomes invisible to
    the bot even though it's live on the account. Calling this periodically
    means every open position gets cross-checked and re-attached within
    PORTFOLIO_RECONCILE_SECONDS, so its WIN/LOSS still reaches the console
    and stats instead of vanishing.
    """
    try:
        ws_obj.send(json.dumps({"portfolio": 1}))
    except Exception as e:
        print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] portfolio request error: {e}")


def _handle_portfolio_response(s, ws_obj, portfolio_data):
    contracts = portfolio_data.get("contracts", []) or []
    recovered = []
    for c in contracts:
        cid = c.get("contract_id")
        if cid is None:
            continue
        cid_str = str(cid)

        with s["trade_map_lock"]:
            already_tracked = cid_str in s["trade_map"]
        if already_tracked:
            continue

        symbol = c.get("symbol") or c.get("underlying") or c.get("underlying_symbol") or ""
        if symbol not in VOLATILITIES:
            continue  # not a contract this slot/strategy trades — leave it alone

        contract_type = _normalise_contract_type(c.get("contract_type", "CALL"))
        stake         = float(c.get("buy_price", 0.0) or 0.0)

        recovered_info = {
            "vol":           symbol,
            "stake":         stake,
            "contract_type": contract_type,
            "barrier":       0.0,
            "pattern":       "RECOVERED_FROM_PORTFOLIO",
            "is_reentry":    False,
        }
        with s["trade_map_lock"]:
            s["trade_map"][cid_str] = recovered_info
        with s["contract_poll_lock"]:
            s["contract_poll_times"][cid_str] = 0.0
        with s["contract_lock"]:
            if s["contracts_open"].get(symbol) is None:
                s["contracts_open"][symbol] = {
                    "contract_type": contract_type, "stake": stake,
                    "barrier": 0.0, "pending": False, "contract_id": cid_str,
                }
                s["contract_opened_at"][symbol] = time.time()

        _subscribe_to_contract_with_retry(s, ws_obj, cid)
        recovered.append((symbol, cid_str, stake))

    if recovered:
        for symbol, cid_str, stake in recovered:
            print(Fore.YELLOW + (
                f"🧩 RECONCILED UNTRACKED CONTRACT | Slot{s['slot_id']} | {symbol} | "
                f"contract_id={cid_str} | stake=${stake:.2f} — now tracking for result"
            ), flush=True)


def _subscribe_to_contract_with_retry(s, ws_obj, contract_id, attempts=3):
    """
    Sends the proposal_open_contract subscribe request with automatic
    retries on transient send failures, so a single flaky send doesn't
    silently orphan a contract for the rest of its lifetime.
    """
    def _run():
        for attempt in range(1, attempts + 1):
            try:
                ws_obj.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": int(contract_id),
                    "subscribe": 1,
                }))
                return
            except Exception as e:
                print(Fore.RED + (
                    f"❌ Slot{s['slot_id']} [NEW] subscribe contract {contract_id} "
                    f"error (attempt {attempt}/{attempts}): {e}"
                ))
                time.sleep(1.5 * attempt)
        print(Fore.RED + (
            f"❌ Slot{s['slot_id']} [NEW] giving up subscribing to contract {contract_id} "
            f"after {attempts} attempts — will retry on next portfolio reconciliation"
        ))
    threading.Thread(target=_run, daemon=True, name=f"resub-{contract_id}").start()


def _service_tracked_contracts(s, ws_obj, interval=3.0):
    """
    Poll every confirmed contract as well as keeping its subscription alive.

    A subscription normally delivers the final snapshot, but the screenshot
    case shows that the server can accept the buy while the stream update is
    lost.  A one-shot proposal_open_contract request is independent of that
    stream and guarantees that a settled contract is still read and printed.
    """
    if ws_obj is None or not s.get("trade_ws_connected"):
        return
    with s["trade_map_lock"]:
        contract_ids = [
            str(cid) for cid in s["trade_map"]
            if not str(cid).startswith(("PENDING_", "PROP_", "BUY_REQ_"))
        ]
    now = time.time()
    for cid_str in contract_ids:
        with s["contract_poll_lock"]:
            last_poll = s["contract_poll_times"].get(cid_str, 0.0)
            if now - last_poll < interval:
                continue
            s["contract_poll_times"][cid_str] = now
        try:
            ws_obj.send(json.dumps({
                "proposal_open_contract": 1,
                "contract_id": int(cid_str),
            }))
        except Exception as e:
            print(Fore.RED + (
                f"❌ Slot{s['slot_id']} contract result poll error "
                f"contract_id={cid_str}: {e}"
            ), flush=True)


def _request_results_due_on_tick(vol, tick_number):
    """
    Request the final snapshot on the sixth tick for a 5-tick contract.

    The buy response records the tick number at which Deriv confirmed the
    contract.  Five subsequent ticks later (the next tick after the five
    contract ticks), request the contract once immediately.  The normal
    three-second watchdog poll remains as a fallback for delayed tick
    messages or a delayed Deriv settlement response.
    """
    for s in list(slots.values()):
        if not s.get("api_token") or not s.get("trade_ws_connected"):
            continue
        ws_obj = s.get("trade_ws")
        if ws_obj is None:
            continue
        due_ids = []
        with s["trade_map_lock"]:
            for cid, info in s["trade_map"].items():
                if str(cid).startswith(("PENDING_", "PROP_", "BUY_REQ_")):
                    continue
                if info.get("vol") != vol:
                    continue
                entry_tick = info.get("entry_tick_sequence")
                if entry_tick is None:
                    continue
                if tick_number >= int(entry_tick) + _slot_duration_ticks(s):
                    if not info.get("tick_result_requested"):
                        info["tick_result_requested"] = True
                        due_ids.append(str(cid))
        for cid_str in due_ids:
            try:
                ws_obj.send(json.dumps({
                    "proposal_open_contract": 1,
                    "contract_id": int(cid_str),
                }))
                print(Fore.CYAN + (
                    f"📍 RESULT CHECK [tick {tick_number}] | Slot{s['slot_id']} | "
                    f"{vol} | contract_id={cid_str} | "
                    f"{_slot_duration_ticks(s)}-tick expiry reached"
                ), flush=True)
            except Exception as e:
                print(Fore.RED + (
                    f"❌ Slot{s['slot_id']} sixth-tick result request failed "
                    f"contract_id={cid_str}: {e}"
                ), flush=True)


def _log_trade(s, vol, contract_type, barrier, stake, result, profit, pattern="—", is_reentry=False):
    dir_label   = "▲ HIGHER" if contract_type == "CALL" else "▼ LOWER"
    is_win      = result == "WIN"
    theme       = Fore.GREEN if is_win else Fore.RED
    icon        = "🟢" if is_win else "🔴"
    p_sign      = "+" if profit >= 0 else ""
    dp_sign     = "+" if s["daily_profit"] >= 0 else ""
    dp_color    = Fore.GREEN if s["daily_profit"] >= 0 else Fore.RED
    re_lbl      = "  ⚡RE-ENTRY" if is_reentry else ""
    api_label   = s.get("deriv_mode", "?").upper()
    balance     = s.get("current_balance", 0.0)
    ts_str      = datetime.now().strftime("%H:%M:%S")
    W = 58

    def row(label, value, val_color=Fore.WHITE):
        return (f"{theme}│ {Fore.CYAN}{label:<11}{Style.RESET_ALL}"
                f"{val_color}{value}")

    print()
    print(theme + Style.BRIGHT + "┌" + "─" * W + "┐")
    print(theme + Style.BRIGHT +
          f"│ {icon} {result:<4} {Fore.WHITE}{vol:<9}{theme} {dir_label:<10}"
          f"{Fore.YELLOW}{re_lbl}{Style.RESET_ALL}{Fore.LIGHTBLACK_EX} {ts_str}")
    print(theme + "├" + "─" * W + "┤")
    print(row("Stake",     f"${stake:.2f}"))
    print(row("Profit",    f"{p_sign}${profit:.4f}", theme + Style.BRIGHT))
    print(row("Balance",   f"${balance:,.2f}", Fore.YELLOW))
    print(row("Daily P/L", f"{dp_sign}${s['daily_profit']:.2f}  /  target ${DAILY_TP_TARGET:,.0f}", dp_color))
    print(row("Pattern",   f"{pattern}", Fore.MAGENTA))
    print(row("Slot/API",  f"#{s['slot_id']}  [{api_label}]", Fore.LIGHTBLACK_EX))
    print(theme + Style.BRIGHT + "└" + "─" * W + "┘")
    print()


# ╔══════════════════════════════════════════════════════════════╗
# ║   PLACE CONTRACT — NEW API (proposal→buy, HIGHER/LOWER)      ║
# ╚══════════════════════════════════════════════════════════════╝
#
# The NEW API requires a TWO-STEP flow:
#   Step 1: Send a "proposal" request with:
#             contract_type = "HIGHER" or "LOWER"  (NOT "CALL"/"PUT")
#             underlying_symbol = vol               (NOT "symbol")
#   Step 2: Receive proposal_id in the proposal response,
#           then send {"buy": proposal_id, "price": ask_price}
#
# Barrier sign convention (same for both APIs):
#   CALL/HIGHER → negative barrier: "-X.XX"
#   PUT/LOWER   → positive barrier: "+X.XX"

def place_higher_lower_new(
    ws_obj, s, vol, direction, barrier, stake, m_level=0, pattern="—",
    is_reentry=False, recovery_trade=False,
):
    """
    NEW API contract placement.
    direction: "CALL" or "PUT"  (internal representation — converted to HIGHER/LOWER for the API)
    """
    # Use last known good barrier to skip bisection round-trips when possible.
    # If the background prober confirmed this barrier inside the payout window
    # recently enough (BARRIER_CACHE_MAX_AGE), trust it outright: the very
    # first proposal response goes straight to buy, no retry search needed —
    # the search already happened while the candle engine was still watching
    # ticks, not after the signal fired.
    now = time.time()
    with _good_barrier_lock:
        cached_ts = _good_barrier_ts.get(vol)
        barrier   = _good_barrier_cache.get(vol, barrier)
    trust_cache = cached_ts is not None and (now - cached_ts) <= BARRIER_CACHE_MAX_AGE

    contract_type_str = "HIGHER" if direction == "CALL" else "LOWER"
    barrier_val       = abs(barrier)
    barrier_str       = f"-{barrier_val:.2f}" if direction == "CALL" else f"+{barrier_val:.2f}"
    req_id            = int(time.time() * 1000) % 2147483647

    proposal_key = f"PROPOSAL_REQ_{req_id}"
    with s["proposal_map_lock"]:
        s["proposal_map"][proposal_key] = {
            "vol":             vol,
            "stake":           stake,
            "contract_type":   direction,
            "barrier":         barrier,
            "pattern":         pattern,
            "is_reentry":      is_reentry,
            "recovery_trade":  recovery_trade,
            "req_id":          req_id,
            "payout_retry":    0,
            "barrier_too_big":   None,
            "barrier_too_small": None,
            "search_start":    time.time(),   # barrier-search clock starts here
            "trust_cache":     trust_cache,   # pre-warmed & fresh → skip retry search, buy on first response
            "contract_mode":   CONTRACT_MODE_HIGHER_LOWER,
            "is_virtual":      s["symbol_virtual"][vol]["active"],
        }

    try:
        ws_obj.send(json.dumps({
            "proposal":          1,
            "amount":            stake,
            "basis":             "stake",
            "contract_type":     contract_type_str,
            "currency":          CURRENCY,
            "duration":          _slot_duration_ticks(s),
            "duration_unit":     CONTRACT_DURATION_UNIT,
            "underlying_symbol": vol,
            "barrier":           barrier_str,
            "req_id":            req_id,
        }))
    except Exception as e:
        print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] send proposal error: {e}")
        with s["proposal_map_lock"]:
            s["proposal_map"].pop(proposal_key, None)
        _close_contract_slot(s, vol, "proposal-send-error")
        return

    dir_label  = "▲ HIGHER" if direction == "CALL" else "▼ LOWER"
    m_lbl      = f" [MART L{m_level}]" if m_level else ""
    re_lbl     = " | ⚡RE-ENTRY" if is_reentry else ""
    cache_lbl  = " | ⚡warm" if trust_cache else ""
    print(Fore.CYAN + f"📤 PROPOSAL [NEW] {dir_label}{m_lbl}{re_lbl}{cache_lbl} | Slot{s['slot_id']} | {vol} | ${stake:.2f} | barrier={barrier_str}")


# ╔══════════════════════════════════════════════════════════════╗
# ║   PLACE CONTRACT — NEW API (proposal→buy, RISE/FALL)         ║
# ╚══════════════════════════════════════════════════════════════╝
#
# RISE/FALL uses the SAME contract_type strings as Higher/Lower's internal
# CALL/PUT representation, but with NO barrier field at all — it trades off
# the current spot price. Minimum duration is 1 tick (vs 5 for Higher/Lower).
# There is nothing to bisect/target here: payout is whatever Deriv quotes.

def place_rise_fall_new(
    ws_obj, s, vol, direction, barrier, stake, m_level=0, pattern="—",
    is_reentry=False, recovery_trade=False,
):
    """
    NEW API contract placement — RISE/FALL.
    direction: "CALL" (Rise) or "PUT" (Fall) — sent AS-IS to the API, no barrier.
    `barrier` is accepted for call-site compatibility but is not used or sent.
    """
    req_id = int(time.time() * 1000) % 2147483647
    # Recovery RISE/FALL trades are intentionally one tick, regardless of
    # the duration configured for the primary HIGHER/LOWER strategy.
    duration_ticks = (
        CONTRACT_DURATION_RISE_FALL
        if recovery_trade else _slot_duration_ticks(s)
    )

    proposal_key = f"PROPOSAL_REQ_{req_id}"
    with s["proposal_map_lock"]:
        s["proposal_map"][proposal_key] = {
            "vol":             vol,
            "stake":           stake,
            "contract_type":   direction,
            "barrier":         0.0,
            "pattern":         pattern,
            "is_reentry":      is_reentry,
            "recovery_trade":  recovery_trade,
            "req_id":          req_id,
            "contract_mode":   CONTRACT_MODE_RISE_FALL,
            "is_virtual":      s["symbol_virtual"][vol]["active"],
        }

    try:
        ws_obj.send(json.dumps({
            "proposal":          1,
            "amount":            stake,
            "basis":             "stake",
            "contract_type":     direction,
            "currency":          CURRENCY,
            "duration":          duration_ticks,
            "duration_unit":     CONTRACT_DURATION_UNIT,
            "underlying_symbol": vol,
            "req_id":            req_id,
        }))
    except Exception as e:
        print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] send proposal error: {e}")
        with s["proposal_map_lock"]:
            s["proposal_map"].pop(proposal_key, None)
        _close_contract_slot(s, vol, "proposal-send-error")
        return

    dir_label  = "▲ RISE" if direction == "CALL" else "▼ FALL"
    m_lbl      = f" [MART L{m_level}]" if m_level else ""
    re_lbl     = " | ⚡RE-ENTRY" if is_reentry else ""
    print(Fore.CYAN + f"📤 PROPOSAL [NEW] {dir_label}{m_lbl}{re_lbl} | Slot{s['slot_id']} | {vol} | ${stake:.2f} | {duration_ticks}{CONTRACT_DURATION_UNIT}")


# Dispatcher (single API now — New Deriv API only) — routes to RISE/FALL or
# HIGHER/LOWER placement depending on what this slot chose at login.
def place_higher_lower(
    ws_obj, s, vol, direction, barrier, stake, m_level=0, pattern="—",
    is_reentry=False, contract_mode_override=None, recovery_trade=False,
):
    mode = contract_mode_override or s.get("contract_mode", DEFAULT_CONTRACT_MODE)
    if mode == CONTRACT_MODE_RISE_FALL:
        place_rise_fall_new(
            ws_obj, s, vol, direction, barrier, stake, m_level, pattern,
            is_reentry, recovery_trade,
        )
    else:
        place_higher_lower_new(
            ws_obj, s, vol, direction, barrier, stake, m_level, pattern,
            is_reentry, recovery_trade,
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║   BACKGROUND BARRIER PROBER — keeps barrier warm pre-signal  ║
# ╚══════════════════════════════════════════════════════════════╝
# Instead of only searching for the payout-target barrier AFTER a signal
# fires (which burns bisection round-trips on the critical path to placing
# the trade), this continuously probes payout % in the background WHILE the
# candle engine is still analysing ticks. _good_barrier_cache (+ _good_barrier_ts)
# stays warm so that by the time a signal fires, place_higher_lower_new() finds
# a barrier that was ALREADY verified inside the PAYOUT_TARGET_MIN/MAX window
# and — as long as that verification is recent enough (BARRIER_CACHE_MAX_AGE) —
# trusts it outright and buys on the very first proposal response, with zero
# bisection retries on the critical post-signal path.
# Everything here uses its own "BPROBE_REQ_" proposal_map keys, completely
# separate from the real "PROPOSAL_REQ_" trade flow — it never buys.

def _pick_prober_slot(vol):
    """Pick any connected, active HIGHER/LOWER slot whose trade WS can carry a probe for `vol`.
    RISE/FALL slots have no barrier to keep warm, so they're skipped here."""
    for s in list(slots.values()):
        if not s.get("api_token") or not s.get("trading_active"):
            continue
        if s.get("contract_mode", DEFAULT_CONTRACT_MODE) != CONTRACT_MODE_HIGHER_LOWER:
            continue
        if not s.get("trade_ws_connected") or s.get("trade_ws") is None:
            continue
        if s["contracts_open"].get(vol) is not None:
            continue  # a real trade is in flight on this slot — don't add traffic on top
        return s
    return None


def _send_barrier_probe(s, vol, barrier, retry=0, too_big=None, too_small=None):
    trade_ws = s.get("trade_ws")
    if trade_ws is None or not s.get("trade_ws_connected"):
        return
    req_id = int(time.time() * 1000) % 2147483647
    key    = f"BPROBE_REQ_{req_id}"
    with s["proposal_map_lock"]:
        s["proposal_map"][key] = {
            "vol":          vol,
            "barrier":      barrier,
            "retry":        retry,
            "too_big":      too_big,
            "too_small":    too_small,
            "search_start": time.time(),
        }
    try:
        trade_ws.send(json.dumps({
            "proposal":          1,
            "amount":            BARRIER_PROBE_STAKE,
            "basis":             "stake",
            "contract_type":     "HIGHER",   # magnitude-only probe — same |barrier| feeds both CALL & PUT
            "currency":          CURRENCY,
            "duration":          _slot_duration_ticks(s),
            "duration_unit":     CONTRACT_DURATION_UNIT,
            "underlying_symbol": vol,
            "barrier":           f"-{abs(barrier):.2f}",
            "req_id":            req_id,
        }))
    except Exception:
        with s["proposal_map_lock"]:
            s["proposal_map"].pop(key, None)


def _handle_barrier_probe_response(s, probe_info, payout):
    """Continue/settle the background bisection for a probe proposal response."""
    vol           = probe_info["vol"]
    base_barrier  = probe_info["barrier"]
    retry_attempt = probe_info.get("retry", 0)
    search_start  = probe_info.get("search_start", time.time())
    elapsed       = time.time() - search_start
    time_left     = elapsed < BARRIER_SEARCH_TIME_BUDGET

    payout_pct = ((payout - BARRIER_PROBE_STAKE) / BARRIER_PROBE_STAKE) if BARRIER_PROBE_STAKE > 0 else 0

    if (payout_pct < PAYOUT_TARGET_MIN or payout_pct > PAYOUT_TARGET_MAX) and retry_attempt < PAYOUT_MAX_RETRIES and time_left:
        payout_too_low = payout_pct < PAYOUT_TARGET_MIN
        nudged, too_big, too_small = _bisect_barrier(
            base_barrier, payout_too_low,
            probe_info.get("too_big"), probe_info.get("too_small")
        )
        _send_barrier_probe(s, vol, nudged, retry_attempt + 1, too_big, too_small)
        return

    if PAYOUT_TARGET_MIN <= payout_pct <= PAYOUT_TARGET_MAX:
        with _good_barrier_lock:
            _good_barrier_cache[vol] = base_barrier
            _good_barrier_ts[vol]    = time.time()
        if BARRIER_PROBE_DEBUG_PRINT:
            print(Fore.BLUE + (
                f"🔄 [PROBE] {vol} barrier kept warm @ {base_barrier:.2f} "
                f"(payout {payout_pct*100:.1f}%)"
            ))
    # else: gave up this round (time budget / max retries) — leave the cache as-is,
    # the next scheduled probe round will pick up from the last good value.


def _handle_barrier_probe_error(s, probe_info, err_code, err_msg):
    if BARRIER_PROBE_DEBUG_PRINT:
        vol = probe_info.get("vol", "?")
        print(Fore.YELLOW + (
            f"⚠️  [PROBE] {vol} barrier {probe_info.get('barrier', 0):.2f} "
            f"rejected ({err_code}) — will retry next round"
        ))


def _barrier_prober_loop():
    """
    Runs for the lifetime of the process. While the candle engine is still
    analysing ticks (i.e. no signal has fired yet for a symbol), this keeps
    _good_barrier_cache[vol] refreshed to whatever barrier currently lands
    the payout in the PAYOUT_TARGET_MIN/MAX window — so the real trade path
    doesn't have to run the bisection search after the signal fires.
    """
    while not _shutdown_event.is_set():
        for vol in VOLATILITIES:
            if _shutdown_event.is_set():
                break
            eng = candle_engines.get(vol)
            if eng is not None and eng.signal_fired:
                _shutdown_event.wait(BARRIER_PROBE_INTERVAL)
                continue  # a signal just fired for this symbol — stay off its trade path
            s = _pick_prober_slot(vol)
            if s is not None:
                with _good_barrier_lock:
                    start_barrier = _good_barrier_cache.get(vol, _get_barrier(vol))
                _send_barrier_probe(s, vol, start_barrier)
            _shutdown_event.wait(BARRIER_PROBE_INTERVAL)


# ╔══════════════════════════════════════════════════════════════╗
# ║   PER-SLOT TRADE WS — NEW API                                ║
# ╚══════════════════════════════════════════════════════════════╝

def _init_slot_trade_runtime_new(s):
    s.setdefault("trade_ws",            None)
    s.setdefault("trade_ws_thread",     None)
    s.setdefault("trade_ws_connected",  False)
    s.setdefault("trade_ws_authorised", True)  # New API WS is pre-authorized
    if "trade_ws_close_lock" not in s:
        s["trade_ws_close_lock"] = threading.Lock()
    if "proposal_map" not in s:
        s["proposal_map"] = {}
    if "proposal_map_lock" not in s:
        s["proposal_map_lock"] = threading.Lock()
    s.setdefault("_reentry_retry_count", {})  # in-memory only, not persisted


def _make_trade_callbacks_new(s):
    """
    Callbacks for the NEW API per-slot trade WebSocket.
    The WS URL itself is authenticated (obtained via OTP REST call),
    so NO authorize message is sent on connect.
    """
    def on_open(ws_obj):
        s["trade_ws"]            = ws_obj
        s["trade_ws_connected"]  = True
        s["trade_ws_authorised"] = True
        print(Fore.GREEN + f"🟢 Slot{s['slot_id']} [NEW] trade WS CONNECTED")
        hb_stop = threading.Event()
        s["_hb_stop"] = hb_stop
        threading.Thread(
            target=_ws_heartbeat,
            args=(ws_obj, hb_stop, f"trade-new-slot{s['slot_id']}", WS_HEARTBEAT_INTERVAL),
            daemon=True,
        ).start()
        try:
            ws_obj.send(json.dumps({"balance": 1, "subscribe": 1}))
        except Exception as e:
            print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] balance subscribe error: {e}")

        # Re-subscribe to any contracts that were already bought (have a
        # confirmed contract_id) before this reconnect happened, so their
        # WIN/LOSS result still reaches the console instead of vanishing.
        with s["trade_map_lock"]:
            live_cids = [k for k in list(s["trade_map"].keys()) if not str(k).startswith("PENDING_")]
        for cid in live_cids:
            try:
                ws_obj.send(json.dumps({"proposal_open_contract": 1, "contract_id": int(cid), "subscribe": 1}))
            except Exception as e:
                print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] resubscribe contract {cid} error: {e}")
        if live_cids:
            print(Fore.CYAN + f"🔁 Slot{s['slot_id']} resubscribed to {len(live_cids)} in-flight contract(s) after reconnect")

        # Catch-all safety net: ask Deriv for the account's actual open
        # contracts right away too, in case a buy was accepted server-side
        # during the gap where we had no connection (or no confirmed
        # contract_id yet), so it never made it into trade_map at all.
        _request_portfolio_reconciliation(s, ws_obj)

        if s["last_day"] is None:
            s["last_day"] = datetime.now().date()
        if s["tp_reached_today"]:
            s["trading_active"] = False

    def on_message(ws_obj, msg):
        try:
            data = json.loads(msg)
        except Exception:
            return

        _check_for_new_day(s)

        if "error" in data and "msg_type" not in data:
            code    = data["error"].get("code", "")
            msg_txt = data["error"].get("message", "")
            print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] WS error [{code}]: {msg_txt}")
            if code in ("AuthorizationRequired", "Unauthorized", "InvalidToken"):
                tg_send(
                    f"⚠️  <b>Slot #{s['slot_id']} auth error</b>\n\n"
                    f"<code>{html_lib.escape(msg_txt)}</code>\n\nUse /login to re-authenticate.",
                    chat_id=s.get("chat_id")
                )
            return

        msg_type = data.get("msg_type", "")

        # ── Balance update ──
        if msg_type == "balance" or "balance" in data:
            bal = data.get("balance", {})
            if isinstance(bal, dict):
                nb = bal.get("balance")
                if nb is not None:
                    s["current_balance"] = float(nb)
            elif isinstance(bal, (int, float)):
                s["current_balance"] = float(bal)

        # ── Proposal response → send buy ──
        if msg_type == "proposal" or "proposal" in data:
            if "error" in data:
                err_code = data["error"].get("code", "")
                err_msg  = data["error"].get("message", "")
                req_id   = data.get("echo_req", {}).get("req_id")

                with s["proposal_map_lock"]:
                    probe_info = s["proposal_map"].pop(f"BPROBE_REQ_{req_id}", None)
                if probe_info:
                    _handle_barrier_probe_error(s, probe_info, err_code, err_msg)
                    return

                with s["proposal_map_lock"]:
                    info = s["proposal_map"].pop(f"PROPOSAL_REQ_{req_id}", None)
                if info:
                    vol = info["vol"]
                    if err_code == "ContractBuyValidationError" and "barrier" in err_msg.lower():
                        new_b = _reduce_barrier(vol)
                        print(Fore.YELLOW + f"⚡ Barrier adjusted for {vol} → {new_b:.3f}")
                    _close_contract_slot(s, vol, f"proposal-error-{err_code}")

                    # InputValidationFailed on a re-entry is usually a
                    # transient Deriv-side glitch, not a real problem with the
                    # request (the exact same payload succeeds moments
                    # later) — retry the SAME re-entry a couple of times
                    # with a short delay before giving up and falling back to
                    # "wait for the next signal", so a martingale ladder
                    # doesn't stall on a one-off blip.
                    retried = False
                    if info.get("is_reentry") and err_code == "InputValidationFailed":
                        retry_counts = s.setdefault("_reentry_retry_count", {})
                        attempt = retry_counts.get(vol, 0) + 1
                        if attempt <= 2:
                            retry_counts[vol] = attempt
                            retried = True
                            def _retry(_s=s, _vol=vol, _info=info, _attempt=attempt):
                                time.sleep(0.4)
                                _tw = _s.get("trade_ws")
                                if _tw and _s.get("trade_ws_connected") and _s.get("trading_active"):
                                    place_higher_lower(
                                        _tw, _s, _vol, _info["contract_type"], _info["barrier"],
                                        _info["stake"], 0, f"{_info.get('pattern','—')}-RETRY{_attempt}",
                                        is_reentry=True,
                                        contract_mode_override=_info.get("contract_mode"),
                                        recovery_trade=_info.get("recovery_trade", False),
                                    )
                            threading.Thread(target=_retry, daemon=True).start()
                        else:
                            retry_counts[vol] = 0  # give up — fall back below, reset for next time

                    if not retried and info.get("is_reentry"):
                        with s["martingale_lock"]:
                            sm = s["symbol_martingale"][vol]
                            sm["reentry_pending"]   = True
                            sm["reentry_direction"] = info["contract_type"]
                            sm["reentry_barrier"]   = info["barrier"]
                print(Fore.RED + f"❌ [NEW] Proposal error [{err_code}]: {err_msg}")
                return

            proposal_data = data.get("proposal", {})
            proposal_id   = proposal_data.get("id")
            req_id        = data.get("echo_req", {}).get("req_id")
            ask_price     = float(proposal_data.get("ask_price", 0))
            payout        = float(proposal_data.get("payout", 0))

            with s["proposal_map_lock"]:
                probe_info = s["proposal_map"].pop(f"BPROBE_REQ_{req_id}", None)
            if probe_info:
                _handle_barrier_probe_response(s, probe_info, payout)
                return

            if not proposal_id:
                return

            with s["proposal_map_lock"]:
                info = s["proposal_map"].pop(f"PROPOSAL_REQ_{req_id}", None)
            if not info:
                return

            vol           = info["vol"]
            stake         = info["stake"]
            direction     = info["contract_type"]
            base_barrier  = info["barrier"]
            proposal_mode = info.get("contract_mode", DEFAULT_CONTRACT_MODE)
            if info.get("is_reentry"):
                s.setdefault("_reentry_retry_count", {})[vol] = 0  # this attempt succeeded — clear the retry budget

            payout_pct = ((payout - stake) / stake) if stake > 0 else 0

            if proposal_mode == CONTRACT_MODE_RISE_FALL:
                # RISE/FALL has no barrier to tune — accept whatever payout
                # Deriv quotes and buy on the first response.
                print(Fore.GREEN + f"✅ [NEW] Payout {payout_pct*100:.1f}% ({vol})")

            else:
                retry_attempt = info.get("payout_retry", 0)
                trust_cache   = info.get("trust_cache", False)

                search_start = info.get("search_start", time.time())
                elapsed      = time.time() - search_start
                time_left    = elapsed < BARRIER_SEARCH_TIME_BUDGET

                # ── Fast path: the background prober already verified this barrier
                # inside the payout window WHILE the candle engine was still
                # watching ticks (before the signal fired). Trust it and buy on
                # this very first response instead of re-running the bisection —
                # that search already happened; redoing it now is exactly the
                # wasted post-signal round-trip we're trying to eliminate.
                if trust_cache:
                    print(Fore.GREEN + (
                        f"⚡ [NEW] Pre-warmed barrier accepted — payout {payout_pct*100:.1f}% "
                        f"(barrier={base_barrier:.2f}, warm-checked {elapsed*1000:.0f}ms ago) {vol}"
                    ))
                    with _good_barrier_lock:
                        _good_barrier_cache[vol] = base_barrier
                        _good_barrier_ts[vol]    = time.time()
                elif (payout_pct < PAYOUT_TARGET_MIN or payout_pct > PAYOUT_TARGET_MAX) and retry_attempt < PAYOUT_MAX_RETRIES and time_left:
                    payout_too_low = payout_pct < PAYOUT_TARGET_MIN
                    nudged_barrier, new_too_big, new_too_small = _bisect_barrier(
                        base_barrier, payout_too_low,
                        info.get("barrier_too_big"), info.get("barrier_too_small")
                    )
                    barrier_str       = f"-{nudged_barrier:.2f}" if direction == "CALL" else f"+{nudged_barrier:.2f}"
                    new_req_id        = int(time.time() * 1000) % 2147483647
                    new_key           = f"PROPOSAL_REQ_{new_req_id}"
                    contract_type_str = "HIGHER" if direction == "CALL" else "LOWER"
                    direction_word    = "shrink" if payout_too_low else "grow"
                    bound_word        = f"< {PAYOUT_TARGET_MIN*100:.0f}%" if payout_too_low else f"> {PAYOUT_TARGET_MAX*100:.0f}%"
                    with s["proposal_map_lock"]:
                        s["proposal_map"][new_key] = {
                            **info,
                            "barrier":          nudged_barrier,
                            "payout_retry":     retry_attempt + 1,
                            "barrier_too_big":   new_too_big,
                            "barrier_too_small": new_too_small,
                        }
                    print(Fore.YELLOW + (
                        f"💹 [NEW] Payout {payout_pct*100:.1f}% {bound_word} "
                        f"— {direction_word} barrier {base_barrier:.2f}→{nudged_barrier:.2f} "
                        f"retry#{retry_attempt+1} ({vol})"
                    ))
                    try:
                        ws_obj.send(json.dumps({
                            "proposal":          1, "amount": stake, "basis": "stake",
                            "contract_type":     contract_type_str, "currency": CURRENCY,
                            "duration":          _slot_duration_ticks(s), "duration_unit": CONTRACT_DURATION_UNIT,
                            "underlying_symbol": vol, "barrier": barrier_str, "req_id": new_req_id,
                        }))
                    except Exception as e:
                        print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] payout-retry send error: {e}")
                        with s["proposal_map_lock"]:
                            s["proposal_map"].pop(new_key, None)
                        _close_contract_slot(s, vol, "payout-retry-error")
                    return


                # ── Payout in range (or max retries / time budget hit) — proceed to buy ──
                # (skipped for trust_cache — it already logged and cached above)
                if not trust_cache:
                    if payout_pct < PAYOUT_TARGET_MIN or payout_pct > PAYOUT_TARGET_MAX:
                        stop_reason = "time budget" if not time_left else "max retries"
                        print(Fore.YELLOW + (
                            f"⚠️  [NEW] Barrier search stopped ({stop_reason}, {elapsed*1000:.0f}ms) for {vol} — "
                            f"accepting {payout_pct*100:.1f}% (barrier={base_barrier:.2f})"
                        ))
                    else:
                        print(Fore.GREEN + (
                            f"✅ [NEW] Payout {payout_pct*100:.1f}% in range "
                            f"(barrier={base_barrier:.2f}, retry={retry_attempt}) {vol}"
                        ))
                        with _good_barrier_lock:
                            _good_barrier_cache[vol] = base_barrier
                            _good_barrier_ts[vol]    = time.time()

            # ── Virtual trade: skip the real buy entirely — settle from live
            # ticks instead. Everything above (barrier search/payout targeting)
            # ran identically to a real trade for full parity between modes. ──
            if info.get("is_virtual"):
                _register_virtual_contract(
                    s, vol, direction, base_barrier, stake, payout, proposal_mode,
                    pattern=info.get("pattern", "—"),
                    is_reentry=info.get("is_reentry", False),
                    recovery_trade=info.get("recovery_trade", False),
                )
                return

            with s["proposal_map_lock"]:
                s["proposal_map"][f"PROP_{proposal_id}"] = info

            buy_req_id = int(time.time() * 1000) % 2147483647
            with s["proposal_map_lock"]:
                s["proposal_map"][f"BUY_REQ_{buy_req_id}"] = proposal_id
            _remember_pending_buy(s, buy_req_id, proposal_id, info)

            try:
                ws_obj.send(json.dumps({"buy": proposal_id, "price": ask_price, "req_id": buy_req_id}))
                if proposal_mode == CONTRACT_MODE_RISE_FALL:
                    dir_label = "▲ RISE" if direction == "CALL" else "▼ FALL"
                    print(Fore.CYAN + (
                        f"💰 BUY [NEW] {dir_label} | Slot{s['slot_id']} | {vol} | "
                        f"${stake:.2f} | payout={payout_pct*100:.1f}%"
                    ))
                else:
                    dir_label = "▲ HIGHER" if direction == "CALL" else "▼ LOWER"
                    print(Fore.CYAN + (
                        f"💰 BUY [NEW] {dir_label} | Slot{s['slot_id']} | {vol} | "
                        f"${stake:.2f} | payout={payout_pct*100:.1f}% | barrier={base_barrier:.2f}"
                    ))
            except Exception as e:
                print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] buy send error: {e}")
                with s["proposal_map_lock"]:
                    s["proposal_map"].pop(f"PROP_{proposal_id}", None)
                    s["proposal_map"].pop(f"BUY_REQ_{buy_req_id}", None)
                _close_contract_slot(s, vol, "buy-send-error")
                if info.get("is_reentry"):
                    with s["martingale_lock"]:
                        sm = s["symbol_martingale"][vol]
                        sm["reentry_pending"]   = True
                        sm["reentry_direction"] = direction
                        sm["reentry_barrier"]   = base_barrier

        # ── Buy response → confirm contract_id ──
        if msg_type == "buy" or "buy" in data:
            buy_info = data.get("buy", {}) if isinstance(data.get("buy", {}), dict) else {}
            response_proposal_id = buy_info.get("proposal_id") or buy_info.get("id")
            req_id = data.get("echo_req", {}).get("req_id")
            if "error" in data:
                pending = _take_pending_buy(s, req_id, response_proposal_id)
                with s["proposal_map_lock"]:
                    pid = s["proposal_map"].pop(f"BUY_REQ_{req_id}", None) if req_id is not None else None
                    info = s["proposal_map"].pop(f"PROP_{pid}", None) if pid else None
                if info is None and pending:
                    info = pending["info"]
                if info:
                    _close_contract_slot(s, info["vol"], "buy-error")
                    if info.get("is_reentry"):
                        with s["martingale_lock"]:
                            sm = s["symbol_martingale"][info["vol"]]
                            sm["reentry_pending"]   = True
                            sm["reentry_direction"] = info["contract_type"]
                            sm["reentry_barrier"]   = info["barrier"]
                print(Fore.RED + f"❌ [NEW] Buy error [{data['error'].get('code','')}]: {data['error'].get('message','')}")
                return

            contract_id = buy_info.get("contract_id")
            if contract_id is None:
                print(Fore.RED + (
                    f"❌ [NEW] Buy response had no contract_id | req_id={req_id} | "
                    f"response={data}"
                ), flush=True)
                return
            pending = _take_pending_buy(s, req_id, response_proposal_id)

            with s["proposal_map_lock"]:
                pid  = s["proposal_map"].pop(f"BUY_REQ_{req_id}", None) if req_id is not None else None
                info = s["proposal_map"].pop(f"PROP_{pid}", None) if pid else None
            if info is None and pending:
                info = pending["info"]
            if info is None:
                # The slot is authoritative while a buy is pending.  This
                # catches a response whose echo_req and proposal id were both
                # omitted by the transport.
                for candidate_vol in VOLATILITIES:
                    with s["contract_lock"]:
                        slot_info = s["contracts_open"].get(candidate_vol)
                        if slot_info and slot_info.get("pending"):
                            info = {
                                "vol": candidate_vol,
                                "stake": float(slot_info.get("stake", 0.0)),
                                "contract_type": slot_info.get("contract_type", "CALL"),
                                "barrier": float(slot_info.get("barrier", 0.0)),
                                "pattern": "RECOVERED_FROM_BUY_RESPONSE",
                                "is_reentry": False,
                            }
                            break
                if info is None:
                    print(Fore.RED + (
                        f"❌ UNMATCHED ACCEPTED CONTRACT [NEW] | Slot{s['slot_id']} | "
                        f"contract_id={contract_id} | req_id={req_id} — "
                        "cannot safely attribute it; portfolio reconciliation will retry"
                    ), flush=True)
                    return

            if info:
                vol = info["vol"]
                # Count from the tick on which Deriv confirmed the buy.
                # CONTRACT_DURATION=5 therefore requests the result on the
                # following (sixth) tick, while the watchdog remains a
                # fallback if the final snapshot is delayed.
                info["entry_tick_sequence"] = _get_tick_sequence(vol)
                info["tick_result_requested"] = False
                print(Fore.GREEN + (
                    f"✅ TRADE ACCEPTED [NEW] | Slot{s['slot_id']} | {vol} | "
                    f"contract_id={contract_id} | "
                    f"{'▲ HIGHER' if info['contract_type']=='CALL' else '▼ LOWER'} | "
                    f"stake=${float(info['stake']):.2f} | barrier={float(info['barrier']):.2f}"
                ), flush=True)
                with s["contract_lock"]:
                    slot_info = s["contracts_open"].get(vol)
                    if slot_info and slot_info.get("pending"):
                        slot_info["pending"]     = False
                        slot_info["contract_id"] = str(contract_id)
                        s["contract_opened_at"][vol] = time.time()
                with s["trade_map_lock"]:
                    s["trade_map"][str(contract_id)] = info
                with s["contract_poll_lock"]:
                    s["contract_poll_times"][str(contract_id)] = 0.0

            _subscribe_to_contract_with_retry(s, ws_obj, contract_id)

        # ── Contract result ──
        if msg_type == "proposal_open_contract" or "proposal_open_contract" in data:
            if "error" in data:
                # A subscribe/refresh request for a specific contract can be
                # rejected transiently (rate limit, brief desync, etc). If we
                # swallow this silently the contract is orphaned forever and
                # its WIN/LOSS never reaches the console even though Deriv
                # resolves it normally — retry instead of dropping it.
                err_cid = data.get("echo_req", {}).get("contract_id")
                err_msg = data["error"].get("message", "")
                print(Fore.RED + (
                    f"❌ Slot{s['slot_id']} [NEW] proposal_open_contract error "
                    f"(contract_id={err_cid}): {err_msg} — retrying subscribe"
                ), flush=True)
                if err_cid is not None:
                    _subscribe_to_contract_with_retry(s, ws_obj, err_cid)
                return

            poc = data.get("proposal_open_contract")
            if not poc:
                return
            terminal_status = str(poc.get("status", "")).lower()
            if not poc.get("is_sold") and terminal_status not in ("sold", "won", "lost", "expired"):
                return

            cid = poc.get("contract_id")
            cid_str = str(cid)
            with s["processed_results_lock"]:
                if cid_str in s["processed_contract_results"]:
                    return
                # Mark before processing: Deriv can replay the final snapshot
                # several times after a subscription/reconnect.
                s["processed_contract_results"].add(cid_str)
                s["processed_contract_results_order"].append(cid_str)
                # Cap the de-dupe set so it can't grow forever on a bot that
                # stays up for weeks/months — drop the oldest entries once
                # we're well past any realistic re-delivery/reconnect window.
                while len(s["processed_contract_results_order"]) > PROCESSED_RESULTS_MAX:
                    oldest = s["processed_contract_results_order"].popleft()
                    s["processed_contract_results"].discard(oldest)
            with s["trade_map_lock"]:
                info = s["trade_map"].pop(cid_str, None)
            s["orphaned_contracts"].pop(cid_str, None)
            with s["contract_poll_lock"]:
                s["contract_poll_times"].pop(cid_str, None)
            if not info:
                # A reconnect/callback race can remove the normal trade_map entry
                # even though Deriv accepted the contract. Recover from the slot
                # record so the result is still counted and printed.
                info = _recover_trade_info_from_open_slot(s, cid)
                if info:
                    print(Fore.YELLOW + (
                        f"⚠️ RESULT METADATA RECOVERED [NEW] | Slot{s['slot_id']} | "
                        f"{info['vol']} | contract_id={cid}"
                    ), flush=True)
                else:
                    # Last resort: reconstruct minimal info straight out of
                    # this "sold" payload itself so the result still counts
                    # instead of being discarded outright.
                    symbol = (
                        poc.get("underlying")
                        or poc.get("symbol")
                        or poc.get("underlying_symbol")
                        or ""
                    )
                    if symbol in VOLATILITIES:
                        info = {
                            "vol":           symbol,
                            "stake":         float(poc.get("buy_price", 0.0) or 0.0),
                            "contract_type": _normalise_contract_type(poc.get("contract_type", "CALL")),
                            "barrier":       0.0,
                            "pattern":       "RECOVERED_FROM_SOLD_PAYLOAD",
                            "is_reentry":    False,
                        }
                        print(Fore.YELLOW + (
                            f"⚠️ RESULT RECOVERED FROM SOLD PAYLOAD [NEW] | Slot{s['slot_id']} | "
                            f"{symbol} | contract_id={cid}"
                        ), flush=True)
                    else:
                        print(Fore.RED + (
                            f"❌ UNTRACKED SOLD CONTRACT [NEW] | Slot{s['slot_id']} | "
                            f"contract_id={cid} | profit={poc.get('profit', 0)} — "
                            f"unknown symbol, cannot attribute to a slot"
                        ), flush=True)
                        return

            _handle_contract_result(s, ws_obj, poc, info)
            return

        # ── Portfolio reconciliation response ──
        if msg_type == "portfolio":
            portfolio_data = data.get("portfolio", {})
            _handle_portfolio_response(s, ws_obj, portfolio_data)
            return

    def on_error(ws_obj, error):
        err_str = str(error).lower()
        if any(x in err_str for x in ("already closed", "nonetype", "connection reset", "broken pipe")):
            return
        print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] trade WS ERROR: {error}")

    def on_close(ws_obj, code, msg):
        hb_stop = s.pop("_hb_stop", None)
        if hb_stop:
            hb_stop.set()
        s["trade_ws_connected"]  = False
        s["trade_ws_authorised"] = False
        s["trade_ws"]            = None
        _clear_all_pending_contracts(s)
        print(Fore.YELLOW + f"🔴 Slot{s['slot_id']} [NEW] trade WS DISCONNECTED (code={code})")

    return on_open, on_message, on_error, on_close


def run_slot_trade_ws_new(s):
    reconnect_delay = WS_RECONNECT_MIN
    attempt = 0
    while not _shutdown_event.is_set():
        if not s.get("api_token"):
            time.sleep(5)
            continue
        attempt += 1
        print(Fore.YELLOW + f"🔄 Slot{s['slot_id']} [NEW] trade WS connecting… (attempt #{attempt})")
        chat_id = s.get("chat_id")
        ws_url, otp_err = _get_otp_ws_url(chat_id)
        if otp_err:
            print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] OTP failed: {otp_err}")
            if "re-login" in otp_err.lower() or "re-authenticate" in otp_err.lower():
                tg_send(
                    f"⚠️  <b>Slot #{s['slot_id']} needs re-login</b>\n\n"
                    f"{html_lib.escape(otp_err)}\n\nUse /login to reconnect.",
                    chat_id=chat_id
                )
                time.sleep(60)
                continue
            time.sleep(min(reconnect_delay + random.uniform(0, reconnect_delay * WS_RECONNECT_JITTER), WS_RECONNECT_MAX))
            reconnect_delay = min(reconnect_delay * 1.5, WS_RECONNECT_MAX)
            continue

        connected_ok = False
        try:
            on_open, on_message, on_error, on_close = _make_trade_callbacks_new(s)
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(ping_interval=0, sslopt={"cert_reqs": ssl.CERT_NONE},
                           skip_utf8_validation=True, reconnect=0)
            connected_ok = True
        except Exception as e:
            print(Fore.RED + f"❌ Slot{s['slot_id']} [NEW] trade WS CRASH: {e}")
        finally:
            s["trade_ws_connected"]  = False
            s["trade_ws_authorised"] = False
            s["trade_ws"]            = None
            _clear_all_pending_contracts(s)
        if _shutdown_event.is_set():
            break
        reconnect_delay = WS_RECONNECT_MIN if connected_ok else min(reconnect_delay * 1.5, WS_RECONNECT_MAX)
        attempt = 0 if connected_ok else attempt
        actual_delay = min(reconnect_delay + random.uniform(0, reconnect_delay * WS_RECONNECT_JITTER), WS_RECONNECT_MAX)
        print(Fore.YELLOW + f"🔄 Slot{s['slot_id']} [NEW] reconnecting in {actual_delay:.1f}s…")
        time.sleep(actual_delay)


# ╔══════════════════════════════════════════════════════════════╗
# ║              CONTRACT RESULT HANDLER (SHARED)                ║
# ╚══════════════════════════════════════════════════════════════╝

def _handle_contract_result(s, ws_obj, poc, info):
    vol           = info["vol"]
    stake         = info["stake"]
    contract_type = info["contract_type"]
    barrier       = info["barrier"]
    pattern       = info.get("pattern", "—")
    is_reentry    = info.get("is_reentry", False)
    profit        = float(poc.get("profit", 0))

    _close_contract_slot(s, vol, "normal")
    win        = profit > 0
    result_str = "WIN" if win else "LOSS"
    s["daily_profit"] += profit

    with s["martingale_lock"]:
        if win:
            on_symbol_win(s, vol)
        else:
            on_symbol_loss(
                s, vol, contract_type, barrier,
                contract_mode=info.get("contract_mode"),
                is_recovery_trade=info.get("recovery_trade", False),
            )

    record_trade_result(s, vol, contract_type, barrier, stake, profit, result_str,
                        pattern=pattern, is_reentry=is_reentry, is_virtual=False)
    _log_trade(s, vol, contract_type, barrier, stake, result_str, profit, pattern, is_reentry)
    print(Fore.CYAN + (
        f"📌 RESULT RECORDED | Slot{s['slot_id']} | {vol} | "
        f"{result_str} | profit={profit:+.4f} | daily={s['daily_profit']:+.4f}"
    ), flush=True)

    # A real WIN on THIS symbol while running the "virtual until N losses,
    # then real" cycle hands control back to virtual trading for THIS symbol
    # only — martingale is already back at base stake via on_symbol_win()
    # above, so the next virtual round starts clean. Other symbols are
    # untouched — each one runs its own independent virtual/real cycle.
    if win and s.get("virtual_mode") and not s["symbol_virtual"][vol]["active"]:
        sv = s["symbol_virtual"][vol]
        sv["active"] = True
        sv["wins"]   = 0
        sv["losses"] = 0
        save_slots()
        print(Fore.MAGENTA + f"🔁 Slot{s['slot_id']} {vol} real WIN — reverting {vol} to VIRTUAL trading.")

    # Immediate re-entry after LOSS
    if not win:
        _mode = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)
        if _mode == MARTINGALE_MODE_IMMEDIATE:
            _try_fire_immediate_trade(s, vol, contract_type, barrier, pattern="IMMEDIATE-REENTRY")

    if s["daily_profit"] >= DAILY_TP_TARGET and not s["tp_reached_today"]:
        s["tp_reached_today"] = True
        s["trading_active"]   = False
        save_slots()
        api_label    = s.get("deriv_mode", "?").upper()
        total_trades = sum(s["daily_stats"][v]["total_trades"] for v in VOLATILITIES)
        total_wins   = sum(s["daily_stats"][v]["wins"]         for v in VOLATILITIES)
        tg_send(
            f"🎯  <b>Daily Target Hit!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏦  Slot <b>#{s['slot_id']}</b>  |  {api_label} API\n"
            f"💰  Profit: <b>+${s['daily_profit']:,.4f}</b>\n"
            f"🎯  Target: <b>${DAILY_TP_TARGET:,.2f}</b>  ✅\n"
            f"📊  Trades: <b>{total_trades}</b>  ✅ <b>{total_wins}W</b>  ❌ <b>{total_trades-total_wins}L</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Trading paused. See you tomorrow! 🌙",
            chat_id=s.get("chat_id")
        )


# ╔══════════════════════════════════════════════════════════════╗
# ║              VIRTUAL TRADING (paper trades from real signals)║
# ╚══════════════════════════════════════════════════════════════╝
#
# Virtual trades run the EXACT same signal → proposal → payout-targeting
# pipeline as real trades (see place_higher_lower_new / place_rise_fall_new
# and the proposal-response handler above) — the only thing skipped is the
# final "buy" send. Settlement is derived from real, live ticks for that
# symbol (the same tick stream the signal engine already watches), counted
# forward from the current price at proposal-acceptance time. Nothing here
# is randomly generated.

_virtual_contracts: dict = {v: [] for v in VOLATILITIES}
_virtual_contracts_lock = threading.Lock()


def _register_virtual_contract(
    s, vol, direction, barrier, stake, payout, contract_mode,
    pattern="—", is_reentry=False, recovery_trade=False,
):
    entry_price = shared_last_prices[vol][-1] if shared_last_prices.get(vol) else None
    if entry_price is None:
        # No reference price yet — can't simulate honestly, so drop this one
        # and free the reserved contract slot rather than fabricate a result.
        _close_contract_slot(s, vol, "virtual-no-reference-price")
        return

    duration_ticks = (
        CONTRACT_DURATION_RISE_FALL
        if recovery_trade else _slot_duration_ticks(s)
    )
    vc = {
        "slot_id":        s["slot_id"],
        "vol":            vol,
        "direction":      direction,
        "contract_mode":  contract_mode,
        "barrier":        barrier,
        "stake":          stake,
        "payout":         payout,
        "duration_ticks": duration_ticks,
        "ticks_seen":     0,
        "entry_price":    entry_price,
        "pattern":        pattern,
        "is_reentry":     is_reentry,
        "recovery_trade": recovery_trade,
    }
    with _virtual_contracts_lock:
        _virtual_contracts[vol].append(vc)

    if contract_mode == CONTRACT_MODE_RISE_FALL:
        dir_label = "▲ RISE" if direction == "CALL" else "▼ FALL"
        print(Fore.MAGENTA + (
            f"🧪 VIRTUAL [NEW] {dir_label} | Slot{s['slot_id']} | {vol} | "
            f"${stake:.2f} | payout={((payout-stake)/stake*100 if stake else 0):.1f}% | entry={entry_price}"
        ))
    else:
        dir_label = "▲ HIGHER" if direction == "CALL" else "▼ LOWER"
        print(Fore.MAGENTA + (
            f"🧪 VIRTUAL [NEW] {dir_label} | Slot{s['slot_id']} | {vol} | "
            f"${stake:.2f} | payout={((payout-stake)/stake*100 if stake else 0):.1f}% | "
            f"barrier={barrier:.2f} | entry={entry_price}"
        ))


def _advance_virtual_contracts(vol, price, epoch):
    """Called on every live tick for `vol` — steps every open virtual contract
    for that symbol forward one tick, settling any that have reached their
    duration off this real tick price."""
    with _virtual_contracts_lock:
        lst = _virtual_contracts.get(vol)
        if not lst:
            return
        remaining = []
        to_settle = []
        for vc in lst:
            vc["ticks_seen"] += 1
            if vc["ticks_seen"] >= vc["duration_ticks"]:
                to_settle.append(vc)
            else:
                remaining.append(vc)
        _virtual_contracts[vol] = remaining

    for vc in to_settle:
        _settle_virtual_contract(vc, price)


def _settle_virtual_contract(vc, exit_price):
    s = slots.get(vc["slot_id"])
    if s is None:
        return

    entry     = vc["entry_price"]
    direction = vc["direction"]
    if vc["contract_mode"] == CONTRACT_MODE_HIGHER_LOWER:
        level = entry - vc["barrier"] if direction == "CALL" else entry + vc["barrier"]
    else:
        level = entry
    win = (exit_price > level) if direction == "CALL" else (exit_price < level)

    _handle_virtual_contract_result(
        s, vc["vol"], direction, vc["barrier"], vc["stake"], vc["payout"], win,
        pattern=vc["pattern"], is_reentry=vc["is_reentry"],
        contract_mode=vc["contract_mode"],
        recovery_trade=vc.get("recovery_trade", False),
    )


def _try_fire_immediate_trade(s, vol, fallback_direction, fallback_barrier, pattern="IMMEDIATE-REENTRY"):
    """Open+place a contract right now for `vol`, using any pending
    martingale re-entry direction/barrier/stake if set, else the given
    fallbacks. Shared by real-loss immediate re-entry, virtual-loss immediate
    re-entry, and the virtual-loss-limit → real-trading switch (all of which
    need to fire a trade instantly rather than waiting for the next signal).
    Routes virtual or real automatically based on
    s["symbol_virtual"][vol]["active"] at dispatch time (set by
    place_higher_lower/place_rise_fall_new)."""
    if not (s.get("trading_active")
            and not s.get("tp_reached_today")
            and not _is_expired(s.get("chat_id"))):
        return False
    open_cnt = sum(1 for vv in VOLATILITIES if s["contracts_open"].get(vv) is not None)
    if open_cnt >= MAX_CONCURRENT_CONTRACTS:
        return False
    with s["martingale_lock"]:
        sm  = s["symbol_martingale"][vol]
        # IMPORTANT: only trust reentry_direction/reentry_barrier when
        # reentry_pending is actually True. Those two fields are left sitting
        # in the dict as None after a win/reset (on_symbol_win,
        # _reset_all_martingale_to_base) rather than being removed, so a bare
        # sm.get("reentry_direction", fallback_direction) NEVER falls back —
        # dict.get()'s default only kicks in when the key is missing, not
        # when its value is None. That's what was sending contract_type=None
        # to Deriv (InputValidationFailed) right after the virtual→real
        # switch, since _reset_all_martingale_to_base runs immediately before
        # this fires and clears reentry_direction/reentry_barrier to None.
        if sm.get("reentry_pending"):
            _dir = sm.get("reentry_direction") or fallback_direction
            _bar = sm.get("reentry_barrier")
            if _bar is None:
                _bar = fallback_barrier
        else:
            _dir = fallback_direction
            _bar = fallback_barrier
        _stk = sm["next_stake"]
        _lvl = sm["level"]
        recovery_trade = (
            s.get("contract_mode", DEFAULT_CONTRACT_MODE) == CONTRACT_MODE_HIGHER_LOWER
            and sm.get("recovery_active", False)
        )
        trade_contract_mode = (
            CONTRACT_MODE_RISE_FALL
            if recovery_trade else s.get("contract_mode", DEFAULT_CONTRACT_MODE)
        )
        if recovery_trade:
            _bar = 0.0
    opened = _open_contract_slot(s, vol, _dir, _stk, _bar)
    if not opened:
        return False
    with s["martingale_lock"]:
        sm2 = s["symbol_martingale"][vol]
        sm2["reentry_pending"]   = False
        sm2["reentry_direction"] = None
        sm2["reentry_barrier"]   = None
    tw = s.get("trade_ws")
    if tw and s.get("trade_ws_connected"):
        place_higher_lower(
            tw, s, vol, _dir, _bar, _stk, _lvl, pattern,
            is_reentry=True, contract_mode_override=trade_contract_mode,
            recovery_trade=recovery_trade,
        )
        return True
    return False


def _handle_virtual_contract_result(
    s, vol, contract_type, barrier, stake, payout, win,
    pattern="—", is_reentry=False, contract_mode=None, recovery_trade=False,
):
    """Mirrors _handle_contract_result's orchestration (martingale update,
    stats, immediate re-entry, daily TP check) for a virtual settlement."""
    _close_contract_slot(s, vol, "normal")
    profit     = (payout - stake) if win else -stake
    result_str = "WIN" if win else "LOSS"
    # NOTE: virtual (paper) trades never touch daily_profit — that figure,
    # and the daily-target pause it drives, must only reflect real money.

    with s["martingale_lock"]:
        if win:
            on_symbol_win(s, vol)
        else:
            on_symbol_loss(
                s, vol, contract_type, barrier,
                contract_mode=contract_mode,
                is_recovery_trade=recovery_trade,
            )

    record_trade_result(s, vol, contract_type, barrier, stake, profit, result_str,
                        pattern=f"[VIRTUAL] {pattern}", is_reentry=is_reentry, is_virtual=True)
    print(Fore.MAGENTA + (
        f"🧪 VIRTUAL RESULT | Slot{s['slot_id']} | {vol} | "
        f"{result_str} | profit={profit:+.4f} (virtual, not counted toward daily P/L)"
    ), flush=True)

    # Virtual win/loss counters + loss-limit → shift-to-real check.
    # Tracked PER SYMBOL, and as a genuine CONSECUTIVE streak: any virtual
    # WIN resets this symbol's loss streak back to 0, so a win breaking up a
    # losing run means the next loss starts counting from 1 again — it takes
    # `virtual_loss_limit` losses IN A ROW (not lifetime-cumulative) to shift
    # this symbol to real trading.
    switched_to_real = False
    sv = s["symbol_virtual"][vol]
    if win:
        sv["wins"]   = sv.get("wins", 0) + 1   # lifetime tally, display only
        sv["losses"] = 0                        # streak broken — reset it
    else:
        sv["losses"] = sv.get("losses", 0) + 1
        limit = s.get("virtual_loss_limit", 0)
        if sv.get("active") and limit > 0 and sv["losses"] >= limit:
            sv["active"]      = False
            switched_to_real  = True
            # Go live at base stake for THIS symbol only, not the escalated
            # virtual-phase stake — mirrors on_symbol_win()'s reset exactly.
            with s["martingale_lock"]:
                on_symbol_win(s, vol)
            print(Fore.MAGENTA + (
                f"🔁 Slot{s['slot_id']} {vol} virtual phase complete — {sv['losses']} CONSECUTIVE virtual loss(es) "
                f"({sv.get('wins',0)}W lifetime) — switching {vol} to REAL trading at base stake ${s['base_stake']:.2f}."
            ))
    save_slots()

    # Fire the first REAL trade immediately the moment the virtual-loss
    # limit is hit — don't wait for martingale_mode or the next signal.
    # This is unconditional (unlike the block below) precisely so a
    # SIGNAL-mode slot doesn't sit idle in real mode until the next candle
    # signal happens to fire.
    if switched_to_real:
        _try_fire_immediate_trade(
            s, vol, contract_type, barrier,
            pattern="VIRTUAL-TO-REAL",
        )

    # Immediate re-entry after LOSS (same rule real trades use — will itself
    # route virtual or real depending on s["symbol_virtual"][vol]["active"]
    # at dispatch time)
    if not win and not switched_to_real:
        _mode = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)
        if _mode == MARTINGALE_MODE_IMMEDIATE:
            _try_fire_immediate_trade(s, vol, contract_type, barrier, pattern="IMMEDIATE-REENTRY")

    # No daily-target check here — virtual trades don't move daily_profit,
    # so they can never trigger (or falsely appear to trigger) the
    # real-money daily-target pause. That check lives only in
    # _handle_contract_result (the real-trade path).


# ╔══════════════════════════════════════════════════════════════╗
# ║              START SLOT (mode-aware dispatcher)              ║
# ╚══════════════════════════════════════════════════════════════╝

def _init_slot_trade_runtime(s):
    s.setdefault("trade_ws",            None)
    s.setdefault("trade_ws_thread",     None)
    s.setdefault("trade_ws_connected",  False)
    s.setdefault("trade_ws_authorised", False)
    if "trade_ws_close_lock" not in s:
        s["trade_ws_close_lock"] = threading.Lock()
    # New Deriv API uses proposal_map for proposal→buy flow
    if "proposal_map" not in s:
        s["proposal_map"] = {}
    if "proposal_map_lock" not in s:
        s["proposal_map_lock"] = threading.Lock()
    s.setdefault("_reentry_retry_count", {})  # in-memory only, not persisted


def _safe_close_trade_ws(s):
    lock = s.get("trade_ws_close_lock")
    if lock is None:
        return
    with lock:
        ws_ref = s.get("trade_ws")
        if ws_ref is None:
            return
        try:
            ws_ref.close()
        except Exception:
            pass
        s["trade_ws"]           = None
        s["trade_ws_connected"] = False


def start_slot(s):
    _init_slot_trade_runtime(s)
    t = s.get("trade_ws_thread")
    if t and t.is_alive():
        return
    t = threading.Thread(
        target=run_slot_trade_ws_new, args=(s,),
        daemon=True, name=f"trade-new-slot-{s['slot_id']}"
    )
    s["trade_ws_thread"] = t
    t.start()
    print(Fore.GREEN + f"🚀 Slot{s['slot_id']} [NEW API] trade WS thread started")


# ╔══════════════════════════════════════════════════════════════╗
# ║              SHARED TICK WS (uses NEW public endpoint)       ║
# ╚══════════════════════════════════════════════════════════════╝
# One shared public WebSocket subscribes to all symbol ticks.
# The same tick data is used for all slots (New Deriv API only).
# Signal dispatch goes through _dispatch_signal().

def _subscribe_symbol(ws_obj, vol):
    try:
        ws_obj.send(json.dumps({
            "ticks_history": vol,
            "count":         500,
            "end":           "latest",
            "style":         "ticks",
            "subscribe":     1,
            "adjust_start_time": 1,
        }))
        return True
    except Exception as e:
        print(Fore.RED + f"❌ Shared WS subscribe error for {vol}: {e}")
        return False


def _on_shared_open(ws_obj):
    global _shared_ws_obj, _shared_ws_connected
    with _shared_ws_lock:
        _shared_ws_obj       = ws_obj
        _shared_ws_connected = True
    _set_shared_last_tick_time(time.time())
    _shared_heartbeat_stop.clear()
    threading.Thread(
        target=_ws_heartbeat,
        args=(ws_obj, _shared_heartbeat_stop, "shared", WS_HEARTBEAT_INTERVAL),
        daemon=True,
    ).start()
    with _shared_history_lock:
        _shared_history_loaded.clear()
    with _shared_subscribed_lock:
        _shared_subscribed.clear()
    now0 = time.time()
    with _shared_symbol_last_tick_lock:
        for v in VOLATILITIES:
            _shared_symbol_last_tick[v] = now0
    print(Fore.CYAN + "📡 Shared tick WS CONNECTED — subscribing to symbols…")

    def _do_subscribe():
        stop_flag = getattr(ws_obj, "_sub_stop_flag", threading.Event())
        for i, vol in enumerate(VOLATILITIES):
            if stop_flag.is_set() or _shutdown_event.is_set():
                break
            with _shared_subscribed_lock:
                if vol in _shared_subscribed:
                    continue
            ok = _subscribe_symbol(ws_obj, vol)
            if ok:
                with _shared_subscribed_lock:
                    _shared_subscribed.add(vol)
                print(Fore.CYAN + f"📡 Subscribed {vol} ({i+1}/{len(VOLATILITIES)})")
            if i < len(VOLATILITIES) - 1:
                stop_flag.wait(SHARED_WS_RESUB_DELAY)

    stop_flag = threading.Event()
    ws_obj._sub_stop_flag = stop_flag
    threading.Thread(target=_do_subscribe, daemon=True, name="shared-sub").start()


def _on_shared_message(ws_obj, msg):
    try:
        data = json.loads(msg)
    except Exception:
        return

    if "error" in data:
        err_code = data["error"].get("code", "")
        if err_code == "AlreadySubscribed":
            return
        print(Fore.RED + f"❌ Shared WS error: [{err_code}] {data['error'].get('message','')}")
        return

    if data.get("msg_type") == "ping" or "ping" in data:
        _set_shared_last_tick_time(time.time())
        return

    if data.get("msg_type") == "history" or "history" in data:
        vol     = data.get("echo_req", {}).get("ticks_history", "")
        history = data.get("history", {})
        prices  = history.get("prices", []) if isinstance(history, dict) else []
        times   = history.get("times",  []) if isinstance(history, dict) else []
        if prices and times:
            _load_history_into_buffers(vol, prices, times)
        elif prices:
            print(Fore.YELLOW + f"⚠ {vol}: history has no timestamps — skipping candle warm")
        _set_shared_last_tick_time(time.time())
        with _shared_history_lock:
            _shared_history_loaded.add(vol)
        return

    if data.get("msg_type") == "tick" or "tick" in data:
        _set_shared_last_tick_time(time.time())
        tick_data = data.get("tick", {})
        if not isinstance(tick_data, dict):
            return
        vol       = tick_data.get("symbol", "")
        if not vol or vol not in VOLATILITIES:
            return
        raw_price = float(tick_data.get("quote", 0))
        raw_epoch = float(tick_data.get("epoch", 0))
        if raw_epoch < 1_000_000_000:
            return

        tick_number = _advance_tick_sequence(vol)
        _set_symbol_last_tick_time(vol, time.time())
        shared_last_prices[vol].append(raw_price)
        shared_tick_ts_buffers[vol].append((raw_price, raw_epoch))

        # Everything up to and including push_tick is pure in-memory work —
        # it stays inline, synchronous, and in strict tick order on this
        # thread, since it's what actually defines "a signal fired on this
        # tick" for this symbol.
        direction, barrier = candle_engines[vol].push_tick(raw_price, raw_epoch)

        # Everything below here can touch the network (order placement,
        # result-check requests, a recovery re-entry fired off a virtual
        # settlement) or disk (save_slots). None of that is allowed to run
        # on the ingestion thread — this is the single feed for every
        # symbol, so one slow send here would delay every other symbol's
        # next tick too. Hand it to the dedicated worker and get straight
        # back to reading the socket. put_nowait never blocks.
        _tick_work_queue.put_nowait((vol, raw_price, raw_epoch, tick_number, direction, barrier))


def _tick_work_loop():
    """Runs everything that follows a tick's CandleEngine update — virtual
    contract settlement, result-check requests, and signal dispatch (which
    includes both fresh signals and martingale/recovery re-entries) — off
    the tick-ingestion thread. Ticks are processed in the exact order they
    arrived (FIFO queue, single worker), so behaviour is unchanged; only
    the thread doing the work — and therefore who gets blocked by it — is
    different.
    """
    while not _shutdown_event.is_set():
        try:
            vol, price, epoch, tick_number, direction, barrier = _tick_work_queue.get(timeout=1)
        except queue.Empty:
            continue
        try:
            _advance_virtual_contracts(vol, price, epoch)
            _request_results_due_on_tick(vol, tick_number)
            if direction is not None:
                _dispatch_signal(vol, direction, barrier)
        except Exception as e:
            print(Fore.RED + f"❌ Tick worker error ({vol}): {e}")


def _load_history_into_buffers(vol, prices_raw, times_raw):
    shared_last_prices[vol].clear()
    shared_tick_ts_buffers[vol].clear()
    for idx, p in enumerate(prices_raw):
        flt   = float(p)
        epoch = float(times_raw[idx]) if idx < len(times_raw) else time.time()
        shared_last_prices[vol].append(flt)
        shared_tick_ts_buffers[vol].append((flt, epoch))
    eng = candle_engines.get(vol)
    if eng is not None:
        for fp, ep in list(shared_tick_ts_buffers[vol]):
            eng.push_tick(fp, ep)
        with eng.lock:
            eng.tick_window.clear()
            eng.signal_fired  = False
    print(Fore.CYAN + f"📦 HISTORY | {vol} | {len(prices_raw)} ticks loaded")


def _dispatch_signal(vol, direction, barrier):
    """Called when candle engine fires a signal."""
    for s in list(slots.values()):
        if not s.get("api_token") or not s.get("trade_ws_connected"):
            continue
        if not s["trading_active"] or _is_expired(s.get("chat_id")):
            continue
        _check_for_new_day(s)
        # _open_contract_slot() performs the authoritative atomic check and
        # reservation under contract_lock. Keep these fast checks only as an
        # inexpensive early exit; they must not be relied upon for safety.
        with s["contract_lock"]:
            if s["contracts_open"].get(vol) is not None:
                continue
            open_count = sum(
                1 for info in s["contracts_open"].values()
                if info is not None
            )
            if open_count >= MAX_CONCURRENT_CONTRACTS:
                continue

        mode     = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)
        trade_ws = s.get("trade_ws")
        if trade_ws is None or not s.get("trade_ws_connected"):
            continue

        # The candle engine always computes the REVERSAL-oriented direction
        # (down-run → CALL/HIGHER, up-run → PUT/LOWER). Slots running NORMAL
        # mode trade the opposite way (down-run → LOWER, up-run → HIGHER),
        # so flip it per-slot right here — the shared engine itself is
        # untouched and keeps firing one canonical direction per symbol.
        slot_trade_mode = s.get("trade_mode", DEFAULT_TRADE_MODE)
        if slot_trade_mode == TRADE_MODE_NORMAL:
            fresh_direction = "PUT" if direction == "CALL" else "CALL"
        else:
            fresh_direction = direction

        # Immediate re-entry check (for any pending martingale re-entry on this symbol)
        if mode == MARTINGALE_MODE_IMMEDIATE:
            do_reentry = False
            re_dir = re_barrier = re_stake = re_level = None
            re_contract_mode = s.get("contract_mode", DEFAULT_CONTRACT_MODE)
            re_recovery_trade = False
            with s["martingale_lock"]:
                sm = s["symbol_martingale"][vol]
                if sm.get("reentry_pending"):
                    re_dir     = sm["reentry_direction"]
                    re_barrier = sm["reentry_barrier"]
                    re_stake   = sm["next_stake"]
                    re_level   = sm["level"]
                    re_recovery_trade = (
                        s.get("contract_mode", DEFAULT_CONTRACT_MODE) == CONTRACT_MODE_HIGHER_LOWER
                        and sm.get("recovery_active", False)
                    )
                    if re_recovery_trade:
                        re_contract_mode = CONTRACT_MODE_RISE_FALL
                        re_barrier = 0.0
                    do_reentry = True

            if do_reentry:
                opened = _open_contract_slot(s, vol, re_dir, re_stake, re_barrier)
                if opened:
                    with s["martingale_lock"]:
                        sm = s["symbol_martingale"][vol]
                        sm["reentry_pending"]   = False
                        sm["reentry_direction"] = None
                        sm["reentry_barrier"]   = None
                    place_higher_lower(
                        trade_ws, s, vol, re_dir, re_barrier,
                        re_stake, re_level, "RE-ENTRY", is_reentry=True,
                        contract_mode_override=re_contract_mode,
                        recovery_trade=re_recovery_trade,
                    )
                continue

        # Normal signal entry
        with s["martingale_lock"]:
            sm = s["symbol_martingale"][vol]
            if mode == MARTINGALE_MODE_SIGNAL and sm.get("in_martingale"):
                use_direction = sm.get("recovery_direction") or fresh_direction
                use_barrier   = sm.get("recovery_barrier")
                if use_barrier is None:
                    use_barrier = barrier
            else:
                use_direction = fresh_direction
                use_barrier   = barrier
            stake   = sm["next_stake"]
            m_level = sm["level"]
            recovery_trade = (
                s.get("contract_mode", DEFAULT_CONTRACT_MODE) == CONTRACT_MODE_HIGHER_LOWER
                and sm.get("recovery_active", False)
            )
            trade_contract_mode = (
                CONTRACT_MODE_RISE_FALL
                if recovery_trade else s.get("contract_mode", DEFAULT_CONTRACT_MODE)
            )
            if recovery_trade:
                use_barrier = 0.0

        opened = _open_contract_slot(s, vol, use_direction, stake, use_barrier)
        if not opened:
            continue

        if recovery_trade:
            pattern_str = "RISE/FALL-RECOVERY"
        else:
            pattern_str = f"pullback_entry=trend_{'green' if use_direction=='CALL' else 'red'}"
        place_higher_lower(
            trade_ws, s, vol, use_direction, use_barrier, stake, m_level,
            pattern_str, contract_mode_override=trade_contract_mode,
            recovery_trade=recovery_trade,
        )


def _on_shared_error(ws_obj, error):
    err_str = str(error).lower()
    if any(x in err_str for x in ("already closed", "nonetype", "connection reset", "broken pipe")):
        return
    print(Fore.RED + f"❌ Shared tick WS ERROR: {error}")


def _on_shared_close(ws_obj, code, msg):
    global _shared_ws_obj, _shared_ws_connected
    _shared_heartbeat_stop.set()
    stop_flag = getattr(ws_obj, "_sub_stop_flag", None)
    if stop_flag:
        stop_flag.set()
    with _shared_ws_lock:
        _shared_ws_connected = False
        _shared_ws_obj       = None
    with _shared_subscribed_lock:
        _shared_subscribed.clear()
    print(Fore.YELLOW + f"🔴 Shared tick WS DISCONNECTED (code={code})")


def run_shared_tick_ws():
    global _shared_ws_obj, _shared_ws_connected
    reconnect_delay = WS_RECONNECT_MIN
    while not _shutdown_event.is_set():
        with _shared_ws_lock:
            _shared_ws_connected = False
            _shared_ws_obj       = None
        try:
            ws = websocket.WebSocketApp(
                NEW_WS_PUBLIC,
                on_open=_on_shared_open, on_message=_on_shared_message,
                on_error=_on_shared_error, on_close=_on_shared_close,
            )
            ws.run_forever(ping_interval=0, sslopt={"cert_reqs": ssl.CERT_NONE},
                           skip_utf8_validation=True, reconnect=0)
            reconnect_delay = WS_RECONNECT_MIN
        except Exception as e:
            print(Fore.RED + f"❌ Shared tick WS CRASH: {e}")
            reconnect_delay = min(reconnect_delay * 1.5, WS_RECONNECT_MAX)
        finally:
            with _shared_ws_lock:
                _shared_ws_connected = False
                _shared_ws_obj       = None
        if _shutdown_event.is_set():
            break
        actual_delay = min(reconnect_delay + random.uniform(0, reconnect_delay * WS_RECONNECT_JITTER), WS_RECONNECT_MAX)
        print(Fore.YELLOW + f"🔄 Shared tick WS reconnecting in {actual_delay:.1f}s…")
        time.sleep(actual_delay)


def _safe_close_shared_ws():
    global _shared_ws_obj, _shared_ws_connected
    with _shared_ws_close_lock:
        ws_ref = _shared_ws_obj
        if ws_ref is None:
            return
        try:
            ws_ref.close()
        except Exception:
            pass
        _shared_ws_obj       = None
        _shared_ws_connected = False


# ╔══════════════════════════════════════════════════════════════╗
# ║              WATCHDOG                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def watchdog():
    global _shared_ws_thread_ref
    print(Fore.CYAN + "🐕 Watchdog started")
    while not _shutdown_event.is_set():
        time.sleep(WATCHDOG_INTERVAL)
        now = time.time()

        if (now - _get_shared_last_tick_time() > TICK_STALL_SECONDS
                and _shared_ws_connected):
            with _watchdog_kick_lock:
                last_kick = _watchdog_last_kick.get("shared", 0)
            if now - last_kick > WATCHDOG_KICK_GAP:
                print(Fore.RED + "⚠ Shared WS stalled — forcing reconnect")
                with _watchdog_kick_lock:
                    _watchdog_last_kick["shared"] = now
                _safe_close_shared_ws()

        # ── Per-symbol staleness check ──
        # The global check above only proves the *connection* is alive — it
        # stays green as long as ANY symbol keeps ticking. A single symbol
        # (e.g. 1HZ100V) can have its subscription silently die — dropped
        # mid-subscribe, a swallowed "AlreadySubscribed" response, a brief
        # network hiccup — while the other four keep the shared timestamp
        # fresh, so the connection is never torn down and that one symbol
        # never recovers. Check each symbol individually and resubscribe
        # just that symbol without touching the others.
        if _shared_ws_connected:
            ws_obj_now = _shared_ws_obj
            for vol in VOLATILITIES:
                last_tick = _get_symbol_last_tick_time(vol)
                if now - last_tick <= TICK_STALL_SECONDS:
                    continue
                kick_key = f"symbol_{vol}"
                with _watchdog_kick_lock:
                    last_kick_sym = _watchdog_last_kick.get(kick_key, 0)
                if now - last_kick_sym <= WATCHDOG_KICK_GAP:
                    continue
                with _watchdog_kick_lock:
                    _watchdog_last_kick[kick_key] = now
                print(Fore.RED + (
                    f"⚠ {vol} tick stream stalled "
                    f"({now - last_tick:.0f}s no tick) — resubscribing symbol"
                ))
                if ws_obj_now is not None:
                    with _shared_subscribed_lock:
                        _shared_subscribed.discard(vol)
                    ok = _subscribe_symbol(ws_obj_now, vol)
                    if ok:
                        _set_symbol_last_tick_time(vol, now)
                        with _shared_subscribed_lock:
                            _shared_subscribed.add(vol)

        if _shared_ws_thread_ref is not None and not _shared_ws_thread_ref.is_alive():
            print(Fore.RED + "⚠ Shared tick WS thread died — restarting")
            t = threading.Thread(target=run_shared_tick_ws, daemon=True, name="shared-tick-ws")
            _shared_ws_thread_ref = t
            t.start()

        for s in list(slots.values()):
            if not s.get("api_token") or not s.get("setup_complete"):
                continue
            _sweep_stuck_contracts(s)
            _service_orphaned_contracts(s, s.get("trade_ws"))
            _service_tracked_contracts(s, s.get("trade_ws"))

            # Periodic portfolio reconciliation — catches any trade Deriv
            # confirms as open that our local tracking lost (failed
            # subscribe, callback race, brief disconnect) even while the WS
            # stays connected the whole time, so results never silently vanish.
            tw = s.get("trade_ws")
            if tw is not None and s.get("trade_ws_connected"):
                last_recon = s.get("_last_portfolio_reconcile", 0)
                if now - last_recon >= PORTFOLIO_RECONCILE_SECONDS:
                    s["_last_portfolio_reconcile"] = now
                    _request_portfolio_reconciliation(s, tw)

            t = s.get("trade_ws_thread")
            if t is None or not t.is_alive():
                slot_key = f"slot_{s['slot_id']}"
                with _watchdog_kick_lock:
                    last_kick_slot = _watchdog_last_kick.get(slot_key, 0)
                if now - last_kick_slot > WATCHDOG_SLOT_RESTART_GAP:
                    print(Fore.YELLOW + f"⚠ Slot{s['slot_id']} thread dead — restarting")
                    with _watchdog_kick_lock:
                        _watchdog_last_kick[slot_key] = now
                    start_slot(s)

        with _watchdog_kick_lock:
            last_save = _watchdog_last_kick.get("save", 0)
        if now - last_save > 300:
            with _watchdog_kick_lock:
                _watchdog_last_kick["save"] = now
            save_slots()
            save_admin()
            _save_oauth_tokens()

        _revoke_expired_users()


def _send_eod_status_all():
    """Send end-of-day status HTML report to every active slot."""
    print(Fore.CYAN + "📤 Sending end-of-day status reports to all active slots…")
    for s in list(slots.values()):
        if not s.get("chat_id") or not s.get("api_token"):
            continue
        try:
            html_content = _build_status_html(s)
            total_trades = sum(len(_real_trade_log(s["daily_stats"][v])) for v in VOLATILITIES)
            total_wins   = sum(
                sum(1 for tr in _real_trade_log(s["daily_stats"][v]) if tr["result"] == "WIN")
                for v in VOLATILITIES
            )
            total_losses = total_trades - total_wins
            win_rate     = (total_wins / total_trades * 100) if total_trades else 0.0
            profit_sign  = "+" if s["daily_profit"] >= 0 else ""
            profit_emoji = "🟢" if s["daily_profit"] >= 0 else "🔴"
            today_str    = datetime.now().strftime("%d %b %Y")
            caption = (
                f"🌙 <b>End-of-Day Report — {today_str}</b>\n\n"
                f"{profit_emoji} Daily Profit: <b>{profit_sign}${s['daily_profit']:,.4f}</b>\n"
                f"📊 Trades: <b>{total_trades}</b>  ✅ <b>{total_wins}W</b>  ❌ <b>{total_losses}L</b>  🏆 <b>{win_rate:.1f}%</b>\n"
                f"💼 Slot #{s['slot_id']} | {s.get('deriv_mode','?').upper()} API"
            )
            tg_send_html_as_file(
                html_content,
                filename=f"eod_{s['slot_id']}_{datetime.now().strftime('%Y%m%d')}.html",
                caption=caption,
                chat_id=s["chat_id"],
            )
        except Exception as e:
            print(Fore.RED + f"❌ EOD report error for slot {s.get('slot_id')}: {e}")


def midnight_scheduler():
    while not _shutdown_event.is_set():
        now = datetime.now()

        # ── Target 1: 23:59:59 — send end-of-day status ──
        eod_today = now.replace(hour=23, minute=59, second=59, microsecond=0)
        if now >= eod_today:
            eod_today += timedelta(days=1)
        delta_eod = (eod_today - now).total_seconds()

        # ── Target 2: 00:00:05 next day — daily reset ──
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=5, microsecond=0)
        delta_midnight = (midnight - now).total_seconds()

        # Sleep until whichever comes first
        next_delta = min(delta_eod, delta_midnight)
        _shutdown_event.wait(timeout=next_delta)
        if _shutdown_event.is_set():
            break

        now = datetime.now()

        # Fire end-of-day report if we're at/past 23:59:59 and not yet past midnight
        if now.hour == 23 and now.minute == 59 and now.second >= 59:
            _send_eod_status_all()
            # Wait out the remaining seconds until midnight reset
            _shutdown_event.wait(timeout=5)
            if _shutdown_event.is_set():
                break

        # Midnight reset
        for s in list(slots.values()):
            if s.get("chat_id"):
                _check_for_new_day(s)


# ╔══════════════════════════════════════════════════════════════╗
# ║              AUTHORIZATION HELPERS                           ║
# ╚══════════════════════════════════════════════════════════════╝

def _is_authorized(chat_id):
    return chat_id in authorized_ids and not _is_expired(chat_id)


def _is_admin(chat_id):
    return chat_id == ADMIN_CHAT_ID


# ╔══════════════════════════════════════════════════════════════╗
# ║              HTML STATUS REPORT                              ║
# ╚══════════════════════════════════════════════════════════════╝

def _page_css():
    return """
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { background: #f0f2f8; font-family: 'Inter', system-ui, sans-serif; padding: 2rem 1.5rem; color: #1a2634; }
    .container { max-width: 900px; margin: 0 auto; }
    .top-bar { background: white; border-radius: 16px; border: 1px solid #c8d6e8; padding: 0.9rem 1.4rem; margin-bottom: 1.8rem; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 0.6rem; }
    .top-bar .bar-title { font-size: 1rem; font-weight: 700; color: #1e2f3f; display: flex; align-items: center; gap: 0.5rem; }
    .live-dot { width: 9px; height: 9px; border-radius: 50%; background: #22c55e; display: inline-block; }
    .bar-time { font-size: 0.78rem; color: #5b6e8c; font-family: monospace; }
    .page-heading { margin-bottom: 1.6rem; }
    .page-heading h1 { font-size: 1.7rem; font-weight: 700; color: #1e2a3a; }
    .page-heading p { color: #4a627a; margin-top: 0.3rem; font-size: 0.88rem; }
    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 1.2rem; }
    .stat-card { background: white; border-radius: 22px; padding: 1.1rem 1rem; box-shadow: 0 4px 14px rgba(0,0,0,0.04); border: 1px solid #eef2f8; }
    .stat-card .lbl { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; color: #5b6e8c; }
    .stat-card .val { font-size: 1.9rem; font-weight: 800; margin-top: 0.3rem; line-height: 1.1; color: #1e2f3f; }
    .stat-card .sub { font-size: 0.72rem; color: #7c8ea0; margin-top: 0.4rem; }
    .tp-bar-wrap { background: white; border-radius: 18px; padding: 1rem 1.3rem; margin-bottom: 1rem; border: 1px solid #eef2f8; }
    .tp-bar-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; }
    .tp-bar-header span { font-size: 0.8rem; font-weight: 600; color: #5b6e8c; }
    .tp-bar-header strong { color: #2563eb; }
    .tp-track { background: #e2eaf5; border-radius: 999px; height: 9px; overflow: hidden; }
    .tp-fill { height: 100%; border-radius: 999px; background: linear-gradient(90deg,#2563eb,#22c55e); }
    .strategy-card { background: white; border-radius: 24px; margin-bottom: 1.2rem; box-shadow: 0 4px 14px rgba(0,0,0,0.04); border: 1px solid #eef2f8; overflow: hidden; }
    .strategy-header { background: #fafcff; padding: 0.9rem 1.3rem; border-bottom: 1px solid #eef2fa; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; cursor: pointer; gap: 0.5rem; }
    .sym-name { font-weight: 700; font-size: 1rem; font-family: monospace; background: #f1f5f9; padding: 0.15rem 0.9rem; border-radius: 40px; }
    .sym-stats { display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; font-size: 0.78rem; }
    .sym-stats span { background: #f7f9fc; padding: 0.15rem 0.6rem; border-radius: 18px; font-weight: 500; }
    .toggle-icon { font-size: 1rem; font-weight: 700; color: #6082a0; }
    .trade-log { display: none; }
    .trade-log.open { display: block; }
    .log-wrap { overflow-x: auto; padding: 0.8rem 1.3rem 1.3rem; }
    .trade-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; font-family: monospace; }
    .trade-table th { text-align: left; padding: 0.55rem 0.5rem; background: #f8fafd; color: #334155; font-weight: 600; border-bottom: 1px solid #e2e8f0; }
    .trade-table td { padding: 0.45rem 0.5rem; border-bottom: 1px solid #edf2f7; color: #1e293b; }
    .res-win { color:#15803d; font-weight:700; background:#e6f7ec; display:inline-block; padding:0.15rem 0.55rem; border-radius:18px; font-size:0.68rem; }
    .res-loss { color:#b91c1c; font-weight:700; background:#fee9e6; display:inline-block; padding:0.15rem 0.55rem; border-radius:18px; font-size:0.68rem; }
    .p-pos { color:#15803d; font-weight:600; }
    .p-neg { color:#b91c1c; font-weight:600; }
    .mart-banner { background: linear-gradient(90deg,#fff7ed,#fef9ec); border: 1px solid #fbbf24; border-radius: 14px; padding: 0.8rem 1.2rem; margin-bottom: 1rem; font-size: 0.85rem; color: #92400e; font-weight: 600; }
    .mode-banner { background: linear-gradient(90deg,#f0f9ff,#e0f2fe); border: 1px solid #38bdf8; border-radius: 14px; padding: 0.8rem 1.2rem; margin-bottom: 1rem; font-size: 0.85rem; color: #0c4a6e; font-weight: 600; }
    .api-banner { background: linear-gradient(90deg,#fdf4ff,#ede9fe); border: 1px solid #c084fc; border-radius: 14px; padding: 0.8rem 1.2rem; margin-bottom: 1rem; font-size: 0.85rem; color: #581c87; font-weight: 600; }
    .candle-banner { background: linear-gradient(90deg,#f0fdf4,#dcfce7); border: 1px solid #86efac; border-radius: 14px; padding: 0.8rem 1.2rem; margin-bottom: 1rem; font-size: 0.85rem; color: #14532d; font-weight: 600; }
    .page-footer { margin-top: 2rem; text-align: center; font-size: 0.68rem; color: #8196ab; border-top: 1px solid #e2edf5; padding-top: 1.2rem; }
    @media (max-width: 600px) { body { padding: 0.8rem; } }"""


def _page_wrap(title, top_label, ts, body_html):
    css = _page_css()
    js  = "function toggle(h){var l=h.nextElementSibling;var i=h.querySelector('.toggle-icon');var isOpen=l.classList.toggle('open');i.textContent=isOpen?'▲':'▼';}"
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{title}</title>
<style>{css}</style>
</head><body>
<div class="container">
  <div class="top-bar">
    <div class="bar-title"><span class="live-dot"></span> {top_label}</div>
    <div class="bar-time">{ts}</div>
  </div>
  {body_html}
  <div class="page-footer">JAHIM UNIFIED BOT — 1-Min Candle Engine (Deriv-Aligned) | {CONTRACT_DURATION}{CONTRACT_DURATION_UNIT} | {ts}</div>
</div>
<script>{js}</script>
</body></html>"""


def _stat_card(label, value, sub="", color="#1e2f3f"):
    return (f'<div class="stat-card"><div class="lbl">{label}</div>'
            f'<div class="val" style="color:{color};">{value}</div>'
            f'<div class="sub">{sub}</div></div>')


def _tp_bar(pct):
    return (f'<div class="tp-bar-wrap"><div class="tp-bar-header">'
            f'<span>🎯 Daily TP Progress</span>'
            f'<strong>{pct:.1f}% of ${DAILY_TP_TARGET:,.2f}</strong></div>'
            f'<div class="tp-track"><div class="tp-fill" style="width:{min(pct,100):.1f}%;"></div></div></div>')


def _candle_engine_banner():
    lines = []
    for v in VOLATILITIES:
        eng = candle_engines[v]
        lines.append(f"<b>{v}</b>: {eng.status_str()}")
    return ('<div class="candle-banner">🕯️ <b>Candle Engine Status (Deriv-Aligned)</b><br>'
            + " &nbsp;|&nbsp; ".join(lines) + "</div>")


def _is_virtual_trade(tr):
    """True for paper/virtual trades. Checks the explicit flag first, and
    falls back to the legacy '[VIRTUAL] ' pattern prefix for trade_log
    entries persisted before is_virtual existed on the record."""
    if tr.get("is_virtual"):
        return True
    return str(tr.get("pattern", "")).startswith("[VIRTUAL]")


def _real_trade_log(st):
    """Live/real trades only from a symbol's daily_stats — used for the
    HTML status report so virtual (paper) trades never show up there."""
    return [tr for tr in st.get("trade_log", []) if not _is_virtual_trade(tr)]


def _strategy_cards_html(s):
    out = ""
    for v in sorted(VOLATILITIES):
        st         = s["daily_stats"][v]
        real_log   = _real_trade_log(st)
        t          = len(real_log)
        w          = sum(1 for tr in real_log if tr["result"] == "WIN")
        lo         = t - w
        wr         = (w / t * 100) if t else 0.0
        streak     = 0
        for tr in reversed(real_log):
            if tr["result"] == "LOSS":
                streak += 1
            else:
                break
        wr_col     = "#15803d" if wr >= 55 else ("#b07a00" if wr >= 45 else "#b91c1c")
        sm         = s["symbol_martingale"].get(v, {})
        m_badge    = (f' 🔁L{sm["level"]} ${sm["next_stake"]:.2f}' if sm.get("in_martingale") else "")
        eng        = candle_engines[v]
        trend      = eng._trend_direction()
        trend_icon = "🟢" if trend == "green" else ("🔴" if trend == "red" else "⬜")

        log_rows = ""
        for tr in real_log:
            ctype   = tr.get("contract_type", "?")
            res_cls = "res-win" if tr["result"] == "WIN" else "res-loss"
            p_cls   = "p-pos" if tr["profit"] > 0 else "p-neg"
            p_sign  = "+" if tr["profit"] > 0 else ""
            badge   = "▲H" if ctype == "CALL" else "▼L"
            log_rows += (
                f'<tr><td>{tr["time"]}</td><td>{badge}</td>'
                f'<td>${tr["stake"]:.2f}</td>'
                f'<td>{tr.get("barrier","")}</td>'
                f'<td>{tr.get("pattern","—")}</td>'
                f'<td><span class="{res_cls}">{tr["result"]}</span></td>'
                f'<td class="{p_cls}">{p_sign}${abs(tr["profit"]):.4f}</td></tr>'
            )
        if not log_rows:
            log_rows = '<tr><td colspan="7" style="text-align:center;padding:14px;color:#8196ab;">No live trades yet.</td></tr>'

        out += f"""
        <div class="strategy-card">
          <div class="strategy-header" onclick="toggle(this)">
            <div class="sym-name">{v}{m_badge}</div>
            <div class="sym-stats">
              <span>{trend_icon} trend</span>
              <span>📊 {t}</span>
              <span style="color:#15803d;">✅ {w}</span>
              <span style="color:#b91c1c;">❌ {lo}</span>
              <span style="color:{wr_col};font-weight:700;">🏆 {wr:.1f}%</span>
              <span>⚡ streak:{streak}</span>
            </div>
            <div class="toggle-icon">▼</div>
          </div>
          <div class="trade-log">
            <div class="log-wrap">
              <table class="trade-table">
                <thead><tr><th>Time</th><th>Dir</th><th>Stake</th><th>Barrier</th><th>Trend</th><th>Result</th><th>Profit</th></tr></thead>
                <tbody>{log_rows}</tbody>
              </table>
            </div>
          </div>
        </div>"""
    return out


def _build_status_html(s):
    ts           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_trades = sum(len(_real_trade_log(s["daily_stats"][v])) for v in VOLATILITIES)
    total_wins   = sum(
        sum(1 for tr in _real_trade_log(s["daily_stats"][v]) if tr["result"] == "WIN")
        for v in VOLATILITIES
    )
    win_rate     = (total_wins / total_trades * 100) if total_trades else 0.0
    profit_sign  = "+" if s["daily_profit"] >= 0 else ""
    profit_col   = "#15803d" if s["daily_profit"] >= 0 else "#b91c1c"
    pct          = (s["daily_profit"] / DAILY_TP_TARGET * 100) if DAILY_TP_TARGET else 0
    user_label   = html_lib.escape(s["username"] or f"Slot {s['slot_id']}")
    mode         = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE)
    mode_label   = "⚡ Immediate" if mode == MARTINGALE_MODE_IMMEDIATE else "🔍 Signal"
    api_label    = "🆕 Deriv API (PAT)"

    body = f"""
    <div class="page-heading">
      <h1>📊 Live Status — {user_label}</h1>
      <p>Slot #{s['slot_id']} &bull; 1-Min Candle Engine (Deriv-Aligned) &bull; {_slot_duration_ticks(s)}{CONTRACT_DURATION_UNIT} &bull; {ts}</p>
    </div>
    <div class="api-banner">⚡ <b>API Mode: {api_label}</b> &bull; Account: <code>{html_lib.escape(s.get('account_id','—'))}</code></div>
    <div class="mode-banner">🔁 <b>Martingale Mode: {mode_label}</b> — {_slot_martingale_multiplier(s):g}× on loss</div>
    <div class="stat-grid">
      {_stat_card("🏦 Balance",  f"${s['current_balance']:,.2f}", "current", "#2563eb")}
      {_stat_card("💰 Profit",   f"{profit_sign}${s['daily_profit']:,.4f}", str(s.get('last_day') or '—'), profit_col)}
      {_stat_card("🏆 Win Rate", f"{win_rate:.1f}%", f"{total_wins}W/{total_trades - total_wins}L",
                  "#15803d" if win_rate >= 55 else "#b07a00")}
      {_stat_card("⚡ Trades",   str(total_trades), "today", "#1e2f3f")}
    </div>
    {_tp_bar(pct)}
    {_strategy_cards_html(s)}"""

    return _page_wrap(
        title=f"JAHIM BOT — {user_label}",
        top_label=f"📊 JAHIM BOT — {user_label}",
        ts=ts, body_html=body,
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║              TELEGRAM COMMAND HANDLERS                       ║
# ╚══════════════════════════════════════════════════════════════╝

def _handle_myid(chat_id, username):
    exp_str = _expiry_remaining_str(authorized_ids.get(chat_id))
    mode    = "—"
    s       = get_slot_by_chat(chat_id)
    if s:
        mode = s.get("deriv_mode", "new").upper() + " API"
    tg_send(
        f"👤  <b>Your Account Info</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔  Chat ID: <code>{chat_id}</code>\n"
        f"👤  Name: <b>{html_lib.escape(username)}</b>\n"
        f"🔑  Access: {exp_str}\n"
        f"🔌  Deriv Mode: <b>{mode}</b>",
        chat_id=chat_id
    )


def _handle_status(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot found. Use /start to register.", chat_id=chat_id)
        return
    html_content = _build_status_html(s)
    tg_send_html_as_file(
        html_content,
        filename=f"status_{s['slot_id']}_{int(time.time())}.html",
        caption=f"📊 Live Status — Slot #{s['slot_id']} [{s.get('deriv_mode','?').upper()} API]",
        chat_id=chat_id,
    )


def _handle_stop(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot.", chat_id=chat_id)
        return
    if not s["trading_active"]:
        tg_send("⏸️  Already paused.", chat_id=chat_id)
        return
    s["trading_active"] = False
    save_slots()
    tg_send(
        f"⏸️  <b>Bot Paused</b>\n\n"
        f"Slot #{s['slot_id']} | {s.get('deriv_mode','?').upper()} API\n"
        f"No new trades will be placed.\n\n"
        f"Use /startbot to resume.",
        chat_id=chat_id
    )


def _handle_startbot(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Use /start then login.", chat_id=chat_id)
        return
    if s["tp_reached_today"]:
        tg_send("🎯  Daily target already reached. Resumes tomorrow.", chat_id=chat_id)
        return
    if s["trading_active"]:
        tg_send("▶️  Already running.", chat_id=chat_id)
        return
    s["trading_active"] = True
    save_slots()
    mart_label = "⚡ Immediate" if s.get("martingale_mode") == MARTINGALE_MODE_IMMEDIATE else "🔍 Signal"
    tg_send(
        f"▶️  <b>Bot Started!</b>\n\n"
        f"┌─────────────────────────\n"
        f"│  🎰  Slot <b>#{s['slot_id']}</b>  │  {s.get('deriv_mode','?').upper()} API\n"
        f"│  📈  Strategy: 1-min candle + confirm\n"
        f"│  🔁  Martingale: <b>{mart_label}</b>\n"
        f"│  💵  Stake: <b>${s['base_stake']:.2f}</b>\n"
        f"└─────────────────────────\n\n"
        f"Signals scanning… Good luck! 🍀",
        chat_id=chat_id
    )


def _handle_setstake(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setstake", "step": 1}
    tg_send(
        f"💵  <b>Set Stake Amount</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current stake: <b>${s['base_stake']:.2f}</b>\n\n"
        f"Enter the new stake amount (e.g. <code>2.50</code>):",
        chat_id=chat_id
    )


def _handle_setmartingale(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    current = s.get("martingale_mode", MARTINGALE_MODE_IMMEDIATE) if s else MARTINGALE_MODE_IMMEDIATE
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setmartingale", "step": 1}
    tg_send(
        f"🔁  <b>Martingale Mode</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current: <b>{'⚡ Immediate' if current==MARTINGALE_MODE_IMMEDIATE else '🔍 Signal'}</b>\n\n"
        f"<b>1️⃣  ⚡ Immediate</b>\n"
        f"     Re-enter the same symbol right after a loss\n\n"
        f"<b>2️⃣  🔍 Signal</b>\n"
        f"     Wait for next valid signal, then apply martingale stake\n\n"
        f"Reply <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


def _handle_setcontracttype(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    current = s.get("contract_mode", DEFAULT_CONTRACT_MODE)
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setcontracttype", "step": 1}
    tg_send(
        f"📊  <b>Contract Type</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current: <b>{'📈 RISE/FALL' if current==CONTRACT_MODE_RISE_FALL else '🎯 HIGHER/LOWER'}</b>\n\n"
        f"<b>1️⃣  📈 RISE/FALL</b>\n"
        f"     No barrier, 1-tick minimum\n\n"
        f"<b>2️⃣  🎯 HIGHER/LOWER</b>\n"
        f"     Barrier-based, payout-targeted, 5-tick minimum\n\n"
        f"Reply <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


def _handle_setduration(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    mode      = s.get("contract_mode", DEFAULT_CONTRACT_MODE)
    min_ticks = CONTRACT_DURATION_RISE_FALL if mode == CONTRACT_MODE_RISE_FALL else CONTRACT_DURATION
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setduration", "step": 1, "min_ticks": min_ticks}
    tg_send(
        f"⏱  <b>Contract Duration</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current: <b>{_slot_duration_ticks(s)} ticks</b>\n\n"
        f"Minimum for current contract type: <b>{min_ticks}</b>\n"
        f"Enter the new duration in ticks:",
        chat_id=chat_id
    )


def _handle_setmartingalevalue(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setmartingalevalue", "step": 1}
    tg_send(
        f"🔁  <b>Martingale Multiplier</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current: <b>{_slot_martingale_multiplier(s):g}×</b>\n\n"
        f"Enter a new multiplier greater than 1 (e.g. <code>2.5</code>):",
        chat_id=chat_id
    )


def _handle_settrademode(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    current = s.get("trade_mode", DEFAULT_TRADE_MODE)
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "settrademode", "step": 1}
    tg_send(
        f"🔀  <b>Trade Mode</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Current: <b>{'🔄 REVERSAL' if current==TRADE_MODE_REVERSAL else '➡️ NORMAL'}</b>\n\n"
        f"<b>1️⃣  🔄 REVERSAL</b>\n"
        f"     5 consecutive down ticks → trade HIGHER (and vice versa)\n\n"
        f"<b>2️⃣  ➡️ NORMAL</b>\n"
        f"     5 consecutive down ticks → trade LOWER (and vice versa)\n\n"
        f"Reply <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


def _handle_setvirtual(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s = get_slot_by_chat(chat_id)
    if not s:
        tg_send("❌  No slot. Login first.", chat_id=chat_id)
        return
    mode_on = s.get("virtual_mode", DEFAULT_VIRTUAL_MODE)
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "setvirtual", "step": 1}
    if mode_on:
        limit = s.get("virtual_loss_limit", 0)
        per_symbol_lines = "\n".join(
            f"  {v}: {'🎮 virtual' if sv.get('active') else '💳 REAL'} "
            f"({sv.get('losses',0)}/{limit} streak, {sv.get('wins',0)}W lifetime)"
            for v, sv in s.get("symbol_virtual", {}).items()
        )
        status_line = f"Current: <b>✅ ON</b> — per-symbol progress:\n{per_symbol_lines}"
    else:
        status_line = f"Current: <b>🚫 OFF</b> — trading real"
    tg_send(
        f"🧪  <b>Virtual Trading</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{status_line}\n\n"
        f"<b>1️⃣  ✅ YES</b> — trade virtual first\n"
        f"<b>2️⃣  🚫 NO</b> — trade real now\n\n"
        f"Reply <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


def _handle_login_pat(chat_id, username):
    """Initiate PAT (Personal Access Token) login for the Deriv API."""
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "login_pat_token", "step": 1, "username": username}
    tg_send(
        f"🔐  <b>Deriv Login — API Token</b>\n\n"
        f"Paste your Deriv <b>Personal Access Token (PAT)</b> below.\n\n"
        f"It looks like:\n"
        f"<code>pat_a75b3d3660106a681021c8f0ab340c5138f683365a1c422b18df79a7886b8db2</code>\n\n"
        f"Generate one at Deriv → Settings → API Token (grant it <b>Read</b> + <b>Trade</b> scopes).\n\n"
        f"Send /cancel to abort.",
        chat_id=chat_id
    )


def _handle_login(chat_id, username):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized. Contact admin.", chat_id=chat_id)
        return
    _handle_login_pat(chat_id, username)


def _handle_cancel(chat_id):
    with pending_cmd_lock:
        pending_command.pop(chat_id, None)
    tg_send("✅  Cancelled.", chat_id=chat_id)


def _handle_help(chat_id):
    if not _is_authorized(chat_id):
        tg_send("⛔  Not authorized.", chat_id=chat_id)
        return
    s     = get_slot_by_chat(chat_id)
    is_adm = _is_admin(chat_id)
    api_mode = s.get("deriv_mode", "?").upper() + " API" if s else "Not connected"
    tg_send(
        f"📖  <b>JAHIM BOT — Commands</b>\n"
        f"Mode: <b>{api_mode}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>🔐 Account</b>\n"
        f"  /start — Register / welcome screen\n"
        f"  /login — Connect your Deriv account\n"
        f"  /myid — Your chat ID &amp; access info\n\n"
        f"<b>🤖 Trading</b>\n"
        f"  /startbot — Start trading\n"
        f"  /stop — Pause trading\n"
        f"  /status — Live status report (HTML)\n\n"
        f"<b>⚙️ Settings</b>\n"
        f"  /setstake — Change base stake\n"
        f"  /setmartingale — Martingale mode\n"
        f"  /setcontracttype — RISE/FALL or HIGHER/LOWER\n"
        f"  /setduration — Contract duration (ticks)\n"
        f"  /setmartingalevalue — Martingale multiplier\n"
        f"  /settrademode — REVERSAL or NORMAL\n"
        f"  /setvirtual — Virtual trading on/off\n"
        f"  /cancel — Cancel current action\n"
        f"  /help — Show this menu\n"
        + (
            f"\n<b>👑 Admin</b>\n"
            f"  /slots — View all trading slots\n"
            f"  /listids — List authorized users\n"
            f"  /addid [id] [hours] — Add user\n"
            f"  /revokeid [id] — Revoke user"
            if is_adm else ""
        ),
        chat_id=chat_id
    )


def _handle_start(chat_id, username):
    # Admin always gets auto-authorized with unlimited (never-expiring) access.
    if _is_admin(chat_id) and not _is_authorized(chat_id):
        authorized_ids[chat_id] = None
        save_admin()
        tg_send(
            f"👑  <b>Admin Auto-Authorized</b>\n\n"
            f"Welcome, <b>{html_lib.escape(username)}</b> — you now have unlimited access. ♾️",
            chat_id=chat_id
        )

    if not _is_authorized(chat_id):
        tg_send(
            f"⛔  <b>Access Required</b>\n\n"
            f"Hi <b>{html_lib.escape(username)}</b>! 👋\n\n"
            f"You need authorization to use this bot.\n"
            f"Contact {ADMIN_USERNAME} to get access.",
            chat_id=chat_id
        )
        return
    s = get_slot_by_chat(chat_id)
    if s and s.get("api_token"):
        status_line = "▶️  <b>Trading LIVE</b>" if s["trading_active"] else "⏸️  <b>Paused</b>"
        profit_sign = "+" if s["daily_profit"] >= 0 else ""
        profit_col_note = "🟢" if s["daily_profit"] >= 0 else "🔴"
        tg_send(
            f"👋  <b>Welcome back, {html_lib.escape(username)}!</b>\n\n"
            f"┌─────────────────────────\n"
            f"│  🎰  Slot <b>#{s['slot_id']}</b>  │  {s.get('deriv_mode','?').upper()} API\n"
            f"│  💼  <code>{html_lib.escape(s.get('account_id','—'))}</code>\n"
            f"│  💰  Balance: <b>${s['current_balance']:,.2f}</b>\n"
            f"│  {profit_col_note}  Today P/L: <b>{profit_sign}${s['daily_profit']:,.4f}</b>\n"
            f"│  {status_line}\n"
            f"└─────────────────────────\n\n"
            f"▶️ /startbot  ⏸ /stop  📊 /status  ❓ /help",
            chat_id=chat_id
        )
        return
    if free_slot_count() == 0:
        tg_send(
            f"❌  <b>All Slots Full</b>\n\n"
            f"No available trading slots at the moment.\n"
            f"Contact {ADMIN_USERNAME} for access.",
            chat_id=chat_id
        )
        return
    # Ask martingale mode (single API now — New Deriv API only)
    with pending_cmd_lock:
        pending_command[chat_id] = {"cmd": "choose_martingale_on_start", "step": 1, "username": username}
    tg_send(
        f"👋  <b>Welcome, {html_lib.escape(username)}!</b>\n\n"
        f"<b>JAHIM UNIFIED BOT</b> — Automated Deriv Trader (New Deriv API)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Choose your <b>martingale mode</b>:\n\n"
        f"<b>1</b> — ⚡ <b>Immediate</b> — re-enter same symbol immediately after loss\n"
        f"<b>2</b> — 🔍 <b>Signal</b> — wait for next valid signal, then apply martingale stake\n\n"
        f"Send <b>1</b> or <b>2</b>:",
        chat_id=chat_id
    )


def _handle_slots(chat_id):
    if not _is_admin(chat_id):
        tg_send("⛔  Admin only.", chat_id=chat_id)
        return
    lines = [f"<b>🎰 Slots Overview ({MAX_SLOTS} total)</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for i in range(1, MAX_SLOTS + 1):
        s = slots.get(i)
        if not s or not s.get("chat_id"):
            lines.append(f"<code>Slot {i:02d}</code>  ○  <i>empty</i>")
        else:
            api  = s.get("deriv_mode", "?").upper()
            stat = "▶️" if s["trading_active"] else "⏸️"
            profit_sign = "+" if s["daily_profit"] >= 0 else ""
            lines.append(
                f"<code>Slot {i:02d}</code>  {stat}  <b>{html_lib.escape(s['username'] or '?')}</b>\n"
                f"          [{api}]  💰${s['current_balance']:.2f}  P/L:{profit_sign}${s['daily_profit']:.2f}"
            )
    tg_send("\n".join(lines), chat_id=chat_id)


def _handle_listids(chat_id):
    if not _is_admin(chat_id):
        tg_send("⛔  Admin only.", chat_id=chat_id)
        return
    if not authorized_ids:
        tg_send("No authorized users.", chat_id=chat_id)
        return
    lines = ["<b>👥 Authorized Users</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for cid, exp in authorized_ids.items():
        exp_str = _expiry_remaining_str(exp)
        lines.append(f"🆔  <code>{cid}</code>\n     ⏳ {exp_str}")
    tg_send("\n".join(lines), chat_id=chat_id)


def _handle_addid(chat_id, text):
    if not _is_admin(chat_id):
        tg_send("⛔  Admin only.", chat_id=chat_id)
        return
    parts = text.strip().split()
    if len(parts) < 2:
        tg_send("Usage: /addid [chat_id] [hours or 0=permanent]", chat_id=chat_id)
        return
    try:
        target_id = int(parts[1])
        hours     = int(parts[2]) if len(parts) >= 3 else ACCESS_DURATION_HOURS
    except ValueError:
        tg_send("❌  Invalid ID or hours.", chat_id=chat_id)
        return
    expiry = None if hours == 0 else time.time() + hours * 3600
    authorized_ids[target_id] = expiry
    save_admin()
    exp_str = "♾️ Permanent" if expiry is None else f"{hours}h"
    tg_send(f"✅  Added <code>{target_id}</code> | Access: {exp_str}", chat_id=chat_id)
    tg_send(
        f"✅  <b>Access Granted!</b>\n\nWelcome to JAHIM BOT!\nAccess: {exp_str}\n\nUse /start to begin.",
        chat_id=target_id
    )


def _handle_revokeid(chat_id, text):
    if not _is_admin(chat_id):
        tg_send("⛔  Admin only.", chat_id=chat_id)
        return
    parts = text.strip().split()
    if len(parts) < 2:
        tg_send("Usage: /revokeid [chat_id]", chat_id=chat_id)
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        tg_send("❌  Invalid ID.", chat_id=chat_id)
        return
    authorized_ids.pop(target_id, None)
    with slots_lock:
        for s in slots.values():
            if s["chat_id"] == target_id:
                _safe_close_trade_ws(s)
                slots[s["slot_id"]] = _make_empty_slot(s["slot_id"])
                break
    save_admin()
    save_slots()
    tg_send(f"✅  Revoked access for <code>{target_id}</code>.", chat_id=chat_id)
    tg_send("⛔  Your access has been revoked. Contact admin.", chat_id=target_id)


# ╔══════════════════════════════════════════════════════════════╗
# ║              PENDING REPLY PROCESSOR                         ║
# ╚══════════════════════════════════════════════════════════════╝

def _process_pending_reply(chat_id, text, username):
    with pending_cmd_lock:
        state = pending_command.get(chat_id)
    if not state:
        return False

    cmd = state.get("cmd", "")

    if text.strip().lower() in ("/cancel", "/cancle", "/cancell", "/cancal"):
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send("✅  Cancelled.", chat_id=chat_id)
        return True

    # ── Choose martingale mode after /start ──
    if cmd == "choose_martingale_on_start":
        raw = text.strip()
        if raw == "1":
            chosen_m = MARTINGALE_MODE_IMMEDIATE
            m_label  = "⚡ Immediate"
        elif raw == "2":
            chosen_m = MARTINGALE_MODE_SIGNAL
            m_label  = "🔍 Signal"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True
        _pending_martingale_modes[chat_id] = chosen_m
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)

        tg_send(
            f"✅  <b>Setup Complete!</b>\n\n"
            f"Martingale: <b>{m_label}</b>\n\n"
            f"Use <b>/login</b> to connect your Deriv account with an API token.",
            chat_id=chat_id
        )
        return True

    # ── Choose contract type after login (before the slot starts trading) ──
    if cmd == "choose_contract_type_on_login":
        raw = text.strip()
        if raw == "1":
            chosen_c = CONTRACT_MODE_RISE_FALL
            c_label  = "📈 RISE/FALL"
        elif raw == "2":
            chosen_c = CONTRACT_MODE_HIGHER_LOWER
            c_label  = "🎯 HIGHER/LOWER"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["contract_mode"] = chosen_c
        save_slots()
        if chosen_c == CONTRACT_MODE_HIGHER_LOWER:
            with pending_cmd_lock:
                pending_command[chat_id] = {
                    "cmd": "choose_higher_lower_recovery_on_login", "step": 1
                }
            tg_send(
                f"✅  <b>Contract type set: {c_label}</b>\n\n"
                f"How should the bot recover after a <b>HIGHER/LOWER loss</b>?\n\n"
                f"<b>1</b> — 🔁 <b>Martingale</b> — continue with HIGHER/LOWER\n"
                f"<b>2</b> — 📈 <b>Recover with RISE/FALL</b> — trade RISE/FALL until a win, "
                f"apply martingale on losses, then return to HIGHER/LOWER\n\n"
                f"Send <b>1</b> or <b>2</b>:",
                chat_id=chat_id
            )
            return True
        min_ticks = CONTRACT_DURATION_RISE_FALL if chosen_c == CONTRACT_MODE_RISE_FALL else CONTRACT_DURATION
        with pending_cmd_lock:
            pending_command[chat_id] = {"cmd": "choose_duration_on_login", "step": 1, "min_ticks": min_ticks}
        tg_send(
            f"✅  <b>Contract type set: {c_label}</b>\n\n"
            f"⏱  How many <b>ticks</b> should the contract duration be?\n\n"
            f"Minimum for {c_label}: <b>{min_ticks}</b>\n"
            f"Send a whole number (e.g. <b>{min_ticks}</b>):",
            chat_id=chat_id
        )
        return True

    # ── Choose HIGHER/LOWER loss-recovery strategy during onboarding ──
    if cmd == "choose_higher_lower_recovery_on_login":
        raw = text.strip()
        if raw == "1":
            recovery_mode = HIGHER_LOWER_RECOVERY_MARTINGALE
            recovery_label = "🔁 Martingale"
        elif raw == "2":
            recovery_mode = HIGHER_LOWER_RECOVERY_RISE_FALL
            recovery_label = "📈 RISE/FALL until win"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["higher_lower_recovery_mode"] = recovery_mode
        save_slots()
        min_ticks = CONTRACT_DURATION
        with pending_cmd_lock:
            pending_command[chat_id] = {
                "cmd": "choose_duration_on_login", "step": 1, "min_ticks": min_ticks
            }
        tg_send(
            f"✅  <b>Loss recovery set: {recovery_label}</b>\n\n"
            f"⏱  How many <b>ticks</b> should the contract duration be?\n\n"
            f"Minimum for 🎯 HIGHER/LOWER: <b>{min_ticks}</b>\n"
            f"Send a whole number (e.g. <b>{min_ticks}</b>):",
            chat_id=chat_id
        )
        return True

    # ── Choose duration (ticks) after contract type, before login finishes ──
    if cmd == "choose_duration_on_login":
        raw = text.strip()
        min_ticks = state.get("min_ticks", CONTRACT_DURATION)
        if not raw.isdigit() or int(raw) < min_ticks:
            tg_send(f"❌  Send a whole number of <b>{min_ticks}</b> or more.", chat_id=chat_id)
            return True
        ticks = int(raw)

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["duration_ticks"] = ticks
        save_slots()
        with pending_cmd_lock:
            pending_command[chat_id] = {"cmd": "choose_martingale_value_on_login", "step": 1}
        tg_send(
            f"✅  <b>Duration set: {ticks} ticks</b>\n\n"
            f"🔁  What <b>martingale multiplier</b> should be used after a loss?\n\n"
            f"Send a number greater than 1 (e.g. <b>2</b> or <b>2.5</b>):",
            chat_id=chat_id
        )
        return True

    # ── Choose martingale multiplier after duration, before login finishes ──
    if cmd == "choose_martingale_value_on_login":
        raw = text.strip()
        try:
            mval = float(raw)
        except ValueError:
            mval = None
        if mval is None or mval <= 1:
            tg_send("❌  Send a number greater than <b>1</b> (e.g. <b>2</b> or <b>2.5</b>).", chat_id=chat_id)
            return True

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["martingale_multiplier"] = mval
        save_slots()
        with pending_cmd_lock:
            pending_command[chat_id] = {"cmd": "choose_trade_mode_on_login", "step": 1}
        tg_send(
            f"✅  <b>Martingale multiplier set: {mval:g}×</b>\n\n"
            f"Choose your <b>trade mode</b>:\n\n"
            f"<b>1</b> — 🔄 <b>REVERSAL</b> — 5 consecutive down ticks → trade HIGHER (and vice versa)\n"
            f"<b>2</b> — ➡️ <b>NORMAL</b> — 5 consecutive down ticks → trade LOWER (and vice versa)\n\n"
            f"Send <b>1</b> or <b>2</b>:",
            chat_id=chat_id
        )
        return True

    # ── Choose trade mode after login (before the slot starts trading) ──
    if cmd == "choose_trade_mode_on_login":
        raw = text.strip()
        if raw == "1":
            chosen_t = TRADE_MODE_REVERSAL
            t_label  = "🔄 REVERSAL"
        elif raw == "2":
            chosen_t = TRADE_MODE_NORMAL
            t_label  = "➡️ NORMAL"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["trade_mode"] = chosen_t
        save_slots()
        with pending_cmd_lock:
            pending_command[chat_id] = {"cmd": "choose_virtual_on_login", "step": 1}
        tg_send(
            f"✅  <b>Trade mode set: {t_label}</b>\n\n"
            f"Use <b>virtual trading</b> first?\n\n"
            f"<b>1</b> — ✅ YES — trade on virtual/paper signals first\n"
            f"<b>2</b> — 🚫 NO — go straight to real trading\n\n"
            f"Send <b>1</b> or <b>2</b>:",
            chat_id=chat_id
        )
        return True

    # ── Choose virtual trading after login (before the slot starts trading) ──
    if cmd == "choose_virtual_on_login":
        raw = text.strip()
        if raw == "1":
            s = get_slot_by_chat(chat_id)
            if not s:
                with pending_cmd_lock:
                    pending_command.pop(chat_id, None)
                tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
                return True
            with pending_cmd_lock:
                pending_command[chat_id] = {"cmd": "set_virtual_loss_limit_on_login", "step": 1}
            tg_send(
                f"📝  How many <b>virtual losses</b> before shifting Slot #{s['slot_id']} to real trading?\n\n"
                f"Send a number (e.g. <b>3</b>):",
                chat_id=chat_id
            )
            return True
        elif raw == "2":
            s = get_slot_by_chat(chat_id)
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            if not s:
                tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
                return True
            s["virtual_mode"]   = False
            _reset_all_symbol_virtual(s, False)
            s["setup_complete"] = True  # onboarding wizard finished — safe to auto-start on restart now
            save_slots()
            tg_send(
                f"✅  <b>Virtual trading: OFF</b> — trading real from the start.\n\n"
                f"Use /startbot to begin trading · /help for all commands",
                chat_id=chat_id
            )
            start_slot(s)
            return True
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

    # ── Virtual loss limit number after login ──
    if cmd == "set_virtual_loss_limit_on_login":
        raw = text.strip()
        if not raw.isdigit() or int(raw) <= 0:
            tg_send("❌  Send a whole number greater than 0.", chat_id=chat_id)
            return True
        limit = int(raw)

        s = get_slot_by_chat(chat_id)
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        if not s:
            tg_send("❌  Slot not found — please /login again.", chat_id=chat_id)
            return True

        s["virtual_mode"]       = True
        _reset_all_symbol_virtual(s, True)
        s["virtual_loss_limit"] = limit
        s["setup_complete"]     = True  # onboarding wizard finished — safe to auto-start on restart now
        save_slots()
        tg_send(
            f"✅  <b>Virtual trading: ON</b> — will shift to real after "
            f"<b>{limit}</b> virtual loss{'es' if limit != 1 else ''}.\n\n"
            f"Use /startbot to begin trading · /help for all commands",
            chat_id=chat_id
        )
        start_slot(s)
        return True

    # ── PAT token paste (Deriv API login) ──
    if cmd == "login_pat_token":
        token = text.strip().strip('`"\' ')
        if token.lower().startswith("token:"):
            token = token.split(":", 1)[1].strip()

        if not re.match(r'^pat_[A-Za-z0-9]{20,}$', token):
            tg_send(
                "❌  That doesn't look like a valid Deriv PAT.\n\n"
                "It should start with <code>pat_</code> followed by a long alphanumeric string.\n"
                "Paste your token again, or /cancel.",
                chat_id=chat_id
            )
            return True

        username_saved = state.get("username", username)
        tg_send("🔍  Validating token…", chat_id=chat_id)

        def _do_pat_login():
            acct_ok, acct_result = _fetch_accounts(token)
            if not acct_ok:
                tg_send(
                    f"❌  <b>Token validation failed</b>\n\n"
                    f"<code>{html_lib.escape(str(acct_result))}</code>\n\n"
                    f"Paste a valid token, or /cancel.",
                    chat_id=chat_id
                )
                return
            accounts  = acct_result
            real_list = [a for a in accounts if not a.get("is_virtual")]
            demo_list = [a for a in accounts if a.get("is_virtual")]
            with _oauth_tokens_lock:
                _oauth_tokens[chat_id] = {
                    "access_token":  token,
                    "refresh_token": None,
                    # PATs are static/long-lived (revoked manually in Deriv, not expiry-based) —
                    # set a far-future expiry so _get_valid_access_token never tries to refresh them.
                    "expires_at":    time.time() + 86400 * 365 * 10,
                    "accounts":      accounts,
                }
            _save_oauth_tokens()
            with pending_cmd_lock:
                pending_command[chat_id] = {
                    "cmd": "choose_account", "step": 1,
                    "username": username_saved, "real_list": real_list, "demo_list": demo_list,
                }
            tg_send(_build_account_choice_msg(username_saved, real_list, demo_list), chat_id=chat_id)

        threading.Thread(target=_do_pat_login, daemon=True).start()
        return True

    # ── Choose account (Deriv API login flow) ──
    if cmd == "choose_account":
        reply     = text.strip()
        real_list = state.get("real_list", [])
        demo_list = state.get("demo_list", [])
        step      = state.get("step", 1)

        if step == 1:
            if reply not in ("1", "2"):
                tg_send("❌  Reply <b>1</b> for Real or <b>2</b> for Demo.", chat_id=chat_id)
                return True
            chosen_list = real_list if reply == "1" else demo_list
            if not chosen_list:
                other       = "2" if reply == "1" else "1"
                other_label = "Demo 🎮" if reply == "1" else "Real 💳"
                tg_send(f"❌  No account found. Reply <b>{other}</b> for <b>{other_label}</b>.", chat_id=chat_id)
                return True
            if len(chosen_list) == 1:
                with _oauth_tokens_lock:
                    tokens = dict(_oauth_tokens.get(chat_id, {}))
                tokens.pop("accounts", None)
                with pending_cmd_lock:
                    pending_command.pop(chat_id, None)
                _finalize_oauth_login(chat_id, state.get("username", username), chosen_list[0], tokens)
                return True
            lines = []
            for i, a in enumerate(chosen_list, 1):
                aid = a.get("account_id") or a.get("loginid", "?")
                bal = float(a.get("balance", 0) or 0)
                cur = a.get("currency", "USD")
                lines.append(f"{i}. <code>{aid}</code>  —  <b>{bal:,.2f} {cur}</b>")
            label = "Real" if reply == "1" else "Demo"
            with pending_cmd_lock:
                pending_command[chat_id]["step"]        = 2
                pending_command[chat_id]["chosen_list"] = chosen_list
            tg_send(
                f"Choose your <b>{label}</b> account:\n\n" + "\n".join(lines) +
                f"\n\nReply with the number (1–{len(chosen_list)}).",
                chat_id=chat_id
            )
            return True

        if step == 2:
            chosen_list = state.get("chosen_list", real_list)
            try:
                idx = int(reply) - 1
                assert 0 <= idx < len(chosen_list)
            except Exception:
                tg_send(f"❌  Reply with a number between 1 and {len(chosen_list)}.", chat_id=chat_id)
                return True
            chosen = chosen_list[idx]
            with _oauth_tokens_lock:
                tokens = dict(_oauth_tokens.get(chat_id, {}))
            tokens.pop("accounts", None)
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            _finalize_oauth_login(chat_id, state.get("username", username), chosen, tokens)
            return True

    # ── Martingale mode change ──
    if cmd in ("setmartingale", "setmartingale_preregister"):
        raw = text.strip()
        if raw == "1":
            new_mode = MARTINGALE_MODE_IMMEDIATE
            m_label  = "⚡ Immediate"
        elif raw == "2":
            new_mode = MARTINGALE_MODE_SIGNAL
            m_label  = "🔍 Signal"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True
        s = get_slot_by_chat(chat_id)
        if s:
            s["martingale_mode"] = new_mode
            save_slots()
        else:
            _pending_martingale_modes[chat_id] = new_mode
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(f"✅  Martingale mode set to <b>{m_label}</b>.", chat_id=chat_id)
        return True

    # ── Contract type update ──
    if cmd == "setcontracttype":
        raw = text.strip()
        if raw == "1":
            new_mode = CONTRACT_MODE_RISE_FALL
            c_label  = "📈 RISE/FALL"
        elif raw == "2":
            new_mode = CONTRACT_MODE_HIGHER_LOWER
            c_label  = "🎯 HIGHER/LOWER"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True
        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        s["contract_mode"] = new_mode
        save_slots()
        if new_mode == CONTRACT_MODE_HIGHER_LOWER:
            with pending_cmd_lock:
                pending_command[chat_id] = {
                    "cmd": "set_higher_lower_recovery", "step": 1
                }
            tg_send(
                f"✅  <b>Contract type set: {c_label}</b>\n\n"
                f"How should the bot recover after a <b>HIGHER/LOWER loss</b>?\n\n"
                f"<b>1</b> — 🔁 <b>Martingale</b> — continue with HIGHER/LOWER\n"
                f"<b>2</b> — 📈 <b>Recover with RISE/FALL</b> — trade RISE/FALL until a win, "
                f"apply martingale on losses, then return to HIGHER/LOWER\n\n"
                f"Send <b>1</b> or <b>2</b>:",
                chat_id=chat_id
            )
            return True
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(f"✅  Contract type set to <b>{c_label}</b>.", chat_id=chat_id)
        return True

    # ── Choose HIGHER/LOWER loss-recovery strategy from /setcontracttype ──
    if cmd == "set_higher_lower_recovery":
        raw = text.strip()
        if raw == "1":
            recovery_mode = HIGHER_LOWER_RECOVERY_MARTINGALE
            recovery_label = "🔁 Martingale"
        elif raw == "2":
            recovery_mode = HIGHER_LOWER_RECOVERY_RISE_FALL
            recovery_label = "📈 RISE/FALL until win"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        s["higher_lower_recovery_mode"] = recovery_mode
        save_slots()
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(
            f"✅  HIGHER/LOWER loss recovery set to <b>{recovery_label}</b>.",
            chat_id=chat_id
        )
        return True

    # ── Duration (ticks) update ──
    if cmd == "setduration":
        raw = text.strip()
        min_ticks = state.get("min_ticks", CONTRACT_DURATION)
        if not raw.isdigit() or int(raw) < min_ticks:
            tg_send(f"❌  Send a whole number of <b>{min_ticks}</b> or more.", chat_id=chat_id)
            return True
        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        s["duration_ticks"] = int(raw)
        save_slots()
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(f"✅  Duration set to <b>{raw} ticks</b>.", chat_id=chat_id)
        return True

    # ── Martingale multiplier update ──
    if cmd == "setmartingalevalue":
        raw = text.strip()
        try:
            mval = float(raw)
        except ValueError:
            mval = None
        if mval is None or mval <= 1:
            tg_send("❌  Send a number greater than <b>1</b> (e.g. <b>2.5</b>).", chat_id=chat_id)
            return True
        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        s["martingale_multiplier"] = mval
        save_slots()
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(f"✅  Martingale multiplier set to <b>{mval:g}×</b>.", chat_id=chat_id)
        return True

    # ── Trade mode update ──
    if cmd == "settrademode":
        raw = text.strip()
        if raw == "1":
            new_mode = TRADE_MODE_REVERSAL
            t_label  = "🔄 REVERSAL"
        elif raw == "2":
            new_mode = TRADE_MODE_NORMAL
            t_label  = "➡️ NORMAL"
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True
        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        s["trade_mode"] = new_mode
        save_slots()
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        tg_send(f"✅  Trade mode set to <b>{t_label}</b>.", chat_id=chat_id)
        return True

    # ── Virtual trading toggle (via /setvirtual) ──
    if cmd == "setvirtual":
        raw = text.strip()
        if raw == "1":
            s = get_slot_by_chat(chat_id)
            if not s:
                with pending_cmd_lock:
                    pending_command.pop(chat_id, None)
                return True
            with pending_cmd_lock:
                pending_command[chat_id] = {"cmd": "setvirtual_loss_limit", "step": 1}
            tg_send(
                f"📝  How many <b>virtual losses</b> before shifting to real trading?\n\n"
                f"Send a number (e.g. <b>3</b>):",
                chat_id=chat_id
            )
            return True
        elif raw == "2":
            s = get_slot_by_chat(chat_id)
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            if not s:
                return True
            s["virtual_mode"]   = False
            _reset_all_symbol_virtual(s, False)
            save_slots()
            tg_send("✅  Virtual trading turned <b>OFF</b> — trading real now.", chat_id=chat_id)
            return True
        else:
            tg_send("❌  Send <b>1</b> or <b>2</b>.", chat_id=chat_id)
            return True

    # ── Virtual loss limit number (via /setvirtual) ──
    if cmd == "setvirtual_loss_limit":
        raw = text.strip()
        if not raw.isdigit() or int(raw) <= 0:
            tg_send("❌  Send a whole number greater than 0.", chat_id=chat_id)
            return True
        limit = int(raw)
        s = get_slot_by_chat(chat_id)
        with pending_cmd_lock:
            pending_command.pop(chat_id, None)
        if not s:
            return True
        s["virtual_mode"]       = True
        _reset_all_symbol_virtual(s, True)
        s["virtual_loss_limit"] = limit
        save_slots()
        tg_send(
            f"✅  Virtual trading turned <b>ON</b> — will shift to real after "
            f"<b>{limit}</b> virtual loss{'es' if limit != 1 else ''}.",
            chat_id=chat_id
        )
        return True

    # ── Stake update ──
    if cmd == "setstake":
        s = get_slot_by_chat(chat_id)
        if not s:
            with pending_cmd_lock:
                pending_command.pop(chat_id, None)
            return True
        if state["step"] == 1:
            try:
                new_stake = float(text.strip())
                if new_stake <= 0:
                    raise ValueError
            except ValueError:
                tg_send("❌  Invalid amount. Send a positive number.", chat_id=chat_id)
                return True
            with pending_cmd_lock:
                pending_command[chat_id] = {"cmd": "setstake", "step": 2, "value": new_stake}
            tg_send(
                f"💵  Confirm stake change?\n\n"
                f"<b>${s['base_stake']:.2f}</b>  →  <b>${new_stake:.2f}</b>\n\n"
                f"Reply <b>YES</b> to confirm or /cancel.",
                chat_id=chat_id
            )
            return True
        if state["step"] == 2:
            if text.strip().upper() == "YES":
                old = s["base_stake"]
                s["base_stake"] = state["value"]
                with s["martingale_lock"]:
                    for v in VOLATILITIES:
                        sm = s["symbol_martingale"][v]
                        if not sm["in_martingale"]:
                            sm["next_stake"] = s["base_stake"]
                save_slots()
                with pending_cmd_lock:
                    pending_command.pop(chat_id, None)
                tg_send(f"✅  Stake updated: <b>${old:.2f}</b>  →  <b>${s['base_stake']:.2f}</b>", chat_id=chat_id)
            else:
                tg_send("❌  Reply <b>YES</b> or /cancel.", chat_id=chat_id)
            return True

    return False


# ╔══════════════════════════════════════════════════════════════╗
# ║              TELEGRAM POLLING                                ║
# ╚══════════════════════════════════════════════════════════════╝

def _poll_telegram():
    global _tg_offset
    print(Fore.CYAN + "📱 Telegram polling started")
    backoff = 1

    while not _shutdown_event.is_set():
        try:
            r = requests.get(
                f"{TG_API}/getUpdates",
                params={"offset": _tg_offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=40,
            )
            if r.status_code == 429:
                time.sleep(r.json().get("parameters", {}).get("retry_after", backoff))
                continue
            if not r.ok:
                time.sleep(backoff)
                backoff = min(backoff * 2, TG_POLL_BACKOFF_MAX)
                continue
            backoff  = 1
            updates  = r.json().get("result", [])

            for upd in updates:
                _tg_offset = upd["update_id"] + 1
                msg        = upd.get("message", {})
                text       = msg.get("text", "").strip()
                chat_id    = msg.get("chat", {}).get("id")
                username   = msg.get("from", {}).get("first_name", "there")
                if not text or not chat_id:
                    continue

                try:
                    if _process_pending_reply(chat_id, text, username):
                        continue
                    cmd = text.split("@")[0].lower()

                    if   cmd == "/start":           _handle_start(chat_id, username)
                    elif cmd == "/login":           _handle_login(chat_id, username)
                    elif cmd == "/myid":            _handle_myid(chat_id, username)
                    elif cmd == "/status":          _handle_status(chat_id)
                    elif cmd == "/stop":            _handle_stop(chat_id)
                    elif cmd == "/startbot":        _handle_startbot(chat_id)
                    elif cmd == "/setstake":        _handle_setstake(chat_id)
                    elif cmd == "/setmartingale":   _handle_setmartingale(chat_id)
                    elif cmd == "/setcontracttype": _handle_setcontracttype(chat_id)
                    elif cmd == "/setduration":     _handle_setduration(chat_id)
                    elif cmd == "/setmartingalevalue": _handle_setmartingalevalue(chat_id)
                    elif cmd == "/settrademode":    _handle_settrademode(chat_id)
                    elif cmd == "/setvirtual":      _handle_setvirtual(chat_id)
                    elif cmd in ("/cancel", "/cancle", "/cancell", "/cancal"):
                        _handle_cancel(chat_id)
                    elif cmd == "/help":            _handle_help(chat_id)
                    elif cmd == "/slots":           _handle_slots(chat_id)
                    elif cmd == "/listids":         _handle_listids(chat_id)
                    elif text.lower().startswith("/addid"):    _handle_addid(chat_id, text)
                    elif text.lower().startswith("/revokeid"): _handle_revokeid(chat_id, text)
                    else:
                        tg_send(f"❓  Unknown command.\nUse /help to see all available commands.", chat_id=chat_id)
                except Exception as handler_err:
                    print(Fore.RED + f"❌ TG handler error for {chat_id}: {handler_err}")

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as net_err:
            print(Fore.RED + f"❌ TG poll error: {net_err}")
            time.sleep(backoff)
            backoff = min(backoff * 2, TG_POLL_BACKOFF_MAX)
        except Exception as e:
            print(Fore.RED + f"❌ TG poll unexpected: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, TG_POLL_BACKOFF_MAX)


# ╔══════════════════════════════════════════════════════════════╗
# ║              GRACEFUL SHUTDOWN                               ║
# ╚══════════════════════════════════════════════════════════════╝

def _signal_handler(signum, frame):
    print(Fore.YELLOW + f"\n⚡ Signal {signum} — saving state and shutting down…")
    _shutdown_event.set()
    _save_slots_to_disk()  # synchronous, immediate flush — not the debounced save_slots()
    save_admin()
    _save_oauth_tokens()
    print(Fore.GREEN + "✅ State saved. Exiting.")
    sys.exit(0)


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT,  _signal_handler)


# ╔══════════════════════════════════════════════════════════════╗
# ║              MAIN                                            ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    global _shared_ws_thread_ref

    load_slots()
    _load_oauth_tokens()

    print(Fore.MAGENTA + "╔══════════════════════════════════════════════════════════════╗")
    print(Fore.MAGENTA + "║         JAHIM UNIFIED BOT — New Deriv API                   ║")
    print(Fore.MAGENTA + "╚══════════════════════════════════════════════════════════════╝")
    print(Fore.CYAN    + "   Analysis   : 1-min Deriv-aligned candle engine (NEW API)")
    print(Fore.CYAN    + "   New API    : PAT login → OTP WS → proposal→buy (HIGHER/LOWER)")
    print(Fore.CYAN    + "   Tick feed  : NEW public WS endpoint")
    print(Fore.CYAN    + f"   Symbols    : {', '.join(VOLATILITIES)}")
    print(Fore.CYAN    + f"   Slots      : {MAX_SLOTS}")
    print()

    # Start shared tick WS (single feed for all slots, both modes)
    t_shared = threading.Thread(target=run_shared_tick_ws, daemon=True, name="shared-tick-ws")
    _shared_ws_thread_ref = t_shared
    t_shared.start()
    print(Fore.CYAN + "📡 Shared tick WS thread launched")

    # Start trade WS for each persisted slot — but never for a slot that's
    # still mid-onboarding-wizard (contract type / trade mode / virtual mode
    # not yet chosen). api_token+chat_id get set as soon as the account is
    # connected, well before the wizard finishes, so gating on those alone
    # caused a restart mid-setup to start live trading with defaults.
    for s in slots.values():
        if s.get("api_token") and s.get("chat_id") and s.get("setup_complete"):
            start_slot(s)

    threading.Thread(target=watchdog,           daemon=True, name="watchdog").start()
    threading.Thread(target=midnight_scheduler, daemon=True, name="midnight-sched").start()
    threading.Thread(target=_poll_telegram,     daemon=True, name="tg-poller").start()
    threading.Thread(target=_barrier_prober_loop, daemon=True, name="barrier-prober").start()
    print(Fore.CYAN + "🔄 Background barrier prober thread launched")

    # Worker that carries all post-tick network/disk work (order placement,
    # result-check requests, recovery re-entries, slot-state saves) so the
    # tick-ingestion thread above never blocks on anything but reading the
    # next tick off the wire.
    threading.Thread(target=_tick_work_loop,   daemon=True, name="tick-worker").start()
    threading.Thread(target=_slots_saver_loop, daemon=True, name="slots-saver").start()
    print(Fore.CYAN + "⚡ Tick worker + async slot saver threads launched")

    print(Fore.GREEN + "✅ All systems running. Bot is LIVE.\n")

    last_save = time.time()
    while not _shutdown_event.is_set():
        time.sleep(30)
        if time.time() - last_save > 120:
            last_save = time.time()
            save_slots()


if __name__ == "__main__":
    main()
