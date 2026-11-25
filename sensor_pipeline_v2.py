#!/usr/bin/env python3
"""
sensor_pipeline_v2.py — Unified RL + MQTT pipeline (single process)
====================================================================
Reads JSON from STM32 via UART, runs online RL, publishes to MQTT.
Solves serial port conflict: single process owns /dev/ttyUSB0.

MQTT Topics:
  sensors/environment  -> temp, hum, mq135 (Node-RED Tab 1)
  sensors/presence     -> LD2410C radar    (Node-RED Tab 2)
  sensors/rl           -> RL decision      (Node-RED Tab 3)
  sensors/log          -> full frame log   (Node-RED Tab 3)
  sensors/status       -> heartbeat every 5s

Install:
  pip3 install pyserial paho-mqtt numpy
"""

import argparse
import csv
import json
import os
import queue
import signal
import statistics
import sys
import threading
import time
from collections import deque
from datetime import datetime
from enum import Enum, auto

import numpy as np

try:
    import serial
except ImportError:
    print("[ERR] pip3 install pyserial"); sys.exit(1)

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("[ERR] pip3 install paho-mqtt"); sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  BEGIN CONFIG
# ══════════════════════════════════════════════════════════════
DEFAULT_PORT          = "/dev/ttyUSB0"
DEFAULT_BAUD          = 115200
DEFAULT_BASELINE_SECS = 180
DEFAULT_BATCH_SIZE    = 50
DEFAULT_LOG_DIR       = "./sensor_logs"
DEFAULT_SAVE_EVERY    = 500

MQTT_BROKER  = "localhost"
MQTT_PORT    = 1883
MQTT_QOS     = 1

T_ENV        = "sensors/environment"
T_PRESENCE   = "sensors/presence"
T_LOG        = "sensors/log"
T_RL         = "sensors/rl"
T_STATUS     = "sensors/status"
T_LOGFILE    = "sensors/logfile"    # CSV log export -> Node-RED Tab 3

# How often to push CSV snapshot to Node-RED (seconds)
LOGFILE_PUSH_INTERVAL = 10

ALPHA        = 0.1
GAMMA        = 0.9
EPS_START    = 1.0
EPS_END      = 0.05
EPS_DECAY    = 0.9995

MQ_BINS      = [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5]
TEMP_BINS    = [-2.0, -1.0,  0.0, 1.0, 2.0]
HUM_BINS     = [-2.0, -1.0,  0.0, 1.0, 2.0]
N_ACTIONS    = 3
ACTION_NAMES = ["NORMAL", "WARNING", "ALERT"]

REWARD_TABLE = {
    (0, "normal"):  1.0,  (0, "warn"): -1.5,  (0, "alert"): -3.0,
    (1, "normal"): -0.5,  (1, "warn"):  1.5,  (1, "alert"): -1.0,
    (2, "normal"): -2.0,  (2, "warn"):  0.5,  (2, "alert"):  2.0,
}
# ══════════════════════════════════════════════════════════════
#  END CONFIG
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN MQTT PUBLISHER
# ══════════════════════════════════════════════════════════════
class MqttPublisher:
    """
    Non-blocking MQTT publisher using a queue and a daemon thread.

    Main thread calls publish() -> pushes to queue (non-blocking).
    Worker daemon thread drains queue and sends to broker.
    Serial port stays exclusively owned by the main thread.
    If broker is down, messages are dropped silently (queue.Full).
    """

    def __init__(self, broker: str, port: int):
        self._queue     = queue.Queue(maxsize=200)
        self._connected = False
        self._stop      = False

        self._client = mqtt.Client(
            client_id    = "sensor_pipeline_unified",
            clean_session = True,
        )
        self._client.on_connect    = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect(broker, port, keepalive=60)
            self._client.loop_start()
            for _ in range(30):          # wait up to 3 s
                if self._connected:
                    break
                time.sleep(0.1)
            if self._connected:
                print(f"[MQTT] Connected to {broker}:{port}")
            else:
                print("[MQTT] Connection timeout — continuing without MQTT")
        except Exception as e:
            print(f"[MQTT] Cannot connect: {e} — continuing without MQTT")

        self._thread = threading.Thread(
            target=self._worker, daemon=True, name="mqtt-worker"
        )
        self._thread.start()

    def _on_connect(self, client, userdata, flags, rc):
        self._connected = (rc == 0)

    def _on_disconnect(self, client, userdata, rc):
        self._connected = False

    def _worker(self):
        """Drain queue and publish to broker."""
        while not self._stop:
            try:
                topic, payload, retain = self._queue.get(timeout=1.0)
                if self._connected:
                    self._client.publish(
                        topic,
                        json.dumps(payload, ensure_ascii=False),
                        qos    = MQTT_QOS,
                        retain = retain,
                    )
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[MQTT] Publish error: {e}")

    def publish(self, topic: str, payload: dict, retain: bool = True):
        """Non-blocking push to internal queue."""
        try:
            self._queue.put_nowait((topic, payload, retain))
        except queue.Full:
            pass   # drop when broker is unreachable

    def close(self):
        self._stop = True
        self._thread.join(timeout=3.0)
        self._client.loop_stop()
        self._client.disconnect()
        print("[MQTT] Disconnected")

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()
# ══════════════════════════════════════════════════════════════
#  END MQTT PUBLISHER
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN UART RECEIVER
# ══════════════════════════════════════════════════════════════
class UartReceiver:
    """Sole owner of the serial port. Reads and parses JSON from STM32."""

    def __init__(self, port: str, baud: int):
        try:
            self.ser = serial.Serial(
                port     = port,
                baudrate = baud,
                bytesize = serial.EIGHTBITS,
                parity   = serial.PARITY_NONE,
                stopbits = serial.STOPBITS_ONE,
                timeout  = 2.0,
            )
            print(f"[OK] Serial {self.ser.name} @ {baud} baud")
        except serial.SerialException as e:
            print(f"[ERR] Cannot open serial port: {e}")
            sys.exit(1)

    def read_one(self) -> dict | None:
        """Return parsed JSON dict if valid env frame, else None."""
        try:
            raw  = self.ser.readline()
            if not raw:
                return None
            line = raw.decode("ascii", errors="ignore").strip()
            if line.startswith("["):
                print(f"  [STM32] {line}")
                return None
            if not line.startswith("{"):
                return None
            d = json.loads(line)
            return d if d.get("type") == "env" else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def close(self):
        self.ser.close()
# ══════════════════════════════════════════════════════════════
#  END UART RECEIVER
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN BASELINE COLLECTOR
# ══════════════════════════════════════════════════════════════
class BaselineCollector:
    """
    Phase 1: Collect sensor readings for a fixed duration.
    Computes mean/std used for z-score normalization in Phase 2.
    Saves results to checkpoint.json.
    """

    FIELDS = [
        "mq135_v", "mq135_v_corr", "cf", "temp", "hum",
        "move_cm", "move_e", "static_cm", "static_e", "detect_cm",
    ]

    def __init__(self, duration_secs: int):
        self.duration    = duration_secs
        self.buffers     = {f: [] for f in self.FIELDS}
        self.start_time  = time.time()
        self.frame_count = 0

    @property
    def elapsed(self)  -> float: return time.time() - self.start_time
    @property
    def remaining(self)-> float: return max(0.0, self.duration - self.elapsed)
    @property
    def progress(self) -> float: return min(1.0, self.elapsed / self.duration)
    @property
    def is_done(self)  -> bool:  return self.elapsed >= self.duration

    def add(self, d: dict):
        for f in self.FIELDS:
            v = d.get(f)
            if v is not None:
                self.buffers[f].append(float(v))
        self.frame_count += 1

    def progress_bar(self, width: int = 34) -> str:
        filled = int(self.progress * width)
        bar    = "█" * filled + "░" * (width - filled)
        m, s   = int(self.remaining // 60), int(self.remaining % 60)
        return f"[{bar}] {self.progress*100:5.1f}%  left {m:02d}:{s:02d}"

    def compute_stats(self) -> dict:
        result = {}
        for field, values in self.buffers.items():
            if not values:
                continue
            vs = sorted(values)
            n  = len(vs)

            def pct(p):
                i = (p / 100) * (n - 1)
                lo, hi = int(i), min(int(i) + 1, n - 1)
                return vs[lo] + (i - lo) * (vs[hi] - vs[lo])

            result[field] = {
                "mean": round(statistics.mean(values), 6),
                "std" : round(statistics.stdev(values) if n > 1 else 0.01, 6),
                "p25" : round(pct(25), 6),
                "p75" : round(pct(75), 6),
                "p95" : round(pct(95), 6),
            }
        return result

    def save_checkpoint(self, log_dir: str) -> str:
        os.makedirs(log_dir, exist_ok=True)
        stats   = self.compute_stats()
        mq      = stats.get("mq135_v_corr", {})
        tmp     = stats.get("temp", {})
        hum     = stats.get("hum",  {})
        mq_mean = mq.get("mean", 0)
        mq_std  = max(mq.get("std", 0.01), 1e-6)
        ckpt    = {
            "meta": {
                "created_at"   : datetime.now().isoformat(),
                "baseline_secs": self.duration,
                "frame_count"  : self.frame_count,
            },
            "baseline": stats,
            "rl_hints": {
                "mq135_warn_threshold" : round(mq_mean + 1.5 * mq_std, 6),
                "mq135_alert_threshold": round(mq_mean + 2.5 * mq_std, 6),
                "temp_normal_range"    : [tmp.get("p25", 0), tmp.get("p75", 0)],
                "hum_normal_range"     : [hum.get("p25", 0), hum.get("p75", 0)],
            },
        }
        path = os.path.join(log_dir, "checkpoint.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(ckpt, f, indent=2, ensure_ascii=False)
        return path
# ══════════════════════════════════════════════════════════════
#  END BASELINE COLLECTOR
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN BASELINE EVALUATOR
# ══════════════════════════════════════════════════════════════
class BaselineEvaluator:
    """
    Loads statistics from checkpoint.json.
    Provides z-score normalization, anomaly scoring, and true-level
    classification used as the RL reward signal.
    """

    def __init__(self, checkpoint_path: str):
        with open(checkpoint_path, encoding="utf-8") as f:
            ckpt = json.load(f)
        bl    = ckpt.get("baseline", {})
        hints = ckpt.get("rl_hints", {})
        mq    = bl.get("mq135_v_corr", {})
        tmp   = bl.get("temp", {})
        hum   = bl.get("hum",  {})
        self.mq_mean   = mq.get("mean", 0.0)
        self.mq_std    = max(mq.get("std", 0.01), 1e-6)
        self.warn_thr  = hints.get("mq135_warn_threshold",  self.mq_mean + 1.5 * self.mq_std)
        self.alert_thr = hints.get("mq135_alert_threshold", self.mq_mean + 2.5 * self.mq_std)
        self.t_mean    = tmp.get("mean", 25.0)
        self.t_std     = max(tmp.get("std", 1.0), 1e-6)
        self.h_mean    = hum.get("mean", 60.0)
        self.h_std     = max(hum.get("std", 1.0), 1e-6)

    def mq_z(self, v: float) -> float: return (v - self.mq_mean) / self.mq_std
    def t_z(self,  v: float) -> float: return (v - self.t_mean)  / self.t_std
    def h_z(self,  v: float) -> float: return (v - self.h_mean)  / self.h_std

    def true_level(self, mq_vc: float, temp: float = None, hum: float = None) -> str:
        """Return 'normal', 'warn', or 'alert' based on sensor readings."""
        env_bonus = 0.0
        if temp is not None:
            tz = abs(self.t_z(temp))
            env_bonus += 0.4 if tz > 2.0 else (0.2 if tz > 1.5 else 0.0)
        if hum is not None:
            hz = abs(self.h_z(hum))
            env_bonus += 0.3 if hz > 2.0 else (0.15 if hz > 1.5 else 0.0)
        if mq_vc >= self.alert_thr:
            return "alert"
        if mq_vc >= self.warn_thr:
            return "alert" if env_bonus >= 0.7 else "warn"
        if self.mq_z(mq_vc) > 1.0 and env_bonus >= 0.5:
            return "warn"
        return "normal"

    def anomaly_score(self, mq_vc: float, temp: float = None, hum: float = None) -> float:
        mq_z = abs(self.mq_z(mq_vc))
        tz   = abs(self.t_z(temp)) if temp is not None else 0.0
        hz   = abs(self.h_z(hum))  if hum  is not None else 0.0
        return round(0.6 * mq_z + 0.25 * tz + 0.15 * hz, 4)

    def to_state(self, d: dict) -> tuple:
        """Discretize continuous readings into RL state tuple."""
        mq_vc = float(d.get("mq135_v_corr", self.mq_mean))
        temp  = float(d.get("temp",  self.t_mean))
        hum   = float(d.get("hum",   self.h_mean))
        pres  = int(d.get("presence", 0))
        return (
            int(np.digitize(self.mq_z(mq_vc), MQ_BINS)),
            int(np.digitize(self.t_z(temp),   TEMP_BINS)),
            int(np.digitize(self.h_z(hum),    HUM_BINS)),
            pres,
        )
# ══════════════════════════════════════════════════════════════
#  END BASELINE EVALUATOR
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN ONLINE RL AGENT
# ══════════════════════════════════════════════════════════════
class OnlineRLAgent:
    """
    Tabular Q-learning agent — online, real-time.
    Applies Bellman update every batch_size frames.
    Epsilon decays from EPS_START to EPS_END over time.
    """

    def __init__(self, batch_size: int = 50, qtable_path: str = None):
        n_states     = (len(MQ_BINS) + 1, len(TEMP_BINS) + 1, len(HUM_BINS) + 1, 2)
        self.q_shape = n_states + (N_ACTIONS,)
        if qtable_path and os.path.exists(qtable_path):
            self.Q = np.load(qtable_path)
            print(f"[OK] Q-table loaded: {qtable_path}")
        else:
            self.Q = np.zeros(self.q_shape)
        self.batch_size     = batch_size
        self.epsilon        = EPS_START
        self.batch_buf      = []
        self.total_frames   = 0
        self.total_batches  = 0
        self.total_reward   = 0.0
        self.action_counts  = [0, 0, 0]
        self.recent_rewards = deque(maxlen=100)

    def select_action(self, state: tuple) -> int:
        """Epsilon-greedy: explore randomly or exploit best Q-value."""
        if np.random.random() < self.epsilon:
            return np.random.randint(N_ACTIONS)
        return int(np.argmax(self.Q[state]))

    def observe(self, state: tuple, action: int, reward: float, next_state: tuple) -> bool:
        """Record experience; return True when a batch update was applied."""
        self.batch_buf.append({"s": state, "a": action, "r": reward, "ns": next_state})
        self.total_frames  += 1
        self.total_reward  += reward
        self.action_counts[action] += 1
        self.recent_rewards.append(reward)
        self.epsilon = max(EPS_END, self.epsilon * EPS_DECAY)
        if len(self.batch_buf) >= self.batch_size:
            self._update_batch()
            return True
        return False

    def _update_batch(self):
        """Bellman update for all experiences in the current batch."""
        for exp in self.batch_buf:
            s, a, r, ns = exp["s"], exp["a"], exp["r"], exp["ns"]
            td_target = r + GAMMA * np.max(self.Q[ns])
            self.Q[s][a] += ALPHA * (td_target - self.Q[s][a])
        self.batch_buf.clear()
        self.total_batches += 1

    def save(self, path: str):
        np.save(path, self.Q)

    @property
    def avg_reward_100(self) -> float:
        return sum(self.recent_rewards) / len(self.recent_rewards) if self.recent_rewards else 0.0

    @property
    def batch_progress(self) -> int:
        return len(self.batch_buf)
# ══════════════════════════════════════════════════════════════
#  END ONLINE RL AGENT
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  BEGIN MAIN
# ══════════════════════════════════════════════════════════════
class Phase(Enum):
    BASELINE = auto()
    ONLINE   = auto()


# ══════════════════════════════════════════════════════════════
#  BEGIN TERMINAL DISPLAY
# ══════════════════════════════════════════════════════════════

# ANSI color codes
C = {
    "reset"  : "\033[0m",
    "bold"   : "\033[1m",
    "cyan"   : "\033[96m",
    "green"  : "\033[92m",
    "yellow" : "\033[93m",
    "red"    : "\033[91m",
    "white"  : "\033[97m",
    "gray"   : "\033[90m",
    "magenta": "\033[95m",
    "blue"   : "\033[94m",
    "bg_dark": "\033[40m",
}

def _c(color: str, text: str) -> str:
    return f"{C.get(color,'')}{text}{C['reset']}"

def _bar(value: float, total: float, width: int = 24,
         fill: str = "█", empty: str = "░",
         color: str = "green") -> str:
    """Render a colored progress bar."""
    ratio  = min(max(value / total, 0.0), 1.0) if total else 0.0
    filled = int(ratio * width)
    bar    = fill * filled + empty * (width - filled)
    return _c(color, bar)

def _reward_bar(avg: float, width: int = 28) -> str:
    """Render reward bar centered at 0, green=positive, red=negative."""
    center = width // 2
    if avg >= 0:
        filled = min(int(avg * center), center)
        bar = "─" * center + "│" + _c("green", "►" * filled) + "─" * (center - filled)
    else:
        filled = min(int(abs(avg) * center), center)
        bar = "─" * (center - filled) + _c("red", "◄" * filled) + "│" + "─" * center
    return "[" + bar + "]"

def print_frame(
    wall_time   : str,
    frame_no    : int,
    epsilon     : float,
    is_random   : bool,
    # sensor
    mq_vc       : float,
    mq_mean     : float,
    mq_warn_thr : float,
    mq_z        : float,
    temp        : float,
    temp_z      : float,
    hum         : float,
    hum_z       : float,
    presence    : int,
    anomaly     : float,
    # RL
    state       : tuple,
    true_level  : str,
    action      : int,
    reward      : float,
    avg_r100    : float,
    q_values    : list,
    # batch
    batch_prog  : int,
    batch_size  : int,
    batch_no    : int,
    updated     : bool,
    # action stats
    action_counts: list,
    total_frames : int,
):
    """Print rich terminal UI matching the original dashboard style."""

    SEP = _c("gray", "─" * 56)
    ts  = wall_time[11:19] if len(wall_time) > 11 else wall_time

    # ── Header ────────────────────────────────────────────────
    explore_str = (_c("yellow", "RANDOM (exploring)") if is_random
                   else _c("cyan",   "GREEDY (exploiting)"))
    print(f"\n{_c('cyan', f'[{ts}]')} "
          f"Frame #{_c('white', str(frame_no))}  "
          f"ε={_c('yellow', f'{epsilon:.3f}')}  "
          f"{explore_str}")
    print(SEP)

    # ── SENSOR ────────────────────────────────────────────────
    print(_c("bold", "SENSOR"))

    # MQ135
    diff     = mq_vc - mq_warn_thr
    diff_str = (f"cách warn {abs(diff):.4f}V  "
                f"(mean={mq_mean:.4f}V)")
    ok_str   = _c("green", "✓") if diff < 0 else _c("red", "✗ OVER WARN")
    print(f"  MQ135   : {_c('white', f'{mq_vc:.4f}V')} {ok_str}  "
          f"{_c('gray', diff_str)}")

    # Temp / Hum / Presence
    pres_str = (_c("green", "có người") if presence
                else _c("gray",  "không có"))
    print(f"  Temp    : {_c('white', f'{temp:.1f}°C')}  "
          f"Hum: {_c('cyan', f'{hum:.1f}%')}  "
          f"Presence: {pres_str}")

    # Anomaly
    lvl_color = {"normal": "green", "warn": "yellow", "alert": "red"}.get(true_level, "white")
    print(f"  Anomaly : {_c('white', f'{anomaly:.3f}')}  "
          f"{_c('gray', f'(MQ z={mq_z:+.2f}  T z={temp_z:+.2f}  H z={hum_z:+.2f})')}")
    print(SEP)

    # ── RL DECISION ───────────────────────────────────────────
    print(_c("bold", "RL DECISION"))

    # State
    state_str = (f"mq_bin={state[0]}  temp_bin={state[1]}  "
                 f"hum_bin={state[2]}  presence={state[3]}")
    print(f"  State   : {_c('gray', state_str)}")

    # True level
    print(f"  True env: {_c(lvl_color, true_level.upper())}")

    # Action
    a_names  = ["NORMAL", "WARNING", "ALERT"]
    a_colors = ["green",  "yellow",  "red"]
    correct  = (
        (action == 0 and true_level == "normal") or
        (action == 1 and true_level == "warn")   or
        (action == 2 and true_level == "alert")
    )
    verdict  = _c("green", "← CORRECT") if correct else _c("red", "← WRONG")
    print(f"  Action  : {_c('bold', '')}✓ "
          f"{_c(a_colors[action], a_names[action]):<10}  {verdict}")

    # Reward bar
    r_color = "green" if reward > 0 else ("red" if reward < 0 else "gray")
    bar_str = _reward_bar(avg_r100 / 3.0)
    print(f"  Reward  : {_c(r_color, f'{reward:+.1f}')}  "
          f"{bar_str}  "
          f"{_c('gray', f'avg₁₀₀={avg_r100:.3f}')}")

    # Q-values
    best_a = int(max(range(N_ACTIONS), key=lambda i: q_values[i]))
    qv_parts = []
    for i, (name, qv) in enumerate(zip(["N", "W", "A"], q_values)):
        col   = a_colors[i]
        mark  = " <- best" if i == best_a else ""
        qv_parts.append(f"{_c(col, name)}:{_c('white', f'{qv:+.3f}')}{_c('cyan', mark)}")
    print(f"  Q-values: {('  ').join(qv_parts)}")
    print(SEP)

    # ── BATCH ─────────────────────────────────────────────────
    print(_c("bold", "BATCH"))
    bar = _bar(batch_prog, batch_size, width=30,
               color="cyan" if not updated else "green")
    prog_str = f"[{bar}]"
    if updated:
        print(f"  {_c('green', '✓ UPDATE DONE')}  "
              f"Batch #{_c('white', str(batch_no))}  "
              f"{_c('gray', f'(total {total_frames} frames learned)')}")
    else:
        print(f"  {prog_str}  "
              f"{_c('white', str(batch_prog))}/{batch_size}")

    # Action distribution
    if total_frames > 0:
        print(f"\n  {_c('bold','BATCH UPDATE')} #{batch_no}  "
              f"total {total_frames} frames\n")
        print(f"  Epsilon : {_c('yellow', f'{epsilon:.4f}')}  "
              f"{_c('gray', '(exploring)' if is_random else '(exploiting)')}")
        print(f"  Avg R₁₀₀: {_c('green' if avg_r100>=0 else 'red', f'{avg_r100:.3f}')}\n")
        print(f"  {_c('gray','Action distribution (this session):')}")
        for i, (name, cnt) in enumerate(zip(a_names, action_counts)):
            pct = cnt / total_frames * 100 if total_frames else 0
            bar = _bar(pct, 100, width=20, color=a_colors[i])
            print(f"    {_c(a_colors[i], f'{name:<7}')}  [{bar}]  "
                  f"{_c('white', f'{pct:.1f}%')}")

    print(SEP)

# ══════════════════════════════════════════════════════════════
#  END TERMINAL DISPLAY
# ══════════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(description="Unified RL + MQTT sensor pipeline")
    parser.add_argument("--port",          default=DEFAULT_PORT)
    parser.add_argument("--baud",          type=int, default=DEFAULT_BAUD)
    parser.add_argument("--baseline-secs", type=int, default=DEFAULT_BASELINE_SECS)
    parser.add_argument("--batch-size",    type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--log-dir",       default=DEFAULT_LOG_DIR)
    parser.add_argument("--save-every",    type=int, default=DEFAULT_SAVE_EVERY)
    parser.add_argument("--load-qtable",   default=None,  help="Resume from existing Q-table .npy")
    parser.add_argument("--no-mqtt",       action="store_true", help="Disable MQTT publishing")
    parser.add_argument("--new-log",       action="store_true", help="Start a new timestamped CSV instead of appending")
    args = parser.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)

    print("=" * 60)
    print("  Unified Pipeline — RL + MQTT (1 process · 1 serial port)")
    print(f"  Serial  : {args.port} @ {args.baud} baud")
    print(f"  MQTT    : {'disabled' if args.no_mqtt else MQTT_BROKER + ':' + str(MQTT_PORT)}")
    print(f"  Log dir : {os.path.abspath(args.log_dir)}")
    print(f"  Baseline: {args.baseline_secs}s   Batch: {args.batch_size} frames")
    print("=" * 60 + "\n")

    uart     = UartReceiver(args.port, args.baud)
    mqtt_pub = None if args.no_mqtt else MqttPublisher(MQTT_BROKER, MQTT_PORT)

    phase        = Phase.BASELINE
    baseline     = BaselineCollector(args.baseline_secs)
    evaluator    = None
    agent        = None
    q_path       = None
    frame_no     = 0
    online_frame = 0
    prev_state   = None
    stop_flag    = False
    last_status  = time.time()
    last_logfile = time.time()

    def handle_sigint(sig, frame_):
        nonlocal stop_flag
        stop_flag = True
    signal.signal(signal.SIGINT, handle_sigint)

    ts_start   = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Fixed filename: always write to sensor_log.csv (append mode)
    # Keeps all history across restarts, Node-RED always reads same file
    # Use --new-log flag to start fresh with a timestamped file instead
    if args.new_log:
        csv_path  = os.path.join(args.log_dir, f"online_rl_{ts_start}.csv")
        csv_mode  = "w"   # new file each run
    else:
        csv_path  = os.path.join(args.log_dir, "sensor_log.csv")
        csv_mode  = "a"   # append to existing file

    file_exists = os.path.exists(csv_path) and csv_mode == "a"
    csv_file   = open(csv_path, csv_mode, newline="", encoding="utf-8")
    csv_cols   = [
        "wall_time", "frame_no", "presence", "temp", "hum",
        "mq135_v_corr", "move_cm", "static_cm", "state",
        "action", "action_name", "reward", "true_level",
        "epsilon", "avg_reward_100", "batch_no",
    ]
    csv_writer = csv.DictWriter(csv_file, fieldnames=csv_cols)
    # Write header only for new files (not when appending)
    if not file_exists:
        csv_writer.writeheader()
    else:
        # Write a session separator so restarts are visible in the log
        csv_file.write(f"# ── Session restarted at {datetime.now().isoformat()} ──\n")
        csv_file.flush()

    print("PHASE 1 — BASELINE (waiting for sensor data...)\n")

    try:
        while not stop_flag:
            d = uart.read_one()
            if d is None:
                continue

            frame_no  += 1
            wall_time  = datetime.now().isoformat(timespec="milliseconds")

            # ── Publish env + presence on every frame ─────────
            if mqtt_pub:
                mqtt_pub.publish(T_ENV, {
                    "wall_time"    : wall_time,
                    "temp"         : d.get("temp",          0),
                    "hum"          : d.get("hum",           0),
                    "dht_ok"       : d.get("dht_ok",        0),
                    "mq135_v"      : round(d.get("mq135_v",      0), 4),
                    "mq135_v_corr" : round(d.get("mq135_v_corr", 0), 4),
                    "cf"           : round(d.get("cf",           1), 4),
                })
                mqtt_pub.publish(T_PRESENCE, {
                    "wall_time" : wall_time,
                    "presence"  : d.get("presence",  0),
                    "state"     : d.get("state",  "NONE"),
                    "move_cm"   : d.get("move_cm",   0),
                    "move_e"    : d.get("move_e",    0),
                    "static_cm" : d.get("static_cm", 0),
                    "static_e"  : d.get("static_e",  0),
                    "detect_cm" : d.get("detect_cm", 0),
                    "ema_move"  : round(d.get("ema_move", 0), 2),
                    "ema_stat"  : round(d.get("ema_stat", 0), 2),
                })

            # ══════════════════════════════════════════════════
            #  PHASE 1 — BASELINE
            # ══════════════════════════════════════════════════
            if phase == Phase.BASELINE:
                baseline.add(d)
                if frame_no % 20 == 0:
                    print(f"\r  [BASELINE] {baseline.progress_bar()}", end="", flush=True)
                if baseline.is_done:
                    print("\n\n[OK] Baseline complete!")
                    ckpt_path = baseline.save_checkpoint(args.log_dir)
                    evaluator = BaselineEvaluator(ckpt_path)
                    agent     = OnlineRLAgent(
                        batch_size  = args.batch_size,
                        qtable_path = args.load_qtable,
                    )
                    q_path = os.path.join(args.log_dir, f"qtable_online_{ts_start}.npy")
                    phase  = Phase.ONLINE
                    print("\nPHASE 2 — ONLINE RL (Ctrl+C to stop)\n")
                continue

            # ══════════════════════════════════════════════════
            #  PHASE 2 — ONLINE RL
            # ══════════════════════════════════════════════════
            online_frame += 1

            state      = evaluator.to_state(d)
            mq_vc      = float(d.get("mq135_v_corr", evaluator.mq_mean))
            temp       = float(d.get("temp",  evaluator.t_mean))
            hum        = float(d.get("hum",   evaluator.h_mean))
            true_level = evaluator.true_level(mq_vc, temp, hum)
            action     = agent.select_action(state)
            reward     = REWARD_TABLE.get((action, true_level), 0.0)
            next_s     = prev_state if prev_state else state
            updated    = agent.observe(state, action, reward, next_s)
            prev_state = state

            # ── Publish RL decision ───────────────────────────
            if mqtt_pub:
                qv = agent.Q[state].tolist()
                mqtt_pub.publish(T_RL, {
                    "wall_time"  : wall_time,
                    "action"     : action,
                    "action_name": ACTION_NAMES[action],
                    "reward"     : reward,
                    "true_level" : true_level,
                    "epsilon"    : round(agent.epsilon, 4),
                    "avg_r100"   : round(agent.avg_reward_100, 4),
                    "q_normal"   : round(qv[0], 4),
                    "q_warning"  : round(qv[1], 4),
                    "q_alert"    : round(qv[2], 4),
                    "batch_no"   : agent.total_batches,
                    "batch_prog" : agent.batch_progress,
                }, retain=False)

                log_payload = d.copy()
                log_payload.update({
                    "wall_time"  : wall_time,
                    "frame_no"   : online_frame,
                    "action"     : action,
                    "action_name": ACTION_NAMES[action],
                    "reward"     : reward,
                    "true_level" : true_level,
                })
                mqtt_pub.publish(T_LOG, log_payload, retain=False)

            # ── CSV log ───────────────────────────────────────
            csv_writer.writerow({
                "wall_time"     : wall_time,
                "frame_no"      : online_frame,
                "presence"      : d.get("presence", 0),
                "temp"          : round(temp, 2),
                "hum"           : round(hum, 2),
                "mq135_v_corr"  : round(mq_vc, 4),
                "move_cm"       : d.get("move_cm", 0),
                "static_cm"     : d.get("static_cm", 0),
                "state"         : d.get("state", "NONE"),
                "action"        : action,
                "action_name"   : ACTION_NAMES[action],
                "reward"        : reward,
                "true_level"    : true_level,
                "epsilon"       : round(agent.epsilon, 4),
                "avg_reward_100": round(agent.avg_reward_100, 4),
                "batch_no"      : agent.total_batches,
            })
            csv_file.flush()

            # ── Auto-save Q-table ─────────────────────────────
            if online_frame % args.save_every == 0:
                agent.save(q_path)
                print(f"[SAVE] Q-table @ frame {online_frame} -> {q_path}")

            # ── Terminal output ───────────────────────────────
            print_frame(
                wall_time    = wall_time,
                frame_no     = online_frame,
                epsilon      = agent.epsilon,
                is_random    = (np.random.random() < agent.epsilon),
                mq_vc        = mq_vc,
                mq_mean      = evaluator.mq_mean,
                mq_warn_thr  = evaluator.warn_thr,
                mq_z         = evaluator.mq_z(mq_vc),
                temp         = temp,
                temp_z       = evaluator.t_z(temp),
                hum          = hum,
                hum_z        = evaluator.h_z(hum),
                presence     = int(d.get("presence", 0)),
                anomaly      = evaluator.anomaly_score(mq_vc, temp, hum),
                state        = state,
                true_level   = true_level,
                action       = action,
                reward       = reward,
                avg_r100     = agent.avg_reward_100,
                q_values     = agent.Q[state].tolist(),
                batch_prog   = agent.batch_progress,
                batch_size   = args.batch_size,
                batch_no     = agent.total_batches,
                updated      = updated,
                action_counts= agent.action_counts,
                total_frames = agent.total_frames,
            )

            # ── Heartbeat every 5 seconds ─────────────────────
            now = time.time()
            if mqtt_pub and now - last_status >= 5.0:
                last_status = now
                mqtt_pub.publish(T_STATUS, {
                    "online"     : True,
                    "wall_time"  : wall_time,
                    "frame_count": online_frame,
                    "mqtt_queue" : mqtt_pub.queue_size,
                    "phase"      : "ONLINE",
                    "epsilon"    : round(agent.epsilon, 4),
                }, retain=True)

            # ── Push CSV log snapshot every 10 seconds ─────────
            if mqtt_pub and now - last_logfile >= LOGFILE_PUSH_INTERVAL:
                last_logfile = now
                try:
                    # Flush buffer first so file is up to date
                    csv_file.flush()

                    # Read last 100 rows from CSV (avoid huge payload)
                    rows = []
                    with open(csv_path, "r", encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            rows.append(row)
                    last_rows = rows[-100:]  # keep last 100 rows only

                    # Build table-friendly list of objects for Node-RED
                    table_rows = []
                    for r in last_rows:
                        table_rows.append({
                            "time"      : r.get("wall_time", "")[-12:-4],
                            "presence"  : r.get("presence",  ""),
                            "temp"      : r.get("temp",      "") + "°C",
                            "hum"       : r.get("hum",       "") + "%",
                            "mq135"     : r.get("mq135_v_corr", "") + "V",
                            "action"    : r.get("action_name", ""),
                            "reward"    : r.get("reward",    ""),
                            "level"     : r.get("true_level", ""),
                        })

                    mqtt_pub.publish(T_LOGFILE, {
                        "wall_time"  : wall_time,
                        "total_rows" : len(rows),
                        "rows"       : table_rows,
                        "csv_path"   : csv_path,
                    }, retain=True)

                    print(f"[LOGFILE] Pushed {len(table_rows)} rows to Node-RED")
                except Exception as e:
                    print(f"[LOGFILE] Error reading CSV: {e}")

    finally:
        print("\n\n[Shutting down...]")
        if agent and online_frame > 0:
            if agent.batch_buf:
                agent._update_batch()
            agent.save(q_path)
            print(f"[OK] Q-table -> {q_path}")
        csv_file.close()
        print(f"[OK] CSV     -> {csv_path}")
        if mqtt_pub:
            mqtt_pub.publish(
                T_STATUS,
                {"online": False, "wall_time": datetime.now().isoformat()},
                retain=True,
            )
            mqtt_pub.close()
        uart.close()


if __name__ == "__main__":
    main()
# ══════════════════════════════════════════════════════════════
#  END MAIN
# ══════════════════════════════════════════════════════════════
