#!/usr/bin/env python3
"""
Kepco BIT 802E Comprehensive Simulator

Emulates a Kepco BIT 802E interface card with Telnet-first behavior.
Listens on TCP port 5024 (Telnet) and 5025 (socket compatibility).
Provides a live dashboard showing all internal registers, LIST data,
output state, and command log — ideal for testing kepco_ui.py without
real hardware.

Usage:
    python kepco_simulator.py
    python kepco_simulator.py --telnet-port 6024 --socket-port 6025
"""

import socket
import threading
import time
import math
import random
import argparse
import queue as _queue

# ── GUI ─────────────────────────────────────────────────────────────────────
import customtkinter as ctk

# ── Constants ───────────────────────────────────────────────────────────────
DEFAULT_TELNET_PORT = 5024
DEFAULT_SOCKET_PORT = 5025
MAX_LIST_POINTS    = 1002
MAX_SEQ_POINTS     = 512
INPUT_BUFFER_LIMIT = 253        # BIT 802E input buffer size
NOISE_AMPLITUDE    = 0.002      # ±2 mV / mA measurement noise
LIST_DWELL_MIN     = 0.0005     # 500 µs
LIST_DWELL_MAX     = 10.0       # 10 s
MAX_RX_LINE_BYTES  = 64_000_000 # guardrail for malformed/no-newline payloads
MAX_LOG_TEXT       = 1200       # truncate oversized log lines for GUI safety

# ── Colour palette (matches kepco_ui.py material style) ─────────────────────
C = dict(
    bg="#121212", surface="#1e1e2e", card="#2a2a3c",
    primary="#7c3aed", primary_h="#6d28d9",
    green="#10b981", red="#ef4444", amber="#f59e0b",
    text="#e2e8f0", text2="#94a3b8", border="#3f3f5c",
    input_bg="#363650", graph_bg="#161625",
    sent="#f472b6",
    chunk_colors=["#818cf8", "#34d399", "#fb923c", "#f472b6"],
)


# ═══════════════════════════════════════════════════════════════════════════
#  Simulated BIT 802E Device Model
# ═══════════════════════════════════════════════════════════════════════════
class KepcoDevice:
    """Full-state model of a BIT 802E + BOP power supply."""

    # ---------- identity ---------------------------------------------------
    IDN = "KEPCO,BOP 50-20M,SIM-001,1.5 (Simulator)"
    SCPI_VERSION = "1995.0"

    def __init__(self, *, on_state_change=None, on_log=None):
        self._lock = threading.Lock()
        self._list_stop_event = threading.Event()
        self._list_thread = None
        self._on_state_change = on_state_change   # callable()
        self._on_log = on_log                     # callable(direction, text)
        self.reset(log=False)

    # ── reset to power-on defaults ────────────────────────────────────────
    def reset(self, *, log=True):
        with self._lock:
            # output
            self.output_on      = False
            self.func_mode      = "VOLT"          # VOLT or CURR
            self.volt_setpoint  = 0.0
            self.curr_setpoint  = 0.0
            self.volt_saved     = 0.0             # saved when OUTP OFF
            self.curr_saved     = 0.0
            # range
            self.volt_range_auto = True
            self.curr_range_auto = True
            self.volt_range      = 1              # 0=quarter, 1=full
            self.curr_range      = 1
            # mode
            self.volt_mode      = "FIX"           # FIX / LIST / TRAN
            self.curr_mode      = "FIX"
            # LIST subsystem
            self.list_volt      = []              # up to 1002 float
            self.list_curr      = []
            self.list_dwel      = []              # up to 1002 float
            self.list_count     = 1
            self.list_count_skip = 0
            self.list_dir       = "UP"
            self.list_gen       = "DSEQ"          # DSEQ / SEQ
            self.list_seq       = []              # up to 512 int
            self.list_query_ptr = 0
            self.list_running   = False
            self.list_step_idx  = 0               # current step while running
            self.list_iteration = 0               # current loop#
            # trigger
            self.init_cont      = True
            self.volt_trig      = 0.0
            self.curr_trig      = 0.0
            # status registers
            self.esr             = 0              # event status register
            self.stb             = 0              # status byte
            self.oper_cond       = 0
            self.oper_enable     = 0
            self.oper_event      = 0
            self.ques_cond       = 0
            self.ques_enable     = 0
            self.ques_event      = 0
            # error queue
            self.error_queue     = []
            # stats
            self.cmd_count       = 0
            self.query_count     = 0
        if log:
            self._notify()

    # ── helpers ────────────────────────────────────────────────────────────
    def _notify(self):
        if self._on_state_change:
            self._on_state_change()

    def _log(self, direction, text):
        if self._on_log:
            if isinstance(text, str) and len(text) > MAX_LOG_TEXT:
                hidden = len(text) - MAX_LOG_TEXT
                text = f"{text[:MAX_LOG_TEXT]} ... [truncated {hidden} chars]"
            self._on_log(direction, text)

    def _push_error(self, code, msg):
        with self._lock:
            self.error_queue.append((code, msg))

    def _pop_error(self):
        with self._lock:
            if self.error_queue:
                return self.error_queue.pop(0)
            return (0, "No error")

    # ── measurements (simulated) ──────────────────────────────────────────
    def measure_volt(self):
        with self._lock:
            if not self.output_on:
                return 0.0
            if self.list_running and self.list_volt:
                idx = min(self.list_step_idx, len(self.list_volt) - 1)
                base = self.list_volt[idx]
            else:
                base = self.volt_setpoint
            return base + random.uniform(-NOISE_AMPLITUDE, NOISE_AMPLITUDE)

    def measure_curr(self):
        with self._lock:
            if not self.output_on:
                return 0.0
            if self.list_running and self.list_curr:
                idx = min(self.list_step_idx, len(self.list_curr) - 1)
                base = self.list_curr[idx]
            else:
                base = self.curr_setpoint
            return base + random.uniform(-NOISE_AMPLITUDE, NOISE_AMPLITUDE)

    # ── LIST execution engine (background thread) ─────────────────────────
    def _list_runner(self):
        """Simulate LIST execution with realistic dwell timing."""
        try:
            with self._lock:
                mode = self.func_mode
                points = list(self.list_volt if mode == "VOLT" else self.list_curr)
                dwells = list(self.list_dwel)
                count  = self.list_count
                skip   = self.list_count_skip
                direction = self.list_dir
                gen    = self.list_gen
                seq    = list(self.list_seq)

            if not points:
                self._push_error(-200, "Execution error; list empty")
                return

            # resolve dwell: if only 1 entry, apply to all; else must match
            if len(dwells) == 1:
                dwells = dwells * len(points)
            elif len(dwells) != len(points):
                self._push_error(-221, "Settings conflict; dwell/point mismatch")
                return

            # build execution order
            if gen == "DSEQ":
                order = list(range(len(points)))
                if direction == "DOWN":
                    order = list(reversed(order))
            else:  # SEQ
                order = [s for s in seq if s < len(points)]
                if direction == "DOWN":
                    order = list(reversed(order))

            if not order:
                self._push_error(-221, "Settings conflict; empty sequence")
                return

            # iteration loop
            iteration = 0
            total_iters = count if count > 0 else 999999  # 0 = indefinite
            while iteration < total_iters and not self._list_stop_event.is_set():
                exec_order = order if iteration == 0 else order[skip:]
                for idx in exec_order:
                    if self._list_stop_event.is_set():
                        return
                    with self._lock:
                        if not self.list_running:
                            return
                        self.list_step_idx = idx
                        self.list_iteration = iteration
                    dwell = dwells[idx] if idx < len(dwells) else dwells[0]
                    if dwell < LIST_DWELL_MIN:
                        dwell = LIST_DWELL_MIN
                    elif dwell > LIST_DWELL_MAX:
                        dwell = LIST_DWELL_MAX
                    self._notify()

                    remaining = dwell
                    while remaining > 0:
                        if self._list_stop_event.is_set():
                            return
                        with self._lock:
                            if not self.list_running:
                                return
                        sl = 0.05 if remaining > 0.05 else remaining
                        time.sleep(sl)
                        remaining -= sl
                iteration += 1
        finally:
            with self._lock:
                self.list_running = False
                if self.volt_mode == "LIST":
                    self.volt_mode = "FIX"
                if self.curr_mode == "LIST":
                    self.curr_mode = "FIX"
            self._list_stop_event.set()
            self._notify()

    def _start_list(self, mode):
        already_running = False
        with self._lock:
            if self.list_running:
                already_running = True
            else:
                self.list_running = True
                self.list_step_idx = 0
                self.list_iteration = 0
        if already_running:
            self._push_error(-221, "Settings conflict; list already running")
            return
        self._list_stop_event.clear()
        t = threading.Thread(target=self._list_runner, daemon=True)
        self._list_thread = t
        t.start()
        self._notify()

    def _stop_list(self):
        with self._lock:
            self.list_running = False
            if self.volt_mode == "LIST":
                self.volt_mode = "FIX"
            if self.curr_mode == "LIST":
                self.curr_mode = "FIX"
        self._list_stop_event.set()
        self._notify()

    # ══════════════════════════════════════════════════════════════════════
    #  SCPI Command Processor
    # ══════════════════════════════════════════════════════════════════════
    def process(self, raw_cmd: str) -> str | None:
        """Process one raw SCPI command string (may be compound with ;:).
        Returns response string for queries, None for commands."""
        # split compound commands   "CMD1;:CMD2"  or  "CMD1;CMD2"
        parts = []
        for seg in raw_cmd.split(";"):
            seg = seg.strip()
            if seg.startswith(":"):
                seg = seg[1:]
            if seg:
                parts.append(seg)

        responses = []
        for part in parts:
            r = self._dispatch(part)
            if r is not None:
                responses.append(r)

        self._notify()
        return ";".join(responses) if responses else None

    def _dispatch(self, cmd: str) -> str | None:
        """Route a single SCPI command to the appropriate handler."""
        cmd_upper = cmd.upper().strip()
        self._log("rx", cmd)

        with self._lock:
            self.cmd_count += 1

        # ── IEEE 488.2 Common Commands ────────────────────────────────────
        if cmd_upper == "*IDN?":
            return self._q("*IDN?", self.IDN)
        if cmd_upper == "*RST":
            self.reset()
            return None
        if cmd_upper == "*CLS":
            with self._lock:
                self.esr = 0
                self.stb = 0
                self.oper_event = 0
                self.ques_event = 0
                self.error_queue.clear()
            return None
        if cmd_upper == "*ESR?":
            with self._lock:
                v = self.esr
                self.esr = 0
            return self._q("*ESR?", str(v))
        if cmd_upper == "*STB?":
            return self._q("*STB?", str(self.stb))
        if cmd_upper == "*OPC?":
            return self._q("*OPC?", "1")
        if cmd_upper == "*OPC":
            with self._lock:
                self.esr |= 1  # OPC bit
            return None
        if cmd_upper == "*WAI":
            return None

        # ── SYSTEM ────────────────────────────────────────────────────────
        if cmd_upper in ("SYST:ERR?", "SYST:ERR:NEXT?", "SYSTEM:ERROR?",
                         "SYSTEM:ERROR:NEXT?"):
            code, msg = self._pop_error()
            return self._q(cmd_upper, f'{code},"{msg}"')
        if cmd_upper in ("SYST:ERR:ALL?", "SYSTEM:ERROR:ALL?"):
            errs = []
            while True:
                code, msg = self._pop_error()
                errs.append(f'{code},"{msg}"')
                if code == 0:
                    break
            return self._q(cmd_upper, ";".join(errs))
        if cmd_upper in ("SYST:VERS?", "SYST:VERSION?",
                         "SYSTEM:VERSION?"):
            return self._q(cmd_upper, self.SCPI_VERSION)

        # ── OUTPUT ────────────────────────────────────────────────────────
        if cmd_upper in ("OUTP ON", "OUTP 1", "OUTPUT ON", "OUTPUT 1",
                         "OUTP:STAT ON", "OUTP:STAT 1"):
            with self._lock:
                self.output_on = True
                self.volt_setpoint = self.volt_saved
                self.curr_setpoint = self.curr_saved
            return None
        if cmd_upper in ("OUTP OFF", "OUTP 0", "OUTPUT OFF", "OUTPUT 0",
                         "OUTP:STAT OFF", "OUTP:STAT 0"):
            with self._lock:
                self.volt_saved = self.volt_setpoint
                self.curr_saved = self.curr_setpoint
                self.output_on = False
                self.volt_setpoint = 0.0
                self.curr_setpoint = 0.0
            return None
        if cmd_upper in ("OUTP?", "OUTPUT?", "OUTP:STAT?"):
            return self._q(cmd_upper,
                           "1" if self.output_on else "0")

        # ── FUNC:MODE ─────────────────────────────────────────────────────
        if cmd_upper in ("FUNC:MODE VOLT", "FUNCTION:MODE VOLT"):
            with self._lock:
                self.func_mode = "VOLT"
                self.volt_mode = "FIX"
                self.curr_mode = "FIX"
            return None
        if cmd_upper in ("FUNC:MODE CURR", "FUNCTION:MODE CURR"):
            with self._lock:
                self.func_mode = "CURR"
                self.volt_mode = "FIX"
                self.curr_mode = "FIX"
            return None
        if cmd_upper in ("FUNC:MODE?", "FUNCTION:MODE?"):
            return self._q(cmd_upper, self.func_mode)

        # ── VOLT / CURR setpoints ─────────────────────────────────────────
        if cmd_upper.startswith("VOLT ") and ":" not in cmd_upper:
            val = self._parse_float(cmd, 5)
            if val is not None:
                with self._lock:
                    self.volt_setpoint = val
                    if self.output_on:
                        self.volt_saved = val
            return None
        if cmd_upper == "VOLT?":
            return self._q("VOLT?", f"{self.volt_setpoint:.6E}")
        if cmd_upper.startswith("CURR ") and ":" not in cmd_upper:
            val = self._parse_float(cmd, 5)
            if val is not None:
                with self._lock:
                    self.curr_setpoint = val
                    if self.output_on:
                        self.curr_saved = val
            return None
        if cmd_upper == "CURR?":
            return self._q("CURR?", f"{self.curr_setpoint:.6E}")

        # ── MEAS ──────────────────────────────────────────────────────────
        if cmd_upper in ("MEAS:VOLT?", "MEAS:SCAL:VOLT?",
                         "MEASURE:VOLTAGE?", "MEASURE:SCALAR:VOLTAGE?",
                         "MEAS:VOLT:DC?", "MEAS:SCAL:VOLT:DC?"):
            return self._q(cmd_upper, f"{self.measure_volt():.6E}")
        if cmd_upper in ("MEAS:CURR?", "MEAS:SCAL:CURR?",
                         "MEASURE:CURRENT?", "MEASURE:SCALAR:CURRENT?",
                         "MEAS:CURR:DC?", "MEAS:SCAL:CURR:DC?"):
            return self._q(cmd_upper, f"{self.measure_curr():.6E}")

        # ── VOLT:MODE / CURR:MODE ────────────────────────────────────────
        if cmd_upper in ("VOLT:MODE FIX", "VOLT:MODE FIXED"):
            self._stop_list()
            with self._lock:
                self.volt_mode = "FIX"
            return None
        if cmd_upper in ("VOLT:MODE LIST",):
            with self._lock:
                self.volt_mode = "LIST"
            self._start_list("VOLT")
            return None
        if cmd_upper == "VOLT:MODE?":
            return self._q("VOLT:MODE?", self.volt_mode)

        if cmd_upper in ("CURR:MODE FIX", "CURR:MODE FIXED"):
            self._stop_list()
            with self._lock:
                self.curr_mode = "FIX"
            return None
        if cmd_upper in ("CURR:MODE LIST",):
            with self._lock:
                self.curr_mode = "LIST"
            self._start_list("CURR")
            return None
        if cmd_upper == "CURR:MODE?":
            return self._q("CURR:MODE?", self.curr_mode)

        # ── VOLT:RANG / CURR:RANG ────────────────────────────────────────
        if cmd_upper == "VOLT:RANG:AUTO?":
            return self._q("VOLT:RANG:AUTO?",
                           "1" if self.volt_range_auto else "0")
        if cmd_upper.startswith("VOLT:RANG:AUTO"):
            flag = "ON" in cmd_upper or "1" in cmd_upper
            with self._lock:
                self.volt_range_auto = flag
            return None
        if cmd_upper.startswith("VOLT:RANG "):
            val = self._parse_int(cmd, 10)
            if val is not None:
                with self._lock:
                    self.volt_range = val
            return None
        if cmd_upper == "VOLT:RANG?":
            return self._q("VOLT:RANG?", str(self.volt_range))

        if cmd_upper == "CURR:RANG:AUTO?":
            return self._q("CURR:RANG:AUTO?",
                           "1" if self.curr_range_auto else "0")
        if cmd_upper.startswith("CURR:RANG:AUTO"):
            flag = "ON" in cmd_upper or "1" in cmd_upper
            with self._lock:
                self.curr_range_auto = flag
            return None
        if cmd_upper.startswith("CURR:RANG "):
            val = self._parse_int(cmd, 10)
            if val is not None:
                with self._lock:
                    self.curr_range = val
            return None
        if cmd_upper == "CURR:RANG?":
            return self._q("CURR:RANG?", str(self.curr_range))

        # ── LIST:CLE / LIST:CLEAR ────────────────────────────────────────
        if cmd_upper in ("LIST:CLE", "LIST:CLEAR"):
            with self._lock:
                self.list_volt.clear()
                self.list_curr.clear()
                self.list_dwel.clear()
                self.list_seq.clear()
                self.list_count = 1
                self.list_count_skip = 0
                self.list_dir = "UP"
                self.list_gen = "DSEQ"
                self.list_query_ptr = 0
            return None

        # ── LIST:VOLT ─────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:VOLT ") and "POIN" not in cmd_upper:
            if self.list_curr:
                self._push_error(-221, "Settings conflict")
                return None
            with self._lock:
                space = MAX_LIST_POINTS - len(self.list_volt)
            if space <= 0:
                return None
            vals = self._parse_float_list(cmd, 10, max_items=space)
            with self._lock:
                self.list_volt.extend(vals[:space])
            return None
        if cmd_upper in ("LIST:VOLT?", "LIST:VOLTAGE?"):
            with self._lock:
                start = self.list_query_ptr
                chunk = self.list_volt[start:start + 16]
            return self._q(cmd_upper,
                           ",".join(f"{v:.6E}" for v in chunk) if chunk else "")
        if cmd_upper in ("LIST:VOLT:POIN?", "LIST:VOLT:POINTS?",
                         "LIST:VOLTAGE:POINTS?"):
            return self._q(cmd_upper, str(len(self.list_volt)))

        # ── LIST:CURR ─────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:CURR ") and "POIN" not in cmd_upper:
            if self.list_volt:
                self._push_error(-221, "Settings conflict")
                return None
            with self._lock:
                space = MAX_LIST_POINTS - len(self.list_curr)
            if space <= 0:
                return None
            vals = self._parse_float_list(cmd, 10, max_items=space)
            with self._lock:
                self.list_curr.extend(vals[:space])
            return None
        if cmd_upper in ("LIST:CURR?", "LIST:CURRENT?"):
            with self._lock:
                start = self.list_query_ptr
                chunk = self.list_curr[start:start + 16]
            return self._q(cmd_upper,
                           ",".join(f"{v:.6E}" for v in chunk) if chunk else "")
        if cmd_upper in ("LIST:CURR:POIN?", "LIST:CURR:POINTS?",
                         "LIST:CURRENT:POINTS?"):
            return self._q(cmd_upper, str(len(self.list_curr)))

        # ── LIST:DWEL ─────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:DWEL ") and "POIN" not in cmd_upper:
            with self._lock:
                space = MAX_LIST_POINTS - len(self.list_dwel)
            if space <= 0:
                return None
            vals = self._parse_float_list(cmd, 10, max_items=space)
            out_of_range = any(v < LIST_DWELL_MIN or v > LIST_DWELL_MAX
                               for v in vals)
            if out_of_range:
                self._push_error(
                    -222,
                    (f"Data out of range; dwell must be "
                     f"{LIST_DWELL_MIN}..{LIST_DWELL_MAX} s"),
                )
            vals = [
                LIST_DWELL_MIN if v < LIST_DWELL_MIN else
                LIST_DWELL_MAX if v > LIST_DWELL_MAX else v
                for v in vals
            ]
            with self._lock:
                self.list_dwel.extend(vals[:space])
            return None
        if cmd_upper in ("LIST:DWEL?", "LIST:DWELL?"):
            with self._lock:
                start = self.list_query_ptr
                chunk = self.list_dwel[start:start + 16]
            return self._q(cmd_upper,
                           ",".join(f"{v:.6E}" for v in chunk) if chunk else "")
        if cmd_upper in ("LIST:DWEL:POIN?", "LIST:DWELL:POINTS?"):
            return self._q(cmd_upper, str(len(self.list_dwel)))

        # ── LIST:COUN ─────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:COUN:SKIP "):
            val = self._parse_int(cmd, 15)
            if val is not None:
                with self._lock:
                    self.list_count_skip = val
            return None
        if cmd_upper in ("LIST:COUN:SKIP?", "LIST:COUNT:SKIP?"):
            return self._q(cmd_upper, str(self.list_count_skip))
        if cmd_upper.startswith("LIST:COUN "):
            val = self._parse_int(cmd, 10)
            if val is not None:
                with self._lock:
                    self.list_count = val
            return None
        if cmd_upper in ("LIST:COUN?", "LIST:COUNT?"):
            return self._q(cmd_upper, str(self.list_count))

        # ── LIST:DIR ──────────────────────────────────────────────────────
        if cmd_upper in ("LIST:DIR UP", "LIST:DIRECTION UP"):
            with self._lock:
                self.list_dir = "UP"
            return None
        if cmd_upper in ("LIST:DIR DOWN", "LIST:DIRECTION DOWN"):
            with self._lock:
                self.list_dir = "DOWN"
            return None
        if cmd_upper in ("LIST:DIR?", "LIST:DIRECTION?"):
            return self._q(cmd_upper, self.list_dir)

        # ── LIST:GEN ──────────────────────────────────────────────────────
        if cmd_upper in ("LIST:GEN DSEQ", "LIST:GEN DSEQUENCE",
                         "LIST:GENERATION DSEQ", "LIST:GENERATION DSEQUENCE"):
            with self._lock:
                self.list_gen = "DSEQ"
            return None
        if cmd_upper in ("LIST:GEN SEQ", "LIST:GEN SEQUENCE",
                         "LIST:GENERATION SEQ", "LIST:GENERATION SEQUENCE"):
            with self._lock:
                self.list_gen = "SEQ"
            return None
        if cmd_upper in ("LIST:GEN?", "LIST:GENERATION?"):
            if self.list_running:
                self._push_error(-221, "Settings conflict; list running")
                return None
            return self._q(cmd_upper, self.list_gen)

        # ── LIST:SEQ ──────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:SEQ ") and "?" not in cmd_upper:
            with self._lock:
                space = MAX_SEQ_POINTS - len(self.list_seq)
            if space <= 0:
                return None
            vals = self._parse_int_list(cmd, 9, max_items=space)
            with self._lock:
                self.list_seq.extend(vals[:space])
            return None
        if cmd_upper in ("LIST:SEQ?", "LIST:SEQUENCE?"):
            with self._lock:
                start = self.list_query_ptr
                chunk = self.list_seq[start:start + 16]
            return self._q(cmd_upper,
                           ",".join(str(v) for v in chunk) if chunk else "")

        # ── LIST:QUER ─────────────────────────────────────────────────────
        if cmd_upper.startswith("LIST:QUER ") and "?" not in cmd_upper:
            val = self._parse_int(cmd, 10)
            if val is not None:
                with self._lock:
                    self.list_query_ptr = val
            return None
        if cmd_upper in ("LIST:QUER?", "LIST:QUERY?"):
            return self._q(cmd_upper, str(self.list_query_ptr))

        # ── STATUS registers ──────────────────────────────────────────────
        if cmd_upper in ("STAT:OPER:COND?", "STATUS:OPERATION:CONDITION?"):
            return self._q(cmd_upper, str(self.oper_cond))
        if cmd_upper in ("STAT:OPER:ENAB?", "STATUS:OPERATION:ENABLE?"):
            return self._q(cmd_upper, str(self.oper_enable))
        if cmd_upper.startswith("STAT:OPER:ENAB"):
            val = self._parse_int(cmd, 15)
            if val is not None:
                with self._lock:
                    self.oper_enable = val
            return None
        if cmd_upper in ("STAT:OPER?", "STATUS:OPERATION?"):
            with self._lock:
                v = self.oper_event
                self.oper_event = 0
            return self._q(cmd_upper, str(v))

        if cmd_upper in ("STAT:QUES:COND?",
                         "STATUS:QUESTIONABLE:CONDITION?"):
            return self._q(cmd_upper, str(self.ques_cond))
        if cmd_upper in ("STAT:QUES:ENAB?",
                         "STATUS:QUESTIONABLE:ENABLE?"):
            return self._q(cmd_upper, str(self.ques_enable))
        if cmd_upper.startswith("STAT:QUES:ENAB"):
            val = self._parse_int(cmd, 15)
            if val is not None:
                with self._lock:
                    self.ques_enable = val
            return None
        if cmd_upper in ("STAT:QUES?", "STATUS:QUESTIONABLE?"):
            with self._lock:
                v = self.ques_event
                self.ques_event = 0
            return self._q(cmd_upper, str(v))

        # ── INIT / TRIG (stub) ────────────────────────────────────────────
        if cmd_upper in ("INIT", "INIT:IMM", "INITIATE:IMMEDIATE"):
            return None
        if cmd_upper in ("ABOR", "ABORT"):
            self._stop_list()
            return None
        if cmd_upper in ("INIT:CONT?", "INITIATE:CONTINUOUS?"):
            return self._q(cmd_upper,
                           "1" if self.init_cont else "0")
        if cmd_upper.startswith("INIT:CONT"):
            flag = "ON" in cmd_upper or "1" in cmd_upper
            with self._lock:
                self.init_cont = flag
            return None
        if cmd_upper in ("*TRG", "TRIG", "TRIGGER"):
            return None

        # ── Unrecognised ──────────────────────────────────────────────────
        self._push_error(-100, f"Command error; unrecognised: {cmd}")
        return None

    # ── query helper ──────────────────────────────────────────────────────
    def _q(self, label, value):
        with self._lock:
            self.query_count += 1
        self._log("tx", f"{value}")
        return value

    # ── parsing helpers ───────────────────────────────────────────────────
    @staticmethod
    def _parse_float(cmd, offset):
        try:
            return float(cmd[offset:].strip())
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _parse_int(cmd, offset):
        try:
            return int(float(cmd[offset:].strip()))
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _parse_float_list(cmd, offset, max_items=None):
        try:
            payload = cmd[offset:]
            if max_items is None:
                return [float(x) for x in payload.split(",")]

            out = []
            start = 0
            while len(out) < max_items:
                idx = payload.find(",", start)
                if idx < 0:
                    token = payload[start:].strip()
                    if token:
                        out.append(float(token))
                    break
                token = payload[start:idx].strip()
                if token:
                    out.append(float(token))
                start = idx + 1
            return out
        except ValueError:
            return []

    @staticmethod
    def _parse_int_list(cmd, offset, max_items=None):
        try:
            payload = cmd[offset:]
            if max_items is None:
                return [int(float(x)) for x in payload.split(",")]

            out = []
            start = 0
            while len(out) < max_items:
                idx = payload.find(",", start)
                if idx < 0:
                    token = payload[start:].strip()
                    if token:
                        out.append(int(float(token)))
                    break
                token = payload[start:idx].strip()
                if token:
                    out.append(int(float(token)))
                start = idx + 1
            return out
        except ValueError:
            return []


# ═══════════════════════════════════════════════════════════════════════════
#  TCP Server
# ═══════════════════════════════════════════════════════════════════════════
class SCPIServer:
    """Multi-client TCP server on localhost."""

    def __init__(self, device: KepcoDevice, port: int, echo: bool = False):
        self.device = device
        self.port = port
        self.echo = echo          # Telnet echo emulation
        self._server_sock = None
        self._accept_thread = None
        self._running = False
        self._clients: list[threading.Thread] = []
        self._client_socks: set[socket.socket] = set()
        self._state_lock = threading.Lock()
        self._list_owner_conn = None
        self.client_count = 0

    def start(self):
        if self._running:
            return
        self._running = True
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.settimeout(1.0)
        last_error = None
        for _ in range(20):
            try:
                self._server_sock.bind(("0.0.0.0", self.port))
                last_error = None
                break
            except OSError as e:
                last_error = e
                if e.errno != 98:
                    break
                time.sleep(0.1)
        if last_error is not None:
            self._running = False
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None
            raise last_error
        self._server_sock.listen(4)
        self._accept_thread = threading.Thread(target=self._accept_loop,
                                               daemon=True)
        self._accept_thread.start()

    def stop(self):
        self._running = False
        if self._server_sock:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None

        with self._state_lock:
            socks = list(self._client_socks)
            self._client_socks.clear()
            self._list_owner_conn = None

        for conn in socks:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass

        self.client_count = 0

        if self._accept_thread and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=1.5)
        self._accept_thread = None

    def _accept_loop(self):
        while self._running:
            try:
                conn, addr = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._state_lock:
                self._client_socks.add(conn)
                self.client_count = len(self._client_socks)
            self.device._log("sys", f"Client connected: {addr}")
            t = threading.Thread(target=self._client_handler,
                                 args=(conn, addr), daemon=True)
            t.start()
            self._clients.append(t)

    def _client_handler(self, conn: socket.socket, addr):
        buf = bytearray()
        try:
            conn.settimeout(0.5)
            while self._running:
                try:
                    data = conn.recv(1024)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not data:
                    break
                buf.extend(data)

                if len(buf) > MAX_RX_LINE_BYTES and b"\n" not in buf:
                    self.device._push_error(-223, "Too much data")
                    self.device._log("sys", "Dropped oversized unterminated input line")
                    buf.clear()
                    continue

                while True:
                    nl = buf.find(b"\n")
                    if nl < 0:
                        break
                    line = bytes(buf[:nl])
                    del buf[:nl + 1]
                    cmd = line.decode("ascii", errors="replace").strip()
                    if not cmd:
                        continue
                    # Echo the command back (Telnet echo emulation)
                    if self.echo:
                        try:
                            conn.sendall((cmd + "\r\n").encode("ascii"))
                        except OSError:
                            break
                    cmd_upper = cmd.upper()
                    if cmd_upper in ("VOLT:MODE LIST", "CURR:MODE LIST"):
                        with self._state_lock:
                            self._list_owner_conn = conn
                    resp = self.device.process(cmd)
                    if resp is not None:
                        conn.sendall((resp + "\n").encode("ascii"))
        finally:
            should_stop = False
            with self._state_lock:
                if self._list_owner_conn is conn:
                    self._list_owner_conn = None
                    should_stop = True
                self._client_socks.discard(conn)
                self.client_count = len(self._client_socks)
            if should_stop:
                self.device._stop_list()
            self.device._log("sys", f"Client disconnected: {addr}")
            try:
                conn.close()
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════════
#  Simulator Dashboard GUI
# ═══════════════════════════════════════════════════════════════════════════
class SimulatorGUI:
    """Live dashboard showing all BIT 802E internal state."""

    MAX_LOG_LINES = 500
    MAX_LOG_QUEUE = 5000

    def __init__(self, telnet_port: int, socket_port: int):
        self.telnet_port = telnet_port
        self.socket_port = socket_port
        self.log_queue: _queue.Queue = _queue.Queue()
        self._refresh_pending = False
        self._refresh_guard = threading.Lock()

        # device + server
        self.device = KepcoDevice(
            on_state_change=self._schedule_refresh,
            on_log=self._enqueue_log,
        )
        self.servers = [
            SCPIServer(self.device, telnet_port, echo=True),
            SCPIServer(self.device, socket_port),
        ]

        # UI
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.root = ctk.CTk()
        self.root.title(
            f"Kepco BIT 802E Simulator  —  Telnet {telnet_port} / "
            f"Socket {socket_port}")
        self.root.geometry("1150x820")
        self.root.configure(fg_color=C["bg"])
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_ui()
        for srv in self.servers:
            srv.start()
        self._refresh_state()
        self._poll_log()

    # ── build UI ──────────────────────────────────────────────────────────
    def _build_ui(self):
        # ═══ Title bar ═══
        top = ctk.CTkFrame(self.root, fg_color=C["primary"],
                           corner_radius=0, height=44)
        top.pack(fill="x")
        top.pack_propagate(False)
        ctk.CTkLabel(top, text=f"  🖥  KEPCO BIT 802E SIMULATOR   ·   "
                     f"TELNET 0.0.0.0:{self.telnet_port}   "
                     f"SOCKET 0.0.0.0:{self.socket_port}",
                     font=ctk.CTkFont(size=15, weight="bold"),
                     text_color="white").pack(side="left", padx=8)
        self.client_lbl = ctk.CTkLabel(
            top, text="Clients: 0", text_color="white",
            font=ctk.CTkFont(size=12))
        self.client_lbl.pack(side="right", padx=14)
        self.cmd_lbl = ctk.CTkLabel(
            top, text="Cmds: 0  Queries: 0", text_color="white",
            font=ctk.CTkFont(size=12))
        self.cmd_lbl.pack(side="right", padx=14)

        # ═══ Main body: paned ═══
        body = ctk.CTkFrame(self.root, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=8, pady=4)

        # Left: state panels
        left = ctk.CTkScrollableFrame(body, width=530, corner_radius=10,
                                      fg_color=C["surface"])
        left.pack(side="left", fill="both", expand=True, padx=(0, 4))

        # Right: log
        right = ctk.CTkFrame(body, width=380, corner_radius=10,
                              fg_color=C["surface"])
        right.pack(side="right", fill="both", padx=(4, 0))

        # ── State panels ──
        self._build_output_panel(left)
        self._build_list_panel(left)
        self._build_status_panel(left)

        # ── Command log ──
        ctk.CTkLabel(right, text="Command Log",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=C["text"]).pack(padx=10, pady=(10, 4))
        self.log_box = ctk.CTkTextbox(
            right, font=ctk.CTkFont(family="Consolas", size=11),
            fg_color=C["graph_bg"], text_color=C["text"],
            activate_scrollbars=True, wrap="word")
        self.log_box.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        ctk.CTkFrame(right, height=2, fg_color=C["border"]).pack(
            fill="x", padx=10, pady=(0, 8))

        ctk.CTkLabel(right, text="Manual Command Console",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=C["text"]).pack(anchor="w", padx=10,
                                                 pady=(0, 4))
        ctk.CTkLabel(
            right,
            text="Run SCPI commands locally in simulator for quick checks.",
            text_color=C["text2"],
            font=ctk.CTkFont(size=10)).pack(anchor="w", padx=10, pady=(0, 6))

        cmd_row = ctk.CTkFrame(right, fg_color="transparent")
        cmd_row.pack(fill="x", padx=10, pady=(0, 4))
        ctk.CTkLabel(cmd_row, text="CMD:",
                     font=ctk.CTkFont(family="Consolas", size=12),
                     text_color=C["text2"]).pack(side="left", padx=(0, 4))
        self.manual_cmd_entry = ctk.CTkEntry(
            cmd_row,
            placeholder_text="e.g. *IDN? or VOLT 5.0",
            font=ctk.CTkFont(family="Consolas", size=12))
        self.manual_cmd_entry.pack(side="left", fill="x", expand=True,
                                   padx=(0, 6))
        self.manual_cmd_entry.bind(
            "<Return>", lambda _e: self._run_manual_command())
        ctk.CTkButton(cmd_row, text="Send ▶", width=84,
                      fg_color=C["primary"], hover_color=C["primary_h"],
                      command=self._run_manual_command).pack(side="left")

        quick_row = ctk.CTkFrame(right, fg_color="transparent")
        quick_row.pack(fill="x", padx=10, pady=(0, 4))
        ctk.CTkButton(quick_row, text="*IDN?", width=72,
                      command=lambda: self._run_manual_preset("*IDN?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="SYST:ERR?", width=92,
                      command=lambda: self._run_manual_preset("SYST:ERR?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="OUTP?", width=72,
                      command=lambda: self._run_manual_preset("OUTP?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="MEAS:VOLT?", width=102,
                      command=lambda: self._run_manual_preset("MEAS:VOLT?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="MEAS:CURR?", width=102,
                      command=lambda: self._run_manual_preset("MEAS:CURR?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left")

        manual_ctrl = ctk.CTkFrame(right, fg_color="transparent")
        manual_ctrl.pack(fill="x", padx=10, pady=(0, 4))
        ctk.CTkButton(manual_ctrl, text="Clear Console", width=120,
                      fg_color="#374151", hover_color="#4b5563",
                      command=self._clear_manual_console).pack(side="left")

        self.manual_console_box = ctk.CTkTextbox(
            right, height=130,
            font=ctk.CTkFont(family="Consolas", size=11),
            fg_color=C["graph_bg"], text_color=C["text"],
            activate_scrollbars=True, wrap="word")
        self.manual_console_box.pack(fill="x", padx=10, pady=(0, 8))

        # ── Bottom: reset button ──
        bot = ctk.CTkFrame(self.root, fg_color="transparent", height=36)
        bot.pack(fill="x", padx=8, pady=(0, 6))
        ctk.CTkButton(bot, text="⟲  Reset Device (*RST)", width=200,
                      fg_color=C["red"], hover_color="#dc2626",
                      command=self._reset_device).pack(side="left", padx=4)
        ctk.CTkButton(bot, text="Clear Log", width=120,
                      fg_color="#374151", hover_color="#4b5563",
                      command=self._clear_log).pack(side="left", padx=4)

    # ── Output / setpoint panel ───────────────────────────────────────────
    def _build_output_panel(self, parent):
        frm = ctk.CTkFrame(parent, corner_radius=10, fg_color=C["card"])
        frm.pack(fill="x", padx=8, pady=(8, 4))
        ctk.CTkLabel(frm, text="Output & Setpoints",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=C["amber"]).pack(anchor="w", padx=12,
                                                  pady=(8, 4))
        grid = ctk.CTkFrame(frm, fg_color="transparent")
        grid.pack(fill="x", padx=12, pady=(0, 10))

        self._out_labels = {}
        fields = [
            ("Output",      "output_on"),
            ("FUNC:MODE",   "func_mode"),
            ("VOLT Setpt",  "volt_setpoint"),
            ("CURR Setpt",  "curr_setpoint"),
            ("MEAS:VOLT",   "meas_volt"),
            ("MEAS:CURR",   "meas_curr"),
            ("VOLT:MODE",   "volt_mode"),
            ("CURR:MODE",   "curr_mode"),
            ("VOLT:RANG",   "volt_range"),
            ("CURR:RANG",   "curr_range"),
            ("V RANG Auto", "volt_range_auto"),
            ("C RANG Auto", "curr_range_auto"),
        ]
        for i, (name, key) in enumerate(fields):
            r, c = divmod(i, 3)
            ctk.CTkLabel(grid, text=f"{name}:", text_color=C["text2"],
                         font=ctk.CTkFont(size=11),
                         width=90, anchor="e").grid(
                row=r, column=c * 2, padx=(0, 2), pady=1, sticky="e")
            lbl = ctk.CTkLabel(grid, text="—", text_color=C["text"],
                               font=ctk.CTkFont(family="Consolas", size=12,
                                                 weight="bold"),
                               width=110, anchor="w")
            lbl.grid(row=r, column=c * 2 + 1, padx=(0, 12), pady=1,
                     sticky="w")
            self._out_labels[key] = lbl

    # ── LIST registers panel ──────────────────────────────────────────────
    def _build_list_panel(self, parent):
        frm = ctk.CTkFrame(parent, corner_radius=10, fg_color=C["card"])
        frm.pack(fill="x", padx=8, pady=4)
        ctk.CTkLabel(frm, text="LIST Subsystem Registers",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=C["amber"]).pack(anchor="w", padx=12,
                                                  pady=(8, 4))

        grid = ctk.CTkFrame(frm, fg_color="transparent")
        grid.pack(fill="x", padx=12, pady=(0, 4))

        self._list_labels = {}
        meta_fields = [
            ("LIST:COUN",   "list_count"),
            ("LIST:DIR",    "list_dir"),
            ("LIST:GEN",    "list_gen"),
            ("COUN:SKIP",   "list_count_skip"),
            ("Running",     "list_running"),
            ("Step Idx",    "list_step_idx"),
            ("Iteration",   "list_iteration"),
            ("QUER Ptr",    "list_query_ptr"),
            ("VOLT Pts",    "list_volt_pts"),
            ("CURR Pts",    "list_curr_pts"),
            ("DWEL Pts",    "list_dwel_pts"),
            ("SEQ Pts",     "list_seq_pts"),
        ]
        for i, (name, key) in enumerate(meta_fields):
            r, c = divmod(i, 3)
            ctk.CTkLabel(grid, text=f"{name}:", text_color=C["text2"],
                         font=ctk.CTkFont(size=11),
                         width=80, anchor="e").grid(
                row=r, column=c * 2, padx=(0, 2), pady=1, sticky="e")
            lbl = ctk.CTkLabel(grid, text="—", text_color=C["text"],
                               font=ctk.CTkFont(family="Consolas", size=12,
                                                 weight="bold"),
                               width=100, anchor="w")
            lbl.grid(row=r, column=c * 2 + 1, padx=(0, 10), pady=1,
                     sticky="w")
            self._list_labels[key] = lbl

        # data tables: VOLT + DWEL
        ctk.CTkLabel(frm, text="LIST:VOLT / LIST:CURR data (first 50 shown)",
                     text_color=C["text2"],
                     font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=12, pady=(4, 0))
        self.list_data_box = ctk.CTkTextbox(
            frm, height=68,
            font=ctk.CTkFont(family="Consolas", size=10),
            fg_color=C["graph_bg"], text_color=C["text"],
            activate_scrollbars=True, wrap="word")
        self.list_data_box.pack(fill="x", padx=12, pady=(2, 4))

        ctk.CTkLabel(frm, text="LIST:DWEL data (first 50 shown)",
                     text_color=C["text2"],
                     font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=12, pady=(2, 0))
        self.list_dwel_box = ctk.CTkTextbox(
            frm, height=46,
            font=ctk.CTkFont(family="Consolas", size=10),
            fg_color=C["graph_bg"], text_color=C["text"],
            activate_scrollbars=True, wrap="word")
        self.list_dwel_box.pack(fill="x", padx=12, pady=(2, 8))

    # ── Status registers panel ────────────────────────────────────────────
    def _build_status_panel(self, parent):
        frm = ctk.CTkFrame(parent, corner_radius=10, fg_color=C["card"])
        frm.pack(fill="x", padx=8, pady=4)
        ctk.CTkLabel(frm, text="Status Registers & Error Queue",
                     font=ctk.CTkFont(size=14, weight="bold"),
                     text_color=C["amber"]).pack(anchor="w", padx=12,
                                                  pady=(8, 4))
        grid = ctk.CTkFrame(frm, fg_color="transparent")
        grid.pack(fill="x", padx=12, pady=(0, 10))

        self._stat_labels = {}
        stat_fields = [
            ("ESR",         "esr"),
            ("STB",         "stb"),
            ("OPER Cond",   "oper_cond"),
            ("OPER Enab",   "oper_enable"),
            ("OPER Event",  "oper_event"),
            ("QUES Cond",   "ques_cond"),
            ("QUES Enab",   "ques_enable"),
            ("QUES Event",  "ques_event"),
            ("Errors",      "error_count"),
        ]
        for i, (name, key) in enumerate(stat_fields):
            r, c = divmod(i, 3)
            ctk.CTkLabel(grid, text=f"{name}:", text_color=C["text2"],
                         font=ctk.CTkFont(size=11),
                         width=80, anchor="e").grid(
                row=r, column=c * 2, padx=(0, 2), pady=1, sticky="e")
            lbl = ctk.CTkLabel(grid, text="—", text_color=C["text"],
                               font=ctk.CTkFont(family="Consolas", size=12,
                                                 weight="bold"),
                               width=100, anchor="w")
            lbl.grid(row=r, column=c * 2 + 1, padx=(0, 10), pady=1,
                     sticky="w")
            self._stat_labels[key] = lbl

    # ── refresh display ───────────────────────────────────────────────────
    def _schedule_refresh(self):
        with self._refresh_guard:
            if self._refresh_pending:
                return
            self._refresh_pending = True
        try:
            self.root.after(0, self._refresh_state)
        except Exception:
            with self._refresh_guard:
                self._refresh_pending = False

    def _refresh_state(self):
        with self._refresh_guard:
            self._refresh_pending = False
        d = self.device
        with d._lock:
            # output panel
            self._out_labels["output_on"].configure(
                text="ON" if d.output_on else "OFF",
                text_color=C["green"] if d.output_on else C["red"])
            self._out_labels["func_mode"].configure(text=d.func_mode)
            self._out_labels["volt_setpoint"].configure(
                text=f"{d.volt_setpoint:.4f} V")
            self._out_labels["curr_setpoint"].configure(
                text=f"{d.curr_setpoint:.4f} A")
            self._out_labels["volt_mode"].configure(text=d.volt_mode)
            self._out_labels["curr_mode"].configure(text=d.curr_mode)
            self._out_labels["volt_range"].configure(
                text="Full" if d.volt_range == 1 else "Quarter")
            self._out_labels["curr_range"].configure(
                text="Full" if d.curr_range == 1 else "Quarter")
            self._out_labels["volt_range_auto"].configure(
                text="ON" if d.volt_range_auto else "OFF")
            self._out_labels["curr_range_auto"].configure(
                text="ON" if d.curr_range_auto else "OFF")

            # LIST meta
            self._list_labels["list_count"].configure(
                text=str(d.list_count))
            self._list_labels["list_dir"].configure(text=d.list_dir)
            self._list_labels["list_gen"].configure(text=d.list_gen)
            self._list_labels["list_count_skip"].configure(
                text=str(d.list_count_skip))
            self._list_labels["list_running"].configure(
                text="▶ RUNNING" if d.list_running else "⏹ STOPPED",
                text_color=C["green"] if d.list_running else C["text2"])
            self._list_labels["list_step_idx"].configure(
                text=str(d.list_step_idx))
            self._list_labels["list_iteration"].configure(
                text=str(d.list_iteration))
            self._list_labels["list_query_ptr"].configure(
                text=str(d.list_query_ptr))
            self._list_labels["list_volt_pts"].configure(
                text=str(len(d.list_volt)))
            self._list_labels["list_curr_pts"].configure(
                text=str(len(d.list_curr)))
            self._list_labels["list_dwel_pts"].configure(
                text=str(len(d.list_dwel)))
            self._list_labels["list_seq_pts"].configure(
                text=str(len(d.list_seq)))

            # LIST data
            data = d.list_volt or d.list_curr
            data_label = "VOLT" if d.list_volt else "CURR"
            data_preview = data[:50]
            dwel_preview = d.list_dwel[:50]

            # status
            self._stat_labels["esr"].configure(text=str(d.esr))
            self._stat_labels["stb"].configure(text=str(d.stb))
            self._stat_labels["oper_cond"].configure(
                text=str(d.oper_cond))
            self._stat_labels["oper_enable"].configure(
                text=str(d.oper_enable))
            self._stat_labels["oper_event"].configure(
                text=str(d.oper_event))
            self._stat_labels["ques_cond"].configure(
                text=str(d.ques_cond))
            self._stat_labels["ques_enable"].configure(
                text=str(d.ques_enable))
            self._stat_labels["ques_event"].configure(
                text=str(d.ques_event))
            self._stat_labels["error_count"].configure(
                text=str(len(d.error_queue)))

            cmd_cnt = d.cmd_count
            q_cnt = d.query_count

        # measurements (unlocked — they acquire their own lock)
        mv = d.measure_volt()
        mc = d.measure_curr()
        self._out_labels["meas_volt"].configure(
            text=f"{mv:.4f} V")
        self._out_labels["meas_curr"].configure(
            text=f"{mc:.4f} A")

        # top bar counters
        self.cmd_lbl.configure(
            text=f"Cmds: {cmd_cnt}  Queries: {q_cnt}")
        total_clients = sum(srv.client_count for srv in self.servers)
        self.client_lbl.configure(
            text=f"Clients: {total_clients}")

        # list data textboxes
        self.list_data_box.configure(state="normal")
        self.list_data_box.delete("1.0", "end")
        if data_preview:
            self.list_data_box.insert("1.0",
                f"[{data_label}] {len(data)} pts: " +
                ", ".join(f"{v:.3f}" for v in data_preview) +
                ("  ..." if len(data) > 50 else ""))
        else:
            self.list_data_box.insert("1.0", "(empty)")
        self.list_data_box.configure(state="disabled")

        self.list_dwel_box.configure(state="normal")
        self.list_dwel_box.delete("1.0", "end")
        if dwel_preview:
            self.list_dwel_box.insert("1.0",
                f"[DWEL] {len(d.list_dwel)} pts: " +
                ", ".join(f"{v:.6f}" for v in dwel_preview) +
                ("  ..." if len(d.list_dwel) > 50 else ""))
        else:
            self.list_dwel_box.insert("1.0", "(empty)")
        self.list_dwel_box.configure(state="disabled")

    # ── log handling ──────────────────────────────────────────────────────
    def _enqueue_log(self, direction, text):
        ts = time.strftime("%H:%M:%S")
        while self.log_queue.qsize() >= self.MAX_LOG_QUEUE:
            try:
                self.log_queue.get_nowait()
            except _queue.Empty:
                break
        if direction == "rx":
            self.log_queue.put(f"[{ts}] ▶ {text}\n")
        elif direction == "tx":
            self.log_queue.put(f"[{ts}] ◀ {text}\n")
        else:
            self.log_queue.put(f"[{ts}] ● {text}\n")

    def _poll_log(self):
        batch = []
        try:
            while True:
                batch.append(self.log_queue.get_nowait())
        except _queue.Empty:
            pass
        if batch:
            self.log_box.insert("end", "".join(batch))
            self.log_box.see("end")
            # trim
            lines = int(self.log_box.index("end-1c").split(".")[0])
            if lines > self.MAX_LOG_LINES:
                self.log_box.delete("1.0", f"{lines - self.MAX_LOG_LINES}.0")
        self.root.after(100, self._poll_log)

    # ── actions ───────────────────────────────────────────────────────────
    def _reset_device(self):
        self.device.reset()

    def _clear_log(self):
        self.log_box.delete("1.0", "end")

    def _run_manual_command(self):
        cmd = self.manual_cmd_entry.get().strip()
        if not cmd:
            return
        self._exec_manual_command(cmd)
        self.manual_cmd_entry.delete(0, "end")

    def _run_manual_preset(self, cmd: str):
        self.manual_cmd_entry.delete(0, "end")
        self.manual_cmd_entry.insert(0, cmd)
        self._exec_manual_command(cmd)

    def _exec_manual_command(self, cmd: str):
        ts = time.strftime("%H:%M:%S")
        resp = self.device.process(cmd)
        self.manual_console_box.insert("end", f"[{ts}] > {cmd}\n")
        if cmd.rstrip().endswith("?"):
            self.manual_console_box.insert(
                "end", f"[{ts}] < {resp if resp is not None else '(no response)'}\n")
        else:
            self.manual_console_box.insert(
                "end", f"[{ts}] {'✓ OK' if resp is None else f'↳ {resp}'}\n")
        self.manual_console_box.see("end")

    def _clear_manual_console(self):
        self.manual_console_box.delete("1.0", "end")

    def _on_close(self):
        for srv in self.servers:
            srv.stop()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Kepco BIT 802E Simulator")
    parser.add_argument(
        "--telnet-port", type=int, default=DEFAULT_TELNET_PORT,
        help=f"Telnet port (default {DEFAULT_TELNET_PORT})")
    parser.add_argument(
        "--socket-port", type=int, default=DEFAULT_SOCKET_PORT,
        help=f"Socket port (default {DEFAULT_SOCKET_PORT})")
    args = parser.parse_args()

    gui = SimulatorGUI(
        telnet_port=args.telnet_port,
        socket_port=args.socket_port)
    gui.run()


if __name__ == "__main__":
    main()
