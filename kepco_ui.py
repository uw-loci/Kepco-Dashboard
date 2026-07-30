#!/usr/bin/env python3
"""
Kepco BIT 802E Waveform Generator - High Performance Edition

Material-design UI with real-time waveform preview, upload progress,
auto-discovery, and verified single-LIST upload.

Hardware Constraints (BIT 802E manual):
  - Max 1000 list points per upload (1002 technically)
  - Dwell time: 0.0005 s (500 us) to 10 s
  - Waveforms over 1000 points are rejected before device communication
  - Use the active mode's RANG 1 to avoid quarter-scale transients

Maintenance Map:
  - KepcoController owns the socket-worker queue, SCPI transport, pacing,
    Telnet echo cleanup, and hardware verification.
  - Discovery and WaveformGen are small stateless helpers.
  - DashboardApp owns UI state, background workers, waveform request assembly,
    output safety checks, and live status/data logging.
  - Worker threads must never touch Tk widgets directly; route UI changes
    through DashboardApp._call_on_ui().
"""

import socket
import math
import csv
import os
import queue
import threading
import time
import ipaddress
import datetime
from dataclasses import dataclass
from enum import Enum
from tkinter import messagebox, filedialog

# -- GUI + plotting ----------------------------------------------------------
import customtkinter as ctk

import matplotlib
matplotlib.use("TkAgg")
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

from solenoid_temperature_reader import DatalogSolenoidTemperatureReader

# -- Constants ---------------------------------------------------------------
# These values encode hardware limits and transport timing assumptions. Keep
# them centralized so UI validation, upload handling, and safety interlocks stay
# aligned with the BIT/BOP behavior documented in the manual.
MIN_DWELL        = 0.0005    # 500 us - hardware minimum
MAX_DWELL        = 10.0      # hardware maximum
MAX_LIST_POINTS  = 1000      # maximum supported LIST waveform size
TELNET_PORT      = 5024      # Telnet fallback endpoint
SCPI_SOCKET_PORT = 5025      # preferred raw SCPI socket endpoint
DISCOVERY_TIMEOUT = 0.25
CHUNK_CMD_LIMIT  = 200       # safe margin for 253-byte SCPI buffer
SCPI_CMD_GAP     = 0.035     # > 25ms spec throughput (PAR 1.2.2)
LIST_VALUES_PER_CMD = 10     # manual examples show max 11 (PAR B.45/B.31)
RECV_TIMEOUT     = 6.0       # socket recv timeout for queries
STATUS_POLL_INTERVAL_MS = 1000
BOP_MAX_VOLTAGE  = 100.0     # BOP 100-2ML voltage rating
BOP_MAX_CURRENT  = 2.0       # BOP 100-2ML current rating
# BIT 802E manual Table 1-4: the signed main channel has 15 magnitude
# bits, while the complementary limit channel has 12 programming bits.
MAIN_CHANNEL_MAGNITUDE_BITS = 15
LIMIT_CHANNEL_PROGRAMMING_BITS = 12
PROGRAMMED_VALUE_LSB_MARGIN = 2.0
PROGRAMMED_VALUE_RELATIVE_TOLERANCE = 1e-4
DEFAULT_VOLTAGE_LIMIT = 40.0
DEFAULT_CURRENT_LIMIT = 2.0
SOLENOID_TEMPERATURE_POLL_MS = 3000
DATALOG_STALE_SECONDS = 10.0
DEFAULT_VOLTAGE_MONITOR_THRESHOLD_PCT = 5.0
DEFAULT_CURRENT_MONITOR_THRESHOLD_PCT = 5.0
# BIT 802E manual Tables 1-2 and 1-3, specifically for the BOP 100-2M.
# The dashboard locks the active channel to full-scale RANG 1, so the
# high-range programming figures apply.  Monitor uncertainty combines these
# worst-case bounds linearly instead of using the generic BIT-card percentage.
BOP_100_2M_VOLTAGE_MEASUREMENT_ACCURACY = 0.060
BOP_100_2M_VOLTAGE_HIGH_RANGE_PROGRAMMING_ACCURACY = 0.012
BOP_100_2M_CURRENT_MEASUREMENT_ACCURACY = 0.001
BOP_100_2M_CURRENT_HIGH_RANGE_PROGRAMMING_ACCURACY = 0.00025
RESISTANCE_MIN_CURRENT_A = BOP_100_2M_CURRENT_MEASUREMENT_ACCURACY
SOLENOID_BASE_RESISTANCE_OHMS = 20.95
SOLENOID_TEMPERATURE_COEFFICIENT_OHMS_PER_C = 0.0470
VERIFY_SNAPSHOT_COUNT = 3


class CommState(Enum):
    """Trust level of the dashboard's current view of the BIT 802E."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    VERIFYING = "verifying"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAULTED = "faulted"


class ProtocolError(ValueError):
    """A syntactically valid transport reply that is unsafe to trust as SCPI."""


class ConnectionLostError(ConnectionError):
    """The SCPI session is unavailable or no longer trusted for this transaction."""


@dataclass(frozen=True)
class StatusSnapshot:
    """One all-or-nothing, validated status-poll result."""

    voltage: float
    current: float
    output_on: bool
    mode: str


def _monitor_float(value):
    """Return a finite float for the pure DC-monitor helpers."""
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def derive_readback_resistance(measured_voltage, measured_current):
    """Return V/I when the current readback is large enough to be meaningful."""
    voltage = _monitor_float(measured_voltage)
    current = _monitor_float(measured_current)
    if (
        voltage is None
        or current is None
        or abs(current) <= RESISTANCE_MIN_CURRENT_A
    ):
        return None
    resistance = voltage / current
    return resistance if math.isfinite(resistance) else None


def dc_monitor_is_active(is_verified, is_on, mode_text, request):
    """Return whether a staged request is eligible for steady-DC monitoring."""
    request = request or {}
    mode = str(mode_text or "").strip().upper()
    request_mode = str(request.get("mode") or "").strip().upper()
    return bool(
        is_verified
        and is_on
        and mode in ("VOLT", "CURR")
        and request.get("kind") == "DC"
        and request_mode == mode
    )


def dc_monitor_tolerance(
        channel, expected_value, threshold_pct,
        propagated_programming_accuracy=0.0):
    """Return a percentage band with model-specific hardware uncertainty.

    ``propagated_programming_accuracy`` is expressed in the monitored
    channel's units.  It is added to measurement accuracy as a conservative
    worst-case bound before comparison with the operator's percentage band.
    """
    channel = str(channel or "").strip().upper()
    expected = _monitor_float(expected_value)
    threshold = _monitor_float(threshold_pct)
    programming_accuracy = _monitor_float(propagated_programming_accuracy)
    if (
        expected is None
        or threshold is None
        or threshold < 0
        or programming_accuracy is None
        or programming_accuracy < 0
    ):
        return None
    if channel == "VOLT":
        measurement_accuracy = (
            BOP_100_2M_VOLTAGE_MEASUREMENT_ACCURACY)
    elif channel == "CURR":
        measurement_accuracy = (
            BOP_100_2M_CURRENT_MEASUREMENT_ACCURACY)
    else:
        raise ValueError(f"Unsupported monitor channel '{channel}'")
    hardware_floor = measurement_accuracy + programming_accuracy
    return max(abs(expected) * threshold / 100.0, hardware_floor)


def evaluate_dc_monitor(
        mode, setpoint, measured_voltage, measured_current,
        solenoid_temp_1, solenoid_temp_2, temperatures_fresh,
        voltage_threshold_pct, current_threshold_pct):
    """Evaluate both DC channels without touching UI state.

    In current mode, current is compared directly with its setpoint and
    voltage is predicted from V=I*R(T).  Voltage mode applies the reciprocal
    relationship: voltage is compared with its setpoint and current is
    predicted from I=V/R(T).
    """
    mode = str(mode or "").strip().upper()
    if mode not in ("VOLT", "CURR"):
        raise ValueError(f"Unsupported DC monitor mode '{mode}'")

    setpoint = _monitor_float(setpoint)
    measured = {
        "VOLT": _monitor_float(measured_voltage),
        "CURR": _monitor_float(measured_current),
    }
    thresholds = {
        "VOLT": voltage_threshold_pct,
        "CURR": current_threshold_pct,
    }
    result_keys = {"VOLT": "voltage", "CURR": "current"}
    controlled_channel = mode
    predicted_channel = "CURR" if mode == "VOLT" else "VOLT"
    results = {
        "voltage": {
            "status": "unavailable", "expected": None, "predicted": False},
        "current": {
            "status": "unavailable", "expected": None, "predicted": False},
    }

    temp_1 = _monitor_float(solenoid_temp_1)
    temp_2 = _monitor_float(solenoid_temp_2)
    resistance = None
    if temperatures_fresh and temp_1 is not None and temp_2 is not None:
        resistance = (
            SOLENOID_BASE_RESISTANCE_OHMS
            + SOLENOID_TEMPERATURE_COEFFICIENT_OHMS_PER_C
            * (temp_1 + temp_2))
        if not math.isfinite(resistance) or resistance <= 0:
            resistance = None

    controlled_programming_accuracy = (
        BOP_100_2M_VOLTAGE_HIGH_RANGE_PROGRAMMING_ACCURACY
        if controlled_channel == "VOLT"
        else BOP_100_2M_CURRENT_HIGH_RANGE_PROGRAMMING_ACCURACY)
    if setpoint is not None:
        tolerance = dc_monitor_tolerance(
            controlled_channel,
            setpoint,
            thresholds[controlled_channel],
            controlled_programming_accuracy,
        )
        controlled_status = "unavailable"
        if measured[controlled_channel] is not None and tolerance is not None:
            controlled_status = (
                "ok"
                if abs(measured[controlled_channel] - setpoint) <= tolerance
                else "triggered")
        results[result_keys[controlled_channel]] = {
            "status": controlled_status,
            "expected": setpoint,
            "predicted": False,
        }

    expected_predicted = None
    if setpoint is not None and resistance is not None:
        expected_predicted = (
            setpoint / resistance
            if mode == "VOLT"
            else setpoint * resistance)

    predicted_status = "unavailable"
    if expected_predicted is not None:
        propagated_programming_accuracy = (
            controlled_programming_accuracy / resistance
            if mode == "VOLT"
            else controlled_programming_accuracy * resistance)
        tolerance = dc_monitor_tolerance(
            predicted_channel,
            expected_predicted,
            thresholds[predicted_channel],
            propagated_programming_accuracy,
        )
        if measured[predicted_channel] is not None and tolerance is not None:
            predicted_status = (
                "ok"
                if abs(
                    measured[predicted_channel] - expected_predicted
                ) <= tolerance
                else "triggered")
    results[result_keys[predicted_channel]] = {
        "status": predicted_status,
        "expected": expected_predicted,
        "predicted": True,
    }
    return results


@dataclass
class _SocketWorkItem:
    """One synchronous request executed by the sole SCPI socket owner."""

    operation: object
    args: tuple
    kwargs: dict
    done: threading.Event
    result: object = None
    error: BaseException | None = None


def validate_voltage(raw):
    """Validate and normalize a `MEAS:VOLT?` reply."""
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"Voltage response is not numeric: {raw!r}") from exc
    if not math.isfinite(value):
        raise ProtocolError("Voltage response is not finite")
    if abs(value) > BOP_MAX_VOLTAGE * 1.10:
        raise ProtocolError(
            f"Voltage response exceeds configured capability: {value:g} V")
    return value


def validate_current(raw):
    """Validate and normalize a `MEAS:CURR?` reply."""
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"Current response is not numeric: {raw!r}") from exc
    if not math.isfinite(value):
        raise ProtocolError("Current response is not finite")
    if abs(value) > BOP_MAX_CURRENT * 1.10:
        raise ProtocolError(
            f"Current response exceeds configured capability: {value:g} A")
    return value


def validate_output(raw):
    """Validate and normalize an `OUTP?` reply to a boolean."""
    value = str(raw).strip().upper()
    if value not in {"0", "1", "OFF", "ON"}:
        raise ProtocolError(f"Invalid OUTP? response: {raw!r}")
    return value in {"1", "ON"}


def validate_mode(raw):
    """Validate and normalize a `FUNC:MODE?` reply."""
    value = str(raw).strip().upper()
    mapping = {
        "0": "VOLT",
        "VOLT": "VOLT",
        "1": "CURR",
        "CURR": "CURR",
    }
    if value not in mapping:
        raise ProtocolError(f"Invalid FUNC:MODE? response: {raw!r}")
    return mapping[value]


def validate_identity(raw):
    """Validate and normalize the mandatory `*IDN?` verification reply."""
    value = str(raw).strip()
    if not value or "KEPCO" not in value.upper():
        raise ProtocolError(f"Invalid *IDN? response: {raw!r}")
    return value


def validate_operation_complete(raw):
    """Require the exact SCPI completion response required by the BIT gate."""
    value = str(raw).strip()
    if value != "1":
        raise ProtocolError(f"Invalid *OPC? response (expected '1'): {raw!r}")
    return True


def validate_remote_mode(raw):
    """Validate the BIT Telnet remote-mode query defined as `0` or `1`."""
    value = str(raw).strip()
    if value not in {"0", "1"}:
        raise ProtocolError(f"Invalid SYST:REM? response: {raw!r}")
    return value == "1"


# -- Material colour palette -------------------------------------------------
C = dict(
    bg="#121212", surface="#1e1e2e", card="#2a2a3c",
    primary="#7c3aed", primary_h="#6d28d9",
    green="#10b981", red="#ef4444", amber="#f59e0b",
    text="#e2e8f0", text2="#94a3b8", border="#3f3f5c",
    input_bg="#363650", graph_bg="#161625",
    waveform="#818cf8",
)


# ===========================================================================
#  SCPI Controller  (hardened for real BIT 802E hardware)
# ===========================================================================
class KepcoController:
    """Thread-safe SCPI control for a Kepco BIT 802E.

    Protocol notes (BIT 802E manual):
      - Prefer raw SCPI socket port 5025; use Telnet 5024 only as fallback
      - PAR 1.2.2: connection throughput ~25 ms per command
      - PAR 4.5.2: *WAI / *OPC? to ensure command completion
      - 253-byte input buffer limit per SCPI message
      - List: max 1002 steps, dwell 500 us ... 10 s

    Design:
      - One dedicated worker thread owns the SCPI socket. Every connection,
        query, command, recovery, and disconnect operation enters its queue.
      - Multi-command operations re-enter directly from the owner thread, so
        status snapshots and other transactions remain indivisible.
      - Every command and query uses one owner-thread transmit throttle so
        consecutive SCPI messages are separated by SCPI_CMD_GAP (35 ms).
      - LIST programming uses *WAI barriers at the manual-derived checkpoints
        (after LIST:CLE, after all values are sent, and after DWEL). *OPC? is
        reserved for the connection health gate.
      - Post-upload, LIST:{mode}:POIN? verifies the card accepted all
        points, and SYST:ERR? drains any queued errors.
    """

    def __init__(self):
        self.sock = None
        self.ip = ""
        self.port = SCPI_SOCKET_PORT
        self.transport = "SOCKET"
        self.comm_state = CommState.DISCONNECTED
        self.last_error = ""
        self.last_identity = ""
        self.last_verified_state = None
        self._recv_buffer = b""
        self._telnet_iac_pending = b""
        self._last_receive_issue = ""
        self._last_tx_time = None
        # The lock remains as a defensive invariant for nested controller
        # methods. Actual cross-thread serialization happens through the sole
        # socket-owner queue below.
        self._lock = threading.RLock()
        self._debug_logger = None
        self._socket_queue = queue.Queue()
        self._socket_worker_ident = None
        self._socket_worker_ready = threading.Event()
        self._socket_worker_state_lock = threading.Lock()
        self._socket_worker_stopping = False
        self._socket_worker = threading.Thread(
            target=self._socket_worker_loop,
            name="kepco-socket-owner",
            daemon=True,
        )
        self._socket_worker.start()
        self._socket_worker_ready.wait()

    def _socket_worker_loop(self):
        """Own the socket and execute queued operations one at a time."""
        self._socket_worker_ident = threading.get_ident()
        self._socket_worker_ready.set()
        while True:
            item = self._socket_queue.get()
            try:
                if item is None:
                    return
                try:
                    item.result = item.operation(*item.args, **item.kwargs)
                except BaseException as exc:
                    item.error = exc
                finally:
                    item.done.set()
            finally:
                self._socket_queue.task_done()

    def _is_socket_worker(self):
        return threading.get_ident() == self._socket_worker_ident

    def run_transaction(self, operation, *args, **kwargs):
        """Run one indivisible operation on the SCPI socket-owner thread.

        Calls made by an operation already running on the owner execute
        directly. This permits high-level transactions to call send_query and
        send_cmd without deadlocking or creating nested queue entries.
        """
        if self._is_socket_worker():
            return operation(*args, **kwargs)

        item = _SocketWorkItem(
            operation=operation,
            args=args,
            kwargs=kwargs,
            done=threading.Event(),
        )
        with self._socket_worker_state_lock:
            if self._socket_worker_stopping:
                raise RuntimeError("SCPI socket worker has stopped")
            self._socket_queue.put(item)
        item.done.wait()
        if item.error is not None:
            raise item.error
        return item.result

    def shutdown_socket_worker(self):
        """Drain accepted work and stop the socket-owner worker."""
        with self._socket_worker_state_lock:
            if self._socket_worker_stopping:
                return
            self._socket_worker_stopping = True
            self._socket_queue.put(None)
        if not self._is_socket_worker():
            self._socket_worker.join()

    def set_debug_logger(self, logger_cb):
        """Register callback(level, message) for comm/network debug logs."""
        self._debug_logger = logger_cb

    def _dbg(self, level, msg):
        cb = self._debug_logger
        if not cb:
            return
        try:
            cb(level, msg)
        except Exception:
            pass

    @property
    def is_transport_connected(self):
        """True only while a TCP socket is still owned by this controller."""
        return self.sock is not None

    @property
    def is_verified(self):
        """True only when the socket has passed the full health verification gate."""
        return self.comm_state is CommState.HEALTHY and self.is_transport_connected

    def _set_comm_state(self, state, reason=""):
        old_state = self.comm_state
        self.comm_state = state
        if old_state is not state:
            detail = f" ({reason})" if reason else ""
            self._dbg("info", f"COMM STATE {old_state.value} -> {state.value}{detail}")

    @staticmethod
    def _validate_identity_reply(raw):
        """Validate identity without colliding with connect's boolean option."""
        return validate_identity(raw)

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self._recv_buffer = b""
        self._telnet_iac_pending = b""
        self._last_receive_issue = ""
        self._last_tx_time = None

    def mark_degraded(self, reason):
        """Revoke permission to control an untrusted SCPI session."""
        if not self._is_socket_worker():
            return self.run_transaction(self.mark_degraded, reason)
        self.last_error = reason
        self._set_comm_state(CommState.DEGRADED, reason)
        self._dbg("warn", f"Communication degraded: {reason}")

    def _reject_invalid_response(self, reason):
        """Reject a malformed/misattributed reply and revoke session trust."""
        self.mark_degraded(reason)
        return False, reason

    def connection_lost(self, reason):
        """Retire a failed transport without silently creating a replacement."""
        if not self._is_socket_worker():
            return self.run_transaction(self.connection_lost, reason)
        self.last_error = reason
        self._close_socket()
        self._set_comm_state(CommState.DEGRADED, reason)
        self._dbg("err", f"Connection lost: {reason}")

    def fault(self, reason):
        """End a failed recovery attempt; a new operator connection is required."""
        if not self._is_socket_worker():
            return self.run_transaction(self.fault, reason)
        self.last_error = reason
        self._close_socket()
        self._set_comm_state(CommState.FAULTED, reason)
        self._dbg("err", f"Communication faulted: {reason}")

    # -- connect / disconnect -----------------------------------------------
    def connect(self, ip, port=None, validate_identity=False):
        if not self._is_socket_worker():
            return self.run_transaction(
                self.connect, ip, port=port,
                validate_identity=validate_identity)
        attempts = [(port, "CUSTOM")] if port is not None else [
            (SCPI_SOCKET_PORT, "SOCKET"),
            (TELNET_PORT, "TELNET"),
        ]
        last_err = ""
        self.last_identity = ""
        self.last_verified_state = None
        self._close_socket()
        self._set_comm_state(CommState.CONNECTING, f"connecting to {ip}")
        for target_port, transport in attempts:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self._dbg("info", f"Connect attempt {ip}:{target_port} ({transport})")
                s.settimeout(5)
                s.connect((ip, target_port))
                # Capture any greeting/negotiation bytes; the transport parser
                # owns them once the socket becomes the active session.
                time.sleep(0.1)
                s.setblocking(False)
                initial_data = b""
                try:
                    initial_data = s.recv(1024)
                except BlockingIOError:
                    pass
                s.setblocking(True)
                s.settimeout(RECV_TIMEOUT)

                self.sock = s
                self.ip = ip
                self.port = target_port
                self.transport = transport
                self._recv_buffer = b""
                self._telnet_iac_pending = b""
                self._last_receive_issue = ""
                if initial_data:
                    self._append_received(initial_data)
                self.last_error = ""
                # A TCP connection is not a trusted device state. Identity
                # validation below is deliberately insufficient to mark this
                # controller healthy; verify_device_state does that.
                self._set_comm_state(CommState.VERIFYING, "socket established")
                self._dbg("ok", f"Connected {ip}:{target_port} via {transport}")
                if validate_identity:
                    idn = self.identity()
                    if idn is None:
                        last_err = self.last_error or "No response to '*IDN?'"
                        self._dbg(
                            "warn",
                            f"Identity check failed {ip}:{target_port} ({transport}): {last_err}",
                        )
                        self.disconnect()
                        continue
                    try:
                        idn = self._validate_identity_reply(idn)
                    except ProtocolError as exc:
                        last_err = str(exc)
                        self._dbg(
                            "warn",
                            f"Identity check failed {ip}:{target_port} "
                            f"({transport}): {last_err}",
                        )
                        self.disconnect()
                        continue
                    self.last_identity = idn
                return True, f"Connected via {transport} ({target_port})"
            except Exception as e:
                last_err = str(e)
                self._dbg("warn", f"Connect failed {ip}:{target_port} ({transport}): {last_err}")
                try:
                    s.close()
                except Exception:
                    pass
        self._close_socket()
        self.last_error = last_err
        self._set_comm_state(CommState.FAULTED, "all connection attempts failed")
        self._dbg("err", f"All connect attempts failed for {ip}: {last_err}")
        return False, last_err

    def disconnect(self):
        if not self._is_socket_worker():
            return self.run_transaction(self.disconnect)
        self._dbg("info", f"Disconnecting from {self.ip}:{self.port} ({self.transport})")
        self._close_socket()
        self.last_verified_state = None
        self._set_comm_state(CommState.DISCONNECTED, "socket closed")
        self._dbg("info", "Disconnected")

    # -- Telnet IAC filtering ----------------------------------------------
    def _decode_telnet_chunk(self, data: bytes) -> bytes:
        """Statefully remove Telnet negotiation bytes from one TCP chunk."""
        data = self._telnet_iac_pending + data
        self._telnet_iac_pending = b""
        out = bytearray()
        i = 0
        n = len(data)
        while i < n:
            b = data[i]
            if b == 0xFF:
                if i + 1 >= n:
                    self._telnet_iac_pending = data[i:]
                    break
                nxt = data[i + 1]
                if nxt in (0xFB, 0xFC, 0xFD, 0xFE):
                    if i + 2 >= n:
                        self._telnet_iac_pending = data[i:]
                        break
                    i += 3
                    continue
                if nxt == 0xFA:
                    end = data.find(b"\xff\xf0", i + 2)
                    if end < 0:
                        self._telnet_iac_pending = data[i:]
                        break
                    i = end + 2
                    continue
                if nxt == 0xFF:
                    out.append(0xFF)
                    i += 2
                    continue
                i += 2
                continue
            out.append(b)
            i += 1
        return bytes(out)

    def _append_received(self, chunk):
        if self.port == TELNET_PORT:
            chunk = self._decode_telnet_chunk(chunk)
        self._recv_buffer += chunk

    def _pop_complete_line(self):
        """Pop one CR/LF-terminated protocol line, preserving any tail."""
        cr = self._recv_buffer.find(b"\r")
        lf = self._recv_buffer.find(b"\n")
        endings = [index for index in (cr, lf) if index >= 0]
        if not endings:
            return None
        end = min(endings)
        consume = end + 1
        if (self._recv_buffer[end:end + 1] == b"\r"
                and self._recv_buffer[end + 1:end + 2] == b"\n"):
            consume += 1
        line = self._recv_buffer[:end]
        self._recv_buffer = self._recv_buffer[consume:]
        return self._decode_response_line(line)

    def _decode_response_line(self, line):
        """Decode one framed response without hiding protocol corruption."""
        try:
            return line.decode("ascii")
        except UnicodeDecodeError as exc:
            self._last_receive_issue = (
                f"Non-ASCII byte in {self.transport} response frame")
            raise ProtocolError(self._last_receive_issue) from exc

    @staticmethod
    def _looks_like_scpi_command(text):
        t = text.strip()
        if not t:
            return False
        up = t.upper()
        if up in ("ON", "OFF", "LIST", "FIX", "VOLT", "CURR", "TRAN"):
            return False
        if "NO ERROR" in up or ("," in up and '"' in up):
            return False
        tok = up.split()[0]
        if tok.endswith("?"):
            return True
        if tok.startswith("*") or ":" in tok:
            return True
        return tok in (
            "OUTP", "VOLT", "CURR", "FUNC", "LIST", "SYST",
            "MEAS", "INIT", "TRIG", "STAT", "FORM", "SOUR",
            "LOAD", "RANG",
        )

    def _clean_response_line(self, line, echo=None):
        line = line.strip()
        if not line:
            return None
        if self.port == TELNET_PORT and ">" in line:
            tail = line.rsplit(">", 1)[1].strip()
            if not tail:
                return None
            line = tail
        if echo and line == echo:
            return None
        if self.port == TELNET_PORT and self._looks_like_scpi_command(line):
            return None
        return line

    # -- socket helpers -----------------------------------------------------
    def _drain_echo(self):
        """Move Telnet echo bytes into the persistent receive buffer.

        The BIT 802E Telnet server echoes every command back verbatim.
        If these echo bytes are never read they accumulate in the card's
        tiny TCP send buffer (~253 bytes, PAR B.2).  When that buffer
        fills the card blocks trying to echo and can no longer read new
        commands -> deadlock / freeze.

        This is intentionally very short - just long enough to pick up
        a single echo line that is already in-flight.

        Returns True if any bytes were read from the device.
        """
        prev = self.sock.gettimeout()
        try:
            self.sock.settimeout(0.02)          # 20 ms
            try:
                data = self.sock.recv(1024)
                if not data:
                    raise ConnectionError("Connection closed by peer")
                self._append_received(data)
                return True
            except (socket.timeout, BlockingIOError):
                return False
        finally:
            try:
                self.sock.settimeout(prev)
            except Exception:
                pass

    def _recv_response(self, sent_cmd=None, timeout=None):
        """Receive exactly one complete, transport-framed SCPI response."""
        timeout = timeout or RECV_TIMEOUT
        echo = sent_cmd.strip() if sent_cmd else None
        prev = self.sock.gettimeout()
        self.sock.settimeout(timeout)
        try:
            self._last_receive_issue = ""
            deadline = time.monotonic() + timeout
            while True:
                while True:
                    framed = self._pop_complete_line()
                    if framed is None:
                        break
                    cleaned = self._clean_response_line(framed, echo=echo)
                    if cleaned is None:
                        continue
                    if self.port == SCPI_SOCKET_PORT and self._recv_buffer.strip(b"\r\n"):
                        self._last_receive_issue = (
                            f"Multiple raw SCPI response frames received for "
                            f"'{sent_cmd or 'query'}'")
                        return None
                    return cleaned

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.sock.settimeout(min(remaining, timeout))
                try:
                    chunk = self.sock.recv(512)
                except socket.timeout:
                    break
                if not chunk:
                    raise ConnectionError("Connection closed by peer")
                self._append_received(chunk)
                if len(self._recv_buffer) > 65536:
                    self._last_receive_issue = "Receive frame exceeded 65536 bytes"
                    break
            if not self._last_receive_issue:
                if self._recv_buffer:
                    preview = self._recv_buffer[:80].decode(
                        "ascii", errors="backslashreplace")
                    self._last_receive_issue = (
                        f"Incomplete {self.transport} response frame for "
                        f"'{sent_cmd or 'query'}': {preview!r}")
                else:
                    self._last_receive_issue = (
                        f"No complete response frame for '{sent_cmd or 'query'}'")
            return None
        except ConnectionError:
            raise
        except socket.timeout:
            return None
        except Exception:
            return None
        finally:
            try:
                self.sock.settimeout(prev)
            except Exception:
                pass

    # -- SCPI primitive: command (no response) ------------------------------
    def _ensure_verified_for_io(self, allow_unverified=False):
        """Authorize one I/O operation without ever reconnecting the session."""
        if self.is_verified:
            return True
        if (allow_unverified and self.is_transport_connected
                and self.comm_state is CommState.VERIFYING):
            return True
        if not self.is_transport_connected:
            self.last_error = "No active SCPI socket"
        else:
            self.last_error = (
                f"Communication state is {self.comm_state.value}, not verified")
        self._dbg("warn", self.last_error)
        raise ConnectionLostError(self.last_error)

    def _wait_for_tx_slot(self):
        """Wait until the shared command/query transmit interval has elapsed."""
        last_tx_time = getattr(self, "_last_tx_time", None)
        if last_tx_time is None:
            return
        remaining = SCPI_CMD_GAP - (time.monotonic() - last_tx_time)
        if remaining > 0:
            time.sleep(remaining)

    def _send_scpi_message(self, cmd):
        """Pace and transmit one SCPI message on the socket-owner thread."""
        self._wait_for_tx_slot()
        self.sock.sendall((cmd + "\n").encode("ascii"))
        self._last_tx_time = time.monotonic()

    @staticmethod
    def _hardware_write_log_message(cmd):
        """Describe a state-changing SCPI write for the operator audit log."""
        text = str(cmd or "").strip()
        upper = text.upper()
        if not text or upper in ("*WAI", "*OPC"):
            # Synchronization barriers do not change programmed hardware state
            # and would obscure the writes that do.
            return None

        for prefix in ("LIST:VOLT ", "LIST:CURR "):
            if upper.startswith(prefix):
                payload = text[len(prefix):]
                point_count = len(payload.split(",")) if payload else 0
                channel = prefix.split(":")[1].strip()
                return (
                    "KEPCO HW WRITE sent: "
                    f"{channel} LIST data ({point_count} point(s))")

        return f"KEPCO HW WRITE sent: {text}"

    def send_cmd(self, cmd, allow_unverified=False):
        """Send a non-query SCPI command with mandatory pacing.

        The shared transmit throttle runs on the socket-owner thread and also
        covers queries. For Telnet, wait through the same pacing interval
        before draining the command echo so it has time to arrive. Returns
        True / None.
        """
        if not self._is_socket_worker():
            return self.run_transaction(
                self.send_cmd, cmd, allow_unverified=allow_unverified)
        self._ensure_verified_for_io(allow_unverified=allow_unverified)
        with self._lock:
            try:
                self._dbg("info", f"TX CMD: {cmd}")
                self._send_scpi_message(cmd)
                if self.port == TELNET_PORT:
                    self._wait_for_tx_slot()
                    self._drain_echo()  # consume Telnet echo
                hardware_log = self._hardware_write_log_message(cmd)
                if hardware_log:
                    self._dbg("ok", hardware_log)
                return True
            except Exception as e:
                self.connection_lost(str(e))
                self._dbg("err", f"CMD failed '{cmd}': {self.last_error}")
                return None

    # -- SCPI primitive: query (expects response) --------------------------
    def send_query(self, cmd, timeout=None, allow_unverified=False):
        """Send a SCPI query and return the response string (or None).

        The persistent receive buffer retains fragmented input until a full
        transport frame is available. Telnet echoes are parsed as framed
        protocol noise; raw SCPI bytes are never discarded before a query.
        """
        if not self._is_socket_worker():
            return self.run_transaction(
                self.send_query, cmd, timeout=timeout,
                allow_unverified=allow_unverified)
        self._ensure_verified_for_io(allow_unverified=allow_unverified)
        with self._lock:
            try:
                self._dbg("info", f"TX QRY: {cmd}")
                self._send_scpi_message(cmd)
                resp = self._recv_response(sent_cmd=cmd, timeout=timeout)
                if resp is None:
                    receive_issue = self._last_receive_issue or "no complete response"
                    reason = (
                        f"Query framing failure for '{cmd}': {receive_issue}; "
                        "session retired to prevent response-stream shift")
                    self._dbg(
                        "warn",
                        f"RX framing failure for '{cmd}': {receive_issue}")
                    self.connection_lost(reason)
                else:
                    self._dbg("ok", f"RX RESP: {cmd} -> {resp}")
                return resp
            except Exception as e:
                self.connection_lost(str(e))
                self._dbg("err", f"QRY failed '{cmd}': {self.last_error}")
                return None

    # -- backward-compat wrapper (used by Manual Override callbacks) --------
    def send(self, cmd, query=False):
        if query:
            return self.send_query(cmd)
        return self.send_cmd(cmd)

    # -- SCPI formatting and limit helpers -----------------------------------
    # The BIT has one complementary hardware-limit channel. Program that
    # channel with one absolute magnitude, as described in manual section
    # 4.5.1.1. The same magnitude is the symmetric software interlock.
    @staticmethod
    def format_scpi_value(value):
        return f"{float(value):.6g}"

    @staticmethod
    def absolute_limit(value, default):
        """Return the one positive limit magnitude supported by the BIT."""
        magnitude = abs(float(default if value is None else value))
        if not math.isfinite(magnitude) or magnitude <= 0:
            raise ValueError("Limit magnitude must be finite and greater than zero")
        return magnitude

    @classmethod
    def complementary_limit_cmd(cls, mode, voltage_compliance=None,
                                current_limit=None):
        """Build the manual-style single complementary-channel limit command."""
        mode = (mode or "VOLT").upper()
        if mode == "CURR":
            magnitude = cls.absolute_limit(
                voltage_compliance, DEFAULT_VOLTAGE_LIMIT)
            return f"VOLT {cls.format_scpi_value(magnitude)}"
        if mode == "VOLT":
            magnitude = cls.absolute_limit(
                current_limit, DEFAULT_CURRENT_LIMIT)
            return f"CURR {cls.format_scpi_value(magnitude)}"
        raise ValueError(f"Unsupported FUNC:MODE '{mode}'")

    @staticmethod
    def programmed_value_tolerance(channel, expected_value,
                                   limit_channel=False):
        """Return a BIT-resolution-aware programmed-value tolerance.

        Programmed-value queries include calibration and quantization.  The
        manual specifies 15 magnitude bits for the main channel, 12 bits for
        the limit channel, and up to two readback LSBs.  Verification must not
        require accuracy finer than the hardware can represent.
        """
        channel = str(channel or "").strip().upper()
        if channel == "VOLT":
            full_scale = BOP_MAX_VOLTAGE
        elif channel == "CURR":
            full_scale = BOP_MAX_CURRENT
        else:
            raise ValueError(f"Unsupported programmed channel '{channel}'")
        bits = (
            LIMIT_CHANNEL_PROGRAMMING_BITS
            if limit_channel else MAIN_CHANNEL_MAGNITUDE_BITS)
        resolution_floor = (
            PROGRAMMED_VALUE_LSB_MARGIN * full_scale / (1 << bits))
        return max(
            resolution_floor,
            abs(float(expected_value)) * PROGRAMMED_VALUE_RELATIVE_TOLERANCE,
        )

    @classmethod
    def default_voltage_limit(cls):
        return DEFAULT_VOLTAGE_LIMIT

    @classmethod
    def default_current_limit(cls):
        return DEFAULT_CURRENT_LIMIT

    def _log_sequence(self, label, cmds):
        self._dbg("info", f"{label}: {'; '.join(cmds)}")

    def send_sequence(self, cmds, label="SCPI sequence"):
        if not self._is_socket_worker():
            return self.run_transaction(
                self.send_sequence, cmds, label=label)
        cmds = [cmd for cmd in cmds if cmd]
        try:
            self._ensure_verified_for_io()
        except ConnectionLostError as exc:
            return False, str(exc)
        with self._lock:
            self._log_sequence(label, cmds)
            for cmd in cmds:
                if self.send_cmd(cmd) is None:
                    return False, f"{label} failed at '{cmd}': {self.last_error}"
            return True, "OK"

    @staticmethod
    def _normalize_source_mode(mode_resp):
        text = str(mode_resp or "").strip().upper()
        if text in ("0", "FIX", "FIXED"):
            return "FIX"
        if text == "1" or "LIST" in text:
            return "LIST"
        return None

    def select_fixed_mode(self, mode, label="Fixed-mode transition"):
        """Select FUNC:MODE and FIX state with query-verified barriers."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.select_fixed_mode, mode, label=label)

        mode = (mode or "VOLT").upper()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported FUNC:MODE '{mode}'"

        mode_resp = self.send_query("FUNC:MODE?")
        if mode_resp is None:
            return False, self.last_error or "FUNC:MODE? verification failed"
        actual_mode = self._normalize_func_mode(mode_resp)
        if actual_mode is None:
            return self._reject_invalid_response(
                f"{label}: invalid FUNC:MODE? response {mode_resp!r}")
        if actual_mode != mode:
            self._dbg("info", f"{label}: requesting FUNC:MODE {mode}")
            if self.send_cmd(f"FUNC:MODE {mode}") is None:
                return False, f"{label} failed to request FUNC:MODE {mode}"
            if not self.sync():
                return False, f"{label} failed waiting for FUNC:MODE {mode}"
            mode_resp = self.send_query("FUNC:MODE?")
            if mode_resp is None:
                return False, self.last_error or "FUNC:MODE? verification failed"
            actual_mode = self._normalize_func_mode(mode_resp)
            if actual_mode is None:
                return self._reject_invalid_response(
                    f"{label}: invalid FUNC:MODE? response {mode_resp!r}")
            if actual_mode != mode:
                return False, (
                    f"{label}: FUNC:MODE verification expected {mode} "
                    f"but received {mode_resp!r}")
        self._dbg("ok", f"{label}: FUNC:MODE verified as {mode_resp!r}")

        source_resp = self.send_query(f"{mode}:MODE?")
        if source_resp is None:
            return False, self.last_error or f"{mode}:MODE? verification failed"
        source_mode = self._normalize_source_mode(source_resp)
        if source_mode is None:
            return self._reject_invalid_response(
                f"{label}: invalid {mode}:MODE? response {source_resp!r}")
        if source_mode != "FIX":
            self._dbg("info", f"{label}: requesting {mode}:MODE FIX")
            if self.send_cmd(f"{mode}:MODE FIX") is None:
                return False, f"{label} failed to request {mode}:MODE FIX"
            if not self.sync():
                return False, f"{label} failed waiting for {mode}:MODE FIX"
            source_resp = self.send_query(f"{mode}:MODE?")
            if source_resp is None:
                return False, self.last_error or f"{mode}:MODE? verification failed"
            source_mode = self._normalize_source_mode(source_resp)
            if source_mode is None:
                return self._reject_invalid_response(
                    f"{label}: invalid {mode}:MODE? response {source_resp!r}")
            if source_mode != "FIX":
                return False, (
                    f"{label}: {mode}:MODE verification expected FIX "
                    f"but received {source_resp!r}")
        self._dbg("ok", f"{label}: {mode}:MODE verified as {source_resp!r}")
        return True, "Fixed mode verified"

    def configure_fixed_mode(self, mode, voltage_compliance=None,
                             current_limit=None, initial_setpoint=None,
                             apply_complementary_limit=True,
                             label="Fixed-mode setup"):
        """Verify mode transitions before applying range, limits, or setpoint."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.configure_fixed_mode,
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                initial_setpoint=initial_setpoint,
                apply_complementary_limit=apply_complementary_limit,
                label=label)

        mode = (mode or "VOLT").upper()
        ok, transition_msg = self.select_fixed_mode(mode, label=label)
        if not ok:
            return False, transition_msg

        # Fix the active range before changing its setpoint so automatic range
        # selection cannot create a quarter/full-scale crossover transient.
        # Then follow the manual's initial-programming rule: operating
        # parameter at zero, followed by one complementary limit magnitude.
        dependent_cmds = [f"{mode}:RANG 1"]
        if initial_setpoint is not None:
            dependent_cmds.append(
                f"{mode} {self.format_scpi_value(initial_setpoint)}")
        if apply_complementary_limit:
            dependent_cmds.append(self.complementary_limit_cmd(
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit))
        dependent_cmds.append("*WAI")
        ok, reason = self.send_sequence(
            dependent_cmds, label=f"{label} dependent range/limits")
        if not ok:
            return False, reason
        return self.verify_programmed_configuration(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            expected_setpoint=initial_setpoint,
            verify_limit=apply_complementary_limit,
            label=f"{label} programmed-state verification")

    def verify_fixed_mode(self, mode, label="Fixed-mode verification"):
        """Verify an existing fixed-mode configuration without rewriting it."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.verify_fixed_mode, mode, label=label)
        mode = (mode or "VOLT").upper()
        mode_resp = self.send_query("FUNC:MODE?")
        actual_mode = self._normalize_func_mode(mode_resp)
        if actual_mode is None:
            return self._reject_invalid_response(
                f"{label}: invalid FUNC:MODE? response {mode_resp!r}")
        if actual_mode != mode:
            return False, (
                f"{label}: expected FUNC:MODE {mode}, received {mode_resp!r}")
        source_resp = self.send_query(f"{mode}:MODE?")
        source_mode = self._normalize_source_mode(source_resp)
        if source_mode is None:
            return self._reject_invalid_response(
                f"{label}: invalid {mode}:MODE? response {source_resp!r}")
        if source_mode != "FIX":
            return False, (
                f"{label}: expected {mode}:MODE FIX, received {source_resp!r}")
        return True, "Fixed mode verified"

    def verify_complementary_limit(
            self, mode, voltage_compliance=None, current_limit=None,
            label="Complementary-limit verification"):
        """Verify only the absolute limit channel for the selected main mode."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.verify_complementary_limit,
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                label=label)
        mode = (mode or "VOLT").upper()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported FUNC:MODE '{mode}'"
        channel = "VOLT" if mode == "CURR" else "CURR"
        expected_limit = float(
            self.complementary_limit_cmd(
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit).split()[1])
        limit_resp = self.send_query(f"{channel}?")
        try:
            actual_limit = abs(float(str(limit_resp).strip()))
            if not (math.isfinite(actual_limit)
                    and math.isfinite(expected_limit)):
                raise ValueError("non-finite programmed limit")
        except (TypeError, ValueError):
            return self._reject_invalid_response(
                f"{label}: invalid {channel}? response {limit_resp!r}")
        tolerance = self.programmed_value_tolerance(
            channel, expected_limit, limit_channel=True)
        difference = abs(actual_limit - expected_limit)
        if difference > tolerance:
            return False, (
                f"{label}: expected {channel} limit magnitude "
                f"{expected_limit:g}, received {limit_resp!r} "
                f"(difference {difference:.6g} exceeds hardware-aware "
                f"tolerance {tolerance:.6g})")
        if difference > 0:
            self._dbg(
                "info",
                f"{label}: accepted calibrated {channel} limit "
                f"{actual_limit:.9g} for requested {expected_limit:.9g} "
                f"(difference {difference:.6g}, tolerance "
                f"{tolerance:.6g})")
        return True, "Complementary limit verified"

    def verify_programmed_configuration(
            self, mode, voltage_compliance=None, current_limit=None,
            expected_setpoint=None, verify_limit=True,
            label="Programmed-state verification"):
        """Verify fixed mode, full range, limit magnitude, and optional setpoint."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.verify_programmed_configuration,
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                expected_setpoint=expected_setpoint,
                verify_limit=verify_limit,
                label=label)
        mode = (mode or "VOLT").upper()
        ok, reason = self.verify_fixed_mode(mode, label=label)
        if not ok:
            return False, reason

        range_resp = self.send_query(f"{mode}:RANG?")
        try:
            if int(float(str(range_resp).strip())) != 1:
                return False, (
                    f"{label}: expected {mode}:RANG 1, received {range_resp!r}")
        except (TypeError, ValueError):
            return self._reject_invalid_response(
                f"{label}: invalid {mode}:RANG? response {range_resp!r}")

        if verify_limit:
            ok, reason = self.verify_complementary_limit(
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                label=label)
            if not ok:
                return False, reason

        if expected_setpoint is not None:
            setpoint_resp = self.send_query(f"{mode}?")
            try:
                actual_setpoint = float(str(setpoint_resp).strip())
                expected_value = float(expected_setpoint)
                if not (math.isfinite(actual_setpoint)
                        and math.isfinite(expected_value)):
                    raise ValueError("non-finite programmed setpoint")
            except (TypeError, ValueError):
                return self._reject_invalid_response(
                    f"{label}: invalid {mode}? response {setpoint_resp!r}")
            tolerance = self.programmed_value_tolerance(
                mode, expected_value, limit_channel=False)
            difference = abs(actual_setpoint - expected_value)
            if difference > tolerance:
                return False, (
                    f"{label}: expected {mode} setpoint {expected_value:g}, "
                    f"received {setpoint_resp!r} "
                    f"(difference {difference:.6g} exceeds hardware-aware "
                    f"tolerance {tolerance:.6g})")
            if difference > 0:
                self._dbg(
                    "info",
                    f"{label}: accepted calibrated {mode} setpoint "
                    f"{actual_setpoint:.9g} for requested "
                    f"{expected_value:.9g} (difference {difference:.6g}, "
                    f"tolerance {tolerance:.6g})")
        return True, "Programmed configuration verified"

    def apply_complementary_limit(self, mode, voltage_compliance=None,
                                  current_limit=None,
                                  label="Complementary limit update"):
        """Change only the one complementary limit after mode verification."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.apply_complementary_limit,
                mode,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                label=label)
        ok, reason = self.verify_fixed_mode(mode, label=label)
        if not ok:
            return False, reason
        command = self.complementary_limit_cmd(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit)
        ok, reason = self.send_sequence(
            [command, "*WAI"], label=label)
        if not ok:
            return False, reason
        return self.verify_complementary_limit(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            label=f"{label} verification")

    def _classify_device_errors(self, errors, label):
        """Log device execution errors; none are globally safe to ignore."""
        blocking = []
        for error in errors or []:
            blocking.append(error)
            self._dbg("err", f"{label}: blocking BIT device error {error}")
        return blocking

    def _drain_preexisting_device_errors(self, label):
        """Clear and visibly report errors that predate a programming action."""
        errors = self.drain_errors(fail_on_timeout=True)
        if errors is None:
            return False, (
                self.last_error or
                f"{label}: timed out draining existing BIT system errors")
        for error in errors:
            self._dbg(
                "warn",
                f"{label}: existing BIT system error drained before "
                f"transaction: {error}")
        return True, ""

    def _require_clean_device_error_queue(self, label):
        """Fail an action if the BIT reports any newly queued device error."""
        errors = self.drain_errors(fail_on_timeout=True)
        if errors is None:
            return False, (
                self.last_error or f"{label}: SYST:ERR? verification timeout")
        blocking = self._classify_device_errors(errors, label)
        if blocking:
            return False, (
                f"{label}: blocking device errors after transaction: "
                f"{'; '.join(blocking)}")
        return True, ""

    def verify_dc_postflight(self, expected_mode=None, expected_output=None,
                             label="DC transaction"):
        """Require a clean error queue and valid live state after DC writes."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.verify_dc_postflight,
                expected_mode=expected_mode,
                expected_output=expected_output,
                label=label)

        error_free, reason = self._require_clean_device_error_queue(label)
        if not error_free:
            return False, reason, None

        snapshot, reason = self.read_status_snapshot()
        if snapshot is None:
            return False, reason, None
        if expected_mode and snapshot.mode != expected_mode:
            reason = (
                f"{label}: postflight mode expected {expected_mode}, "
                f"received {snapshot.mode}")
            return False, reason, None
        if (expected_output is not None
                and snapshot.output_on != bool(expected_output)):
            reason = (
                f"{label}: postflight output expected "
                f"{'ON' if expected_output else 'OFF'}, received "
                f"{'ON' if snapshot.output_on else 'OFF'}")
            return False, reason, None
        self.last_verified_state = snapshot
        self._dbg(
            "ok",
            f"{label}: postflight verified V={snapshot.voltage:.6g}, "
            f"I={snapshot.current:.6g}, "
            f"OUTP={'ON' if snapshot.output_on else 'OFF'}, "
            f"MODE={snapshot.mode}")
        return True, "", snapshot

    # -- synchronization helpers --------------------------------------------
    def sync(self):
        """Ensure all pending operations complete before next command.

        Sends *WAI (Wait-to-Continue, PAR A.17) which blocks the device's
        command processor until all pending operations finish.  Unlike
        *OPC? this is a *command* (no response expected) so it cannot
        time-out waiting for a reply - far more reliable on real
        hardware via Telnet.
        """
        return self.send_cmd("*WAI") is not None

    def drain_errors(self, fail_on_timeout=False):
        """Read and return all queued SYST:ERR entries (stops at '0,...')."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.drain_errors, fail_on_timeout=fail_on_timeout)
        errors = []
        for _ in range(20):
            resp = self.send_query("SYST:ERR?")
            if resp is None:
                if fail_on_timeout:
                    return None
                break
            resp = resp.strip()
            if resp.startswith("0") or "No error" in resp:
                break
            errors.append(resp)
        return errors

    def identity(self):
        return self.send_query("*IDN?", allow_unverified=True)

    def _ensure_telnet_remote_control(self):
        """Enter and verify remote mode before trusting a Telnet session."""
        if self.port != TELNET_PORT:
            return True

        remote_resp = self.send_query("SYST:REM?", allow_unverified=True)
        if remote_resp is None:
            raise ConnectionLostError(
                self.last_error or "No response to 'SYST:REM?'")
        if validate_remote_mode(remote_resp):
            self._dbg("ok", "Telnet remote mode already enabled")
            return True

        self._dbg("info", "Enabling Telnet remote mode")
        if self.send_cmd("SYST:REM 1", allow_unverified=True) is None:
            raise ConnectionLostError(
                self.last_error or "Failed to send 'SYST:REM 1'")

        remote_resp = self.send_query("SYST:REM?", allow_unverified=True)
        if remote_resp is None:
            raise ConnectionLostError(
                self.last_error or "No response verifying Telnet remote mode")
        if not validate_remote_mode(remote_resp):
            raise ProtocolError(
                "Telnet remote-mode verification failed: "
                f"SYST:REM? returned {remote_resp!r}")
        self._dbg("ok", "Telnet remote mode enabled and verified")
        return True

    def read_status_snapshot(self, allow_unverified=False):
        """Read one status snapshot, aborting before any later query on failure."""
        if not self._is_socket_worker():
            return self.run_transaction(
                self.read_status_snapshot,
                allow_unverified=allow_unverified)
        query_validators = (
            ("MEAS:VOLT?", validate_voltage),
            ("MEAS:CURR?", validate_current),
            ("OUTP?", validate_output),
            ("FUNC:MODE?", validate_mode),
        )
        replies = []
        for command, validator in query_validators:
            try:
                raw = self.send_query(command, allow_unverified=allow_unverified)
            except ConnectionLostError as exc:
                return None, str(exc)
            if raw is None:
                return None, self.last_error or f"No response to '{command}'"
            try:
                replies.append(validator(raw))
            except ProtocolError as exc:
                reason = str(exc)
                self.mark_degraded(
                    f"Status snapshot rejected: {reason}; "
                    "session synchronization uncertain")
                return None, reason
        snapshot = StatusSnapshot(
            voltage=replies[0],
            current=replies[1],
            output_on=replies[2],
            mode=replies[3],
        )
        resistance = derive_readback_resistance(
            snapshot.voltage, snapshot.current)
        resistance_text = (
            f"{resistance:.6g} ohm"
            if resistance is not None
            else "unavailable"
        )
        self._dbg(
            "info",
            f"Status snapshot readback: V={snapshot.voltage:.6g} V, "
            f"I={snapshot.current:.6g} A, R={resistance_text}")
        return snapshot, ""

    def verify_device_state(self):
        """Run the full multi-snapshot health gate before enabling control."""
        if not self._is_socket_worker():
            return self.run_transaction(self.verify_device_state)
        if not self.is_transport_connected:
            return False, "no active socket"
        self._set_comm_state(CommState.VERIFYING, "validating device state")
        try:
            identity = validate_identity(self.identity())
            self.last_identity = identity
            self._ensure_telnet_remote_control()
            opc = self.send_query("*OPC?", allow_unverified=True)
            validate_operation_complete(opc)
        except (ProtocolError, ConnectionLostError) as exc:
            reason = str(exc)
            self.mark_degraded(f"verification rejected: {reason}")
            return False, reason

        snapshots = []
        for index in range(VERIFY_SNAPSHOT_COUNT):
            snapshot, reason = self.read_status_snapshot(allow_unverified=True)
            if snapshot is None:
                self.mark_degraded(reason)
                return False, reason
            if snapshots:
                baseline = snapshots[0]
                if snapshot.output_on != baseline.output_on:
                    reason = "OUTP? changed during verification"
                    self.mark_degraded(reason)
                    return False, reason
                if snapshot.mode != baseline.mode:
                    reason = "FUNC:MODE? changed during verification"
                    self.mark_degraded(reason)
                    return False, reason
            snapshots.append(snapshot)
            self._dbg(
                "info",
                f"Verification snapshot {index + 1}/{VERIFY_SNAPSHOT_COUNT}: "
                f"V={snapshot.voltage:.6g}, I={snapshot.current:.6g}, "
                f"OUTP={'ON' if snapshot.output_on else 'OFF'}, MODE={snapshot.mode}")

        snapshot = snapshots[-1]
        self.last_verified_state = snapshot
        self.last_error = ""
        gate_summary = "*IDN?, *OPC?"
        if self.port == TELNET_PORT:
            gate_summary = "*IDN?, Telnet remote mode, *OPC?"
        self._set_comm_state(
            CommState.HEALTHY,
            f"{gate_summary}, and {VERIFY_SNAPSHOT_COUNT} stable snapshots verified")
        self._dbg(
            "ok",
            "Device health verified: "
            f"V={snapshot.voltage:.6g}, I={snapshot.current:.6g}, "
            f"OUTP={'ON' if snapshot.output_on else 'OFF'}, MODE={snapshot.mode}")
        return True, ""

    @staticmethod
    def _normalize_func_mode(mode_resp):
        text = str(mode_resp or "").strip().upper()
        if text == "0":
            return "VOLT"
        if text == "1":
            return "CURR"
        if text in ("VOLT", "CURR"):
            return text
        return None

    def disarm_active_list_mode(self):
        """Return the active source to FIX only when it is in LIST mode.

        Sending both VOLT:MODE FIX and CURR:MODE FIX on this hardware can
        itself enqueue -221 "Settings conflict" errors.  Query the currently
        active FUNC:MODE instead, inspect only that source's mode, and disarm
        it only when a live LIST program is actually armed.
        """
        if not self._is_socket_worker():
            return self.run_transaction(self.disarm_active_list_mode)
        try:
            active_mode = self._normalize_func_mode(self.send_query("FUNC:MODE?"))
            if not active_mode:
                return self._reject_invalid_response(
                    "Could not determine active FUNC:MODE")

            mode_state = self.send_query(f"{active_mode}:MODE?")
            if mode_state is None:
                return False, (
                    f"Could not query {active_mode}:MODE?: {self.last_error}")

            mode_text = str(mode_state).strip().upper()
            normalized_source_mode = self._normalize_source_mode(mode_state)
            if normalized_source_mode is None:
                return self._reject_invalid_response(
                    f"Invalid {active_mode}:MODE? response {mode_state!r}")
            if normalized_source_mode != "LIST":
                return True, "Active mode already fixed"

            # Manual Figure B-3 stops LIST with MODE FIX before issuing a new
            # fixed setpoint.  A source-level command sent while LIST is still
            # executing can be rejected or leave the final list point active.
            fix_cmd = f"{active_mode}:MODE FIX"
            if self.send_cmd(fix_cmd) is None:
                return False, f"Disarm '{fix_cmd}' failed: {self.last_error}"
            if not self.sync():
                return False, f"Disarm wait failed: {self.last_error}"
            fixed_resp = self.send_query(f"{active_mode}:MODE?")
            if fixed_resp is None:
                return False, (
                    f"Could not verify {active_mode}:MODE FIX: "
                    f"{self.last_error}")
            fixed_mode = self._normalize_source_mode(fixed_resp)
            if fixed_mode is None:
                return self._reject_invalid_response(
                    f"Invalid {active_mode}:MODE? response {fixed_resp!r}")
            if fixed_mode != "FIX":
                reason = (
                    f"Disarm verification expected {active_mode}:MODE FIX, "
                    f"received {fixed_resp!r}")
                return False, reason

            zero_cmd = f"{active_mode} 0"
            if self.send_cmd(zero_cmd) is None or not self.sync():
                return False, (
                    f"Disarm zero staging failed at '{zero_cmd}': "
                    f"{self.last_error}")
            zero_resp = self.send_query(f"{active_mode}?")
            try:
                if abs(float(str(zero_resp).strip())) > 1e-6:
                    return False, (
                        f"Disarm zero verification expected {active_mode} 0, "
                        f"received {zero_resp!r}")
            except (TypeError, ValueError):
                return self._reject_invalid_response(
                    f"Disarm zero verification received invalid "
                    f"{active_mode}? response {zero_resp!r}")
            return True, f"{active_mode} LIST mode disarmed"
        except Exception as e:
            return False, str(e)

    # -- Single LIST upload (<= 1000 pts) -----------------------------------
    def upload_list_chunk(self, points, dwell, mode="VOLT",
                          progress_cb=None, voltage_compliance=None,
                          current_limit=None, apply_limit_setup=True):
        """Upload one device-resident LIST with pacing and verification.

        Strategy:
          1. Disarm: switch the active LIST program back to FIX, then zero it
          2. Setup: FUNC:MODE, RANG, zero, optional one-time limit,
             LIST:CLE, *WAI
          3. Values: send LIST:{mode} batches of <= 10 values each,
             each followed only by the mandatory 35 ms gap
          4. Dwell: send LIST:DWEL once after values
          5. Verify: *WAI -> SYST:ERR? -> LIST:{mode}:POIN? -> SYST:ERR?

        Key change from previous revision: *OPC? is NOT used anywhere
        in the upload path.  The manual (PAR A.17) recommends *WAI for
        sequential command synchronization - it blocks the device's
        command processor (no response to time-out on).

        progress_cb(sent, total) is called after each batch if provided.
        """
        if not self._is_socket_worker():
            return self.run_transaction(
                self.upload_list_chunk, points, dwell, mode,
                progress_cb=progress_cb,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                apply_limit_setup=apply_limit_setup)
        with self._lock:
            try:
                self._ensure_verified_for_io()
            except ConnectionLostError as exc:
                return False, str(exc)
            if not points:
                return False, "Empty point list"
            if len(points) > MAX_LIST_POINTS:
                return False, f"LIST exceeds {MAX_LIST_POINTS} points"
            mode = (mode or "VOLT").upper()
            if mode not in ("VOLT", "CURR"):
                return False, f"Unsupported list mode '{mode}'"

            try:
                # Attribute only errors produced by this upload. Historical
                # entries are drained first but remain visible to the operator.
                clean, reason = self._drain_preexisting_device_errors(
                    f"LIST upload preflight ({mode})")
                if not clean:
                    return False, reason

                # -- Phase 1: Disarm any active LIST mode --
                # Live waveform replacement keeps OUTP ON, so the previous
                # waveform may still have the active source armed in LIST mode.
                # Real hardware can reject the next LIST:CLE / LIST:{mode}
                # sequence in that state, so unwind only the currently active
                # LIST program first without toggling OUTP.
                ok, msg = self.disarm_active_list_mode()
                if not ok:
                    return False, msg

                # -- Phase 2: Setup --
                # Real hardware behavior: some BIT firmware revisions reject
                # LIST:DWEL-before-values with -221 Settings conflict.
                #   disarm-active-list -> FUNC:MODE -> RANG -> LIST:CLE -> *WAI
                # NOTE: *CLS is intentionally NOT sent here - the manual
                # examples never use it for list operations, and it forces
                # the card to "operation complete idle" which can confuse
                # subsequent synchronisation on some firmware revisions.
                ok, setup_msg = self.configure_fixed_mode(
                    mode,
                    voltage_compliance=voltage_compliance,
                    current_limit=current_limit,
                    initial_setpoint=0.0,
                    apply_complementary_limit=apply_limit_setup,
                    label=f"LIST upload fixed-mode setup ({mode})")
                if not ok:
                    return False, setup_msg
                setup_cmds = [
                    "LIST:CLE",
                    "*WAI",                   # wait for LIST:CLE (PAR A.17)
                ]
                ok, setup_msg = self.send_sequence(
                    setup_cmds, label=f"LIST upload setup ({mode})")
                if not ok:
                    return False, setup_msg

                # -- Phase 3: Send list values --
                prefix = f"LIST:{mode} "
                total = len(points)
                sent = 0
                buf = []

                def _fmt(v):
                    """Compact value format - matches manual's integer style."""
                    s = f"{v:.4f}"
                    if '.' in s:
                        s = s.rstrip('0').rstrip('.')
                    return s

                for pt in points:
                    v = _fmt(pt)
                    trial = buf + [v]
                    trial_len = len(prefix) + len(",".join(trial))
                    if (trial_len > CHUNK_CMD_LIMIT
                            or len(trial) > LIST_VALUES_PER_CMD) and buf:
                        if self.send_cmd(prefix + ",".join(buf)) is None:
                            return False, (
                                f"List send failed at pt {sent}/{total}: "
                                f"{self.last_error}")
                        sent += len(buf)
                        if progress_cb:
                            progress_cb(sent, total)
                        buf = []
                    buf.append(v)

                if buf:
                    if self.send_cmd(prefix + ",".join(buf)) is None:
                        return False, (
                            f"List send failed at pt {sent}/{total}: "
                            f"{self.last_error}")
                    sent += len(buf)
                    if progress_cb:
                        progress_cb(sent, total)

                # Phase 4: Set dwell after values
                if self.send_cmd(f"LIST:DWEL {dwell:.6f}") is None:
                    return False, f"Dwell send failed: {self.last_error}"

                # Phase 5: Verify
                # *WAI ensures all LIST:{mode} values are ingested before
                # the verification query is processed (PAR A.17).
                if not self.sync():
                    return False, f"Post-upload *WAI failed: {self.last_error}"

                clean, reason = self._require_clean_device_error_queue(
                    f"LIST upload verification ({mode})")
                if not clean:
                    return False, reason

                pcount_str = self.send_query(f"LIST:{mode}:POIN?")
                if pcount_str is None:
                    return False, (
                        f"LIST:{mode}:POIN? verification failed: "
                        f"{self.last_error}")
                clean, reason = self._require_clean_device_error_queue(
                    f"LIST point-count query verification ({mode})")
                if not clean:
                    return False, reason
                try:
                    actual_count = int(pcount_str.strip())
                except (AttributeError, ValueError):
                    return False, (
                        f"Invalid LIST:{mode}:POIN? response {pcount_str!r}")
                if actual_count != total:
                    return False, (
                        f"Point count mismatch: sent {total}, "
                        f"device reports {actual_count}")

                return True, (
                    f"{total} pts @ {dwell*1000:.3f} ms/step (verified)")

            except Exception as e:
                return False, str(e)

    # Run / Stop 
    def run_list(self, mode="VOLT", count=1, enable_output=True,
                 voltage_compliance=None, current_limit=None,
                 apply_limit_setup=True):
        """Start LIST execution.

        When setup is requested, fixed/full-scale mode, zero, and the one
        complementary limit are programmed and verified first.  A normal UI
        upload has already done that work, so output enable only arms the list.

        When enable_output is False the current output state is preserved.  The
        upload path has already applied limits, so live re-arms can skip the
        fixed-source setup that can disturb an active AC waveform.
        """
        if not self._is_socket_worker():
            return self.run_transaction(
                self.run_list, mode, count=count,
                enable_output=enable_output,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                apply_limit_setup=apply_limit_setup)
        mode = (mode or "VOLT").upper()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported list mode '{mode}'"
        with self._lock:
            try:
                clean, reason = self._drain_preexisting_device_errors(
                    f"LIST run preflight ({mode})")
                if not clean:
                    return False, reason
                cmds = []
                if apply_limit_setup:
                    ok, setup_msg = self.configure_fixed_mode(
                        mode,
                        voltage_compliance=voltage_compliance,
                        current_limit=current_limit,
                        initial_setpoint=0.0,
                        label=f"LIST run fixed-mode setup ({mode})")
                    if not ok:
                        return False, setup_msg
                cmds.append(f"LIST:COUN {count}")
                if enable_output:
                    cmds.append("OUTP ON")
                cmds.append(f"{mode}:MODE LIST")
                cmds.append("*WAI")

                ok, run_msg = self.send_sequence(
                    cmds,
                    label=(
                        f"LIST run setup ({mode})"
                        if apply_limit_setup else f"LIST run arm ({mode})"))
                if not ok:
                    return False, run_msg

                clean, reason = self._require_clean_device_error_queue(
                    f"LIST run verification ({mode})")
                if not clean:
                    return False, reason

                outp_resp = self.send_query("OUTP?")
                if outp_resp is None:
                    return False, "Run verification failed: OUTP? unavailable"
                outp = outp_resp.strip().upper()
                mode_resp = self.send_query(f"{mode}:MODE?")
                if mode_resp is None:
                    return False, (
                        f"Run verification failed: {mode}:MODE? unavailable")
                clean, reason = self._require_clean_device_error_queue(
                    f"LIST run readback verification ({mode})")
                if not clean:
                    return False, reason
                mode_state = mode_resp.strip().upper()
                if enable_output and outp not in ("1", "ON"):
                    return False, "Run verification failed: output not enabled"
                if self._normalize_source_mode(mode_state) != "LIST":
                    return False, (
                        f"Run verification failed: {mode}:MODE is '{mode_state}'")
                return True, "Running"
            except Exception as e:
                return False, str(e)

    def stop(self, base_mode="VOLT"):
        """Stop LIST and return only after output OFF is authoritative."""
        if not self._is_socket_worker():
            return self.run_transaction(self.stop, base_mode=base_mode)
        base_mode = (base_mode or "VOLT").upper()
        if base_mode not in ("VOLT", "CURR"):
            return False, f"Unsupported FUNC:MODE '{base_mode}'"
        with self._lock:
            try:
                clean, reason = self._drain_preexisting_device_errors(
                    f"LIST stop preflight ({base_mode})")
                if not clean:
                    return False, reason
                # Only the source selected by FUNC:MODE can safely accept a
                # mode command on BIT 802E hardware.  If LIST is active,
                # disarm that source before switching the output off.
                ok, msg = self.disarm_active_list_mode()
                if not ok:
                    return False, msg
                ok, msg = self.send_sequence(
                    ["OUTP OFF", "*WAI"], label="LIST output disable")
                if not ok:
                    return False, msg

                clean, reason = self._require_clean_device_error_queue(
                    f"LIST stop output-disable verification ({base_mode})")
                if not clean:
                    return False, reason

                # Socket delivery is not proof that the BIT accepted OUTP OFF.
                # Query immediately after the manual-required wait barrier,
                # before any later mode normalization can obscure which step
                # failed. A missing or malformed reply revokes communication
                # trust; a valid ON reply leaves the session usable but makes
                # the requested stop fail.
                outp_resp = self.send_query("OUTP?")
                if outp_resp is None:
                    return False, (
                        "Stop verification failed: OUTP? unavailable; "
                        "physical output state is unknown")
                try:
                    output_on = validate_output(outp_resp)
                except ProtocolError as exc:
                    return self._reject_invalid_response(
                        f"Stop verification failed: {exc}")
                if output_on:
                    return False, (
                        "Stop verification failed: OUTP? reports output ON")

                ok, msg = self.select_fixed_mode(
                    base_mode, label=f"Stop fixed-mode setup ({base_mode})")
                if not ok:
                    return False, msg
                clean, reason = self._require_clean_device_error_queue(
                    f"LIST stop verification ({base_mode})")
                if not clean:
                    return False, reason
                return True, "Output OFF verified; LIST stopped"
            except Exception as e:
                return False, str(e)

#  Network Discovery
#  Stateless helper used by the Scan Network button.
class Discovery:
    """Scan a /24 subnet for Kepco devices (raw SCPI 5025, then Telnet 5024)."""

    @staticmethod
    def _probe(ip_str, timeout=DISCOVERY_TIMEOUT):
        for port in (SCPI_SOCKET_PORT, TELNET_PORT):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(timeout)
                s.connect((ip_str, port))
                if port == TELNET_PORT:
                    # Drain Telnet IAC negotiation before sending a SCPI query.
                    time.sleep(0.1)
                    s.setblocking(False)
                    try:
                        s.recv(1024)
                    except (BlockingIOError, OSError):
                        pass
                    s.setblocking(True)
                    s.settimeout(timeout)
                s.sendall(b"*IDN?\n")
                resp = s.recv(512).decode("ascii", errors="ignore").strip()
                s.close()
                if resp and ("KEPCO" in resp.upper() or "BOP" in resp.upper()
                             or "BIT" in resp.upper()):
                    return (ip_str, resp)
            except Exception:
                pass
        return None

    @staticmethod
    def scan_subnet(base_ip, callback=None, progress_cb=None):
        try:
            net = ipaddress.IPv4Network(base_ip + "/24", strict=False)
        except Exception:
            # Fall back to the lab subnet used by the default IP field.
            net = ipaddress.IPv4Network("192.168.50.0/24")

        hosts = [str(h) for h in net.hosts()]
        results = []
        total = len(hosts)
        done = [0]
        lock = threading.Lock()

        def worker(ip):
            r = Discovery._probe(ip)
            with lock:
                done[0] += 1
                if r:
                    results.append(r)
                if progress_cb and done[0] % 10 == 0:
                    progress_cb(done[0], total)

        batch = 50
        for i in range(0, len(hosts), batch):
            chunk = hosts[i:i + batch]
            thrds = [threading.Thread(target=worker, args=(ip,), daemon=True)
                     for ip in chunk]
            for t in thrds:
                t.start()
            for t in thrds:
                t.join(timeout=3)

        if callback:
            callback(results)
        return results


# ===========================================================================
#  Waveform Mathematics
#  Pure helpers for UI preview and LIST upload payload generation.
# ===========================================================================
class WaveformGen:
    """Generate waveform points with hardware-aware timing constraints."""

    @staticmethod
    def calculate_timing(freq, total_points):
        """Returns (actual_points, dwell, actual_freq, [warnings])."""
        if freq <= 0:
            return 0, 0, 0, ["Frequency must be > 0"]

        period = 1.0 / freq
        ideal_dwell = period / total_points
        warnings = []

        if ideal_dwell < MIN_DWELL:
            max_pts = max(2, int(period / MIN_DWELL))
            warnings.append(
                f"Dwell {ideal_dwell*1e6:.1f} us < min 500 us "
                f"-> reduced to {max_pts} pts"
            )
            total_points = max_pts
            ideal_dwell = period / total_points

        if ideal_dwell > MAX_DWELL:
            warnings.append(f"Dwell {ideal_dwell:.2f} s exceeds max 10 s")
            ideal_dwell = MAX_DWELL

        actual_freq = 1.0 / (total_points * ideal_dwell)
        return total_points, ideal_dwell, actual_freq, warnings

    @staticmethod
    def generate(wave_type, n, amplitude, offset):
        pts = []
        if wave_type == "Sine":
            for i in range(n):
                pts.append(offset + amplitude * math.sin(2 * math.pi * i / n))
        elif wave_type == "Square":
            for i in range(n):
                pts.append(offset + amplitude if i < n / 2
                           else offset - amplitude)
        elif wave_type == "Triangle":
            half = n // 2 or 1
            for i in range(n):
                if i <= half:
                    pts.append(offset - amplitude + (2 * amplitude / half) * i)
                else:
                    pts.append(offset + amplitude
                               - (2 * amplitude / half) * (i - half))
        elif wave_type == "Sawtooth":
            step = (2 * amplitude) / max(n - 1, 1)
            for i in range(n):
                pts.append(offset - amplitude + step * i)
        else:
            pts = [offset] * n
        return pts


# ===========================================================================
#  Application  (Material-themed, customtkinter)
# ===========================================================================
class DashboardApp:
    """Tk application shell around the Kepco controller.

    The app keeps hardware I/O on background threads, mirrors device state into
    UI widgets on the main thread, and stores the latest waveform as a request
    dictionary. That request object is the handoff contract between preview,
    upload, and output toggling.
    """

    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.root = ctk.CTk()
        self.root.title("Kepco BIT 802E - Waveform Generator")
        self.root.geometry("1280x760")
        self.root.minsize(900, 600)

        self.kepco = KepcoController()
        # Current waveform/session state. uploaded_request is the canonical
        # staged payload used by output toggling and status-panel rendering.
        self.csv_points = None
        self.csv_name = ""
        self.preview_points = []
        self.uploaded_request = None
        self.uploaded_waveform_ready = False
        # Canonical dashboard limits. The adjacent entry fields are pending
        # edits; these values change only after the operator presses Set and
        # the local staging/device transaction succeeds.
        self._dashboard_limits = {
            "VOLT": DEFAULT_VOLTAGE_LIMIT,
            "CURR": DEFAULT_CURRENT_LIMIT,
        }
        # None means the physical output has not been verified.  Never coerce
        # this to False: a lost dashboard session does not turn off a BOP.
        self.current_output_on: bool | None = None
        # Background work flags. They prevent duplicate button actions and help
        # status polling yield while command sequences own the hardware.
        self._scan_in_flight = False
        self._connect_in_flight = False
        self._upload_in_flight = False
        self._output_toggle_in_flight = False
        self._manual_operation_in_flight = False
        self._manual_operation_label = ""
        self._manual_device_controls = []
        self._status_poll_enabled = False
        self._status_poll_paused = False
        self._status_poll_in_flight = False
        self._status_poll_in_flight_generation = None
        self._status_poll_timer = None
        self._status_poll_generation = 0
        self._measurement_guard = None
        self._solenoid_temperature_poll_timer = None
        self._last_solenoid_temperature_error_logged = None
        self._last_solenoid_temperature_source_logged = None

        # Operator-facing session logs and optional readback CSV collection.
        self.log_file_handle = None
        self.log_file_path = ""
        self.data_collection_file_handle = None
        self.data_collection_file_path = ""
        self.data_collection_writer = None
        self.data_collection_started_at = None
        self.data_collection_enabled = False
        self._data_collection_switch_updating = False
        self.solenoid_temperature_reader = DatalogSolenoidTemperatureReader()
        self.solenoid_temperatures = {"1": None, "2": None}
        self.solenoid_temperature_timestamp = None
        self.solenoid_temperature_source_path = None
        self.solenoid_temperature_error = None
        self._last_unique_datalog_timestamp = None
        self._last_unique_datalog_seen_at = None
        self.dc_monitor_state = {"voltage": "inactive", "current": "inactive"}

        # UI-thread handoff state. Worker callbacks are queued here and drained
        # by a short root.after loop so Tk widgets stay on the main thread.
        self.current_control_mode = "VOLT"
        self.control_mode_var = ctk.StringVar(value="VOLT")
        self._ui_queue = queue.SimpleQueue()
        self._ui_queue_job = None
        self._ui_shutdown = False
        self._responsive_layout = None
        self._resize_job = None
        self._log_expanded = False
        self._expanded_log_height = None

        self.vmon_threshold_pct = DEFAULT_VOLTAGE_MONITOR_THRESHOLD_PCT
        self.imon_threshold_pct = DEFAULT_CURRENT_MONITOR_THRESHOLD_PCT

        self._init_log_file()
        self.kepco.set_debug_logger(self._controller_debug_log)
        self._build_ui()
        self._start_ui_dispatcher()
        self._reset_live_status()
        self._reset_uploaded_state()
        self._on_wave_change()
        self._start_solenoid_temperature_polling()

        if self.log_file_path:
            self.log(f"Session log file: {self.log_file_path}", "info")
        self.root.bind("<Configure>", self._on_root_configure, add="+")
        self.root.bind(
            "<Alt-l>", lambda _event: self._toggle_log_expansion(), add="+")
        self._apply_responsive_layout(self.root.winfo_width())
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # -- UI construction -----------------------------------------------------
    # The UI is split into a waveform/control tab, a manual SCPI tab, and a
    # status panel. Widget callbacks delegate to the behavioral sections below.

    # -- Responsive resize debounce -----------------------------------------
    # Coalesce window resize events before recalculating compact/wide layout.
    def _on_root_configure(self, event):
        if event.widget is not self.root:
            return
        if self._resize_job is not None:
            self.root.after_cancel(self._resize_job)
        def apply_resize(width=event.width, height=event.height):
            self._resize_job = None
            self._apply_responsive_layout(width)
            if self._log_expanded:
                self._resize_expanded_log(height)
        self._resize_job = self.root.after(
            80, apply_resize)

    # -- Responsive status mode controls -------------------------------------
    # Re-pack mode pills so compact widths stack without horizontal clipping.
    def _pack_status_mode_pills(self, compact):
        if not hasattr(self, "status_mode_labels"):
            return
        for pill in self.status_mode_labels.values():
            pill.pack_forget()
        for pill in self.status_mode_labels.values():
            if compact:
                pill.pack(anchor="w", pady=(0, 4))
            else:
                pill.pack(side="left", padx=(0, 6))

    # -- Responsive AC notice placement --------------------------------------
    # Preserve the AC warning label when the measurement header changes layout.
    def _place_ac_operation_notice(self):
        if (
            not hasattr(self, "status_ac_invalid_lbl")
            or not self._ac_invalid_label_visible
        ):
            return
        self.status_ac_invalid_lbl.pack_forget()
        if self._responsive_layout == "compact":
            self.status_ac_invalid_lbl.pack(anchor="w", pady=(2, 0))
        else:
            self.status_ac_invalid_lbl.pack(side="left", padx=(16, 0))

    # -- Responsive dashboard layout -----------------------------------------
    # Tune widths, wrapping, and measurement density for wide and compact use.
    def _apply_responsive_layout(self, width):
        if width <= 1:
            width = 1440
        layout = "compact" if width < 1260 else "wide"
        if layout == self._responsive_layout:
            return
        self._responsive_layout = layout
        compact = layout == "compact"

        self.main.grid_columnconfigure(
            0, weight=4 if compact else 11, uniform="main_columns")
        self.main.grid_columnconfigure(
            1, weight=3 if compact else 9, uniform="main_columns")

        self.wave_body.grid_columnconfigure(
            0, weight=7, minsize=0)
        self.wave_body.grid_columnconfigure(
            1, weight=0, minsize=170 if compact else 185)
        self.wave_cfg.configure(width=170 if compact else 185)
        self.output_card.configure(
            width=218 if compact else 250,
            height=112 if compact else 118)
        self.wave_cfg_title.configure(wraplength=140 if compact else 155)
        self.csv_lbl.configure(wraplength=58 if compact else 72)
        self.timing_lbl.configure(wraplength=140 if compact else 155)
        self.output_hint_lbl.configure(wraplength=178 if compact else 212)
        self.upload_btn.configure(width=104 if compact else 116, height=32)
        self.idn_lbl.configure(width=250 if compact else 360)

        self.status_content.grid_columnconfigure(
            0, weight=7 if compact else 6, minsize=250 if compact else 0)
        self.status_content.grid_columnconfigure(
            1, weight=1, minsize=124 if compact else 142)
        self.status_content.grid_rowconfigure(0, weight=1)
        self.status_content.grid_rowconfigure(1, weight=4)
        self.status_cfg_title.configure(wraplength=108 if compact else 122)
        self.temp_values.grid_configure(
            row=1,
            column=0,
            sticky="w",
            padx=(0, 0),
            pady=(6, 0))
        for label in getattr(self, "status_cfg_name_labels", []):
            label.configure(width=52 if compact else 60, wraplength=50 if compact else 58)
        for value in getattr(self, "status_cfg_labels", {}).values():
            value.configure(wraplength=54 if compact else 62)

        meas_font_size = 13 if compact else 14
        meas_font = ctk.CTkFont(family="Consolas", size=meas_font_size)
        self.status_meas_volt_lbl.configure(font=meas_font)
        self.status_meas_curr_lbl.configure(font=meas_font)
        self.status_meas_resistance_lbl.configure(font=meas_font)
        self.status_solenoid_temp_1_lbl.configure(font=meas_font)
        self.status_solenoid_temp_2_lbl.configure(font=meas_font)
        live_font = ctk.CTkFont(family="Consolas", size=9, weight="bold")
        for label in getattr(self, "status_live_console_labels", {}).values():
            label.configure(
                font=live_font,
                height=16,
                wraplength=230 if compact else 300)

        self._pack_status_mode_pills(compact)
        self._place_ac_operation_notice()

    def _build_ui(self):
        conn = ctk.CTkFrame(self.root, corner_radius=10)
        conn.pack(fill="x", padx=6, pady=(6, 3))
        conn.grid_columnconfigure(4, weight=1)

        ctk.CTkLabel(conn, text="IP Address:",
                     font=ctk.CTkFont(size=12)).grid(
            row=0, column=0, sticky="w", padx=(10, 4), pady=6)
        self.ip_var = ctk.StringVar(value="192.168.50.10")
        self.ip_combo = ctk.CTkComboBox(
            conn, variable=self.ip_var, values=["192.168.50.10"],
            width=170, height=26, font=ctk.CTkFont(size=12))
        self.ip_combo.grid(row=0, column=1, sticky="w", padx=4, pady=6)

        self.scan_btn = ctk.CTkButton(
            conn, text="Scan Network", width=118, height=28,
            command=self._start_scan,
            fg_color="#374151", hover_color="#4b5563",
            font=ctk.CTkFont(size=11))
        self.scan_btn.grid(row=0, column=2, sticky="w", padx=5, pady=6)

        self.conn_btn = ctk.CTkButton(
            conn, text="Connect", width=96, height=28,
            command=self._toggle_connect,
            fg_color=C["primary"], hover_color=C["primary_h"],
            font=ctk.CTkFont(size=12, weight="bold"))
        self.conn_btn.grid(row=0, column=3, sticky="w", padx=5, pady=6)

        self.status_lbl = ctk.CTkLabel(
            conn, text="Disconnected", text_color=C["red"],
            font=ctk.CTkFont(size=12))
        self.status_lbl.grid(row=0, column=4, sticky="w", padx=10, pady=6)

        self.idn_lbl = ctk.CTkLabel(
            conn, text="", text_color=C["text2"],
            font=ctk.CTkFont(size=10, slant="italic"), anchor="e", width=320)
        self.idn_lbl.grid(row=0, column=5, sticky="e", padx=(6, 10), pady=6)

        self.main = ctk.CTkFrame(self.root, corner_radius=12)
        self.main.grid_columnconfigure(0, weight=11, uniform="main_columns")
        self.main.grid_columnconfigure(1, weight=9, uniform="main_columns")
        self.main.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(self.main, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=(5, 2), pady=5)
        left.grid_rowconfigure(0, weight=1)
        left.grid_columnconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(left, corner_radius=12)
        self.tabview.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)

        wf_tab = self.tabview.add("Waveform Generator")
        man_tab = self.tabview.add("Manual Override")
        self._build_waveform_tab(wf_tab)
        self._build_manual_tab(man_tab)

        right = ctk.CTkFrame(self.main, corner_radius=12)
        right.grid(row=0, column=1, sticky="nsew", padx=(2, 5), pady=5)
        right.grid_rowconfigure(0, weight=1)
        right.grid_columnconfigure(0, weight=1)
        self._build_status_panel(right)

        self.log_wrap = ctk.CTkFrame(self.root, corner_radius=10)
        # Reserve the log's requested height before the expanding main panel.
        # This lets an expanded log crop/condense graph content instead of
        # being constrained by the graphs' requested canvas heights.
        self.log_wrap.pack(
            side="bottom", fill="x", padx=6, pady=(0, 6))
        log_header = ctk.CTkFrame(self.log_wrap, fg_color="transparent")
        log_header.pack(fill="x", padx=6, pady=(4, 0))
        ctk.CTkLabel(
            log_header,
            text="Event Log",
            font=ctk.CTkFont(size=12, weight="bold")).pack(
            side="left", padx=(2, 0))
        self.log_toggle_btn = ctk.CTkButton(
            log_header,
            text="Expand Log",
            width=96,
            height=24,
            command=self._toggle_log_expansion,
            fg_color="#374151",
            hover_color="#4b5563",
            font=ctk.CTkFont(size=11))
        self.log_toggle_btn.pack(side="right")
        self.log_text = ctk.CTkTextbox(
            self.log_wrap, height=66,
            font=ctk.CTkFont(family="Consolas", size=10),
            activate_scrollbars=True)
        self.log_text.pack(fill="both", padx=5, pady=5, expand=True)
        self.main.pack(
            side="top", fill="both", expand=True, padx=6, pady=3)

    @staticmethod
    def _expanded_log_target_height(window_height):
        """Use about 18% of the window while retaining the main dashboard."""
        return max(150, min(300, int(float(window_height) * 0.18)))

    def _resize_expanded_log(self, window_height=None):
        if not self._log_expanded or not hasattr(self, "log_text"):
            return
        if window_height is None:
            window_height = self.root.winfo_height()
        target = self._expanded_log_target_height(window_height)
        if target == self._expanded_log_height:
            return
        self._expanded_log_height = target
        self.log_text.configure(height=target)

    def _set_plots_condensed_for_log(self, condensed):
        """Reduce graph height requests so the expanded log gets its space."""
        heights = {
            "preview_canvas": 80 if condensed else 205,
            "status_canvas": 70 if condensed else 145,
        }
        for canvas_name, height in heights.items():
            canvas = getattr(self, canvas_name, None)
            if canvas is not None:
                canvas.get_tk_widget().configure(height=height)

    def _set_log_expanded(self, expanded):
        self._log_expanded = bool(expanded)
        if self._log_expanded:
            self._set_plots_condensed_for_log(True)
            self._expanded_log_height = None
            self._resize_expanded_log()
            self.log_toggle_btn.configure(text="Collapse Log")
        else:
            self._expanded_log_height = None
            self.log_text.configure(height=66)
            self._set_plots_condensed_for_log(False)
            self.log_toggle_btn.configure(text="Expand Log")

    def _toggle_log_expansion(self):
        self._set_log_expanded(not self._log_expanded)

    def _build_waveform_tab(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=4, pady=4)

        ctk.CTkLabel(
            outer, text="Control Panel",
            font=ctk.CTkFont(size=16, weight="bold")).pack(
            anchor="w", padx=3, pady=(0, 4))

        self.wave_body = ctk.CTkFrame(outer, fg_color="transparent")
        self.wave_body.pack(fill="both", expand=True)
        self.wave_body.grid_columnconfigure(0, weight=7)
        self.wave_body.grid_columnconfigure(1, weight=0, minsize=185)
        self.wave_body.grid_rowconfigure(0, weight=1)
        self.wave_body.grid_rowconfigure(1, weight=0)

        preview_card = ctk.CTkFrame(self.wave_body, corner_radius=12)
        preview_card.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))
        ctk.CTkLabel(
            preview_card, text="Preview Waveform",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(7, 3))
        preview_plot_wrap = ctk.CTkFrame(
            preview_card, corner_radius=10, fg_color=C["graph_bg"])
        preview_plot_wrap.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.preview_fig, self.preview_ax, self.preview_canvas = self._build_plot(
            preview_plot_wrap, (5.4, 2.05))

        self.wave_cfg = ctk.CTkFrame(self.wave_body, width=185, corner_radius=12)
        self.wave_cfg.grid(row=0, column=1, sticky="nsew", pady=(0, 4))

        self.wave_cfg_title = ctk.CTkLabel(
            self.wave_cfg, text="Waveform\nConfiguration",
            font=ctk.CTkFont(size=12, weight="bold"),
            justify="left", anchor="w", wraplength=155)
        self.wave_cfg_title.pack(fill="x", padx=7, pady=(6, 3))

        self._lbl(self.wave_cfg, "Waveform Type")
        self.wave_var = ctk.StringVar(value="Sine")
        self.wave_combo = ctk.CTkComboBox(
            self.wave_cfg, variable=self.wave_var,
            values=["DC", "Sine", "Square", "Triangle",
                    "Sawtooth", "CSV Custom (untested)"],
            command=self._on_wave_change,
            height=24, font=ctk.CTkFont(size=11))
        self.wave_combo.pack(fill="x", padx=7, pady=(0, 2))

        self.csv_frame = ctk.CTkFrame(self.wave_cfg, fg_color="transparent")
        self.csv_btn = ctk.CTkButton(
            self.csv_frame, text="Load CSV", width=72, height=24,
            command=self._load_csv,
            fg_color="#374151", hover_color="#4b5563",
            font=ctk.CTkFont(size=10))
        self.csv_btn.pack(side="left", padx=(0, 5))
        self.csv_lbl = ctk.CTkLabel(
            self.csv_frame, text="No file",
            text_color=C["text2"], font=ctk.CTkFont(size=9),
            anchor="w", justify="left", wraplength=72)
        self.csv_lbl.pack(side="left", fill="x", expand=True)

        self.freq_label = self._lbl(self.wave_cfg, "Frequency (Hz)")
        self.freq_entry = ctk.CTkEntry(
            self.wave_cfg, placeholder_text="40.0",
            height=24, font=ctk.CTkFont(size=11))
        self.freq_entry.insert(0, "40.0")
        self.freq_entry.pack(fill="x", padx=7, pady=(0, 2))

        self.amp_label = self._lbl(self.wave_cfg, "Amplitude (V / A)")
        self.amp_entry = ctk.CTkEntry(
            self.wave_cfg, placeholder_text="10.0",
            height=24, font=ctk.CTkFont(size=11))
        self.amp_entry.insert(0, "10.0")
        self.amp_entry.pack(fill="x", padx=7, pady=(0, 2))

        self.off_label = self._lbl(self.wave_cfg, "Offset (V / A)")
        self.off_entry = ctk.CTkEntry(
            self.wave_cfg, placeholder_text="0.0",
            height=24, font=ctk.CTkFont(size=11))
        self.off_entry.insert(0, "0.0")
        self.off_entry.pack(fill="x", padx=7, pady=(0, 2))

        self.pts_label = self._lbl(self.wave_cfg, "Total Points (max 1000)")
        self.pts_entry = ctk.CTkEntry(
            self.wave_cfg, placeholder_text="1000",
            height=24, font=ctk.CTkFont(size=11))
        self.pts_entry.insert(0, "1000")
        self.pts_entry.pack(fill="x", padx=7, pady=(0, 2))

        self.loop_label = self._lbl(self.wave_cfg, "Loop Count (0 = infinite)")
        self.loop_entry = ctk.CTkEntry(
            self.wave_cfg, placeholder_text="0",
            height=24, font=ctk.CTkFont(size=11))
        self.loop_entry.insert(0, "0")
        self.loop_entry.pack(fill="x", padx=7, pady=(0, 3))

        ctk.CTkButton(
            self.wave_cfg, text="Preview Waveform", command=self._preview,
            fg_color="#374151", hover_color="#4b5563",
            height=24, font=ctk.CTkFont(size=10)).pack(
            fill="x", padx=7, pady=(1, 2))

        self.timing_lbl = ctk.CTkLabel(
            self.wave_cfg, text="", text_color=C["amber"],
            font=ctk.CTkFont(size=9), wraplength=155, justify="left")
        self.timing_lbl.pack(fill="x", padx=7, pady=(1, 4))

        self.wave_footer = ctk.CTkFrame(self.wave_body, corner_radius=12)
        self.wave_footer.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 1))
        self.wave_footer.grid_columnconfigure(0, weight=1)
        self.wave_footer.grid_columnconfigure(1, weight=0)

        left_controls = ctk.CTkFrame(self.wave_footer, fg_color="transparent")
        left_controls.grid(row=0, column=0, sticky="ew", padx=(10, 6), pady=6)
        left_controls.grid_columnconfigure(1, weight=1)

        self.upload_btn = ctk.CTkButton(
            left_controls, text="Upload", width=116, height=32,
            command=self._upload_waveform,
            fg_color=C["green"], hover_color="#059669",
            text_color="#000", font=ctk.CTkFont(size=12, weight="bold"))
        self.upload_btn.grid(row=0, column=0, rowspan=2, sticky="ns", padx=(0, 8))
        self.prog_lbl = ctk.CTkLabel(
            left_controls, text="No upload yet",
            text_color=C["text2"], font=ctk.CTkFont(size=11, weight="bold"))
        self.prog_lbl.grid(row=0, column=1, sticky="w", pady=(0, 4))
        self.progress = ctk.CTkProgressBar(left_controls, height=10)
        self.progress.grid(row=1, column=1, sticky="ew")
        self.progress.set(0)
        self.data_collection_switch = ctk.CTkSwitch(
            left_controls, text="Collect data", width=132,
            command=self._toggle_data_collection,
            switch_width=30, switch_height=16,
            progress_color=C["amber"], text_color=C["text2"],
            font=ctk.CTkFont(size=10))
        self.data_collection_switch.grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))

        self.output_card = ctk.CTkFrame(self.wave_footer, width=250, height=118, corner_radius=12)
        switch_card = self.output_card
        switch_card.grid(row=0, column=1, sticky="ns", padx=(0, 10), pady=6)
        switch_card.pack_propagate(False)
        switch_row = ctk.CTkFrame(switch_card, fg_color="transparent")
        switch_row.pack(fill="x", padx=10, pady=(8, 4))
        ctk.CTkLabel(
            switch_row, text="Output Control",
            font=ctk.CTkFont(size=13, weight="bold")).pack(side="left")
        self.output_state_badge = ctk.CTkLabel(
            switch_row, text="OFFLINE", width=64, height=20, corner_radius=10,
            fg_color=C["red"], font=ctk.CTkFont(size=10, weight="bold"))
        self.output_state_badge.pack(side="right")
        self.output_summary_lbl = ctk.CTkLabel(
            switch_card, text="Disconnected",
            text_color=C["text2"], anchor="w",
            font=ctk.CTkFont(size=10))
        self.output_summary_lbl.pack(fill="x", padx=10, pady=(0, 5))
        self.output_toggle_btn = ctk.CTkButton(
            switch_card, text="Connect to Arm Output",
            command=self._toggle_output, height=34,
            corner_radius=8, font=ctk.CTkFont(size=12, weight="bold"),
            fg_color="#374151", hover_color="#4b5563")
        self.output_toggle_btn.pack(fill="x", padx=10)
        self.output_hint_lbl = ctk.CTkLabel(
            switch_card, text="Upload a waveform to enable output.",
            text_color=C["text2"], wraplength=212,
            justify="left", font=ctk.CTkFont(size=9))
        self.output_hint_lbl.pack(fill="x", padx=10, pady=(5, 8))

    def _build_manual_tab(self, parent):
        outer = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=4, pady=4)

        ctk.CTkLabel(
            outer, text="Config/Override Panel",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=3, pady=(0, 4))

        console = ctk.CTkFrame(outer, corner_radius=12)
        console.pack(fill="x", pady=(0, 5))

        ctk.CTkLabel(
            console, text="Manual Command Console",
            font=ctk.CTkFont(size=12, weight="bold")).pack(
            anchor="w", padx=8, pady=(6, 2))
        ctk.CTkLabel(
            console,
            text="Enter any SCPI command/query, or use quick commands below.",
            text_color=C["text2"], font=ctk.CTkFont(size=9)).pack(
            anchor="w", padx=8, pady=(0, 3))

        scpi_row = ctk.CTkFrame(console, fg_color="transparent")
        scpi_row.pack(fill="x", padx=8, pady=(0, 3))
        ctk.CTkLabel(
            scpi_row, text="CMD:",
            font=ctk.CTkFont(family="Consolas", size=10)).pack(
            side="left", padx=(0, 4))
        self.scpi_entry = ctk.CTkEntry(
            scpi_row, placeholder_text="e.g. *IDN? or FUNC:MODE CURR",
            height=24, font=ctk.CTkFont(family="Consolas", size=10))
        self.scpi_entry.pack(side="left", fill="x", expand=True, padx=4)
        self.scpi_entry.bind("<Return>", lambda _e: self._man_send_scpi())
        self.man_scpi_send_btn = ctk.CTkButton(
            scpi_row, text="Send", width=54, height=24, command=self._man_send_scpi,
            fg_color=C["primary"], hover_color=C["primary_h"])
        self.man_scpi_send_btn.pack(side="left", padx=4)
        self._manual_device_controls.append(self.man_scpi_send_btn)

        quick_row = ctk.CTkFrame(console, fg_color="transparent")
        quick_row.pack(fill="x", padx=8, pady=(0, 2))
        for label, cmd in [
            ("*IDN?", "*IDN?"),
            ("SYST:ERR?", "SYST:ERR?"),
            ("OUTP?", "OUTP?"),
            ("MEAS:VOLT?", "MEAS:VOLT?"),
            ("MEAS:CURR?", "MEAS:CURR?"),
        ]:
            btn = ctk.CTkButton(
                quick_row, text=label, width=76, height=22,
                command=lambda c=cmd: self._man_send_preset(c),
                fg_color="#374151", hover_color="#4b5563")
            btn.pack(side="left", padx=(0, 4))
            self._manual_device_controls.append(btn)

        quick_row2 = ctk.CTkFrame(console, fg_color="transparent")
        quick_row2.pack(fill="x", padx=8, pady=(0, 2))
        for label, cmd in [
            ("*OPC?", "*OPC?"),
            ("FUNC:MODE?", "FUNC:MODE?"),
            ("LIST:VOLT:POIN?", "LIST:VOLT:POIN?"),
            ("LIST:CURR:POIN?", "LIST:CURR:POIN?"),
        ]:
            btn = ctk.CTkButton(
                quick_row2, text=label, width=94, height=22,
                command=lambda c=cmd: self._man_send_preset(c),
                fg_color="#374151", hover_color="#4b5563")
            btn.pack(side="left", padx=(0, 4))
            self._manual_device_controls.append(btn)

        scpi_ctrl = ctk.CTkFrame(console, fg_color="transparent")
        scpi_ctrl.pack(fill="x", padx=8, pady=(0, 2))
        self.man_health_btn = ctk.CTkButton(
            scpi_ctrl, text="Health Check", width=90, height=22,
            command=self._man_health_check,
            fg_color="#374151", hover_color="#4b5563")
        self.man_health_btn.pack(side="left", padx=(0, 5))
        self._manual_device_controls.append(self.man_health_btn)
        ctk.CTkButton(
            scpi_ctrl, text="Clear Console", width=90, height=22,
            command=self._man_clear_scpi,
            fg_color="#374151", hover_color="#4b5563").pack(side="left")

        self.scpi_resp = ctk.CTkTextbox(
            console, height=58,
            font=ctk.CTkFont(family="Consolas", size=9),
            activate_scrollbars=True)
        self.scpi_resp.pack(fill="x", padx=8, pady=(2, 6))

        cards = ctk.CTkFrame(outer, fg_color="transparent")
        cards.pack(fill="x")
        cards.grid_columnconfigure(0, weight=1)
        cards.grid_columnconfigure(1, weight=1)
        cards.grid_rowconfigure(0, weight=0)
        cards.grid_rowconfigure(1, weight=0)

        left_controls = ctk.CTkFrame(cards, fg_color="transparent")
        left_controls.grid(row=0, column=0, sticky="new", padx=(0, 4), pady=(0, 6))

        right_controls = ctk.CTkFrame(cards, fg_color="transparent")
        right_controls.grid(row=0, column=1, sticky="new", padx=(4, 0), pady=(0, 6))

        mode_card = ctk.CTkFrame(right_controls, corner_radius=12)
        mode_card.pack(fill="x", pady=(0, 4))
        ctk.CTkLabel(
            mode_card, text="Set Control Mode",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 4))
        mode_row = ctk.CTkFrame(mode_card, fg_color="transparent")
        mode_row.pack(fill="x", padx=10, pady=(0, 6))
        self.mode_buttons = {}
        for mode in ("VOLT", "CURR"):
            btn = ctk.CTkButton(
                mode_row, text="Voltage" if mode == "VOLT" else "Current",
                width=70, height=24,
                command=lambda m=mode: self._select_control_mode(m))
            btn.pack(side="left", padx=(0, 6))
            self.mode_buttons[mode] = btn
            self._manual_device_controls.append(btn)
        ctk.CTkLabel(
            mode_card, text="Waveform uploads use the selected mode.",
            text_color=C["text2"], font=ctk.CTkFont(size=12),
            justify="left", wraplength=220).pack(
            anchor="w", padx=10, pady=(0, 8))

        limits_card = ctk.CTkFrame(left_controls, corner_radius=12)
        limits_card.pack(fill="x", pady=(0, 4))
        ctk.CTkLabel(
            limits_card, text="Set Absolute V/I Limits",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 6))

        v_row = ctk.CTkFrame(limits_card, fg_color="transparent")
        v_row.pack(fill="x", padx=10, pady=(0, 4))
        ctk.CTkLabel(v_row, text="Voltage limit (+/- V):",
                     font=ctk.CTkFont(size=12), anchor="w", wraplength=175).pack(fill="x")
        v_ctrl = ctk.CTkFrame(v_row, fg_color="transparent")
        v_ctrl.pack(fill="x", pady=(2, 0))
        self.soft_volt_limit_display = ctk.CTkLabel(
            v_ctrl,
            text=self._format_symmetric_display(
                self._dashboard_limits["VOLT"]),
            width=62,
            height=24,
            anchor="center",
            corner_radius=5,
            fg_color=C["graph_bg"],
            text_color="#ffffff",
            font=ctk.CTkFont(size=12))
        self.soft_volt_limit_display.pack(side="left", padx=(0, 4))
        self.soft_volt_limit_entry = ctk.CTkEntry(
            v_ctrl, width=62, height=24, font=ctk.CTkFont(size=12))
        self.soft_volt_limit_entry.insert(0, str(DEFAULT_VOLTAGE_LIMIT))
        self.soft_volt_limit_entry.pack(side="left", padx=(0, 4))
        self.soft_volt_limit_set_btn = ctk.CTkButton(
            v_ctrl, text="Set", width=34, height=24,
            command=lambda: self._set_software_limit("VOLT"),
            fg_color="#374151", hover_color="#4b5563")
        self.soft_volt_limit_set_btn.pack(side="left")
        self._manual_device_controls.append(self.soft_volt_limit_set_btn)

        c_row = ctk.CTkFrame(limits_card, fg_color="transparent")
        c_row.pack(fill="x", padx=10, pady=(0, 8))
        ctk.CTkLabel(c_row, text="Current limit (+/- A):",
                     font=ctk.CTkFont(size=12), anchor="w", wraplength=175).pack(fill="x")
        c_ctrl = ctk.CTkFrame(c_row, fg_color="transparent")
        c_ctrl.pack(fill="x", pady=(2, 0))
        self.soft_curr_limit_display = ctk.CTkLabel(
            c_ctrl,
            text=self._format_symmetric_display(
                self._dashboard_limits["CURR"]),
            width=62,
            height=24,
            anchor="center",
            corner_radius=5,
            fg_color=C["graph_bg"],
            text_color="#ffffff",
            font=ctk.CTkFont(size=12))
        self.soft_curr_limit_display.pack(side="left", padx=(0, 4))
        self.soft_curr_limit_entry = ctk.CTkEntry(
            c_ctrl, width=62, height=24, font=ctk.CTkFont(size=12))
        self.soft_curr_limit_entry.insert(0, str(DEFAULT_CURRENT_LIMIT))
        self.soft_curr_limit_entry.pack(side="left", padx=(0, 4))
        self.soft_curr_limit_set_btn = ctk.CTkButton(
            c_ctrl, text="Set", width=34, height=24,
            command=lambda: self._set_software_limit("CURR"),
            fg_color="#374151", hover_color="#4b5563")
        self.soft_curr_limit_set_btn.pack(side="left")
        self._manual_device_controls.append(self.soft_curr_limit_set_btn)

        range_card = ctk.CTkFrame(right_controls, corner_radius=12)
        range_card.pack(fill="x")
        ctk.CTkLabel(
            range_card, text="Range Control",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 4))
        ctk.CTkLabel(
            range_card, text="Full-scale avoids quarter-scale transients.",
            text_color=C["text2"], font=ctk.CTkFont(size=12),
            justify="left", wraplength=220).pack(
            anchor="w", padx=10, pady=(0, 5))
        range_row = ctk.CTkFrame(range_card, fg_color="transparent")
        range_row.pack(fill="x", padx=10, pady=(0, 6))
        self.man_range_var = ctk.StringVar(value="Auto")
        self.man_range_combo = ctk.CTkComboBox(
            range_row, variable=self.man_range_var,
            values=["Auto", "Full Scale", "Quarter Scale"],
            width=116, height=24, font=ctk.CTkFont(size=12))
        self.man_range_combo.pack(side="left", padx=(0, 6))
        self.man_range_set_btn = ctk.CTkButton(
            range_row, text="Set", width=40, height=24,
            command=self._man_set_range,
            fg_color="#374151", hover_color="#4b5563")
        self.man_range_set_btn.pack(side="left")
        self._manual_device_controls.append(self.man_range_set_btn)
        ctk.CTkFrame(range_card, height=2, fg_color=C["border"]).pack(
            fill="x", padx=10, pady=(2, 6))
        self.man_reset_btn = ctk.CTkButton(
            range_card, text="Reset Device (*RST)",
            command=self._man_reset,
            fg_color=C["red"], hover_color="#dc2626",
            height=26, font=ctk.CTkFont(size=12, weight="bold"))
        self.man_reset_btn.pack(fill="x", padx=10, pady=(0, 8))
        self._manual_device_controls.append(self.man_reset_btn)

        monitor_card = ctk.CTkFrame(left_controls, corner_radius=12)
        monitor_card.pack(fill="x")
        ctk.CTkLabel(
            monitor_card, text="DC Monitor Thresholds",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 4))

        monitor_row = ctk.CTkFrame(monitor_card, fg_color="transparent")
        monitor_row.pack(fill="x", padx=10, pady=(0, 8))

        v_ctrl = ctk.CTkFrame(monitor_row, fg_color="transparent")
        v_ctrl.pack(fill="x", pady=(0, 6))
        ctk.CTkLabel(
            v_ctrl, text="Voltage tolerance (%):",
            text_color=C["text2"], font=ctk.CTkFont(size=12),
            justify="left", wraplength=150).pack(anchor="w")
        voltage_threshold_row = ctk.CTkFrame(
            v_ctrl, fg_color="transparent")
        voltage_threshold_row.pack(anchor="w", pady=(2, 0))
        self.vmon_threshold_display = ctk.CTkLabel(
            voltage_threshold_row,
            text=self._format_symmetric_display(self.vmon_threshold_pct),
            width=52,
            height=24,
            anchor="center",
            corner_radius=5,
            fg_color=C["graph_bg"],
            text_color="#ffffff",
            font=ctk.CTkFont(size=12))
        self.vmon_threshold_display.pack(side="left", padx=(0, 4))
        self.vmon_threshold_entry = ctk.CTkEntry(
            voltage_threshold_row, width=52, height=24,
            font=ctk.CTkFont(size=12))
        self.vmon_threshold_entry.insert(0, str(DEFAULT_VOLTAGE_MONITOR_THRESHOLD_PCT))
        self.vmon_threshold_entry.pack(side="left")
        ctk.CTkButton(
            voltage_threshold_row, text="Set", width=34, height=24,
            command=lambda: self._set_monitor_threshold("VOLT"),
            fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(4, 0))

        i_ctrl = ctk.CTkFrame(monitor_row, fg_color="transparent")
        i_ctrl.pack(fill="x")
        ctk.CTkLabel(
            i_ctrl, text="Current tolerance (%):",
            text_color=C["text2"], font=ctk.CTkFont(size=12),
            justify="left", wraplength=150).pack(anchor="w")
        current_threshold_row = ctk.CTkFrame(i_ctrl, fg_color="transparent")
        current_threshold_row.pack(anchor="w", pady=(2, 0))
        self.imon_threshold_display = ctk.CTkLabel(
            current_threshold_row,
            text=self._format_symmetric_display(self.imon_threshold_pct),
            width=52,
            height=24,
            anchor="center",
            corner_radius=5,
            fg_color=C["graph_bg"],
            text_color="#ffffff",
            font=ctk.CTkFont(size=12))
        self.imon_threshold_display.pack(side="left", padx=(0, 4))
        self.imon_threshold_entry = ctk.CTkEntry(
            current_threshold_row, width=52, height=24,
            font=ctk.CTkFont(size=12))
        self.imon_threshold_entry.insert(0, str(DEFAULT_CURRENT_MONITOR_THRESHOLD_PCT))
        self.imon_threshold_entry.pack(side="left")
        ctk.CTkButton(
            current_threshold_row, text="Set", width=34, height=24,
            command=lambda: self._set_monitor_threshold("CURR"),
            fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(4, 0))

        self._update_mode_buttons(self.control_mode_var.get())

    def _build_status_panel(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        outer.grid_columnconfigure(0, weight=1)
        outer.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(
            outer, text="Status Panel",
            font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=3, pady=(0, 4))

        self.status_content = ctk.CTkFrame(outer, fg_color="transparent")
        content = self.status_content
        content.grid(row=1, column=0, sticky="nsew")
        content.grid_columnconfigure(0, weight=6)
        content.grid_columnconfigure(1, weight=1, minsize=142)
        content.grid_rowconfigure(0, weight=1)
        content.grid_rowconfigure(1, weight=4)

        self.status_plot_card = ctk.CTkFrame(content, corner_radius=12)
        plot_card = self.status_plot_card
        plot_card.grid(row=0, column=0, sticky="nsew", padx=(0, 5), pady=(0, 4))
        ctk.CTkLabel(
            plot_card, text="Active/Uploaded Waveform",
            font=ctk.CTkFont(size=12, weight="bold")).pack(
            anchor="w", padx=8, pady=(5, 2))
        status_plot_wrap = ctk.CTkFrame(
            plot_card, corner_radius=10, fg_color=C["graph_bg"])
        status_plot_wrap.pack(fill="both", expand=True, padx=5, pady=(0, 5))
        self.status_fig, self.status_ax, self.status_canvas = self._build_plot(
            status_plot_wrap, (4.5, 1.45))

        self.status_cfg_card = ctk.CTkFrame(content, corner_radius=12)
        cfg_card = self.status_cfg_card
        cfg_card.grid(row=0, column=1, sticky="nsew", pady=(0, 6))
        self.status_cfg_title = ctk.CTkLabel(
            cfg_card, text="Waveform\nConfiguration",
            font=ctk.CTkFont(size=12, weight="bold"),
            justify="left", anchor="w", wraplength=122)
        self.status_cfg_title.pack(fill="x", padx=7, pady=(6, 3))
        self.status_cfg_labels = {}
        self.status_cfg_name_labels = []
        for key, title in [
            ("wave", "Waveform"),
            ("mode", "Mode"),
            ("frequency", "Freq"),
            ("amplitude", "Amp/Val"),
            ("offset", "Offset"),
            ("points", "Points"),
            ("loop", "Loops"),
            ("device_state", "State"),
        ]:
            row = ctk.CTkFrame(cfg_card, fg_color="transparent")
            row.pack(fill="x", padx=7, pady=(0, 1))
            name = ctk.CTkLabel(
                row, text=title, text_color=C["text2"],
                font=ctk.CTkFont(size=9), width=60,
                anchor="w", justify="left", wraplength=62)
            name.pack(side="left")
            value = ctk.CTkLabel(
                row, text="--", font=ctk.CTkFont(size=9),
                justify="left", anchor="w", wraplength=68)
            value.pack(side="left", fill="x", expand=True)
            self.status_cfg_name_labels.append(name)
            self.status_cfg_labels[key] = value

        self.status_meas_card = ctk.CTkFrame(content, corner_radius=12, fg_color=C["graph_bg"])
        meas_card = self.status_meas_card
        meas_card.grid(row=1, column=0, sticky="nsew", padx=(0, 5))
        title_font = ctk.CTkFont(size=12, weight="bold")
        meas_title_row = ctk.CTkFrame(meas_card, fg_color="transparent")
        meas_title_row.pack(fill="x", padx=8, pady=(6, 3))
        ctk.CTkLabel(
            meas_title_row, text="Live Measurements",
            font=title_font).pack(side="left")
        self.status_ac_invalid_lbl = ctk.CTkLabel(
            meas_title_row, text="Invalid During AC Operation",
            text_color=C["red"], font=title_font)
        self._ac_invalid_label_visible = False

        meas_values = ctk.CTkFrame(meas_card, fg_color="transparent")
        self.meas_values = meas_values
        meas_values.pack(fill="x", padx=8, pady=(1, 4))
        meas_values.grid_columnconfigure(0, weight=1)

        vi_values = ctk.CTkFrame(meas_values, fg_color="transparent")
        self.vi_values = vi_values
        vi_values.grid(row=0, column=0, sticky="w")
        self.status_meas_volt_lbl = ctk.CTkLabel(
            vi_values, text="Voltage:  ---.----  V",
            font=ctk.CTkFont(family="Consolas", size=14),
            text_color="#60a5fa")
        self.status_meas_volt_lbl.pack(anchor="w", pady=(0, 1))
        self.status_meas_curr_lbl = ctk.CTkLabel(
            vi_values, text="Current:  ---.----  A",
            font=ctk.CTkFont(family="Consolas", size=14),
            text_color="#34d399")
        self.status_meas_curr_lbl.pack(anchor="w", pady=1)
        self.status_meas_resistance_lbl = ctk.CTkLabel(
            vi_values, text="Resistance:  ---.----  \N{OHM SIGN}",
            font=ctk.CTkFont(family="Consolas", size=14),
            text_color="#fb923c")
        self.status_meas_resistance_lbl.pack(anchor="w", pady=(1, 0))

        temp_values = ctk.CTkFrame(meas_values, fg_color="transparent")
        self.temp_values = temp_values
        temp_values.grid(row=1, column=0, sticky="w", pady=(3, 0))
        temp_font = ctk.CTkFont(family="Consolas", size=14)
        self.status_solenoid_temp_1_lbl = ctk.CTkLabel(
            temp_values, text="Solenoid 1:  --.-  \N{DEGREE SIGN}C",
            font=temp_font, text_color="#facc15")
        self.status_solenoid_temp_1_lbl.pack(anchor="w", pady=(0, 1))
        self.status_solenoid_temp_2_lbl = ctk.CTkLabel(
            temp_values, text="Solenoid 2:  --.-  \N{DEGREE SIGN}C",
            font=temp_font, text_color="#facc15")
        self.status_solenoid_temp_2_lbl.pack(anchor="w", pady=(1, 0))

        self.status_live_console = ctk.CTkFrame(
            meas_card, corner_radius=6, fg_color="#111827",
            border_width=1, border_color=C["border"])
        self.status_live_console.pack(fill="x", padx=8, pady=(0, 6))
        self.status_live_console_labels = {}
        for key in ("output", "voltage", "current", "datalog", "stale"):
            label = ctk.CTkLabel(
                self.status_live_console,
                text="",
                height=16,
                justify="left",
                anchor="w",
                wraplength=300,
                text_color=C["text2"],
                font=ctk.CTkFont(family="Consolas", size=9, weight="bold"))
            # The first visible line needs top padding so its glyphs do not
            # clip against the console border.  Output warnings can occupy
            # that first position when communication is unverified.
            label.pack(
                fill="x", padx=6,
                pady=(3 if key in ("output", "voltage") else 0, 3))
            self.status_live_console_labels[key] = label
        self.status_live_console_labels["stale"].pack_forget()

        self.status_info_card = ctk.CTkFrame(content, corner_radius=12)
        info_card = self.status_info_card
        info_card.grid(row=1, column=1, sticky="nsew")
        ctk.CTkLabel(
            info_card, text="Output Status",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(8, 5))
        out_row = ctk.CTkFrame(info_card, fg_color="transparent")
        out_row.pack(fill="x", padx=10, pady=(0, 8))
        self.status_output_pill = ctk.CTkLabel(
            out_row, text="Output: UNKNOWN", width=120, height=24,
            corner_radius=6, fg_color=C["amber"],
            text_color="#111827",
            font=ctk.CTkFont(size=11, weight="bold"))
        self.status_output_pill.pack(side="left")
        ctk.CTkLabel(
            info_card, text="Control Mode",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            anchor="w", padx=10, pady=(0, 5))
        self.status_mode_row = ctk.CTkFrame(info_card, fg_color="transparent")
        mode_row = self.status_mode_row
        mode_row.pack(fill="x", padx=10, pady=(0, 8))
        self.status_mode_labels = {}
        for mode, label in (("VOLT", "Volt"), ("CURR", "Curr")):
            pill = ctk.CTkLabel(
                mode_row, text=label, width=56, height=24,
                corner_radius=6, fg_color=C["card"],
                font=ctk.CTkFont(size=11, weight="bold"))
            pill.pack(side="left", padx=(0, 6))
            self.status_mode_labels[mode] = pill

    # -- Plot rendering ------------------------------------------------------
    # Both preview and status plots share the same renderer so waveform and
    # empty-state behavior stay consistent.
    def _build_plot(self, parent, figsize):
        fig = Figure(figsize=figsize, dpi=100, facecolor=C["graph_bg"])
        ax = fig.add_subplot(111)
        self._style_ax(ax)
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.configure(
            width=int(figsize[0] * 100),
            height=int(figsize[1] * 100))
        canvas_widget.pack(fill="both", expand=True, padx=4, pady=4)
        return fig, ax, canvas

    def _style_ax(self, ax):
        ax.set_facecolor(C["graph_bg"])
        for spine in ax.spines.values():
            spine.set_color(C["border"])
        ax.tick_params(colors=C["text2"], labelsize=8, pad=2)
        ax.xaxis.label.set_color(C["text2"])
        ax.yaxis.label.set_color(C["text2"])
        ax.grid(True, color="#2a2a40", linewidth=0.5, alpha=0.6)

    def _draw_waveform_plot(self, fig, ax, canvas, points=None,
                            empty_title="No waveform uploaded"):
        ax.clear()
        self._style_ax(ax)
        ax.set_xlabel("Sample Index", fontsize=9, labelpad=3)
        ax.set_ylabel("Amplitude (V / A)", fontsize=9, labelpad=3)

        if not points:
            ax.set_title(empty_title, color=C["text2"], fontsize=9, pad=4)
            fig.tight_layout(pad=0.7)
            canvas.draw_idle()
            return

        ax.plot(
            range(len(points)), points,
            color=C["waveform"], linewidth=1.3)
        ax.set_title(
            f"Waveform - {len(points)} points",
            color=C["text"], fontsize=9, pad=4)
        fig.tight_layout(pad=0.7)
        canvas.draw_idle()

    def _update_preview_plot(self, points=None):
        self._draw_waveform_plot(
            self.preview_fig, self.preview_ax, self.preview_canvas,
            points=points, empty_title="No waveform - configure and preview")

    def _update_status_plot(
            self, points=None, empty_title="No waveform uploaded"):
        self._draw_waveform_plot(
            self.status_fig, self.status_ax, self.status_canvas,
            points=points, empty_title=empty_title)

    @staticmethod
    def _lbl(parent, text):
        label = ctk.CTkLabel(
            parent, text=text, text_color=C["text2"],
            font=ctk.CTkFont(size=9), justify="left", wraplength=155)
        label.pack(fill="x", padx=7, pady=(2, 0))
        return label

    # -- Session logging and readback collection -----------------------------
    # All diagnostics are retained in logs/*.log.  The bottom event panel is
    # deliberately quieter: routine controller traffic, including hardware
    # writes and polling, stays file-only while errors remain visible.
    def _init_log_file(self):
        try:
            log_dir = os.path.join(os.getcwd(), "logs")
            os.makedirs(log_dir, exist_ok=True)
            stamp = time.strftime("%Y-%m-%d_%H%M%S")
            self.log_file_path = os.path.join(
                log_dir, f"kepco_dashboard_date_{stamp}.log")
            self.log_file_handle = open(
                self.log_file_path, "a", encoding="utf-8")
            self.log_file_handle.write(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [INFO] Log started\n")
            self.log_file_handle.flush()
        except Exception:
            self.log_file_handle = None
            self.log_file_path = ""

    @staticmethod
    def _log_level(tag):
        levels = {
            "ok": "INFO",
            "info": "INFO",
            "warn": "WARNING",
            "err": "ERROR",
            "critical": "CRITICAL ERROR",
        }
        return levels.get(str(tag).lower(), "INFO")

    def _write_log_file_line(self, ts, tag, msg):
        if not self.log_file_handle:
            return
        try:
            self.log_file_handle.write(
                f"[{ts}] [{self._log_level(tag)}] {msg}\n")
            self.log_file_handle.flush()
        except Exception:
            self.log_file_handle = None

    def _close_log_file(self):
        if not self.log_file_handle:
            return
        try:
            self.log_file_handle.write(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [INFO] Log closed\n")
            self.log_file_handle.flush()
            self.log_file_handle.close()
        except Exception:
            pass
        finally:
            self.log_file_handle = None

    def _toggle_data_collection(self):
        if self._data_collection_switch_updating:
            return

        if self.data_collection_switch.get():
            if not self._start_data_collection():
                self._set_data_collection_switch(False)
        else:
            self._stop_data_collection()

    def _set_data_collection_switch(self, selected):
        if not hasattr(self, "data_collection_switch"):
            return
        self._data_collection_switch_updating = True
        try:
            if selected:
                self.data_collection_switch.select()
            else:
                self.data_collection_switch.deselect()
        finally:
            self._data_collection_switch_updating = False

    def _start_data_collection(self):
        if self.data_collection_file_handle:
            self.data_collection_enabled = True
            return True

        try:
            log_dir = os.path.join(os.getcwd(), "logs")
            os.makedirs(log_dir, exist_ok=True)
            stamp = time.strftime("%Y-%m-%d_%H%M%S")
            self.data_collection_file_path = os.path.join(
                log_dir, f"kepco_readback_collection_date_{stamp}.csv")
            self.data_collection_file_handle = open(
                self.data_collection_file_path, "a",
                encoding="utf-8", newline="")
            self.data_collection_writer = csv.writer(
                self.data_collection_file_handle)
            self.data_collection_writer.writerow([
                "timestamp",
                "elapsed_s",
                "readback_voltage_v",
                "readback_current_a",
                "derived_resistance_ohm",
                "output_state",
                "mode",
            ])
            self.data_collection_file_handle.flush()
            self.data_collection_started_at = time.time()
            self.data_collection_enabled = True
            self.log(
                f"Data collection enabled: {self.data_collection_file_path}",
                "ok")
            return True
        except Exception as exc:
            self.data_collection_enabled = False
            self._close_data_collection_file()
            self.log(f"Data collection failed: {exc}", "err")
            messagebox.showerror(
                "Data Collection",
                f"Could not start data collection.\n{exc}")
            return False

    def _stop_data_collection(self, log_message=True):
        was_enabled = (
            self.data_collection_enabled
            or self.data_collection_file_handle is not None
        )
        path = self.data_collection_file_path
        self.data_collection_enabled = False
        self._close_data_collection_file()
        if log_message and was_enabled and path:
            self.log(f"Data collection disabled: {path}", "info")

    def _close_data_collection_file(self):
        try:
            if self.data_collection_file_handle:
                self.data_collection_file_handle.flush()
                self.data_collection_file_handle.close()
        except Exception:
            pass
        finally:
            self.data_collection_file_handle = None
            self.data_collection_file_path = ""
            self.data_collection_writer = None
            self.data_collection_started_at = None

    def _record_data_collection_sample(self, v, c, outp, mode):
        if (
            not self.data_collection_enabled
            or not self.data_collection_writer
            or not self.data_collection_file_handle
        ):
            return

        try:
            now = time.time()
            started_at = self.data_collection_started_at or now
            voltage = self._as_float(v)
            current = self._as_float(c)
            resistance = derive_readback_resistance(voltage, current)
            output_text = str(outp).strip().upper()
            if output_text in ("1", "ON"):
                output_text = "ON"
            elif output_text in ("0", "OFF"):
                output_text = "OFF"

            mode_text = str(mode).strip().upper()
            if mode_text == "0":
                mode_text = "VOLT"
            elif mode_text == "1":
                mode_text = "CURR"

            self.data_collection_writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                f"{now - started_at:.3f}",
                voltage if voltage is not None else str(v).strip(),
                current if current is not None else str(c).strip(),
                resistance if resistance is not None else "",
                output_text,
                mode_text,
            ])
            self.data_collection_file_handle.flush()
        except Exception as exc:
            self.data_collection_enabled = False
            self._close_data_collection_file()
            self._set_data_collection_switch(False)
            self.log(f"Data collection stopped: {exc}", "err")

    def _start_solenoid_temperature_polling(self):
        self._poll_solenoid_temperatures()

    def _schedule_solenoid_temperature_poll(self, delay_ms=SOLENOID_TEMPERATURE_POLL_MS):
        if self._ui_shutdown:
            return
        if self._solenoid_temperature_poll_timer:
            try:
                self.root.after_cancel(self._solenoid_temperature_poll_timer)
            except Exception:
                pass
            self._solenoid_temperature_poll_timer = None
        self._solenoid_temperature_poll_timer = self.root.after(
            delay_ms, self._poll_solenoid_temperatures)

    def _stop_solenoid_temperature_polling(self):
        if self._solenoid_temperature_poll_timer:
            try:
                self.root.after_cancel(self._solenoid_temperature_poll_timer)
            except Exception:
                pass
            self._solenoid_temperature_poll_timer = None

    def _poll_solenoid_temperatures(self):
        self._solenoid_temperature_poll_timer = None
        if self._ui_shutdown:
            return

        snapshot = self.solenoid_temperature_reader.read_latest()
        self.solenoid_temperature_error = snapshot.error
        self.solenoid_temperature_source_path = snapshot.source_path
        self.solenoid_temperature_timestamp = snapshot.timestamp
        self._note_datalog_timestamp(snapshot.timestamp)

        if snapshot.error:
            self.solenoid_temperatures = {"1": None, "2": None}
            self._update_solenoid_temperature_display()
            self._refresh_datalog_console_lines()
            if snapshot.error != self._last_solenoid_temperature_error_logged:
                self.log(
                    f"Solenoid temperature read unavailable: {snapshot.error}",
                    "warn")
                self._last_solenoid_temperature_error_logged = snapshot.error
            self._schedule_solenoid_temperature_poll()
            return

        self.solenoid_temperatures = {
            "1": snapshot.solenoid_1,
            "2": snapshot.solenoid_2,
        }
        self._update_solenoid_temperature_display()
        self._refresh_datalog_console_lines()

        if (
            snapshot.source_path
            and snapshot.source_path != self._last_solenoid_temperature_source_logged
        ):
            self.log(
                f"Reading solenoid temperatures from {snapshot.source_path}",
                "info")
            self._last_solenoid_temperature_source_logged = snapshot.source_path

        if self._last_solenoid_temperature_error_logged is not None:
            self.log("Solenoid temperature read recovered.", "ok")
            self._last_solenoid_temperature_error_logged = None

        self._schedule_solenoid_temperature_poll()

    # -- UI thread dispatcher ------------------------------------------------
    # Tk/customtkinter widgets are not thread-safe. Background workers enqueue
    # closures with _call_on_ui(), and this dispatcher drains them on the main
    # loop at a modest cadence.
    def _start_ui_dispatcher(self):
        if self._ui_shutdown or self._ui_queue_job is not None:
            return
        self._ui_queue_job = self.root.after(20, self._drain_ui_queue)

    def _drain_ui_queue(self):
        self._ui_queue_job = None
        if self._ui_shutdown:
            return
        while True:
            try:
                callback = self._ui_queue.get_nowait()
            except queue.Empty:
                break
            callback()
        self._start_ui_dispatcher()

    def _stop_ui_dispatcher(self):
        self._ui_shutdown = True
        if self._ui_queue_job:
            try:
                self.root.after_cancel(self._ui_queue_job)
            except Exception:
                pass
            self._ui_queue_job = None
        while True:
            try:
                self._ui_queue.get_nowait()
            except queue.Empty:
                break

    def _call_on_ui(self, callback):
        if self._ui_shutdown:
            return False
        if threading.current_thread() is threading.main_thread():
            callback()
            return True
        self._ui_queue.put(callback)
        return True

    def log(self, msg, tag="info", visible=None):
        """Persist every entry; render only operator-relevant event entries."""
        ts = time.strftime("%H:%M:%S")
        level = self._log_level(tag)
        if visible is None:
            # Plain INFO is diagnostic/application chatter.  Successful
            # actions, warnings, and errors are operator-facing events.
            visible = str(tag).lower() in ("ok", "warn", "err", "critical")
        if visible:
            self.log_text.insert("end", f"[{ts}] [{level}] {msg}\n")
            self.log_text.see("end")
        self._write_log_file_line(ts, tag, msg)

    def _controller_debug_log(self, level, msg):
        tag = "info"
        if level == "ok":
            tag = "ok"
        elif level == "warn":
            tag = "warn"
        elif level == "err":
            tag = "err"

        # TX/RX/polling traffic is still written to the session file, but it
        # must not displace operator events in the visible event panel.
        visible = (
            tag in ("err", "critical")
            or "existing BIT system error" in msg)
        if threading.current_thread() is threading.main_thread():
            self.log(f"[COMM] {msg}", tag, visible=visible)
        else:
            self._call_on_ui(
                lambda: self.log(f"[COMM] {msg}", tag, visible=visible))

    def _log_safe(self, msg, tag="info"):
        self._call_on_ui(lambda: self.log(msg, tag))

    # -- Connection, reset, and status mirroring -----------------------------
    # These helpers keep the app's local flags, status panel, and output button
    # synchronized whenever the device connects, drops, resets, or polls.
    def _set_connected_state(self, idn=""):
        """Render the controller's current transport trust state."""
        state = self.kepco.comm_state
        if self.kepco.is_verified:
            self.conn_btn.configure(
                text="Disconnect", fg_color=C["red"], hover_color="#dc2626")
            self.status_lbl.configure(text="Connected", text_color=C["green"])
            self.idn_lbl.configure(text=idn)
        else:
            reconnectable = state is CommState.DEGRADED and self.kepco.is_transport_connected
            self.conn_btn.configure(
                text="Recover" if reconnectable else "Connect",
                fg_color=C["primary"], hover_color=C["primary_h"])
            state_text = {
                CommState.CONNECTING: "Connecting",
                CommState.VERIFYING: "Connected — Verifying Device State",
                CommState.DEGRADED: "Communication degraded",
                CommState.FAULTED: "Communication faulted",
            }.get(state, "Disconnected")
            state_color = C["amber"] if state in (
                CommState.CONNECTING, CommState.VERIFYING,
                CommState.DEGRADED) else C["red"]
            self.status_lbl.configure(text=state_text, text_color=state_color)
            self.idn_lbl.configure(text=idn if self.kepco.is_transport_connected else "")
        self.scan_btn.configure(
            state=(
                "disabled"
                if self.kepco.is_transport_connected or self._connect_in_flight
                else "normal"))
        self._update_output_controls()

    def _handle_comm_failure(self, context):
        if self.kepco.is_verified:
            return
        self._stop_status_polling()
        self._connect_in_flight = False
        self._upload_in_flight = False
        self._output_toggle_in_flight = False
        self._manual_operation_in_flight = False
        self._manual_operation_label = ""
        self._set_connected_state()
        self._reset_live_status(output_state=None)
        self._reset_uploaded_state()
        self.log(
            f"Connection lost during {context}: {self.kepco.last_error}",
            "critical")

    def _resume_or_handle_transaction_failure(self, context, delay_ms=100):
        """Resume polling after a command rejection; lock only on comm loss."""
        if self.kepco.is_verified:
            self._resume_status_polling(delay_ms)
        else:
            self._handle_comm_failure(context)

    def _reset_live_status(self, output_state: bool | None = None,
                           control_mode=None):
        """Clear live readbacks without claiming an unverified output is OFF."""
        self._measurement_guard = None
        self.current_control_mode = control_mode or "VOLT"
        self.status_meas_volt_lbl.configure(text="Voltage:  ---.----  V")
        self.status_meas_curr_lbl.configure(text="Current:  ---.----  A")
        self.status_meas_resistance_lbl.configure(
            text="Resistance:  ---.----  \N{OHM SIGN}")
        self._set_dc_monitors_inactive()
        self._set_status_mode_display(None)
        self.control_mode_var.set(self.current_control_mode)
        if hasattr(self, "mode_buttons"):
            self._update_mode_buttons(self.current_control_mode)
        self._set_output_ui_state(output_state)
        self._refresh_ac_operation_notice()

    def _set_unverified_upload_state(
            self,
            device_state,
            progress_text,
            plot_title):
        """Clear actionable upload state and explain why output is locked."""
        self.uploaded_request = None
        self.uploaded_waveform_ready = False
        self._update_status_plot(None, empty_title=plot_title)
        for label in self.status_cfg_labels.values():
            label.configure(text="--")
        self.status_cfg_labels["device_state"].configure(text=device_state)
        self.prog_lbl.configure(text=progress_text)
        self.progress.set(0)
        self._set_dc_monitors_inactive()
        self._refresh_ac_operation_notice()
        self._update_output_controls()

    def _reset_uploaded_state(self):
        self._set_unverified_upload_state(
            device_state="No waveform uploaded",
            progress_text="No upload yet",
            plot_title="No waveform uploaded")

    def _refresh_uploaded_status_panel(self):
        """Mirror the staged waveform request into the right-side status card."""
        req = self.uploaded_request
        if not req:
            self._reset_uploaded_state()
            return

        unit = "V" if req["mode"] == "VOLT" else "A"
        frequency = "--" if req["wave"] == "DC" else f"{req['actual_frequency']:.4f} Hz"
        amplitude = f"{req['amplitude']:.4f} {unit}" if req["amplitude"] is not None else "--"
        offset = f"{req['offset']:.4f} {unit}" if req["offset"] is not None else "--"
        loop = "--" if req["wave"] == "DC" else ("Infinite" if req["loop"] == 0 else str(req["loop"]))
        wave_name = req["wave"]
        if req["wave"] == "CSV Custom (untested)" and req["csv_name"]:
            wave_name = f"CSV ({req['csv_name']})"

        device_state = (
            "Fixed setpoint staged and verified"
            if req["wave"] == "DC" else "LIST uploaded and verified")

        values = {
            "wave": wave_name,
            "mode": "Volt" if req["mode"] == "VOLT" else "Curr",
            "frequency": frequency,
            "amplitude": amplitude,
            "offset": offset,
            "points": str(req["point_count"]),
            "loop": loop,
            "device_state": device_state,
        }
        for key, value in values.items():
            self.status_cfg_labels[key].configure(text=value)
        self._update_status_plot(req["plot_points"])

    def _set_output_ui_state(self, is_on: bool | None):
        self.current_output_on = is_on
        self._refresh_output_toggle_button()
        self._set_status_output_display(is_on)
        if not is_on:
            self._set_dc_monitors_inactive()
        self._refresh_ac_operation_notice(is_on)

    def _set_status_output_display(self, is_on):
        if is_on is None:
            self.status_output_pill.configure(
                text="Output: UNKNOWN", fg_color=C["amber"], text_color="#111827")
            self._set_live_console_line(
                "output",
                "Output: UNKNOWN — Verify the KEPCO before interacting with the load.",
                C["amber"])
            return
        self.status_output_pill.configure(
            text="Output: ON" if is_on else "Output: OFF",
            fg_color=C["green"] if is_on else C["red"],
            text_color="#ffffff")
        self._set_live_console_line("output", "", C["amber"], visible=False)

    # -- Output button rendering --------------------------------------------
    # The output button has several logical locks: disconnected, unknown output
    # state, no waveform, waveform upload, in-flight transition, and ready/armed.
    @staticmethod
    def _output_toggle_allowed(
            connected,
            uploaded_waveform_ready,
            current_output_on,
            upload_in_flight,
            output_toggle_in_flight):
        return (
            bool(connected)
            and isinstance(current_output_on, bool)
            and not upload_in_flight
            and not output_toggle_in_flight
            and (
                uploaded_waveform_ready
                or current_output_on
            )
        )

    def _uploaded_waveform_matches_selected_mode(self):
        return bool(
            self.uploaded_waveform_ready
            and self.uploaded_request
            and self.uploaded_request.get("mode")
            == self.control_mode_var.get().upper())

    def _can_toggle_output(self):
        return (
            not getattr(self, "_manual_operation_in_flight", False)
            and self._output_toggle_allowed(
                self.kepco.is_verified,
                self._uploaded_waveform_matches_selected_mode(),
                self.current_output_on,
                self._upload_in_flight,
                self._output_toggle_in_flight)
        )

    def _refresh_output_toggle_button(self, can_toggle=None):
        if can_toggle is None:
            can_toggle = self._can_toggle_output()
        uploaded_for_selected_mode = (
            self._uploaded_waveform_matches_selected_mode())

        badge_text = "OFFLINE"
        badge_color = C["red"]
        badge_text_color = "#ffffff"
        summary = "Disconnected"
        button_text = "Connect to Arm Output"
        button_color = "#374151"
        button_hover = "#4b5563"
        button_text_color = "#e5e7eb"

        if self.kepco.is_verified:
            if self._output_toggle_in_flight:
                badge_text = "APPLYING"
                badge_color = C["amber"]
                badge_text_color = "#111827"
                summary = "Applying output change"
                button_text = "Applying..."
                button_color = "#475569"
                button_hover = "#475569"
            elif self.current_output_on is None:
                badge_text = "UNKNOWN"
                badge_color = C["amber"]
                badge_text_color = "#111827"
                summary = "Waiting for verified output state"
                button_text = "Output State Unknown"
                button_color = "#475569"
                button_hover = "#475569"
            elif self.current_output_on:
                badge_text = "LIVE"
                badge_color = C["green"]
                summary = "Output enabled"
                button_text = "Disable Output"
                button_color = C["red"]
                button_hover = "#dc2626"
            elif not uploaded_for_selected_mode:
                badge_text = "LOCKED"
                badge_color = C["amber"]
                badge_text_color = "#111827"
                summary = "Awaiting waveform upload"
                button_text = "Upload Waveform First"
            elif uploaded_for_selected_mode:
                badge_text = "READY"
                badge_color = C["primary"]
                summary = "Waveform uploaded and armed"
                button_text = "Enable Output"
                button_color = C["green"]
                button_hover = "#059669"
                button_text_color = "#000000"

        self.output_state_badge.configure(
            text=badge_text,
            fg_color=badge_color,
            text_color=badge_text_color)
        self.output_summary_lbl.configure(text=summary)
        self.output_toggle_btn.configure(
            text=button_text,
            fg_color=button_color,
            hover_color=button_hover,
            text_color=button_text_color,
            state="normal" if can_toggle else "disabled")

    def _update_output_controls(self):
        upload_state = "normal" if (
            self.kepco.is_verified
            and isinstance(self.current_output_on, bool)
            and not self._upload_in_flight
            and not getattr(
                self, "_manual_operation_in_flight", False)) else "disabled"
        self.upload_btn.configure(state=upload_state)

        can_toggle = self._can_toggle_output()
        self._refresh_output_toggle_button(can_toggle)

        if not self.kepco.is_verified:
            hint = (
                "Device state is unverified; output state is unknown."
                if self.kepco.comm_state in (CommState.DEGRADED, CommState.FAULTED)
                else "Verify communication with a Kepco before controlling output."
            )
        elif getattr(self, "_manual_operation_in_flight", False):
            hint = "Manual device operation in progress..."
        elif self._output_toggle_in_flight:
            hint = "Applying output change..."
        elif self.current_output_on is None:
            hint = "Output state is unknown; waiting for verified device status."
        elif (
                self.current_output_on
                and not self._uploaded_waveform_matches_selected_mode()):
            hint = "Output is ON; disable output before uploading a waveform."
        elif not self._uploaded_waveform_matches_selected_mode():
            hint = "Upload a waveform to enable output."
        else:
            hint = "Output follows the last uploaded waveform."
        self.output_hint_lbl.configure(text=hint)
        self._update_manual_device_controls()

    def _primary_control_operation_active(self):
        """Return True while a non-manual workflow owns device semantics."""
        return any((
            getattr(self, "_scan_in_flight", False),
            getattr(self, "_connect_in_flight", False),
            getattr(self, "_upload_in_flight", False),
            getattr(self, "_output_toggle_in_flight", False),
        ))

    def _update_manual_device_controls(self):
        """Disable manual device actions while any control workflow is active."""
        busy = (
            self._primary_control_operation_active()
            or getattr(self, "_manual_operation_in_flight", False))
        state = "disabled" if busy else "normal"
        for widget in getattr(self, "_manual_device_controls", ()):
            try:
                widget.configure(state=state)
            except Exception:
                pass

    def _update_mode_buttons(self, active_mode):
        for mode, btn in self.mode_buttons.items():
            active = mode == active_mode
            btn.configure(
                fg_color=C["green"] if active else "#4b5563",
                hover_color="#059669" if active else "#6b7280")

    def _invalidate_upload_after_control_mode_change(
            self, previous_mode, selected_mode):
        """Clear a staged request that belongs to the opposite control mode."""
        previous_mode = str(previous_mode).strip().upper()
        selected_mode = str(selected_mode).strip().upper()
        if previous_mode == selected_mode:
            return False

        had_staged_request = bool(
            self.uploaded_waveform_ready or self.uploaded_request)
        # Set the safety state explicitly before refreshing dependent widgets.
        self.uploaded_waveform_ready = False
        self.uploaded_request = None
        self._reset_uploaded_state()
        if had_staged_request:
            self.log(
                f"Control mode changed {previous_mode} -> {selected_mode}; "
                f"the previous waveform was cleared. Upload a new "
                f"{selected_mode} waveform before enabling output.",
                "warn")
        return had_staged_request

    def _set_status_mode_display(self, mode):
        for key, label in self.status_mode_labels.items():
            active = key == mode
            label.configure(
                fg_color=C["green"] if active else C["card"],
                text_color="#ffffff" if active else C["text"])

    def _is_ac_operation_active(self, is_on=None):
        req = self.uploaded_request or {}
        output_on = self.current_output_on if is_on is None else bool(is_on)
        return bool(
            self.kepco.is_verified
            and output_on
            and req.get("kind") == "LIST"
            and req.get("wave") not in ("", None, "DC")
        )

    def _refresh_ac_operation_notice(self, is_on=None):
        if not hasattr(self, "status_ac_invalid_lbl"):
            return
        visible = self._is_ac_operation_active(is_on)
        if visible:
            self.status_meas_volt_lbl.configure(text="Voltage:  ---.----  V")
            self.status_meas_curr_lbl.configure(text="Current:  ---.----  A")
            self.status_meas_resistance_lbl.configure(
                text="Resistance:  ---.----  \N{OHM SIGN}")
        if visible and not self._ac_invalid_label_visible:
            self._ac_invalid_label_visible = True
            self._place_ac_operation_notice()
        elif not visible and self._ac_invalid_label_visible:
            self.status_ac_invalid_lbl.pack_forget()
            self._ac_invalid_label_visible = False

    def _set_live_console_line(self, key, text, color, visible=True):
        if not hasattr(self, "status_live_console_labels"):
            return
        label = self.status_live_console_labels.get(key)
        if label is None:
            return
        if visible:
            try:
                label.pack_info()
                packed = True
            except Exception:
                packed = False
            if not packed:
                label.pack(
                    fill="x", padx=6,
                    pady=(3 if key in ("output", "voltage") else 0, 3))
            label.configure(text=text, text_color=color)
        else:
            label.pack_forget()

    def _refresh_datalog_console_lines(self):
        if self.solenoid_temperature_error:
            self._set_live_console_line(
                "datalog", "Data Log file not found or invalid", C["amber"])
            self._set_live_console_line(
                "stale", "Warning: stale values from Data Log",
                C["amber"], visible=False)
            return

        self._set_live_console_line("datalog", "Data Log file valid", C["green"])
        stale_age = self._datalog_stale_age_seconds()
        stale = stale_age is not None and stale_age > DATALOG_STALE_SECONDS
        self._set_live_console_line(
            "stale",
            "Warning: stale values from Data Log",
            C["amber"],
            visible=stale)

    def _set_dc_monitors_inactive(self):
        self._set_live_console_line(
            "voltage", "Voltage monitor inactive", C["text2"])
        self._set_live_console_line(
            "current", "Current monitor inactive", C["text2"])
        self.dc_monitor_state = {"voltage": "inactive", "current": "inactive"}
        self._refresh_datalog_console_lines()

    def _read_monitor_thresholds(self):
        return self.vmon_threshold_pct, self.imon_threshold_pct

    def _set_monitor_threshold(self, channel):
        channel = (channel or "").upper()
        if channel == "VOLT":
            entry = self.vmon_threshold_entry
            display = self.vmon_threshold_display
            name = "Voltage"
        elif channel == "CURR":
            entry = self.imon_threshold_entry
            display = self.imon_threshold_display
            name = "Current"
        else:
            raise ValueError(f"Unsupported monitor threshold '{channel}'")

        threshold_pct = self._as_float(entry.get())
        if threshold_pct is None or threshold_pct <= 0:
            messagebox.showerror(
                "Invalid Monitor Threshold",
                f"Please enter a positive percentage value for the "
                f"{name.lower()} threshold.")
            return

        if channel == "VOLT":
            self.vmon_threshold_pct = threshold_pct
        else:
            self.imon_threshold_pct = threshold_pct
        self._normalize_limit_entry_text(entry, threshold_pct)
        display.configure(
            text=self._format_symmetric_display(threshold_pct))

    def _update_dc_monitors(self, voltage, current, is_on, mode_text):
        req = self.uploaded_request or {}
        if not dc_monitor_is_active(
                self.kepco.is_verified, is_on, mode_text, req):
            self._set_dc_monitors_inactive()
            return

        voltage_threshold_pct, current_threshold_pct = self._read_monitor_thresholds()
        stale_age = self._datalog_stale_age_seconds()
        temperatures_fresh = bool(
            not self.solenoid_temperature_error
            and stale_age is not None
            and stale_age <= DATALOG_STALE_SECONDS)
        results = evaluate_dc_monitor(
            mode_text,
            req.get("amplitude"),
            voltage,
            current,
            self.solenoid_temperatures.get("1"),
            self.solenoid_temperatures.get("2"),
            temperatures_fresh,
            voltage_threshold_pct,
            current_threshold_pct,
        )

        for channel, label in (("voltage", "Voltage"), ("current", "Current")):
            result = results[channel]
            status = result["status"]
            if status == "unavailable":
                unavailable_text = (
                    f"Cannot compute expected {channel}"
                    if result["predicted"]
                    else f"Cannot compare {channel} setpoint")
                self._set_live_console_line(
                    channel, unavailable_text, C["amber"])
                self.dc_monitor_state[channel] = "unavailable"
                continue

            if status == "ok":
                text = f"{label} within expected range"
                color = C["green"]
            elif result["predicted"]:
                unit = "V" if channel == "voltage" else "A"
                text = (
                    f"{label} outside expected range "
                    f"(expected {result['expected']:.3f} {unit})")
                color = C["red"]
            else:
                text = f"{label} outside expected range"
                color = C["red"]
            self._set_live_console_line(
                channel, text, color)
            self.dc_monitor_state[channel] = status
        self._refresh_datalog_console_lines()

    @staticmethod
    def _as_float(value):
        try:
            number = float(value)
        except Exception:
            return None
        return number if math.isfinite(number) else None

    def _format_measurement_value(self, value):
        numeric = self._as_float(value)
        return f"{numeric:.4f}" if numeric is not None else "---.----"

    def _format_resistance_value(self, voltage, current):
        resistance = derive_readback_resistance(voltage, current)
        return f"{resistance:.4f}" if resistance is not None else "---.----"

    def _format_temperature_value(self, value):
        numeric = self._as_float(value)
        return f"{numeric:.1f}" if numeric is not None else "--.-"

    def _update_solenoid_temperature_display(self):
        if not hasattr(self, "status_solenoid_temp_1_lbl"):
            return
        temp_1 = self._format_temperature_value(self.solenoid_temperatures.get("1"))
        temp_2 = self._format_temperature_value(self.solenoid_temperatures.get("2"))
        self.status_solenoid_temp_1_lbl.configure(
            text=f"Solenoid 1:  {temp_1}  \N{DEGREE SIGN}C")
        self.status_solenoid_temp_2_lbl.configure(
            text=f"Solenoid 2:  {temp_2}  \N{DEGREE SIGN}C")

    def _note_datalog_timestamp(self, timestamp):
        if not timestamp:
            return
        if timestamp != self._last_unique_datalog_timestamp:
            self._last_unique_datalog_timestamp = timestamp
            self._last_unique_datalog_seen_at = time.time()

    @staticmethod
    def _timestamp_to_epoch(timestamp):
        text = str(timestamp or "").strip()
        if not text:
            return None

        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"

        try:
            parsed = datetime.datetime.fromisoformat(text)
        except ValueError:
            return None

        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return time.mktime(parsed.timetuple()) + parsed.microsecond / 1_000_000

        return parsed.timestamp()

    def _datalog_stale_age_seconds(self):
        timestamp = self.solenoid_temperature_timestamp
        if not timestamp:
            return None

        epoch = self._timestamp_to_epoch(timestamp)
        if epoch is not None:
            return max(0.0, time.time() - epoch)

        self._note_datalog_timestamp(timestamp)
        if self._last_unique_datalog_seen_at is None:
            return None
        return max(0.0, time.time() - self._last_unique_datalog_seen_at)

    def _set_live_measurement_axis(self, mode, value):
        if self._is_ac_operation_active():
            text = "---.----"
        else:
            text = self._format_measurement_value(value)
        if mode == "VOLT":
            self.status_meas_volt_lbl.configure(text=f"Voltage:  {text}  V")
        elif mode == "CURR":
            self.status_meas_curr_lbl.configure(text=f"Current:  {text}  A")

    def _set_entry_enabled(self, entry, enabled):
        entry.configure(
            state="normal" if enabled else "disabled",
            fg_color=C["input_bg"] if enabled else "#2d2d3a",
            text_color=C["text"] if enabled else C["text2"])

    # -- Waveform input and request assembly ---------------------------------
    # All waveform types are normalized into the same request dictionary. Later
    # upload/output code should read from that request instead of re-reading UI
    # widgets, except when deliberately re-validating current software limits.
    def _on_wave_change(self, _=None):
        wave = self.wave_var.get()
        if wave == "CSV Custom (untested)":
            self.csv_frame.pack(fill="x", padx=7, pady=(0, 3), after=self.wave_combo)
        else:
            self.csv_frame.pack_forget()

        if wave == "DC":
            self.amp_label.configure(text="Setpoint (V / A)")
            self._set_entry_enabled(self.freq_entry, False)
            self._set_entry_enabled(self.off_entry, False)
            self._set_entry_enabled(self.pts_entry, False)
            self._set_entry_enabled(self.loop_entry, False)
            self._set_entry_enabled(self.amp_entry, True)
            self.timing_lbl.configure(
                text="DC uses a fixed VOLT/CURR setpoint and does not use LIST.")
        elif wave == "CSV Custom (untested)":
            self.amp_label.configure(text="Amplitude (from CSV)")
            self._set_entry_enabled(self.freq_entry, True)
            self._set_entry_enabled(self.off_entry, False)
            self._set_entry_enabled(self.pts_entry, False)
            self._set_entry_enabled(self.loop_entry, True)
            self._set_entry_enabled(self.amp_entry, False)
            if self.csv_points:
                self.pts_entry.configure(state="normal")
                self.pts_entry.delete(0, "end")
                self.pts_entry.insert(0, str(len(self.csv_points)))
                self._set_entry_enabled(self.pts_entry, False)
            self.timing_lbl.configure(
                text="CSV uses the loaded file values and the selected frequency.")
        else:
            self.amp_label.configure(text="Amplitude (V / A)")
            self._set_entry_enabled(self.freq_entry, True)
            self._set_entry_enabled(self.off_entry, True)
            self._set_entry_enabled(self.pts_entry, True)
            self._set_entry_enabled(self.loop_entry, True)
            self._set_entry_enabled(self.amp_entry, True)
            self.timing_lbl.configure(text="")

    def _load_csv(self):
        path = filedialog.askopenfilename(
            filetypes=[("CSV", "*.csv"), ("All", "*.*")])
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8-sig") as handle:
                points = [
                    float(value)
                    for row in csv.reader(handle)
                    for value in row
                    if value.strip()
            ]
            if len(points) < 2:
                raise ValueError("CSV must contain at least 2 numeric points.")
            if len(points) > MAX_LIST_POINTS:
                raise ValueError(
                    f"CSV contains {len(points)} points; the dashboard supports "
                    f"a maximum of {MAX_LIST_POINTS} points in one LIST. "
                    "Shorten the CSV before loading it.")
            self.csv_points = points
            self.csv_name = os.path.basename(path)
            self.csv_lbl.configure(text=f"{self.csv_name} ({len(points)} pts)")
            self.pts_entry.configure(state="normal")
            self.pts_entry.delete(0, "end")
            self.pts_entry.insert(0, str(len(points)))
            self._set_entry_enabled(self.pts_entry, False)
            self.log(f"Loaded CSV: {self.csv_name} -> {len(points)} points", "ok")
        except Exception as exc:
            messagebox.showerror("CSV Error", str(exc))

    def _read_float(self, entry, name):
        try:
            return float(entry.get().strip())
        except Exception:
            messagebox.showerror("Input Error", f"Invalid {name}.")
            return None

    def _read_int(self, entry, name):
        try:
            return int(entry.get().strip())
        except Exception:
            messagebox.showerror("Input Error", f"Invalid {name}.")
            return None

    def _normalize_limit_entry_text(self, entry, value):
        if threading.current_thread() is not threading.main_thread():
            return
        entry.delete(0, "end")
        entry.insert(0, KepcoController.format_scpi_value(value))

    @staticmethod
    def _format_symmetric_display(value):
        return f"+/-{float(value)}"

    def _read_absolute_limit(self, entry, name, maximum):
        limit = abs(float(entry.get().strip()))
        if not math.isfinite(limit) or limit <= 0:
            raise ValueError(f"{name} must be a finite value greater than zero.")
        if limit > maximum:
            raise ValueError(f"{name} must not exceed {maximum:.1f}.")
        self._normalize_limit_entry_text(entry, limit)
        return limit

    def _read_limit_candidate(self, mode, show_error=False):
        mode = (mode or "").upper()
        if mode not in ("VOLT", "CURR"):
            raise ValueError(f"Unsupported limit channel '{mode}'")
        entry = (
            self.soft_volt_limit_entry
            if mode == "VOLT" else self.soft_curr_limit_entry)
        name = "Voltage limit" if mode == "VOLT" else "Current limit"
        maximum = BOP_MAX_VOLTAGE if mode == "VOLT" else BOP_MAX_CURRENT
        try:
            return self._read_absolute_limit(entry, name, maximum)
        except Exception as exc:
            if show_error:
                messagebox.showerror(
                    "Device Limits",
                    str(exc) if str(exc) else
                    f"{name} must be a valid absolute value.")
            return None

    def _commit_dashboard_limit(self, mode, value):
        """Store one accepted limit and refresh its read-only display box."""
        mode = (mode or "").upper()
        if mode not in ("VOLT", "CURR"):
            raise ValueError(f"Unsupported limit channel '{mode}'")
        value = KepcoController.absolute_limit(
            value,
            DEFAULT_VOLTAGE_LIMIT if mode == "VOLT" else DEFAULT_CURRENT_LIMIT)
        self._dashboard_limits[mode] = value
        display_name = (
            "soft_volt_limit_display"
            if mode == "VOLT" else "soft_curr_limit_display")
        display = getattr(self, display_name, None)
        if display is not None and threading.current_thread() is threading.main_thread():
            display.configure(text=self._format_symmetric_display(value))

    def _get_request_limits(self, req):
        req = req or {}
        voltage_limit = req.get(
            "voltage_limit",
            req.get("voltage_compliance", KepcoController.default_voltage_limit()))
        current_limit = req.get(
            "current_limit", KepcoController.default_current_limit())
        return (
            KepcoController.absolute_limit(
                voltage_limit, DEFAULT_VOLTAGE_LIMIT),
            KepcoController.absolute_limit(
                current_limit, DEFAULT_CURRENT_LIMIT),
        )

    def _request_with_latest_ui_limits(self, req):
        if not req:
            return None
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return None
        context = (
            "Uploaded DC setpoint"
            if req.get("kind") == "DC" else "Uploaded waveform")
        if not self._check_points_within_limits(
                req["mode"], req["points"], context, limits):
            return None
        limited_req = dict(req)
        limited_req["voltage_limit"] = limits["VOLT"]
        limited_req["current_limit"] = limits["CURR"]
        return limited_req

    def _log_scpi_sequence(self, label, cmds):
        self._log_safe(f"{label}: {'; '.join(cmds)}", "info")

    def _drain_and_log_existing_device_errors(self, context):
        """Preserve queued BIT errors before beginning a DC transaction."""
        try:
            errors = self.kepco.drain_errors(fail_on_timeout=True)
        except ConnectionLostError as exc:
            reason = str(exc)
            self._log_safe(
                f"{context}: could not read BIT system error queue: {reason}",
                "err")
            return False, reason

        if errors is None:
            reason = self.kepco.last_error or "SYST:ERR? timed out"
            self._log_safe(
                f"{context}: could not read BIT system error queue: {reason}",
                "err")
            return False, reason

        for error in errors:
            self._log_safe(
                f"{context}: existing BIT system error: {error}", "warn")
        return True, ""

    def _capture_device_errors_after_failure(self, ok, msg, context):
        """Attach and log queued execution errors after a failed device action."""
        if ok or not self.kepco.is_verified:
            return ok, msg
        queue_clean, queue_msg = self.kepco._require_clean_device_error_queue(
            f"{context} failed-operation error check")
        if not queue_clean:
            msg = f"{msg}; {queue_msg}" if msg else queue_msg
        return ok, msg

    def _run_dc_transaction(self, operation, context, expected_mode=None,
                            expected_output=None):
        """Run one paused DC operation plus mandatory postflight checks."""
        def transaction():
            ok, msg = self._drain_and_log_existing_device_errors(
                f"{context} preflight")
            if not ok:
                return False, msg, None

            ok, msg = operation()
            if not ok:
                ok, msg = self._capture_device_errors_after_failure(
                    ok, msg, context)
                return False, msg or f"{context} command transaction failed", None

            verified, verify_msg, snapshot = self.kepco.verify_dc_postflight(
                expected_mode=expected_mode,
                expected_output=expected_output,
                label=context)
            if not verified:
                return False, verify_msg, None
            return True, msg, snapshot

        return self.kepco.run_transaction(transaction)

    def _resume_after_dc_postflight(self, snapshot):
        """Apply a verified snapshot before allowing periodic polling again."""
        if snapshot is None:
            return False
        self._apply_live_status(
            snapshot.voltage,
            snapshot.current,
            "ON" if snapshot.output_on else "OFF",
            snapshot.mode)
        self._resume_status_polling(STATUS_POLL_INTERVAL_MS)
        return True

    def _safe_prepare_output(self, mode, initial_setpoint=0.0,
                             voltage_compliance=None, current_limit=None,
                             label="Safe output prepare"):
        mode = (mode or "VOLT").upper()
        if voltage_compliance is None:
            voltage_compliance = KepcoController.default_voltage_limit()
        if current_limit is None:
            current_limit = KepcoController.default_current_limit()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported FUNC:MODE '{mode}'"

        # A previous LIST program can leave the active source armed. Query and
        # disarm only that source; sending MODE FIX to both sources can itself
        # enqueue -221.  Do not send *CLS here: the DC transaction preflight
        # already drained and logged errors, and *CLS is not part of the
        # manual's optimized fixed-output programming sequence.
        ok, msg = self.kepco.disarm_active_list_mode()
        if not ok:
            return False, f"{label} list disarm failed: {msg}"

        return self.kepco.configure_fixed_mode(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            initial_setpoint=initial_setpoint,
            label=label)

    def _ensure_dc_configuration(self, mode, voltage_compliance,
                                 current_limit, label):
        """Reuse a correct zero-staged setup; otherwise program it once."""
        ok, reason = self.kepco.verify_programmed_configuration(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            expected_setpoint=0.0,
            label=f"{label} existing-state check")
        if ok:
            return True, "Existing fixed-mode setup already verified"
        if not self.kepco.is_verified:
            return False, reason
        self._log_safe(
            f"{label}: existing setup is not reusable ({reason}); "
            "programming the manual-recommended zero/limit sequence",
            "info")
        return self._safe_prepare_output(
            mode,
            initial_setpoint=0.0,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            label=label)

    def _get_software_limits(self, show_error=False):
        return dict(self._dashboard_limits)

    def _set_software_limit(self, mode):
        mode = (mode or "").upper()
        limit = self._read_limit_candidate(mode, show_error=True)
        if limit is None:
            return
        limits = self._get_software_limits()
        limits[mode] = limit
        unit = "V" if mode == "VOLT" else "A"
        if not self.kepco.is_verified:
            self._commit_dashboard_limit(mode, limit)
            self.log(
                f"{mode} absolute limit staged locally at {limit:.4f} {unit}.",
                "warn")
            return

        expected_output = (
            self.current_output_on
            if isinstance(self.current_output_on, bool) else None)

        def operation():
            transaction_state = {
                "active_mode": None,
                "sent": False,
                "commands": [],
            }

            def limit_transaction():
                mode_resp = self.kepco.send("FUNC:MODE?", query=True)
                active = KepcoController._normalize_func_mode(mode_resp)
                transaction_state["active_mode"] = active
                if not active:
                    return False, "Could not confirm device control mode"
                complementary = (
                    (active == "CURR" and mode == "VOLT")
                    or (active == "VOLT" and mode == "CURR")
                )
                if not complementary:
                    return True, "Limit is software-only for active channel"
                command = KepcoController.complementary_limit_cmd(
                    active,
                    voltage_compliance=limits["VOLT"],
                    current_limit=limits["CURR"])
                transaction_state["commands"] = [command]
                sent, send_msg = self.kepco.apply_complementary_limit(
                    active,
                    voltage_compliance=limits["VOLT"],
                    current_limit=limits["CURR"],
                    label=f"{mode} device limit command")
                transaction_state["sent"] = sent
                return sent, send_msg

            ok, msg, snapshot = self._run_dc_transaction(
                limit_transaction,
                f"{mode} device limit change",
                expected_output=expected_output)
            return ok, msg, snapshot, transaction_state

        def completed(result, error):
            if error:
                result = (
                    False,
                    error,
                    None,
                    {"active_mode": None, "sent": False, "commands": []})
            ok, msg, snapshot, transaction_state = result
            self._set_software_limit_done(
                mode, limit, unit, ok, msg, snapshot, transaction_state)

        self._start_manual_operation(
            f"{mode} limit update", operation, completed)

    def _set_software_limit_done(
            self, mode, limit, unit, ok, msg, snapshot, transaction_state):
        """Commit a manual limit only after its worker verification completes."""
        active_mode = transaction_state["active_mode"]
        if not active_mode:
            self.log(
                "Limit command not sent; could not confirm device control mode.",
                "err")
            self._resume_or_handle_transaction_failure(
                "query control mode for limit set")
            return

        if active_mode != self.control_mode_var.get().upper():
            self.current_control_mode = active_mode
            self.control_mode_var.set(active_mode)
            self._update_mode_buttons(active_mode)

        if ok and not transaction_state["sent"]:
            self._commit_dashboard_limit(mode, limit)
            self.log(
                f"{mode} is the active output channel in {active_mode} mode; "
                "this field is staged as a UI/software limit only.",
                "warn")
            self._resume_after_dc_postflight(snapshot)
            return

        cmds = transaction_state["commands"]
        self.log(f"Sending device limit command: {'; '.join(cmds)}", "info")
        self.log(
            f"{mode} absolute limit is {limit:.4f} {unit}; "
            "complementary device limit programmed and verified"
            if ok else f"Failed to send {mode} limit command: {msg}",
            "ok" if ok else "err")
        if ok:
            self._commit_dashboard_limit(mode, limit)
            self._resume_after_dc_postflight(snapshot)
        else:
            self._resume_or_handle_transaction_failure(f"set {mode} limit")

    def _check_interlock(self, mode, points, context):
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return False
        return self._check_points_within_limits(mode, points, context, limits)

    def _check_points_within_limits(self, mode, points, context, limits):
        limit = limits[mode]
        high = max(float(point) for point in points)
        low = min(float(point) for point in points)
        unit = "V" if mode == "VOLT" else "A"
        if high > limit + 1e-12 or low < -limit - 1e-12:
            messagebox.showerror(
                "Software Interlock",
                f"{context} exceeds the configured {mode.lower()} limit.\n\n"
                f"Requested range: {low:.4f} to {high:.4f} {unit}\n"
                f"Limit range: {-limit:.4f} to {limit:.4f} {unit}")
            return False
        return True

    def _set_timing_lines(self, lines):
        self.timing_lbl.configure(text="\n".join(lines))

    def _build_dc_request(self, mode):
        value = self._read_float(self.amp_entry, "DC setpoint")
        if value is None or not self._check_interlock(mode, [value], "DC setpoint"):
            return None
        unit = "V" if mode == "VOLT" else "A"
        self._set_timing_lines([
            f"Mode: {mode}",
            f"Setpoint: {value:.4f} {unit}",
            "DC upload uses fixed commands and does not use LIST.",
        ])
        return {
            "wave": "DC", "mode": mode, "kind": "DC",
            "amplitude": value, "offset": None,
            "point_count": 1, "loop": 0, "dwell": None,
            "actual_frequency": 0.0,
            "points": [value], "plot_points": [value, value],
            "csv_name": None,
        }

    def _build_csv_request(self, mode):
        if not self.csv_points:
            messagebox.showerror("Input Error", "Load a CSV file first.")
            return None
        if len(self.csv_points) > MAX_LIST_POINTS:
            messagebox.showerror(
                "Input Error",
                f"CSV waveforms are limited to {MAX_LIST_POINTS} points. "
                f"The loaded file contains {len(self.csv_points)} points.")
            return None
        freq = self._read_float(self.freq_entry, "frequency")
        loop = self._read_int(self.loop_entry, "loop count")
        if freq is None or loop is None or loop < 0:
            if loop is not None and loop < 0:
                messagebox.showerror("Input Error", "Loop count must be 0 or greater.")
            return None
        point_count = len(self.csv_points)
        actual, dwell, actual_freq, warns = WaveformGen.calculate_timing(freq, point_count)
        if actual == 0:
            messagebox.showerror("Input Error", "\n".join(warns))
            return None
        points = list(self.csv_points[:actual])
        if len(points) < 2 or not self._check_interlock(mode, points, "CSV waveform"):
            if len(points) < 2:
                messagebox.showerror("Input Error", "CSV must contain at least 2 points.")
            return None
        lines = [
            f"Points: {len(points)} (single LIST)",
            f"Dwell: {dwell * 1000:.4f} ms",
            f"Actual frequency: {actual_freq:.4f} Hz",
        ]
        lines.extend([f"Warning: {warning}" for warning in warns])
        self._set_timing_lines(lines)
        return {
            "wave": "CSV Custom (untested)", "mode": mode, "kind": "LIST",
            "amplitude": None, "offset": None,
            "point_count": len(points), "loop": loop, "dwell": dwell,
            "actual_frequency": actual_freq,
            "points": points, "plot_points": points,
            "csv_name": self.csv_name,
        }

    def _build_standard_request(self, mode):
        freq = self._read_float(self.freq_entry, "frequency")
        amp = self._read_float(self.amp_entry, "amplitude")
        offset = self._read_float(self.off_entry, "offset")
        pts = self._read_int(self.pts_entry, "total points")
        loop = self._read_int(self.loop_entry, "loop count")
        if None in (freq, amp, offset, pts, loop):
            return None
        if pts < 2 or loop < 0:
            messagebox.showerror(
                "Input Error",
                "Need at least 2 points and a loop count of 0 or greater.")
            return None
        if pts > MAX_LIST_POINTS:
            messagebox.showerror(
                "Input Error",
                f"LIST waveforms are limited to {MAX_LIST_POINTS} points. "
                f"Requested: {pts}.")
            return None
        actual, dwell, actual_freq, warns = WaveformGen.calculate_timing(freq, pts)
        if actual == 0:
            messagebox.showerror("Input Error", "\n".join(warns))
            return None
        points = WaveformGen.generate(self.wave_var.get(), actual, amp, offset)
        if not self._check_interlock(mode, points, f"{self.wave_var.get()} waveform"):
            return None
        lines = [
            f"Points: {len(points)} (single LIST)",
            f"Dwell: {dwell * 1000:.4f} ms",
            f"Actual frequency: {actual_freq:.4f} Hz",
        ]
        lines.extend([f"Warning: {warning}" for warning in warns])
        self._set_timing_lines(lines)
        return {
            "wave": self.wave_var.get(), "mode": mode, "kind": "LIST",
            "amplitude": amp, "offset": offset,
            "point_count": len(points), "loop": loop, "dwell": dwell,
            "actual_frequency": actual_freq,
            "points": points, "plot_points": points,
            "csv_name": None,
        }

    def _read_waveform_request(self):
        """Build the canonical request dict used by preview/upload/output."""
        mode = self.control_mode_var.get().upper()
        wave = self.wave_var.get()
        if wave == "DC":
            req = self._build_dc_request(mode)
        elif wave == "CSV Custom (untested)":
            req = self._build_csv_request(mode)
        else:
            req = self._build_standard_request(mode)
        if not req:
            return None
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return None
        req["voltage_limit"] = limits["VOLT"]
        req["current_limit"] = limits["CURR"]
        return req

    def _preview(self):
        req = self._read_waveform_request()
        if not req:
            return
        self.preview_points = req["plot_points"]
        self._update_preview_plot(req["plot_points"])
        self.log(
            f"Preview ready: {req['wave']} in {req['mode']} mode "
            f"({req['point_count']} point(s))",
            "info")

    # -- Manual SCPI controls ------------------------------------------------
    # Manual actions use the same safety/state helpers as the main workflow so
    # the status panel and connection-loss behavior remain coherent.
    def _man_require_conn(self):
        if not self.kepco.is_verified:
            self.log("Device state is not verified; controls are locked.", "warn")
            return False
        if self._primary_control_operation_active():
            self.log(
                "Manual device controls are locked while another control "
                "operation is active.",
                "warn")
            return False
        if getattr(self, "_manual_operation_in_flight", False):
            label = getattr(self, "_manual_operation_label", "") or "manual operation"
            self.log(f"Wait for the active {label} to finish.", "warn")
            return False
        return True

    def _begin_manual_operation(self, label):
        """Atomically reserve application-level control for one manual action."""
        if not self._man_require_conn():
            return False
        self._manual_operation_in_flight = True
        self._manual_operation_label = label
        self._pause_status_polling()
        if hasattr(self, "conn_btn"):
            self.conn_btn.configure(state="disabled")
        if hasattr(self, "upload_btn"):
            self._update_output_controls()
        else:
            self._update_manual_device_controls()
        return True

    def _finish_manual_operation(self):
        """Release the manual-operation gate on the Tk thread."""
        self._manual_operation_in_flight = False
        self._manual_operation_label = ""
        if hasattr(self, "conn_btn") and not (
                self._connect_in_flight or self._scan_in_flight):
            self.conn_btn.configure(state="normal")
        if hasattr(self, "upload_btn"):
            self._update_output_controls()
        else:
            self._update_manual_device_controls()

    def _start_manual_operation(self, label, operation, completion):
        """Run device-facing manual work off Tk and deliver one UI result."""
        if not self._begin_manual_operation(label):
            return False

        def worker():
            try:
                result = operation()
                error = None
            except Exception as exc:
                result = None
                error = str(exc)

            def deliver():
                try:
                    completion(result, error)
                finally:
                    # Keep all primary/manual controls gated until the result
                    # has reconciled readiness, output, mode, and comm state.
                    self._finish_manual_operation()

            self._call_on_ui(deliver)

        threading.Thread(
            target=worker,
            name=f"kepco-manual-{label.replace(' ', '-').lower()}",
            daemon=True).start()
        return True

    def _select_control_mode(self, mode):
        mode = mode.upper()
        previous_mode = self.current_control_mode
        if mode == previous_mode:
            self.control_mode_var.set(mode)
            self._update_mode_buttons(mode)
            return
        if self.kepco.is_verified and self.current_output_on:
            self.control_mode_var.set(previous_mode)
            self._update_mode_buttons(previous_mode)
            messagebox.showwarning(
                "Output Enabled",
                "Disable output before changing the control mode. This avoids "
                "a live voltage/current crossover transient.")
            return
        if not self.kepco.is_verified:
            self.current_control_mode = mode
            self.control_mode_var.set(mode)
            self._update_mode_buttons(mode)
            self._invalidate_upload_after_control_mode_change(
                previous_mode, mode)
            self.log(f"Control mode preset to {mode}", "info")
            return
        limits = self._get_software_limits(show_error=True)
        if not limits:
            self.control_mode_var.set(previous_mode)
            self._update_mode_buttons(previous_mode)
            return
        expected_output = (
            self.current_output_on
            if isinstance(self.current_output_on, bool) else None)

        def mode_transaction():
            return self._ensure_dc_configuration(
                mode,
                limits["VOLT"],
                limits["CURR"],
                label=f"Manual {mode} fixed-mode selection")

        def operation():
            return self._run_dc_transaction(
                mode_transaction,
                f"Manual {mode} mode change",
                expected_mode=mode,
                expected_output=expected_output)

        def completed(result, error):
            if error:
                result = (False, error, None)
            self._select_control_mode_done(
                previous_mode, mode, *result)

        if not self._start_manual_operation(
                f"{mode} mode change", operation, completed):
            self.control_mode_var.set(previous_mode)
            self._update_mode_buttons(previous_mode)

    def _select_control_mode_done(
            self, previous_mode, mode, ok, msg, snapshot):
        """Apply a verified manual mode result on the Tk thread."""
        self.log(
            f"Control mode -> {mode}" if ok else f"Failed to set control mode: {msg}",
            "ok" if ok else "err")
        if ok:
            self.current_control_mode = mode
            self.control_mode_var.set(mode)
            self._update_mode_buttons(mode)
            self._invalidate_upload_after_control_mode_change(
                previous_mode, mode)
            self._resume_after_dc_postflight(snapshot)
        else:
            self.control_mode_var.set(previous_mode)
            self._update_mode_buttons(previous_mode)
            self._resume_or_handle_transaction_failure("set control mode")

    def _man_set_range(self):
        if not self._man_require_conn():
            return
        if self.current_output_on:
            messagebox.showwarning(
                "Output Enabled",
                "Disable output before changing range. A range transition can "
                "disarm LIST operation or create a scale crossover transient.")
            return
        choice = self.man_range_var.get()
        mode = self.control_mode_var.get()
        cmds = []
        label = ""
        if choice == "Auto":
            cmds = [f"{mode}:RANG:AUTO ON"]
            label = "Auto"
        elif choice == "Full Scale":
            cmds = [f"{mode}:RANG:AUTO OFF", f"{mode}:RANG 1"]
            label = "Full Scale"
        else:
            cmds = [f"{mode}:RANG:AUTO OFF", f"{mode}:RANG 4"]
            label = "Quarter Scale"
        expected_output = (
            self.current_output_on
            if isinstance(self.current_output_on, bool) else None)

        def range_transaction():
            fixed_ok, fixed_msg = self.kepco.select_fixed_mode(
                mode, label=f"Manual {mode} range fixed-mode setup")
            if not fixed_ok:
                return False, fixed_msg
            return self.kepco.send_sequence(
                cmds + ["*WAI"], label=f"Manual {mode} range -> {label}")

        def operation():
            return self._run_dc_transaction(
                range_transaction,
                f"Manual {mode} range change",
                expected_mode=mode,
                expected_output=expected_output)

        def completed(result, error):
            if error:
                result = (False, error, None)
            self._man_set_range_done(mode, label, *result)

        self._start_manual_operation(
            f"{mode} range change", operation, completed)

    def _man_set_range_done(self, mode, label, ok, msg, snapshot):
        self.log(
            f"{mode} range -> {label}" if ok else f"Failed to set range: {msg}",
            "ok" if ok else "err")
        if ok:
            self._set_unverified_upload_state(
                device_state="Range changed - re-upload required",
                progress_text="Range changed - re-upload required",
                plot_title="Range changed - device waveform state unverified")
            self._resume_after_dc_postflight(snapshot)
        else:
            self._resume_or_handle_transaction_failure("set range")

    def _man_reset(self):
        def operation():
            return self._run_dc_transaction(
                lambda: self.kepco.send_sequence(
                    ["*RST", "*WAI"], label="Device reset"),
                "Device reset",
                expected_mode="VOLT",
                expected_output=False)

        def completed(result, error):
            if error:
                result = (False, error, None)
            self._man_reset_done(*result)

        self._start_manual_operation("device reset", operation, completed)

    def _man_reset_done(self, ok, msg, snapshot):
        if ok:
            self.current_control_mode = "VOLT"
            self.control_mode_var.set("VOLT")
            self._update_mode_buttons("VOLT")
            # *RST is a verified command whose documented state is output OFF.
            self._reset_live_status(output_state=False)
            self._reset_uploaded_state()
            self.log("Device reset (*RST)", "ok")
            self._resume_after_dc_postflight(snapshot)
        else:
            self.log(f"Reset failed: {msg}", "err")
            self._resume_or_handle_transaction_failure("reset")

    def _man_send_scpi(self):
        cmd = self.scpi_entry.get().strip()
        if not cmd:
            return
        if self._man_exec_scpi_command(cmd):
            self.scpi_entry.delete(0, "end")

    def _man_exec_scpi_command(self, cmd):
        is_query = cmd.rstrip().endswith("?")
        ts = time.strftime("%H:%M:%S")

        def operation():
            if is_query:
                return self.kepco.send(cmd, query=True)

            def manual_command():
                return self.kepco.send_sequence(
                    [cmd, "*WAI"], label=f"Manual SCPI command '{cmd}'")

            return self._run_dc_transaction(
                manual_command, f"Manual SCPI command '{cmd}'")

        def completed(result, error):
            self._man_exec_scpi_done(
                cmd, is_query, ts, result, error)

        started = self._start_manual_operation(
            f"SCPI {'query' if is_query else 'command'}", operation, completed)
        if started:
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.see("end")
            self.log(f"SCPI: {cmd}", "info")
        return started

    def _man_exec_scpi_done(self, cmd, is_query, ts, result, error):
        if is_query:
            resp = None if error else result
            self.scpi_resp.insert(
                "end", f"[{ts}] < {resp or '(no response)'}\n")
            if resp is None:
                if error:
                    self.log(f"SCPI query failed: {error}", "err")
                self._resume_or_handle_transaction_failure(
                    f"SCPI query '{cmd}'")
            else:
                self._resume_status_polling(150)
        else:
            if error:
                ok, msg, snapshot = False, error, None
            else:
                ok, msg, snapshot = result
            self.scpi_resp.insert(
                "end", f"[{ts}] {'OK' if ok else 'FAILED'}\n")
            if ok:
                # Arbitrary writes cannot be classified reliably. Even if the
                # live output/mode snapshot is valid, it cannot prove that LIST
                # values, dwell, range, or the staged DC setpoint still match.
                self._set_unverified_upload_state(
                    device_state="Manual command applied - re-upload required",
                    progress_text="Manual command applied - re-upload required",
                    plot_title=(
                        "Manual command applied - device waveform state "
                        "unverified"))
                self._resume_after_dc_postflight(snapshot)
            else:
                self._resume_or_handle_transaction_failure(
                    f"SCPI command '{cmd}'")
        self.scpi_resp.see("end")

    def _man_send_preset(self, cmd):
        if self._man_exec_scpi_command(cmd):
            self.scpi_entry.delete(0, "end")
            self.scpi_entry.insert(0, cmd)

    def _man_clear_scpi(self):
        self.scpi_resp.delete("1.0", "end")
        self.log("SCPI console cleared", "info")

    def _man_health_check(self):
        ts = time.strftime("%H:%M:%S")

        def health_check_transaction():
            results = []

            def run_query(cmd):
                resp = self.kepco.send(cmd, query=True)
                results.append((cmd, resp))
                return resp

            run_query("*IDN?")
            mode_resp = run_query("FUNC:MODE?")
            run_query("OUTP?")

            mode_text = str(mode_resp or "").strip().upper()
            active_mode = "CURR" if mode_text in ("1", "CURR") else "VOLT"
            run_query(f"LIST:{active_mode}:POIN?")
            run_query("SYST:ERR?")
            run_query("*ESR?")
            return results

        def operation():
            return self.kepco.run_transaction(health_check_transaction)

        def completed(result, error):
            results = [] if result is None else result
            self._man_health_check_done(ts, results, error=error)

        self._start_manual_operation(
            "health check", operation, completed)

    def _man_health_check_done(self, ts, results, error=None):
        self.scpi_resp.insert("end", f"[{ts}] ==== Health Check ====\n")
        missing = bool(error)
        for cmd, resp in results:
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.insert("end", f"[{ts}] < {resp or '(no response)'}\n")
            if resp is None:
                missing = True
        if error:
            self.scpi_resp.insert("end", f"[{ts}] ERROR: {error}\n")
        self.scpi_resp.insert("end", f"[{ts}] =====================\n")
        self.scpi_resp.see("end")
        self.log(
            "Manual health check failed" if missing
            else "Manual health check complete",
            "err" if missing else "ok")
        if missing:
            self._resume_or_handle_transaction_failure("health check")
        else:
            self._resume_status_polling(150)

    # -- Live status polling -------------------------------------------------
    # Polling runs continuously while connected. Its complete four-query
    # snapshot is submitted as one socket-owner transaction; UI pauses avoid
    # unnecessary queued polls during long operator transactions.
    def _schedule_status_poll(self, delay_ms=STATUS_POLL_INTERVAL_MS):
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None
        if self._status_poll_enabled:
            self._status_poll_timer = self.root.after(delay_ms, self._status_poll_tick)

    def _start_status_polling(self, delay_ms=100):
        self._status_poll_generation += 1
        self._status_poll_enabled = True
        self._status_poll_paused = False
        self._schedule_status_poll(delay_ms)

    def _stop_status_polling(self):
        self._status_poll_generation += 1
        self._status_poll_enabled = False
        self._status_poll_paused = False
        self._status_poll_in_flight = False
        self._status_poll_in_flight_generation = None
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None

    def _pause_status_polling(self):
        self._status_poll_generation += 1
        self._status_poll_paused = True
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None

    def _resume_status_polling(self, delay_ms=100):
        self._status_poll_generation += 1
        self._status_poll_paused = False
        if self._status_poll_enabled:
            self._schedule_status_poll(delay_ms)

    def _status_poll_tick(self):
        self._status_poll_timer = None
        if (not self._status_poll_enabled or self._status_poll_paused
                or not self.kepco.is_verified):
            return
        if self._status_poll_in_flight:
            self._schedule_status_poll(250)
            return
        self._status_poll_in_flight = True
        generation = self._status_poll_generation
        self._status_poll_in_flight_generation = generation
        threading.Thread(
            target=self._status_poll_worker,
            args=(generation,),
            daemon=True).start()

    def _status_poll_worker(self, generation):
        """Collect one atomic snapshot; never query past the first failure."""
        def poll_transaction():
            # Recheck on the owner thread. A Disconnect/Recover request may
            # have paused polling after this background thread was created but
            # before its queue entry reached the socket owner.
            if (generation != self._status_poll_generation
                    or not self._status_poll_enabled
                    or self._status_poll_paused):
                return None, "", True
            snapshot, reason = self.kepco.read_status_snapshot()
            return snapshot, reason, False

        snapshot, reason, cancelled = self.kepco.run_transaction(
            poll_transaction)
        if cancelled:
            self._call_on_ui(
                lambda: self._status_poll_cancelled(generation))
            return
        if snapshot is None:
            self._call_on_ui(
                lambda: self._status_poll_failed(reason, generation))
            return
        self._call_on_ui(
            lambda: self._status_poll_done(snapshot, generation))

    def _release_status_poll(self, generation):
        """Clear the in-flight marker only for the poll that owns it."""
        if self._status_poll_in_flight_generation == generation:
            self._status_poll_in_flight = False
            self._status_poll_in_flight_generation = None

    def _status_poll_cancelled(self, generation):
        """Release the in-flight marker for a poll cancelled before I/O."""
        self._release_status_poll(generation)

    def _status_poll_failed(self, reason, generation):
        """Discard a partial poll because SCPI response alignment is uncertain."""
        self._release_status_poll(generation)
        self._handle_comm_failure("status polling")

    def _status_poll_done(self, snapshot, generation):
        """Apply one previously validated, complete status snapshot."""
        self._release_status_poll(generation)
        if (generation != self._status_poll_generation
                or not self._status_poll_enabled
                or self._status_poll_paused
                or not self.kepco.is_verified):
            return

        self.kepco.last_verified_state = snapshot
        v = snapshot.voltage
        c = snapshot.current
        outp = "ON" if snapshot.output_on else "OFF"
        mode = snapshot.mode
        self._record_data_collection_sample(v, c, outp, mode)

        poll_mode = str(mode).strip().upper()
        if poll_mode == "0":
            poll_mode = "VOLT"
        elif poll_mode == "1":
            poll_mode = "CURR"

        guard = self._measurement_guard
        if guard:
            measured = self._as_float(v if guard["mode"] == "VOLT" else c)
            out_text = str(outp).strip().upper()
            # After a live DC -> AC handoff the first polled sample can still
            # mirror the prior fixed setpoint, so prefer the new waveform center once.
            if (
                poll_mode == guard["mode"]
                and out_text in ("1", "ON")
                and measured is not None
                and abs(measured - guard["previous_value"]) <= 5e-4
            ):
                if guard["mode"] == "VOLT":
                    v = guard["expected_value"]
                else:
                    c = guard["expected_value"]
            self._measurement_guard = None

        self._apply_live_status(v, c, outp, mode)
        if self._status_poll_enabled and not self._status_poll_paused:
            self._schedule_status_poll(STATUS_POLL_INTERVAL_MS)

    def _apply_live_status(self, v, c, outp, mode):
        """Normalize raw SCPI status replies and update local/UI state."""
        out_text = str(outp).strip().upper()
        is_on = out_text in ("1", "ON")
        mode_text = str(mode).strip().upper()
        if mode_text == "0":
            mode_text = "VOLT"
        elif mode_text == "1":
            mode_text = "CURR"
        if mode_text not in ("VOLT", "CURR"):
            mode_text = None

        ac_operation = self._is_ac_operation_active(is_on)
        v_str = "---.----" if ac_operation else self._format_measurement_value(v)
        c_str = "---.----" if ac_operation else self._format_measurement_value(c)
        resistance_str = (
            "---.----"
            if ac_operation
            else self._format_resistance_value(v, c)
        )
        self.status_meas_volt_lbl.configure(text=f"Voltage:  {v_str}  V")
        self.status_meas_curr_lbl.configure(text=f"Current:  {c_str}  A")
        self.status_meas_resistance_lbl.configure(
            text=f"Resistance:  {resistance_str}  \N{OHM SIGN}")
        self._set_status_output_display(is_on)
        self._set_status_mode_display(mode_text)

        if mode_text:
            self.current_control_mode = mode_text
            self.control_mode_var.set(mode_text)
            self._update_mode_buttons(mode_text)

        if not self._output_toggle_in_flight:
            self._set_output_ui_state(is_on)
        else:
            self.current_output_on = is_on
            self._set_status_output_display(is_on)

        self._refresh_ac_operation_notice(is_on)
        self._update_dc_monitors(v, c, is_on, mode_text)
        self._update_output_controls()

    # -- Upload ---------------------------------------------------------------
    # Every supported LIST waveform fits in the BIT card's single LIST buffer.
    # Oversized requests are rejected during request construction and checked
    # again in the worker before any device command is sent.
    def _upload_waveform(self):
        """Start upload/preparation for the current waveform request."""
        if self._upload_in_flight:
            return
        if getattr(self, "_manual_operation_in_flight", False):
            self.log(
                "Wait for the active manual device operation before uploading.",
                "warn")
            return
        if not self.kepco.is_verified:
            messagebox.showerror("Error", "Verify communication with a device first.")
            return
        req = self._read_waveform_request()
        if not req:
            return
        if not isinstance(self.current_output_on, bool):
            messagebox.showwarning(
                "Output State Unknown",
                "Wait for verified output status before uploading a waveform.")
            return
        if req["kind"] == "LIST" and self.current_output_on:
            messagebox.showwarning(
                "Output Enabled",
                "Disable output before uploading or replacing a LIST waveform.")
            return

        previous_request = (
            self.uploaded_request
            if self.uploaded_waveform_ready else None)
        self.preview_points = req["plot_points"]
        self._update_preview_plot(req["plot_points"])
        self._upload_in_flight = True
        self._pause_status_polling()
        self._set_unverified_upload_state(
            device_state="Upload in progress - not verified",
            progress_text="Uploading...",
            plot_title="Upload in progress")
        threading.Thread(
            target=self._upload_request_worker,
            args=(req, previous_request),
            daemon=True).start()

    def _upload_request_worker(self, req, previous_request=None):
        """Run the upload path off the UI thread, then report completion."""
        postflight_snapshot = None
        try:
            def upload_transaction():
                if not isinstance(self.current_output_on, bool):
                    return False, "Output state is unknown", None
                if req["kind"] == "DC":
                    expected_output = (
                        self.current_output_on
                        if isinstance(self.current_output_on, bool) else None)
                    dc_ok, dc_msg, snapshot = self._run_dc_transaction(
                        lambda: self._apply_dc_request(
                            req, previous_request=previous_request),
                        "DC setpoint staging",
                        expected_mode=req["mode"],
                        expected_output=expected_output)
                    return dc_ok, dc_msg, snapshot
                if self.current_output_on:
                    return (
                        False,
                        "Disable output before uploading a LIST waveform",
                        None)
                if req["point_count"] > MAX_LIST_POINTS:
                    return (
                        False,
                        f"LIST request has {req['point_count']} points; "
                        f"maximum is {MAX_LIST_POINTS}",
                        None)
                list_ok, list_msg = self._upload_single_chunk_request(req)
                return list_ok, list_msg, None

            ok, msg, postflight_snapshot = (
                self.kepco.run_transaction(upload_transaction))
        except Exception as exc:
            ok = False
            msg = str(exc)
        self._call_on_ui(
            lambda: self._upload_request_done(
                req, ok, msg, postflight_snapshot))

    def _apply_dc_request(self, req, previous_request=None):
        """Stage or live-update a fixed DC setpoint with safe limit setup."""
        self._call_on_ui(lambda: self.progress.set(0.5))
        mode = req["mode"]
        value = req["amplitude"]
        voltage_compliance, current_limit = self._get_request_limits(req)
        prev_req = (
            previous_request
            if previous_request is not None else
            self.uploaded_request or {})
        live_dc_update = (
            self.current_output_on
            and prev_req.get("kind") == "DC"
            and prev_req.get("mode") == mode
        )

        if self.current_output_on and not live_dc_update:
            return False, (
                "Disable output before staging a DC request in a different "
                "mode; live FUNC:MODE changes are intentionally blocked")

        setpoint_cmd = (
            f"{mode} {KepcoController.format_scpi_value(value)}")

        if live_dc_update:
            previous_voltage_compliance, previous_current_limit = (
                self._get_request_limits(prev_req))
            complementary_limit_changed = (
                current_limit != previous_current_limit
                if mode == "VOLT"
                else voltage_compliance != previous_voltage_compliance)
            if complementary_limit_changed:
                ok, msg = self.kepco.apply_complementary_limit(
                    mode,
                    voltage_compliance=voltage_compliance,
                    current_limit=current_limit,
                    label=f"DC live {mode} complementary limit update")
                if not ok:
                    return False, f"DC limit update failed: {msg}"
                ok, msg = self.kepco.verify_programmed_configuration(
                    mode,
                    voltage_compliance=voltage_compliance,
                    current_limit=current_limit,
                    verify_limit=False,
                    label="DC live setpoint preflight")
                if not ok:
                    return False, msg
            else:
                ok, msg = self.kepco.verify_programmed_configuration(
                    mode,
                    voltage_compliance=voltage_compliance,
                    current_limit=current_limit,
                    label="DC live setpoint preflight")
                if not ok:
                    return False, msg
            self._log_scpi_sequence("DC live setpoint update", [setpoint_cmd])
            ok, msg = self.kepco.send_sequence(
                [setpoint_cmd, "*WAI"], label="DC live setpoint update")
            if not ok:
                return False, msg
        else:
            ok, msg = self._ensure_dc_configuration(
                mode,
                voltage_compliance,
                current_limit,
                label="DC fixed-output safe prepare")
            if not ok:
                return False, msg

        self._call_on_ui(lambda: self.progress.set(1.0))
        unit = "V" if mode == "VOLT" else "A"
        if live_dc_update:
            return True, f"DC setpoint updated live to {value:.4f} {unit}"
        return True, f"DC setpoint staged at {value:.4f} {unit}"

    def _upload_single_chunk_request(self, req):
        """Upload one LIST buffer while verified output remains OFF."""
        if self.current_output_on is not False:
            return False, "LIST upload requires verified output OFF"
        voltage_compliance, current_limit = self._get_request_limits(req)

        def progress_cb(sent, total):
            pct = sent / max(total, 1)
            self._call_on_ui(lambda p=pct: self.progress.set(p))
            self._call_on_ui(
                lambda s=sent, t=total: self.prog_lbl.configure(
                    text=f"Uploading... {s}/{t} pts"))

        ok, msg = self.kepco.upload_list_chunk(
            req["points"], req["dwell"], req["mode"],
            progress_cb=progress_cb,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit)
        ok, msg = self._capture_device_errors_after_failure(
            ok, msg, f"LIST upload ({req['mode']})")
        if not ok:
            return False, msg
        return True, msg

    def _upload_request_done(self, req, ok, msg, postflight_snapshot=None):
        self._upload_in_flight = False
        communication_failed = not self.kepco.is_verified
        if ok and communication_failed:
            ok = False
            msg = (
                f"{msg}; communication became unverified before the upload "
                "could be committed")
        if ok:
            self.uploaded_request = req
            self.uploaded_waveform_ready = True
            self._refresh_ac_operation_notice()
            self._refresh_uploaded_status_panel()
            self.log(msg, "ok")
            self.prog_lbl.configure(
                text=(
                    "Setpoint staged and verified"
                    if req["kind"] == "DC"
                    else "Uploaded and verified"))
            self.progress.set(1.0)
        else:
            self.log(f"Upload failed: {msg}", "err")
            if communication_failed:
                self._handle_comm_failure("upload")
            self._set_unverified_upload_state(
                device_state=(
                    "Upload failed - device waveform state unverified"),
                progress_text="Upload failed - re-upload required",
                plot_title=(
                    "Upload failed - device waveform state unverified"))

        if req["kind"] != "DC" and self.kepco.is_verified:
            self._resume_status_polling()
        elif ok:
            self._resume_after_dc_postflight(postflight_snapshot)
        elif not communication_failed:
            self._resume_or_handle_transaction_failure("upload")
        self._update_output_controls()

    # -- Output control ------------------------------------------------------
    # The Output button is a router: DC uses fixed setpoint commands and LIST
    # waveforms arm the one verified device buffer directly.
    def _toggle_output(self):
        """Turn output on/off while preserving interlocks and staged limits."""
        if self._output_toggle_in_flight:
            return
        if getattr(self, "_manual_operation_in_flight", False):
            self.log(
                "Output control is locked during a manual device operation.",
                "warn")
            return
        if self._upload_in_flight:
            self.log(
                "Output control is locked until the waveform upload completes.",
                "warn")
            return
        if not self.kepco.is_verified:
            self._set_output_ui_state(None)
            self.log("Device state is not verified; output control is locked.", "warn")
            return
        if not isinstance(self.current_output_on, bool):
            self.log(
                "Output state is unknown; waiting for verified device status.",
                "warn")
            return

        target_on = not self.current_output_on
        req = self.uploaded_request

        if target_on and not self.uploaded_waveform_ready:
            self._set_output_ui_state(False)
            self.log("Upload a waveform before enabling output.", "warn")
            return
        selected_mode = self.control_mode_var.get().upper()
        if target_on and (
                not req or req.get("mode") != selected_mode):
            self.uploaded_waveform_ready = False
            self._update_output_controls()
            self.log(
                f"The staged waveform does not match {selected_mode} control "
                f"mode. Upload a new {selected_mode} waveform before enabling "
                "output.",
                "warn")
            return
        if target_on:
            # Re-check limits at the moment output is enabled because the
            # operator can edit software-limit fields after upload.
            req = self._request_with_latest_ui_limits(req)
            if not req:
                self._set_output_ui_state(False)
                return

        self._output_toggle_in_flight = True
        self._pause_status_polling()
        self._update_output_controls()
        threading.Thread(
            target=self._output_toggle_worker,
            args=(target_on, req),
            daemon=True).start()

    def _enable_dc_output(self, req):
        """Enable a previously staged DC request without reconfiguring it."""
        mode = req["mode"]
        value = req["amplitude"]
        voltage_compliance, current_limit = self._get_request_limits(req)
        ok, msg = self.kepco.verify_programmed_configuration(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            expected_setpoint=0.0,
            label="DC output enable staged-state check")
        if not ok:
            return False, msg

        # The manual initializes the active parameter at zero with its
        # complementary limit, then changes only the active parameter. Program
        # and verify the requested value while output is still OFF before the
        # one OUTP ON transition.
        setpoint_cmd = f"{mode} {KepcoController.format_scpi_value(value)}"
        cmds = [setpoint_cmd, "*WAI"]
        self._log_scpi_sequence("DC apply staged setpoint", cmds)
        ok, msg = self.kepco.send_sequence(
            cmds, label="DC apply staged setpoint")
        if not ok:
            return False, msg
        ok, msg = self.kepco.verify_programmed_configuration(
            mode,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            expected_setpoint=value,
            label="DC programmed setpoint check")
        if not ok:
            return False, msg

        self._log_scpi_sequence("DC output enable", ["OUTP ON", "*WAI"])
        ok, msg = self.kepco.send_sequence(
            ["OUTP ON", "*WAI"], label="DC output enable")
        if not ok:
            return False, msg
        unit = "V" if mode == "VOLT" else "A"
        return True, f"Output ON; {mode} setpoint {value:.4f} {unit}"

    def _output_toggle_worker(self, target_on, req):
        """Apply one output transition on a worker thread."""
        postflight_snapshot = None
        dc_transaction = bool(
            (target_on and req and req.get("kind") == "DC")
            or (not target_on and (not req or req.get("kind") == "DC")))
        try:
            def output_transaction():
                mode = (req or {}).get("mode") or self.current_control_mode
                if dc_transaction:
                    def dc_output_operation():
                        if target_on:
                            return self._enable_dc_output(req)
                        return self.kepco.send_sequence(
                            [
                                "OUTP OFF",
                                f"{mode} 0",
                                "*WAI",
                            ],
                            label="DC output disable")

                    return self._run_dc_transaction(
                        dc_output_operation,
                        "DC output enable" if target_on else "DC output disable",
                        expected_mode=mode,
                        expected_output=target_on)
                if target_on:
                    count = 0 if req["loop"] == 0 else max(req["loop"], 1)
                    voltage_compliance, current_limit = self._get_request_limits(req)
                    list_ok, list_msg = self.kepco.run_list(
                        mode,
                        count=count,
                        enable_output=True,
                        voltage_compliance=voltage_compliance,
                        current_limit=current_limit,
                        apply_limit_setup=False)
                    list_ok, list_msg = self._capture_device_errors_after_failure(
                        list_ok, list_msg, f"LIST output enable ({mode})")
                    return list_ok, list_msg, None
                if req and req["kind"] == "LIST":
                    list_ok, list_msg = self.kepco.stop(base_mode=mode)
                    list_ok, list_msg = self._capture_device_errors_after_failure(
                        list_ok, list_msg, f"LIST output disable ({mode})")
                    return list_ok, list_msg, None
                sent = bool(self.kepco.send("OUTP OFF"))
                return (
                    sent,
                    "Output OFF" if sent else "Failed to turn output OFF",
                    None)

            ok, msg, postflight_snapshot = self.kepco.run_transaction(
                output_transaction)
        except Exception as exc:
            ok = False
            msg = str(exc)
        self._call_on_ui(
            lambda: self._output_toggle_done(
                target_on, ok, msg, dc_transaction, postflight_snapshot))

    def _output_toggle_done(self, target_on, ok, msg, dc_transaction=False,
                            postflight_snapshot=None):
        self._output_toggle_in_flight = False
        if ok:
            self._set_output_ui_state(target_on)
            transition = "Output ON" if target_on else "Output OFF"
            detail = str(msg or "").strip()
            if not detail or detail.upper() == "OK":
                success_message = transition
            elif transition.lower() in detail.lower():
                success_message = detail
            else:
                success_message = f"{transition}; {detail}"
            self.log(success_message, "ok")
            if target_on:
                self.prog_lbl.configure(text="Output enabled")
            else:
                self.prog_lbl.configure(text="Idle")
                self.progress.set(0)
            if dc_transaction:
                self._resume_after_dc_postflight(postflight_snapshot)
            else:
                self._resume_status_polling()
            self._update_output_controls()
            return
        # A command may have reached the device even when its postflight
        # verification failed. Never infer the physical state from the
        # requested transition; the immediate resumed snapshot will reconcile it.
        self._set_output_ui_state(None)
        self.log(msg, "err")
        self._update_output_controls()
        self._resume_or_handle_transaction_failure("output toggle")

    # -- Discovery and connection lifecycle ----------------------------------
    # Network scan, connect, disconnect, and application close all end by
    # reconciling local UI state with the controller's connection state.
    def _start_scan(self):
        if self._scan_in_flight:
            return
        if getattr(self, "_manual_operation_in_flight", False):
            self.log(
                "Wait for the active manual device operation before scanning.",
                "warn")
            return
        if self.kepco.is_transport_connected or self._connect_in_flight:
            messagebox.showwarning(
                "Connection Active",
                "Disconnect before scanning. A scan can open another socket "
                "to the Kepco and disrupt the active control session.")
            return
        self._scan_in_flight = True
        self.scan_btn.configure(state="disabled", text="Scanning...")
        self.conn_btn.configure(state="disabled")
        self._update_output_controls()
        self.log(
            "Scanning local subnet for Kepco devices "
            "(raw SCPI 5025 first, Telnet 5024 fallback)...",
            "info")
        ip = self.ip_var.get().strip()
        base = ".".join(ip.split(".")[:3]) + ".0" if ip else "192.168.50.0"

        def done(results):
            self._call_on_ui(lambda: self._scan_done(results))

        def progress(done_count, total_count):
            self._call_on_ui(lambda: self.progress.set(done_count / total_count))

        threading.Thread(
            target=Discovery.scan_subnet,
            args=(base, done, progress),
            daemon=True).start()

    def _scan_done(self, results):
        self._scan_in_flight = False
        self.scan_btn.configure(state="normal", text="Scan Network")
        self.conn_btn.configure(state="normal")
        self._update_output_controls()
        self.progress.set(0)
        if results:
            ips = [ip for ip, _idn in results]
            self.ip_combo.configure(values=ips)
            self.ip_var.set(ips[0])
            self.log(f"Network scan complete: {len(results)} device(s) found", "ok")
            for ip, idn in results:
                self.log(f"Found {ip} -> {idn}", "ok")
        else:
            self.log("Network scan complete: 0 devices found", "warn")

    def _toggle_connect(self):
        if self._connect_in_flight or self._scan_in_flight:
            return
        if getattr(self, "_manual_operation_in_flight", False):
            self.log(
                "Wait for the active manual device operation before changing "
                "the connection.",
                "warn")
            return

        if (self.kepco.comm_state is CommState.DEGRADED
                and self.kepco.is_transport_connected):
            self.log("Controlled communication recovery requested", "warn")
            self._connect_in_flight = True
            self._pause_status_polling()
            self.scan_btn.configure(state="disabled")
            self.conn_btn.configure(state="disabled", text="Recovering...")
            self._update_output_controls()
            threading.Thread(target=self._recover_worker, daemon=True).start()
            return

        if not self.kepco.is_transport_connected:
            ip = self.ip_var.get().strip()
            self.log(f"Connect requested for {ip}", "info")
            self._connect_in_flight = True
            self.scan_btn.configure(state="disabled")
            self.conn_btn.configure(state="disabled", text="Connecting...")
            self._update_output_controls()
            threading.Thread(target=self._connect_worker, args=(ip,), daemon=True).start()
        else:
            if self._upload_in_flight or self._output_toggle_in_flight:
                messagebox.showwarning(
                    "Waveform Busy",
                    "Wait for the active output transaction or upload "
                    "to finish before disconnecting.")
                return
            self.log("Disconnect requested", "info")
            self._connect_in_flight = True
            self._pause_status_polling()
            self.conn_btn.configure(state="disabled", text="Disconnecting...")
            self._update_output_controls()
            threading.Thread(target=self._disconnect_worker, daemon=True).start()

    def _connect_worker(self, ip):
        """Connect, then verify a complete device state off the Tk event loop."""
        def connect_transaction():
            ok, msg = self.kepco.connect(ip, validate_identity=True)
            if ok:
                self._call_on_ui(self._show_verifying_device_state)
                self._log_safe(
                    "Socket and identity available; verifying device state...",
                    "info")
                ok, verify_msg = self.kepco.verify_device_state()
                msg = (
                    "Verified device state" if ok else
                    f"Device verification failed: {verify_msg}")
                if not ok:
                    self.kepco.fault(msg)
            return ok, msg, self.kepco.last_identity or None

        ok, msg, idn = self.kepco.run_transaction(connect_transaction)
        self._call_on_ui(lambda: self._connect_done(ok, msg, ip, idn))

    def _recover_worker(self):
        """Attempt a fresh socket plus full verification after degradation."""
        ip = self.kepco.ip

        def recovery_transaction():
            ok, _ = self.kepco.connect(ip, validate_identity=True)
            if ok:
                self._call_on_ui(self._show_verifying_device_state)
                ok, reason = self.kepco.verify_device_state()
            else:
                reason = self.kepco.last_error or "transport reconnect failed"
            if not ok:
                self.kepco.fault(f"Automatic recovery failed: {reason}")
            msg = (
                "Recovery verified device state" if ok else
                self.kepco.last_error)
            return ok, msg, self.kepco.ip, self.kepco.last_identity or None

        ok, msg, connected_ip, idn = self.kepco.run_transaction(
            recovery_transaction)
        self._call_on_ui(
            lambda: self._connect_done(ok, msg, connected_ip, idn))

    def _show_verifying_device_state(self):
        """Keep controls locked while the background health gate is running."""
        if self.kepco.comm_state is CommState.VERIFYING:
            self._set_connected_state(self.kepco.last_identity)

    def _connect_done(self, ok, msg, ip, idn):
        self._connect_in_flight = False
        self.conn_btn.configure(state="normal")
        if ok:
            self._set_connected_state(idn or "Unknown device")
            snapshot = self.kepco.last_verified_state
            self._reset_live_status(
                output_state=snapshot.output_on if snapshot else None,
                control_mode=snapshot.mode if snapshot else None)
            self._reset_uploaded_state()
            self.log(
                f"Connected to {ip} via {self.kepco.transport} "
                f"({self.kepco.port}): {idn or 'Unknown device'}",
                "ok")
            self._start_status_polling()
        else:
            if self.kepco.is_transport_connected:
                self.kepco.disconnect()
            self._set_connected_state()
            self._reset_live_status(output_state=None)
            self._reset_uploaded_state()
            self.log(f"Connection failed: {msg}", "err")

    def _disconnect_worker(self):
        """Verify output is safe before closing the socket."""
        def disconnect_transaction():
            if not self.kepco.is_verified:
                # Do not issue output commands against an untrusted response
                # stream. Closing the socket leaves physical output unknown.
                self.kepco.disconnect()
                return True, "", True
            ok, err_msg = self._safe_output_off_before_disconnect()
            if ok:
                self.kepco.disconnect()
            return ok, err_msg, False

        ok, err_msg, output_unknown = self.kepco.run_transaction(
            disconnect_transaction)
        self._call_on_ui(
            lambda: self._disconnect_done(
                ok, err_msg, output_unknown=output_unknown))

    def _safe_output_off_before_disconnect(self):
        """Return True only after output OFF and near-zero V/I are verified."""
        if not self.kepco.is_verified:
            return False, "Device state is not verified; output state is unknown"

        def _parse_num(raw):
            try:
                return float(str(raw).strip())
            except Exception:
                return None

        snapshot = self.kepco.last_verified_state
        base_mode = (
            snapshot.mode if snapshot is not None else
            self.uploaded_request["mode"]
            if self.uploaded_request else self.current_control_mode
        )

        # Be deliberately conservative: try the full stop/zero/off sequence
        # twice, and keep the app connected if readback cannot verify safety.
        for attempt in range(2):
            errors = []
            try:
                ok_stop, stop_msg = self.kepco.stop(base_mode=base_mode)
            except ConnectionLostError as exc:
                return False, str(exc)
            if not ok_stop:
                errors.append(f"stop failed ({stop_msg})")

            try:
                safe_ok, safe_msg = self.kepco.send_sequence(
                    [f"{base_mode} 0", "*WAI", "OUTP OFF", "*WAI"],
                    label=f"Disconnect safe-zero ({base_mode})")
            except ConnectionLostError as exc:
                return False, str(exc)
            if not safe_ok:
                errors.append(safe_msg)
                return False, "; ".join(errors)

            try:
                outp = (self.kepco.send("OUTP?", query=True) or "").strip().upper()
                v = _parse_num(self.kepco.send("MEAS:VOLT?", query=True))
                c = _parse_num(self.kepco.send("MEAS:CURR?", query=True))
            except ConnectionLostError as exc:
                return False, str(exc)
            outp_ok = outp in ("0", "OFF")
            zero_ok = (
                v is not None and c is not None
                and abs(v) <= 0.05 and abs(c) <= 0.05
            )

            if outp_ok and zero_ok:
                return True, ""

            errors.append(
                f"verification failed (OUTP?='{outp}', "
                f"MEAS:VOLT?='{v}', MEAS:CURR?='{c}')")
            if attempt == 0:
                time.sleep(0.1)
            else:
                return False, "; ".join(errors)

        return False, "safety verification failed"

    def _disconnect_done(self, ok, err_msg, output_unknown=False):
        self._connect_in_flight = False
        self.conn_btn.configure(state="normal")
        if not ok:
            self._set_connected_state(self.idn_lbl.cget("text"))
            self._reset_live_status(output_state=None)
            self.log(
                f"Disconnect blocked by safety interlock: {err_msg}",
                "critical")
            messagebox.showerror(
                "Safety Interlock",
                "Disconnect blocked.\n"
                "Output could not be verified OFF at 0V/0A.\n"
                f"Details: {err_msg}")
            self._resume_or_handle_transaction_failure("disconnect safety check")
            return

        self._stop_status_polling()
        self._set_connected_state()
        self._reset_live_status(output_state=None if output_unknown else False)
        self._reset_uploaded_state()
        self.log("Disconnected.", "info")

    # -- Shutdown ------------------------------------------------------------
    # Closing the window follows the same safety gate as Disconnect, then tears
    # down polling, data collection, session logging, and queued UI callbacks.
    def _on_close(self):
        self._pause_status_polling()

        def close_transaction():
            # Check transport state only after all earlier queue entries have
            # completed. This also covers a Connect worker that was launched
            # just before the operator closed the application.
            if not self.kepco.is_transport_connected:
                return True, ""
            if not self.kepco.is_verified:
                # Match Disconnect: do not issue output commands against an
                # untrusted response stream. Retire the socket and close with
                # the physical output state unknown.
                self.kepco.disconnect()
                return True, ""
            ok, err_msg = self._safe_output_off_before_disconnect()
            if ok:
                self.kepco.disconnect()
            return ok, err_msg

        ok, err_msg = self.kepco.run_transaction(close_transaction)
        if not ok:
            self._set_connected_state(self.kepco.last_identity)
            self._reset_live_status(output_state=None)
            self.log(
                f"Close blocked by safety interlock: {err_msg}",
                "critical")
            messagebox.showerror(
                "Safety Interlock",
                "Close blocked.\n"
                "Output could not be verified OFF at 0V/0A.\n"
                f"Details: {err_msg}")
            self._resume_or_handle_transaction_failure("close safety check")
            return
        self._stop_status_polling()
        self._stop_solenoid_temperature_polling()
        self._stop_data_collection()
        self.log("Application closed.", "info")
        self._close_log_file()
        self._stop_ui_dispatcher()
        self.kepco.shutdown_socket_worker()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


# ===========================================================================
if __name__ == "__main__":
    DashboardApp().run()
