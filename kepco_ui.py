#!/usr/bin/env python3
"""
Kepco BIT 802E Waveform Generator - High Performance Edition

Material-design UI with real-time waveform preview, chunk-send
indication, auto-discovery, and optimized multi-list upload.

Hardware Constraints (BIT 802E manual):
  - Max 1000 list points per upload (1002 technically)
  - Dwell time: 0.0005 s (500 us) to 10 s
  - For >1000 points: sequential multi-list upload required
  - Use the active mode's RANG 1 to avoid quarter-scale transients
"""

import socket
import math
import csv
import os
import queue
import threading
import time
import ipaddress
from tkinter import messagebox, filedialog

# -- GUI + plotting ----------------------------------------------------------
import customtkinter as ctk

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.lines as mlines
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# -- Constants ---------------------------------------------------------------
MIN_DWELL        = 0.0005    # 500 us - hardware minimum
MAX_DWELL        = 10.0      # hardware maximum
MAX_LIST_POINTS  = 1000      # per single LIST upload
MAX_TOTAL_POINTS = 4000      # 4 x 1000 chunks
TELNET_PORT      = 5024      # manual 2.4.2 / 4.5: Telnet first
SCPI_SOCKET_PORT = 5025      # alternate direct socket endpoint
DISCOVERY_TIMEOUT = 0.25
CHUNK_CMD_LIMIT  = 200       # safe margin for 253-byte SCPI buffer
SCPI_CMD_GAP     = 0.035     # > 25ms spec throughput (PAR 1.2.2)
LIST_VALUES_PER_CMD = 10     # manual examples show max 11 (PAR B.45/B.31)
RECV_TIMEOUT     = 3.0       # socket recv timeout for queries
BOP_MAX_VOLTAGE  = 100.0     # BOP 100-2ML voltage rating
BOP_MAX_CURRENT  = 2.0       # BOP 100-2ML current rating
DEFAULT_POSITIVE_VOLTAGE_COMPLIANCE = 20.0
DEFAULT_NEGATIVE_VOLTAGE_COMPLIANCE = -20.0
DEFAULT_POSITIVE_CURRENT_LIMIT = 2.0
DEFAULT_NEGATIVE_CURRENT_LIMIT = -2.0
DEFAULT_VOLTAGE_COMPLIANCE = DEFAULT_POSITIVE_VOLTAGE_COMPLIANCE
DEFAULT_CURRENT_LIMIT = DEFAULT_POSITIVE_CURRENT_LIMIT

# -- Material colour palette -------------------------------------------------
C = dict(
    bg="#121212", surface="#1e1e2e", card="#2a2a3c",
    primary="#7c3aed", primary_h="#6d28d9",
    green="#10b981", red="#ef4444", amber="#f59e0b",
    text="#e2e8f0", text2="#94a3b8", border="#3f3f5c",
    input_bg="#363650", graph_bg="#161625",
    chunk_colors=["#818cf8", "#34d399", "#fb923c", "#f472b6"],
    sent="#f472b6",
)


# ===========================================================================
#  SCPI Controller  (hardened for real BIT 802E hardware)
# ===========================================================================
class KepcoController:
    """Thread-safe SCPI control for a Kepco BIT 802E.

    Protocol notes (BIT 802E manual):
      - PAR 2.4.2 / 4.5: Telnet-first on port 5024, socket fallback 5025
      - PAR 1.2.2: connection throughput ~25 ms per command
      - PAR 4.5.2: *WAI / *OPC? to ensure command completion
      - 253-byte input buffer limit per SCPI message
      - List: max 1002 steps, dwell 500 us ... 10 s

    Design:
      - Every non-query command sleeps SCPI_CMD_GAP (35 ms) *inside* the
        lock so no other thread can violate the pacing constraint.
      - *OPC? sync is used only at key checkpoints (after LIST:CLE, after
        all values sent, after DWEL) - NOT after every single LIST:VOLT.
      - Post-upload, LIST:{mode}:POIN? verifies the card accepted all
        points, and SYST:ERR? drains any queued errors.
    """

    def __init__(self):
        self.sock = None
        self.ip = ""
        self.port = TELNET_PORT
        self.transport = "TELNET"
        self.connected = False
        self.last_error = ""
        self.last_identity = ""
        self._query_timeout_count = 0
        # Re-entrant so higher-level upload/run/stop transactions can hold the
        # device lock while individual send_cmd/send_query helpers re-enter it.
        self._lock = threading.RLock()
        self._debug_logger = None

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

    # -- connect / disconnect -----------------------------------------------
    def connect(self, ip, port=None, validate_identity=False):
        attempts = [(port, "CUSTOM")] if port is not None else [
            (TELNET_PORT, "TELNET"),
            (SCPI_SOCKET_PORT, "SOCKET"),
        ]
        last_err = ""
        self.last_identity = ""
        for target_port, transport in attempts:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self._dbg("info", f"Connect attempt {ip}:{target_port} ({transport})")
                s.settimeout(5)
                s.connect((ip, target_port))
                # Drain Telnet IAC negotiation the card sends on connect
                time.sleep(0.1)
                s.setblocking(False)
                try:
                    s.recv(1024)
                except BlockingIOError:
                    pass
                s.setblocking(True)
                s.settimeout(RECV_TIMEOUT)

                self.sock = s
                self.ip = ip
                self.port = target_port
                self.transport = transport
                self.connected = True
                self.last_error = ""
                self._query_timeout_count = 0
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
                    self.last_identity = idn
                return True, f"Connected via {transport} ({target_port})"
            except Exception as e:
                last_err = str(e)
                self._dbg("warn", f"Connect failed {ip}:{target_port} ({transport}): {last_err}")
                try:
                    s.close()
                except Exception:
                    pass
        self.connected = False
        self.last_error = last_err
        self._dbg("err", f"All connect attempts failed for {ip}: {last_err}")
        return False, last_err

    def disconnect(self):
        self._dbg("info", f"Disconnecting from {self.ip}:{self.port} ({self.transport})")
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.connected = False
        self._query_timeout_count = 0
        self._dbg("info", "Disconnected")

    def _safe_reconnect(self):
        if not self.ip:
            return False
        self._dbg("warn", f"Attempting reconnect to {self.ip} (all transports)")
        ok, _ = self.connect(self.ip, validate_identity=True)
        if ok:
            self._dbg("ok", f"Reconnect succeeded to {self.ip}:{self.port}")
        else:
            self._dbg("err", f"Reconnect failed to {self.ip}")
        return ok

    # -- Telnet IAC filtering ----------------------------------------------
    @staticmethod
    def _strip_iac(data: bytes) -> bytes:
        """Remove Telnet IAC (0xFF) negotiation sequences from raw bytes."""
        if 0xFF not in data:
            return data
        out = bytearray()
        i = 0
        n = len(data)
        while i < n:
            b = data[i]
            if b == 0xFF and i + 1 < n:
                nxt = data[i + 1]
                if nxt in (0xFB, 0xFC, 0xFD, 0xFE):   # WILL/WONT/DO/DONT
                    i += 3
                    continue
                elif nxt == 0xFA:                        # SB sub-negotiation
                    end = data.find(b"\xff\xf0", i + 2)
                    i = (end + 2) if end >= 0 else (i + 2)
                    continue
                elif nxt == 0xFF:                        # escaped 0xFF
                    out.append(0xFF)
                    i += 2
                    continue
                else:
                    i += 2
                    continue
            out.append(b)
            i += 1
        return bytes(out)

    # -- socket helpers -----------------------------------------------------
    def _drain_echo(self):
        """Quick non-blocking drain of Telnet echo after every send_cmd.

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
                return bool(data)
            except (socket.timeout, OSError):
                return False
        finally:
            try:
                self.sock.settimeout(prev)
            except Exception:
                pass

    def _drain_stale(self):
        """Drain all stale data (accumulated echoes) from the socket.

        Uses a short timeout so we wait long enough for any in-flight
        bytes to arrive but don't block indefinitely.
        """
        prev = self.sock.gettimeout()
        try:
            self.sock.settimeout(0.05)          # 50 ms
            while True:
                try:
                    data = self.sock.recv(4096)
                    if not data:
                        break
                except socket.timeout:
                    break
                except OSError:
                    break
        except Exception:
            pass
        finally:
            try:
                self.sock.settimeout(prev)
            except Exception:
                pass

    def _recv_response(self, sent_cmd=None, timeout=None):
        """Receive one SCPI response line, skipping Telnet echo lines.

        The BIT 802E Telnet server echoes every command back before
        sending the actual response.  If *sent_cmd* is provided, any
        complete line that exactly matches it is silently discarded.
        """
        timeout = timeout or RECV_TIMEOUT
        echo = sent_cmd.strip() if sent_cmd else None
        prev = self.sock.gettimeout()
        self.sock.settimeout(timeout)

        def _clean_line(line: str):
            """Normalize one line by removing Telnet prompt noise.

            Real BIT Telnet responses can include shell-style prompt prefixes
            like 'KEPCO ... >CMD?' or 'KEPCO ... >0,"No error"'.
            """
            line = line.strip()
            if not line:
                return None

            # If a device prompt prefix exists, keep only the payload
            # right of the last prompt marker.
            if ">" in line:
                tail = line.rsplit(">", 1)[1].strip()
                if tail:
                    line = tail

            def _looks_like_scpi_command(text: str):
                t = text.strip()
                if not t:
                    return False
                up = t.upper()
                # Keep common non-command responses.
                if up in ("ON", "OFF", "LIST", "FIX", "VOLT", "CURR", "TRAN"):
                    return False
                if "NO ERROR" in up or ("," in up and '"' in up):
                    return False
                tok = up.split()[0]
                if tok.endswith("?"):
                    return True
                if tok.startswith("*") or ":" in tok:
                    return True
                if tok in (
                    "OUTP", "VOLT", "CURR", "FUNC", "LIST", "SYST",
                    "MEAS", "INIT", "TRIG", "STAT", "FORM", "SOUR",
                    "LOAD", "RANG",
                ):
                    return True
                return False

            if echo and line == echo:
                return None
            if _looks_like_scpi_command(line):
                return None
            return line or None

        try:
            raw = b""
            deadline = time.time() + timeout
            while time.time() < deadline:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                self.sock.settimeout(min(remaining, timeout))
                try:
                    chunk = self.sock.recv(512)
                except socket.timeout:
                    break
                if not chunk:
                    raise ConnectionError("Connection closed by peer")
                raw += chunk
                # Strip Telnet IAC sequences then decode
                clean = (self._strip_iac(raw)
                         if self.port == TELNET_PORT else raw)
                text = clean.decode("ascii", errors="ignore")
                parts = text.replace("\r\n", "\n").replace(
                    "\r", "\n").split("\n")
                trailing = parts[-1]
                complete = parts[:-1]
                for line in complete:
                    line = _clean_line(line)
                    if line is not None:
                        return line
                # Only echo / empty lines so far - keep the tail
                raw = trailing.encode("ascii", errors="ignore")
                if len(raw) > 8192:
                    break
            # Timeout - check anything left in buffer
            if raw:
                text = raw.decode("ascii", errors="ignore")
                for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
                    cleaned = _clean_line(line)
                    if cleaned is not None:
                        return cleaned
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
    def send_cmd(self, cmd):
        """Send a non-query SCPI command with mandatory pacing.

        The SCPI_CMD_GAP sleep is *inside* the lock so that concurrent
        threads cannot send a second command faster than the 25 ms
        throughput limit.  After the gap, we drain the Telnet echo to
        prevent the card's TCP send-buffer from filling up (which would
        deadlock the card).  Returns True / None.
        """
        if not self.connected and not self._safe_reconnect():
            return None
        with self._lock:
            try:
                self._dbg("info", f"TX CMD: {cmd}")
                self.sock.sendall((cmd + "\n").encode("ascii"))
                time.sleep(SCPI_CMD_GAP)
                if self.port == TELNET_PORT:
                    self._drain_echo()  # consume Telnet echo
                return True
            except Exception as e:
                self.last_error = str(e)
                self._dbg("err", f"CMD failed '{cmd}': {self.last_error}")
                self.disconnect()
                return None

    # -- SCPI primitive: query (expects response) --------------------------
    def send_query(self, cmd, timeout=None):
        """Send a SCPI query and return the response string (or None).

        Before sending, any stale data in the socket (echoes from prior
        send_cmd calls) is drained.  After sending, the response reader
        skips the Telnet echo of this query.
        """
        if not self.connected and not self._safe_reconnect():
            return None
        with self._lock:
            try:
                self._drain_stale()
                self._dbg("info", f"TX QRY: {cmd}")
                self.sock.sendall((cmd + "\n").encode("ascii"))
                resp = self._recv_response(sent_cmd=cmd, timeout=timeout)
                if resp is None:
                    self._query_timeout_count += 1
                    self.last_error = f"No response to '{cmd}'"
                    self._dbg("warn", f"RX timeout for '{cmd}' (streak={self._query_timeout_count})")
                    if self._query_timeout_count >= 2:
                        # Do a receive-only confirmation wait (no re-send), so
                        # stateful queries like SYST:ERR? are not consumed twice.
                        self._dbg("warn", f"Query timeout threshold hit; confirming link without re-send ({cmd})")
                        confirm = self._recv_response(
                            sent_cmd=cmd,
                            timeout=min(timeout or RECV_TIMEOUT, 1.0),
                        )
                        if confirm is not None:
                            self._query_timeout_count = 0
                            self.last_error = ""
                            self._dbg("ok", f"RX RESP (confirm): {cmd} -> {confirm}")
                            return confirm
                        self.last_error = (
                            f"No response to '{cmd}' (connection lost)")
                        self._dbg("err", f"Query timeout confirm failed; disconnecting ({cmd})")
                        self.disconnect()
                else:
                    self._query_timeout_count = 0
                    self._dbg("ok", f"RX RESP: {cmd} -> {resp}")
                return resp
            except Exception as e:
                self.last_error = str(e)
                self._dbg("err", f"QRY failed '{cmd}': {self.last_error}")
                self.disconnect()
                return None

    # -- backward-compat wrapper (used by Manual Override callbacks) --------
    def send(self, cmd, query=False, post_delay=0.0):
        if query:
            return self.send_query(cmd)
        return self.send_cmd(cmd)

    @staticmethod
    def format_scpi_value(value):
        return f"{float(value):.6g}"

    @classmethod
    def limit_pair(cls, values, default_positive, default_negative):
        if values is None:
            return float(default_positive), float(default_negative)
        if isinstance(values, dict):
            pos = values.get("positive", values.get("pos", default_positive))
            neg = values.get("negative", values.get("neg", default_negative))
            return float(pos), float(neg)
        if isinstance(values, (list, tuple)) and len(values) >= 2:
            return float(values[0]), float(values[1])
        magnitude = abs(float(values))
        return magnitude, -magnitude

    @classmethod
    def signed_limit_cmds(cls, channel, limits, negative_limit=None):
        if negative_limit is None:
            if isinstance(limits, dict):
                pos, neg = cls.limit_pair(limits, 0.0, 0.0)
            elif isinstance(limits, (list, tuple)) and len(limits) >= 2:
                pos, neg = cls.limit_pair(limits, limits[0], limits[1])
            else:
                magnitude = abs(float(limits))
                pos, neg = magnitude, -magnitude
        else:
            pos, neg = float(limits), float(negative_limit)
        return [
            f"{channel} {cls.format_scpi_value(pos)}",
            f"{channel} {cls.format_scpi_value(neg)}",
        ]

    @classmethod
    def bipolar_limit_cmds(cls, channel, magnitude):
        magnitude = abs(float(magnitude))
        return cls.signed_limit_cmds(channel, (magnitude, -magnitude))

    @classmethod
    def default_voltage_limits(cls):
        return (
            DEFAULT_POSITIVE_VOLTAGE_COMPLIANCE,
            DEFAULT_NEGATIVE_VOLTAGE_COMPLIANCE,
        )

    @classmethod
    def default_current_limits(cls):
        return (
            DEFAULT_POSITIVE_CURRENT_LIMIT,
            DEFAULT_NEGATIVE_CURRENT_LIMIT,
        )

    def _log_sequence(self, label, cmds):
        self._dbg("info", f"{label}: {'; '.join(cmds)}")

    def send_sequence(self, cmds, label="SCPI sequence"):
        cmds = [cmd for cmd in cmds if cmd]
        if not self.connected and not self._safe_reconnect():
            return False, "Not connected"
        with self._lock:
            self._log_sequence(label, cmds)
            for cmd in cmds:
                if self.send_cmd(cmd) is None:
                    return False, f"{label} failed at '{cmd}': {self.last_error}"
            return True, "OK"

    def _limit_setup_cmds(self, mode, voltage_compliance=None,
                          current_limit=None):
        mode = (mode or "VOLT").upper()
        if mode not in ("VOLT", "CURR"):
            raise ValueError(f"Unsupported FUNC:MODE '{mode}'")
        voltage_compliance = (
            self.default_voltage_limits()
            if voltage_compliance is None else voltage_compliance
        )
        current_limit = (
            self.default_current_limits()
            if current_limit is None else current_limit
        )
        cmds = [
            f"FUNC:MODE {mode}",
            f"{mode}:RANG 1",
        ]
        if mode == "CURR":
            cmds.extend(self.signed_limit_cmds("VOLT", voltage_compliance))
        else:
            cmds.extend(self.signed_limit_cmds("CURR", current_limit))
        return cmds

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
        return self.send_query("*IDN?")

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
        try:
            active_mode = self._normalize_func_mode(self.send_query("FUNC:MODE?"))
            if not active_mode:
                return False, "Could not determine active FUNC:MODE"

            mode_state = self.send_query(f"{active_mode}:MODE?")
            if mode_state is None:
                return False, (
                    f"Could not query {active_mode}:MODE?: {self.last_error}")

            mode_text = str(mode_state).strip().upper()
            if "LIST" not in mode_text:
                return True, "Active mode already fixed"

            for cmd in [f"{active_mode} 0", f"{active_mode}:MODE FIX", "*WAI"]:
                if self.send_cmd(cmd) is None:
                    return False, f"Disarm '{cmd}' failed: {self.last_error}"
            return True, f"{active_mode} LIST mode disarmed"
        except Exception as e:
            return False, str(e)

    # -- List upload (single chunk <= 1000 pts) -----------------------------
    def upload_list_chunk(self, points, dwell, mode="VOLT",
                          progress_cb=None, voltage_compliance=None,
                          current_limit=None):
        """Upload one chunk (<= 1000 points) with paced writes + verification.

                Strategy:
                    1. Disarm: switch the active LIST program back to FIX
                    2. Setup: FUNC:MODE, RANG, LIST:CLE, *WAI
          3. Values: send LIST:{mode} batches of <= 20 values each,
             each followed only by the mandatory 35 ms gap
                    4. Dwell: send LIST:DWEL once after values
                    5. Verify: *WAI -> LIST:{mode}:POIN? -> SYST:ERR?

        Key change from previous revision: *OPC? is NOT used anywhere
        in the upload path.  The manual (PAR A.17) recommends *WAI for
        sequential command synchronization - it blocks the device's
        command processor (no response to time-out on).

        progress_cb(sent, total) is called after each batch if provided.
        """
        with self._lock:
            if not self.connected and not self._safe_reconnect():
                return False, "Not connected"
            if not points:
                return False, "Empty point list"
            if len(points) > MAX_LIST_POINTS:
                return False, f"Chunk exceeds {MAX_LIST_POINTS} points"
            mode = (mode or "VOLT").upper()
            if mode not in ("VOLT", "CURR"):
                return False, f"Unsupported list mode '{mode}'"

            try:
                # Clear stale error queue first so prior test noise is not
                # reported as a fresh upload failure.
                self.drain_errors()

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
                setup_cmds = self._limit_setup_cmds(
                    mode, voltage_compliance, current_limit)
                setup_cmds.extend([
                    "LIST:CLE",
                    "*WAI",                   # wait for LIST:CLE (PAR A.17)
                ])
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

                pcount_str = self.send_query(f"LIST:{mode}:POIN?")
                if pcount_str is not None:
                    try:
                        actual_count = int(pcount_str.strip())
                        if actual_count != total:
                            return False, (
                                f"Point count mismatch: sent {total}, "
                                f"device reports {actual_count}")
                    except ValueError:
                        pass  # non-numeric, skip verify

                errors = self.drain_errors(fail_on_timeout=True)
                if errors is None:
                    return False, "SYST:ERR? timeout during verification"
                if errors:
                    return False, f"Device errors: {'; '.join(errors)}"

                return True, (
                    f"{total} pts @ {dwell*1000:.3f} ms/step (verified)")

            except Exception as e:
                return False, str(e)

    # Run / Stop 
    def run_list(self, mode="VOLT", count=1, enable_output=True,
                 voltage_compliance=None, current_limit=None,
                 apply_limit_setup=True):
        """Start LIST execution.

        When enable_output is True the standard sequence is:
          setup limits -> zero fixed source -> COUNT -> OUTP ON -> {mode}:MODE LIST

        When enable_output is False the current output state is preserved.  The
        upload path has already applied limits, so live re-arms can skip the
        fixed-source setup that can disturb an active AC waveform.
        """
        mode = (mode or "VOLT").upper()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported list mode '{mode}'"
        with self._lock:
            try:
                cmds = []
                if apply_limit_setup:
                    cmds.extend(self._limit_setup_cmds(
                        mode, voltage_compliance, current_limit))
                if enable_output:
                    cmds.append(f"{mode} 0")
                cmds.append(f"LIST:COUN {count}")
                if enable_output:
                    cmds.append("OUTP ON")
                cmds.append(f"{mode}:MODE LIST")

                ok, run_msg = self.send_sequence(
                    cmds,
                    label=(
                        f"LIST run setup ({mode})"
                        if apply_limit_setup else f"LIST run arm ({mode})"))
                if not ok:
                    return False, run_msg

                outp = (self.send_query("OUTP?") or "").strip().upper()
                mode_state = (self.send_query(f"{mode}:MODE?") or "").strip().upper()
                if enable_output and outp not in ("1", "ON"):
                    return False, "Run verification failed: output not enabled"
                if mode_state and "LIST" not in mode_state:
                    return False, (
                        f"Run verification failed: {mode}:MODE is '{mode_state}'")
                return True, "Running"
            except Exception as e:
                return False, str(e)

    def stop(self, base_mode="VOLT"):
        """Stop LIST, return to safe fixed-output state."""
        base_mode = (base_mode or "VOLT").upper()
        with self._lock:
            try:
                for cmd in [
                    "VOLT:MODE FIX",
                    "CURR:MODE FIX",
                    "OUTP OFF",
                    f"FUNC:MODE {base_mode}",
                ]:
                    if self.send_cmd(cmd) is None:
                        return False, f"Stop '{cmd}' failed: {self.last_error}"
                return True, "Stopped"
            except Exception as e:
                return False, str(e)

#  Network Discovery
class Discovery:
    """Scan a /24 subnet for Kepco devices (Telnet 5024 first, then 5025)."""

    @staticmethod
    def _probe(ip_str, timeout=DISCOVERY_TIMEOUT):
        for port in (TELNET_PORT, SCPI_SOCKET_PORT):
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
            net = ipaddress.IPv4Network("192.168.50.0/24") #let's use 50

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
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.root = ctk.CTk()
        self.root.title("Kepco BIT 802E - Waveform Generator")
        self.root.geometry("1365x845")
        self.root.minsize(1180, 760)

        self.kepco = KepcoController()
        self.stop_event = threading.Event()

        self.csv_points = None
        self.csv_name = ""
        self.preview_points = []
        self.uploaded_request = None
        self.uploaded_waveform_ready = False
        self.current_output_on = False
        self.sequence_active = False
        self.is_running = False

        self._connect_in_flight = False
        self._upload_in_flight = False
        self._output_toggle_in_flight = False
        self._status_poll_enabled = False
        self._status_poll_paused = False
        self._status_poll_in_flight = False
        self._status_poll_timer = None
        self._measurement_guard = None

        self.log_file_handle = None
        self.log_file_path = ""
        self.data_collection_file_handle = None
        self.data_collection_file_path = ""
        self.data_collection_writer = None
        self.data_collection_started_at = None
        self.data_collection_enabled = False
        self._data_collection_switch_updating = False

        self.current_control_mode = "VOLT"
        self.control_mode_var = ctk.StringVar(value="VOLT")
        self._ui_queue = queue.SimpleQueue()
        self._ui_queue_job = None
        self._ui_shutdown = False

        self._init_log_file()
        self.kepco.set_debug_logger(self._controller_debug_log)
        self._build_ui()
        self._start_ui_dispatcher()
        self._reset_live_status()
        self._reset_uploaded_state()
        self._on_wave_change()

        if self.log_file_path:
            self.log(f"Session log file: {self.log_file_path}", "info")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        conn = ctk.CTkFrame(self.root, corner_radius=10)
        conn.pack(fill="x", padx=12, pady=(10, 4))

        ctk.CTkLabel(conn, text="IP Address:",
                     font=ctk.CTkFont(size=13)).pack(side="left", padx=(14, 4))
        self.ip_var = ctk.StringVar(value="192.168.50.10")
        self.ip_combo = ctk.CTkComboBox(
            conn, variable=self.ip_var, values=["192.168.50.10"],
            width=200, font=ctk.CTkFont(size=13))
        self.ip_combo.pack(side="left", padx=4)

        self.scan_btn = ctk.CTkButton(
            conn, text="Scan Network", width=140,
            command=self._start_scan,
            fg_color="#374151", hover_color="#4b5563",
            font=ctk.CTkFont(size=12))
        self.scan_btn.pack(side="left", padx=6)

        self.conn_btn = ctk.CTkButton(
            conn, text="Connect", width=110,
            command=self._toggle_connect,
            fg_color=C["primary"], hover_color=C["primary_h"],
            font=ctk.CTkFont(size=13, weight="bold"))
        self.conn_btn.pack(side="left", padx=6)

        self.status_lbl = ctk.CTkLabel(
            conn, text="Disconnected", text_color=C["red"],
            font=ctk.CTkFont(size=13))
        self.status_lbl.pack(side="left", padx=14)

        self.idn_lbl = ctk.CTkLabel(
            conn, text="", text_color=C["text2"],
            font=ctk.CTkFont(size=11, slant="italic"))
        self.idn_lbl.pack(side="right", padx=14)

        main = ctk.CTkFrame(self.root, corner_radius=12)
        main.pack(fill="both", expand=True, padx=12, pady=4)
        main.grid_columnconfigure(0, weight=11)
        main.grid_columnconfigure(1, weight=9)
        main.grid_rowconfigure(0, weight=1)

        left = ctk.CTkFrame(main, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=(8, 4), pady=8)
        left.grid_rowconfigure(0, weight=1)
        left.grid_columnconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(left, corner_radius=12)
        self.tabview.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        wf_tab = self.tabview.add("Waveform Generator")
        man_tab = self.tabview.add("Manual Override")
        self._build_waveform_tab(wf_tab)
        self._build_manual_tab(man_tab)

        right = ctk.CTkFrame(main, corner_radius=12)
        right.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
        right.grid_rowconfigure(0, weight=1)
        right.grid_columnconfigure(0, weight=1)
        self._build_status_panel(right)

        log_wrap = ctk.CTkFrame(self.root, corner_radius=10)
        log_wrap.pack(fill="both", padx=12, pady=(0, 10))
        self.log_text = ctk.CTkTextbox(
            log_wrap, height=120,
            font=ctk.CTkFont(family="Consolas", size=11),
            activate_scrollbars=True)
        self.log_text.pack(fill="both", padx=6, pady=6, expand=True)

    def _build_waveform_tab(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(
            outer, text="Control Panel",
            font=ctk.CTkFont(size=18, weight="bold")).pack(
            anchor="w", padx=6, pady=(4, 10))

        body = ctk.CTkFrame(outer, fg_color="transparent")
        body.pack(fill="both", expand=True)
        body.grid_columnconfigure(0, weight=5)
        body.grid_columnconfigure(1, weight=3)
        body.grid_rowconfigure(0, weight=1)
        body.grid_rowconfigure(1, weight=0)

        preview_card = ctk.CTkFrame(body, corner_radius=12)
        preview_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 6))
        ctk.CTkLabel(
            preview_card, text="Preview Waveform",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 8))
        preview_plot_wrap = ctk.CTkFrame(
            preview_card, corner_radius=10, fg_color=C["graph_bg"])
        preview_plot_wrap.pack(fill="both", expand=True, padx=12, pady=(0, 8))
        self.preview_fig, self.preview_ax, self.preview_canvas = self._build_plot(
            preview_plot_wrap, (6.6, 3.8))

        cfg = ctk.CTkFrame(body, width=285, corner_radius=12)
        cfg.grid(row=0, column=1, sticky="nsew", pady=(0, 6))

        ctk.CTkLabel(
            cfg, text="Waveform Configuration",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            padx=14, pady=(14, 10))

        self._lbl(cfg, "Waveform Type")
        self.wave_var = ctk.StringVar(value="Sine")
        self.wave_combo = ctk.CTkComboBox(
            cfg, variable=self.wave_var,
            values=["DC", "Sine", "Square", "Triangle",
                    "Sawtooth", "CSV Custom (untested)"],
            command=self._on_wave_change)
        self.wave_combo.pack(fill="x", padx=14, pady=(0, 6))

        self.csv_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        self.csv_btn = ctk.CTkButton(
            self.csv_frame, text="Load CSV", width=110,
            command=self._load_csv,
            fg_color="#374151", hover_color="#4b5563")
        self.csv_btn.pack(side="left", padx=(0, 8))
        self.csv_lbl = ctk.CTkLabel(
            self.csv_frame, text="No file",
            text_color=C["text2"], font=ctk.CTkFont(size=11))
        self.csv_lbl.pack(side="left")

        self.freq_label = self._lbl(cfg, "Frequency (Hz)")
        self.freq_entry = ctk.CTkEntry(cfg, placeholder_text="40.0")
        self.freq_entry.insert(0, "40.0")
        self.freq_entry.pack(fill="x", padx=14, pady=(0, 6))

        self.amp_label = self._lbl(cfg, "Amplitude (V / A)")
        self.amp_entry = ctk.CTkEntry(cfg, placeholder_text="10.0")
        self.amp_entry.insert(0, "10.0")
        self.amp_entry.pack(fill="x", padx=14, pady=(0, 6))

        self.off_label = self._lbl(cfg, "Offset (V / A)")
        self.off_entry = ctk.CTkEntry(cfg, placeholder_text="0.0")
        self.off_entry.insert(0, "0.0")
        self.off_entry.pack(fill="x", padx=14, pady=(0, 6))

        self.pts_label = self._lbl(cfg, "Total Points (max 4000)")
        self.pts_entry = ctk.CTkEntry(cfg, placeholder_text="1000")
        self.pts_entry.insert(0, "1000")
        self.pts_entry.pack(fill="x", padx=14, pady=(0, 6))

        self.loop_label = self._lbl(cfg, "Loop Count (0 = infinite)")
        self.loop_entry = ctk.CTkEntry(cfg, placeholder_text="0")
        self.loop_entry.insert(0, "0")
        self.loop_entry.pack(fill="x", padx=14, pady=(0, 8))

        ctk.CTkButton(
            cfg, text="Preview Waveform", command=self._preview,
            fg_color="#374151", hover_color="#4b5563",
            font=ctk.CTkFont(size=12)).pack(
            fill="x", padx=14, pady=(4, 4))

        self.timing_lbl = ctk.CTkLabel(
            cfg, text="", text_color=C["amber"],
            font=ctk.CTkFont(size=11), wraplength=250, justify="left")
        self.timing_lbl.pack(fill="x", padx=14, pady=(4, 12))

        footer = ctk.CTkFrame(body, corner_radius=14)
        footer.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 2))
        footer.grid_columnconfigure(0, weight=1)
        footer.grid_columnconfigure(1, weight=0)

        left_controls = ctk.CTkFrame(footer, fg_color="transparent")
        left_controls.grid(row=0, column=0, sticky="ew", padx=(14, 12), pady=12)
        left_controls.grid_columnconfigure(1, weight=1)

        self.upload_btn = ctk.CTkButton(
            left_controls, text="Upload", width=150, height=42,
            command=self._upload_waveform,
            fg_color=C["green"], hover_color="#059669",
            text_color="#000", font=ctk.CTkFont(size=14, weight="bold"))
        self.upload_btn.grid(row=0, column=0, rowspan=2, sticky="ns", padx=(0, 14))
        self.prog_lbl = ctk.CTkLabel(
            left_controls, text="No upload yet",
            text_color=C["text2"], font=ctk.CTkFont(size=12, weight="bold"))
        self.prog_lbl.grid(row=0, column=1, sticky="w", pady=(1, 6))
        self.progress = ctk.CTkProgressBar(left_controls, height=14)
        self.progress.grid(row=1, column=1, sticky="ew")
        self.progress.set(0)
        self.data_collection_switch = ctk.CTkSwitch(
            left_controls, text="Collect data", width=150,
            command=self._toggle_data_collection,
            switch_width=34, switch_height=18,
            progress_color=C["amber"], text_color=C["text2"],
            font=ctk.CTkFont(size=11))
        self.data_collection_switch.grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))

        switch_card = ctk.CTkFrame(footer, width=290, height=144, corner_radius=12)
        switch_card.grid(row=0, column=1, sticky="ns", padx=(0, 14), pady=10)
        switch_card.pack_propagate(False)
        switch_row = ctk.CTkFrame(switch_card, fg_color="transparent")
        switch_row.pack(fill="x", padx=18, pady=(16, 8))
        ctk.CTkLabel(
            switch_row, text="Output Control",
            font=ctk.CTkFont(size=16, weight="bold")).pack(side="left")
        self.output_state_badge = ctk.CTkLabel(
            switch_row, text="OFFLINE", width=78, height=24, corner_radius=12,
            fg_color=C["red"], font=ctk.CTkFont(size=11, weight="bold"))
        self.output_state_badge.pack(side="right")
        self.output_summary_lbl = ctk.CTkLabel(
            switch_card, text="Disconnected",
            text_color=C["text2"], anchor="w",
            font=ctk.CTkFont(size=12))
        self.output_summary_lbl.pack(fill="x", padx=18, pady=(0, 10))
        self.output_toggle_btn = ctk.CTkButton(
            switch_card, text="Connect to Arm Output",
            command=self._toggle_output, height=48,
            corner_radius=10, font=ctk.CTkFont(size=15, weight="bold"),
            fg_color="#374151", hover_color="#4b5563")
        self.output_toggle_btn.pack(fill="x", padx=18)
        self.output_hint_lbl = ctk.CTkLabel(
            switch_card, text="Upload a waveform to enable output.",
            text_color=C["text2"], wraplength=250,
            justify="left", font=ctk.CTkFont(size=11))
        self.output_hint_lbl.pack(fill="x", padx=18, pady=(10, 14))

    def _build_manual_tab(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(
            outer, text="Config/Override Panel",
            font=ctk.CTkFont(size=18, weight="bold")).pack(
            anchor="w", padx=6, pady=(4, 10))

        console = ctk.CTkFrame(outer, corner_radius=12)
        console.pack(fill="both", expand=True, pady=(0, 10))

        ctk.CTkLabel(
            console, text="Manual Command Console",
            font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 4))
        ctk.CTkLabel(
            console,
            text="Enter any SCPI command/query, or use quick commands below.",
            text_color=C["text2"], font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=14, pady=(0, 8))

        scpi_row = ctk.CTkFrame(console, fg_color="transparent")
        scpi_row.pack(fill="x", padx=14, pady=(0, 6))
        ctk.CTkLabel(
            scpi_row, text="CMD:",
            font=ctk.CTkFont(family="Consolas", size=12)).pack(
            side="left", padx=(0, 4))
        self.scpi_entry = ctk.CTkEntry(
            scpi_row, placeholder_text="e.g. *IDN? or FUNC:MODE CURR",
            font=ctk.CTkFont(family="Consolas", size=12))
        self.scpi_entry.pack(side="left", fill="x", expand=True, padx=4)
        self.scpi_entry.bind("<Return>", lambda _e: self._man_send_scpi())
        ctk.CTkButton(
            scpi_row, text="Send", width=80, command=self._man_send_scpi,
            fg_color=C["primary"], hover_color=C["primary_h"]).pack(
            side="left", padx=4)

        quick_row = ctk.CTkFrame(console, fg_color="transparent")
        quick_row.pack(fill="x", padx=14, pady=(0, 4))
        for label, cmd in [
            ("*IDN?", "*IDN?"),
            ("SYST:ERR?", "SYST:ERR?"),
            ("OUTP?", "OUTP?"),
            ("MEAS:VOLT?", "MEAS:VOLT?"),
            ("MEAS:CURR?", "MEAS:CURR?"),
        ]:
            ctk.CTkButton(
                quick_row, text=label, width=98,
                command=lambda c=cmd: self._man_send_preset(c),
                fg_color="#374151", hover_color="#4b5563").pack(
                side="left", padx=(0, 6))

        quick_row2 = ctk.CTkFrame(console, fg_color="transparent")
        quick_row2.pack(fill="x", padx=14, pady=(0, 6))
        for label, cmd in [
            ("*OPC?", "*OPC?"),
            ("FUNC:MODE?", "FUNC:MODE?"),
            ("LIST:VOLT:POIN?", "LIST:VOLT:POIN?"),
            ("LIST:CURR:POIN?", "LIST:CURR:POIN?"),
        ]:
            ctk.CTkButton(
                quick_row2, text=label, width=118,
                command=lambda c=cmd: self._man_send_preset(c),
                fg_color="#374151", hover_color="#4b5563").pack(
                side="left", padx=(0, 6))

        scpi_ctrl = ctk.CTkFrame(console, fg_color="transparent")
        scpi_ctrl.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkButton(
            scpi_ctrl, text="Health Check", width=120,
            command=self._man_health_check,
            fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 8))
        ctk.CTkButton(
            scpi_ctrl, text="Clear Console", width=120,
            command=self._man_clear_scpi,
            fg_color="#374151", hover_color="#4b5563").pack(side="left")

        self.scpi_resp = ctk.CTkTextbox(
            console, height=150,
            font=ctk.CTkFont(family="Consolas", size=11),
            activate_scrollbars=True)
        self.scpi_resp.pack(fill="both", expand=True, padx=14, pady=(4, 14))

        cards = ctk.CTkFrame(outer, fg_color="transparent")
        cards.pack(fill="x")
        cards.grid_columnconfigure(0, weight=1)
        cards.grid_columnconfigure(1, weight=1)
        cards.grid_columnconfigure(2, weight=1)

        mode_card = ctk.CTkFrame(cards, corner_radius=12)
        mode_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        ctk.CTkLabel(
            mode_card, text="Set Control Mode",
            font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 6))
        mode_row = ctk.CTkFrame(mode_card, fg_color="transparent")
        mode_row.pack(fill="x", padx=14, pady=(0, 10))
        self.mode_buttons = {}
        for mode in ("VOLT", "CURR"):
            btn = ctk.CTkButton(
                mode_row, text="Voltage" if mode == "VOLT" else "Current",
                width=100,
                command=lambda m=mode: self._select_control_mode(m))
            btn.pack(side="left", padx=(0, 8))
            self.mode_buttons[mode] = btn
        ctk.CTkLabel(
            mode_card, text="Waveform uploads use the selected mode.",
            text_color=C["text2"], font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=14, pady=(0, 12))

        limits_card = ctk.CTkFrame(cards, corner_radius=12)
        limits_card.grid(row=0, column=1, sticky="nsew", padx=4)
        ctk.CTkLabel(
            limits_card, text="Set V/I Limits",
            font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 6))

        v_row = ctk.CTkFrame(limits_card, fg_color="transparent")
        v_row.pack(fill="x", padx=14, pady=(0, 6))
        ctk.CTkLabel(v_row, text="Voltage limit (V):",
                     anchor="w", wraplength=220).pack(fill="x")
        v_ctrl = ctk.CTkFrame(v_row, fg_color="transparent")
        v_ctrl.pack(fill="x", pady=(3, 0))
        ctk.CTkLabel(v_ctrl, text="+", width=14).pack(side="left")
        self.soft_volt_pos_limit_entry = ctk.CTkEntry(v_ctrl, width=68)
        self.soft_volt_pos_limit_entry.insert(
            0, str(DEFAULT_POSITIVE_VOLTAGE_COMPLIANCE))
        self.soft_volt_pos_limit_entry.pack(side="left", padx=(0, 4))
        ctk.CTkLabel(v_ctrl, text="-", width=14).pack(side="left")
        self.soft_volt_neg_limit_entry = ctk.CTkEntry(v_ctrl, width=68)
        self.soft_volt_neg_limit_entry.insert(
            0, str(DEFAULT_NEGATIVE_VOLTAGE_COMPLIANCE))
        self.soft_volt_neg_limit_entry.pack(side="left", padx=(0, 4))
        ctk.CTkButton(
            v_ctrl, text="Set", width=54,
            command=lambda: self._set_software_limit("VOLT"),
            fg_color="#374151", hover_color="#4b5563").pack(side="left")

        c_row = ctk.CTkFrame(limits_card, fg_color="transparent")
        c_row.pack(fill="x", padx=14, pady=(0, 12))
        ctk.CTkLabel(c_row, text="Current limit (A):",
                     anchor="w", wraplength=220).pack(fill="x")
        c_ctrl = ctk.CTkFrame(c_row, fg_color="transparent")
        c_ctrl.pack(fill="x", pady=(3, 0))
        ctk.CTkLabel(c_ctrl, text="+", width=14).pack(side="left")
        self.soft_curr_pos_limit_entry = ctk.CTkEntry(c_ctrl, width=68)
        self.soft_curr_pos_limit_entry.insert(
            0, str(DEFAULT_POSITIVE_CURRENT_LIMIT))
        self.soft_curr_pos_limit_entry.pack(side="left", padx=(0, 4))
        ctk.CTkLabel(c_ctrl, text="-", width=14).pack(side="left")
        self.soft_curr_neg_limit_entry = ctk.CTkEntry(c_ctrl, width=68)
        self.soft_curr_neg_limit_entry.insert(
            0, str(DEFAULT_NEGATIVE_CURRENT_LIMIT))
        self.soft_curr_neg_limit_entry.pack(side="left", padx=(0, 4))
        ctk.CTkButton(
            c_ctrl, text="Set", width=54,
            command=lambda: self._set_software_limit("CURR"),
            fg_color="#374151", hover_color="#4b5563").pack(side="left")

        range_card = ctk.CTkFrame(cards, corner_radius=12)
        range_card.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        ctk.CTkLabel(
            range_card, text="Range Control",
            font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 6))
        ctk.CTkLabel(
            range_card, text="Full-scale avoids quarter-scale transients.",
            text_color=C["text2"], font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=14, pady=(0, 8))
        range_row = ctk.CTkFrame(range_card, fg_color="transparent")
        range_row.pack(fill="x", padx=14, pady=(0, 10))
        self.man_range_var = ctk.StringVar(value="Auto")
        self.man_range_combo = ctk.CTkComboBox(
            range_row, variable=self.man_range_var,
            values=["Auto", "Full Scale", "Quarter Scale"],
            width=150)
        self.man_range_combo.pack(side="left", padx=(0, 8))
        ctk.CTkButton(
            range_row, text="Set", width=60,
            command=self._man_set_range,
            fg_color="#374151", hover_color="#4b5563").pack(side="left")
        ctk.CTkFrame(range_card, height=2, fg_color=C["border"]).pack(
            fill="x", padx=14, pady=(2, 10))
        ctk.CTkButton(
            range_card, text="Reset Device (*RST)",
            command=self._man_reset,
            fg_color=C["red"], hover_color="#dc2626",
            font=ctk.CTkFont(size=13, weight="bold")).pack(
            fill="x", padx=14, pady=(0, 14))

        self._update_mode_buttons(self.control_mode_var.get())

    def _build_status_panel(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        outer.grid_columnconfigure(0, weight=1)
        outer.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(
            outer, text="Status Panel",
            font=ctk.CTkFont(size=18, weight="bold")).grid(
            row=0, column=0, sticky="w", padx=6, pady=(4, 10))

        content = ctk.CTkFrame(outer, fg_color="transparent")
        content.grid(row=1, column=0, sticky="nsew")
        content.grid_columnconfigure(0, weight=4)
        content.grid_columnconfigure(1, weight=2)
        content.grid_rowconfigure(0, weight=3)
        content.grid_rowconfigure(1, weight=2)

        plot_card = ctk.CTkFrame(content, corner_radius=12)
        plot_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        ctk.CTkLabel(
            plot_card, text="Active/Uploaded Waveform",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 8))
        status_plot_wrap = ctk.CTkFrame(
            plot_card, corner_radius=10, fg_color=C["graph_bg"])
        status_plot_wrap.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.status_fig, self.status_ax, self.status_canvas = self._build_plot(
            status_plot_wrap, (5.3, 3.2))

        cfg_card = ctk.CTkFrame(content, corner_radius=12)
        cfg_card.grid(row=0, column=1, sticky="nsew", pady=(0, 10))
        ctk.CTkLabel(
            cfg_card, text="Waveform Configuration",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=14, pady=(12, 8))
        self.status_cfg_labels = {}
        for key, title in [
            ("wave", "Waveform Type"),
            ("mode", "Control Mode"),
            ("frequency", "Frequency"),
            ("amplitude", "Amplitude / Value"),
            ("offset", "Offset"),
            ("points", "Total Points"),
            ("loop", "Loop Count"),
            ("device_state", "Device State"),
        ]:
            row = ctk.CTkFrame(cfg_card, fg_color="transparent")
            row.pack(fill="x", padx=14, pady=(0, 5))
            ctk.CTkLabel(
                row, text=title, text_color=C["text2"],
                font=ctk.CTkFont(size=11), width=110, anchor="w").pack(side="left")
            value = ctk.CTkLabel(
                row, text="--", font=ctk.CTkFont(size=11),
                justify="left", anchor="w")
            value.pack(side="left", fill="x", expand=True)
            self.status_cfg_labels[key] = value

        meas_card = ctk.CTkFrame(content, corner_radius=12, fg_color=C["graph_bg"])
        meas_card.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        ctk.CTkLabel(
            meas_card, text="Live Measurements",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=20, pady=(14, 8))
        self.status_meas_volt_lbl = ctk.CTkLabel(
            meas_card, text="Voltage:  ---.----  V",
            font=ctk.CTkFont(family="Consolas", size=20),
            text_color="#60a5fa")
        self.status_meas_volt_lbl.pack(anchor="w", padx=20, pady=(6, 4))
        self.status_meas_curr_lbl = ctk.CTkLabel(
            meas_card, text="Current:  ---.----  A",
            font=ctk.CTkFont(family="Consolas", size=20),
            text_color="#34d399")
        self.status_meas_curr_lbl.pack(anchor="w", padx=20, pady=(4, 14))
        self.status_meas_warn_lbl = ctk.CTkLabel(
            meas_card,
            text="",
            height=30,
            justify="left",
            anchor="w",
            wraplength=340,
            text_color=C["amber"],
            font=ctk.CTkFont(size=11, weight="bold"))
        self.status_meas_warn_lbl.pack(fill="x", padx=20, pady=(0, 14))

        info_card = ctk.CTkFrame(content, corner_radius=12)
        info_card.grid(row=1, column=1, sticky="nsew")
        ctk.CTkLabel(
            info_card, text="Output Status",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=18, pady=(18, 8))
        out_row = ctk.CTkFrame(info_card, fg_color="transparent")
        out_row.pack(fill="x", padx=18, pady=(0, 20))
        self.status_output_pill = ctk.CTkLabel(
            out_row, text="OFF", width=72, height=34,
            corner_radius=6, fg_color=C["red"],
            text_color="#ffffff",
            font=ctk.CTkFont(size=13, weight="bold"))
        self.status_output_pill.pack(side="left")

        ctk.CTkLabel(
            info_card, text="Control Mode",
            font=ctk.CTkFont(size=15, weight="bold")).pack(
            anchor="w", padx=18, pady=(0, 8))
        mode_row = ctk.CTkFrame(info_card, fg_color="transparent")
        mode_row.pack(fill="x", padx=18, pady=(0, 18))
        self.status_mode_labels = {}
        for mode, label in (("VOLT", "Volt"), ("CURR", "Curr")):
            pill = ctk.CTkLabel(
                mode_row, text=label, width=72, height=34,
                corner_radius=6, fg_color=C["card"],
                font=ctk.CTkFont(size=13, weight="bold"))
            pill.pack(side="left", padx=(0, 8))
            self.status_mode_labels[mode] = pill

    def _build_plot(self, parent, figsize):
        fig = Figure(figsize=figsize, dpi=100, facecolor=C["graph_bg"])
        ax = fig.add_subplot(111)
        self._style_ax(ax)
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=6, pady=6)
        return fig, ax, canvas

    def _style_ax(self, ax):
        ax.set_facecolor(C["graph_bg"])
        for spine in ax.spines.values():
            spine.set_color(C["border"])
        ax.tick_params(colors=C["text2"], labelsize=9)
        ax.xaxis.label.set_color(C["text2"])
        ax.yaxis.label.set_color(C["text2"])
        ax.grid(True, color="#2a2a40", linewidth=0.5, alpha=0.6)

    def _draw_waveform_plot(self, fig, ax, canvas, points=None, chunk_idx=-1,
                            empty_title="No waveform uploaded"):
        ax.clear()
        self._style_ax(ax)
        ax.set_xlabel("Sample Index")
        ax.set_ylabel("Amplitude (V / A)")

        if not points:
            ax.set_title(empty_title, color=C["text2"], fontsize=11)
            fig.tight_layout(pad=1.2)
            canvas.draw_idle()
            return

        chunk_sz = MAX_LIST_POINTS
        chunks = [points[i:i + chunk_sz] for i in range(0, len(points), chunk_sz)]
        colors = C["chunk_colors"]

        for ci, chunk in enumerate(chunks):
            start = ci * chunk_sz
            xs = list(range(start, start + len(chunk)))
            color = colors[ci % len(colors)]
            lw = 1.3
            alpha = 1.0

            if chunk_idx >= 0:
                if ci < chunk_idx:
                    alpha = 0.30
                elif ci == chunk_idx:
                    color = C["sent"]
                    lw = 2.8
                else:
                    alpha = 0.45

            ax.plot(xs, chunk, color=color, linewidth=lw, alpha=alpha)

        if len(chunks) > 1:
            for ci in range(1, len(chunks)):
                ax.axvline(ci * chunk_sz, color=C["border"],
                           linestyle="--", linewidth=0.7, alpha=0.6)
            if chunk_idx < 0:
                handles = [
                    mlines.Line2D(
                        [], [], color=colors[i % len(colors)], linewidth=2,
                        label=f"Chunk {i + 1} ({len(chunks[i])} pts)")
                    for i in range(len(chunks))
                ]
                ax.legend(handles=handles, fontsize=8, loc="upper right",
                          facecolor=C["card"], edgecolor=C["border"],
                          labelcolor=C["text2"])

        title = (
            f"Waveform - {len(points)} points, {len(chunks)} chunk(s)"
            if chunk_idx < 0
            else f"Uploading chunk {chunk_idx + 1}/{len(chunks)}"
        )
        ax.set_title(title, color=C["text"], fontsize=11)
        fig.tight_layout(pad=1.2)
        canvas.draw_idle()

    def _update_preview_plot(self, points=None):
        self._draw_waveform_plot(
            self.preview_fig, self.preview_ax, self.preview_canvas,
            points=points, empty_title="No waveform - configure and preview")

    def _update_status_plot(self, points=None, chunk_idx=-1):
        self._draw_waveform_plot(
            self.status_fig, self.status_ax, self.status_canvas,
            points=points, chunk_idx=chunk_idx,
            empty_title="No waveform uploaded")

    @staticmethod
    def _lbl(parent, text):
        label = ctk.CTkLabel(
            parent, text=text, text_color=C["text2"],
            font=ctk.CTkFont(size=12))
        label.pack(anchor="w", padx=14, pady=(6, 1))
        return label

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

    def _write_log_file_line(self, ts, tag, msg):
        if not self.log_file_handle:
            return
        try:
            self.log_file_handle.write(f"[{ts}] [{tag.upper()}] {msg}\n")
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
                output_text,
                mode_text,
            ])
            self.data_collection_file_handle.flush()
        except Exception as exc:
            self.data_collection_enabled = False
            self._close_data_collection_file()
            self._set_data_collection_switch(False)
            self.log(f"Data collection stopped: {exc}", "err")

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

    def log(self, msg, tag="info"):
        ts = time.strftime("%H:%M:%S")
        sym = {"info": "[i]", "ok": "[ok]", "warn": "[!]", "err": "[x]"}.get(tag, "[.]")
        self.log_text.insert("end", f"[{ts}] {sym} {msg}\n")
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

        if threading.current_thread() is threading.main_thread():
            self.log(f"[COMM] {msg}", tag)
        else:
            self._call_on_ui(lambda: self.log(f"[COMM] {msg}", tag))

    def _log_safe(self, msg, tag="info"):
        self._call_on_ui(lambda: self.log(msg, tag))

    def _set_connected_state(self, connected, idn=""):
        if connected:
            self.conn_btn.configure(
                text="Disconnect", fg_color=C["red"], hover_color="#dc2626")
            self.status_lbl.configure(text="Connected", text_color=C["green"])
            self.idn_lbl.configure(text=idn)
        else:
            self.conn_btn.configure(
                text="Connect", fg_color=C["primary"], hover_color=C["primary_h"])
            self.status_lbl.configure(text="Disconnected", text_color=C["red"])
            self.idn_lbl.configure(text="")
        self._update_output_controls()

    def _handle_comm_failure(self, context):
        if self.kepco.connected:
            return
        self._stop_status_polling()
        self._connect_in_flight = False
        self._upload_in_flight = False
        self._output_toggle_in_flight = False
        self.stop_event.set()
        self.sequence_active = False
        self.is_running = False
        self._set_connected_state(False)
        self._reset_live_status()
        self._reset_uploaded_state()
        self.log(f"Connection lost during {context}: {self.kepco.last_error}", "err")

    def _reset_live_status(self):
        self._measurement_guard = None
        self.current_output_on = False
        self.current_control_mode = "VOLT"
        self.status_meas_volt_lbl.configure(text="Voltage:  ---.----  V")
        self.status_meas_curr_lbl.configure(text="Current:  ---.----  A")
        self._set_status_output_display(False)
        self._set_status_mode_display(None)
        self.control_mode_var.set("VOLT")
        if hasattr(self, "mode_buttons"):
            self._update_mode_buttons("VOLT")
        self._set_output_ui_state(False)
        self._refresh_live_measurement_warning()

    def _reset_uploaded_state(self):
        self.uploaded_request = None
        self.uploaded_waveform_ready = False
        self._update_status_plot(None)
        for label in self.status_cfg_labels.values():
            label.configure(text="--")
        self.status_cfg_labels["device_state"].configure(text="No waveform uploaded")
        self.prog_lbl.configure(text="No upload yet")
        self.progress.set(0)
        self._set_output_ui_state(False)
        self._refresh_live_measurement_warning()
        self._update_output_controls()

    def _refresh_uploaded_status_panel(self):
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

        if req["wave"] == "DC":
            device_state = "Fixed setpoint staged"
        elif req["point_count"] <= MAX_LIST_POINTS:
            device_state = "LIST uploaded"
        else:
            device_state = "First chunk uploaded; full sequence staged"

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

    def _set_output_ui_state(self, is_on):
        self.current_output_on = bool(is_on)
        self._refresh_output_toggle_button()
        self._set_status_output_display(is_on)
        self._refresh_live_measurement_warning()

    def _set_status_output_display(self, is_on):
        self.status_output_pill.configure(
            text="ON" if is_on else "OFF",
            fg_color=C["green"] if is_on else C["red"],
            text_color="#ffffff")

    def _refresh_output_toggle_button(self, can_toggle=None):
        if can_toggle is None:
            can_toggle = (
                self.kepco.connected
                and self.uploaded_waveform_ready
                and not self._output_toggle_in_flight
            )
            if self.sequence_active:
                can_toggle = True

        badge_text = "OFFLINE"
        badge_color = C["red"]
        badge_text_color = "#ffffff"
        summary = "Disconnected"
        button_text = "Connect to Arm Output"
        button_color = "#374151"
        button_hover = "#4b5563"
        button_text_color = "#e5e7eb"

        if self.kepco.connected and not self.uploaded_waveform_ready:
            badge_text = "LOCKED"
            badge_color = C["amber"]
            badge_text_color = "#111827"
            summary = "Awaiting waveform upload"
            button_text = "Upload Waveform First"
        elif self._output_toggle_in_flight:
            badge_text = "APPLYING"
            badge_color = C["amber"]
            badge_text_color = "#111827"
            summary = "Applying output change"
            button_text = "Applying..."
            button_color = "#475569"
            button_hover = "#475569"
        elif self.current_output_on:
            badge_text = "LIVE"
            badge_color = C["green"]
            summary = "Streaming waveform" if self.sequence_active else "Output enabled"
            button_text = "Disable Output"
            button_color = C["red"]
            button_hover = "#dc2626"
        elif self.kepco.connected and self.uploaded_waveform_ready:
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
        upload_state = "disabled" if (self._upload_in_flight or self.sequence_active) else "normal"
        self.upload_btn.configure(state=upload_state)

        can_toggle = (
            self.kepco.connected
            and self.uploaded_waveform_ready
            and not self._output_toggle_in_flight
        )
        if self.sequence_active:
            can_toggle = True

        self._refresh_output_toggle_button(can_toggle)

        if not self.kepco.connected:
            hint = "Connect to a Kepco to control output."
        elif not self.uploaded_waveform_ready:
            hint = "Upload a waveform to enable output."
        elif self.sequence_active:
            hint = "Streaming multi-chunk waveform."
        elif self._output_toggle_in_flight:
            hint = "Applying output change..."
        else:
            hint = "Output follows the last uploaded waveform."
        self.output_hint_lbl.configure(text=hint)

    def _update_mode_buttons(self, active_mode):
        for mode, btn in self.mode_buttons.items():
            active = mode == active_mode
            btn.configure(
                fg_color=C["green"] if active else "#4b5563",
                hover_color="#059669" if active else "#6b7280")

    def _set_status_mode_display(self, mode):
        for key, label in self.status_mode_labels.items():
            active = key == mode
            label.configure(
                fg_color=C["green"] if active else C["card"],
                text_color="#ffffff" if active else C["text"])

    def _is_live_measurement_warning_active(self):
        req = self.uploaded_request or {}
        return bool(
            self.kepco.connected
            and self.current_output_on
            and req.get("kind") == "LIST"
            and req.get("wave") not in ("", None, "DC")
        )

    def _set_live_measurement_warning_visible(self, visible):
        text = ""
        if visible and self._is_live_measurement_warning_active():
            text = (
                "Warning: BIT 802E readback may be inaccurate while "
                "LIST-driven AC output is active."
            )
        self.status_meas_warn_lbl.configure(text=text, text_color=C["amber"])

    def _refresh_live_measurement_warning(self):
        if not hasattr(self, "status_meas_warn_lbl"):
            return
        self._set_live_measurement_warning_visible(
            self._is_live_measurement_warning_active())

    @staticmethod
    def _as_float(value):
        try:
            return float(value)
        except Exception:
            return None

    def _set_live_measurement_axis(self, mode, value):
        numeric = self._as_float(value)
        text = f"{numeric:.4f}" if numeric is not None else "---.----"
        if mode == "VOLT":
            self.status_meas_volt_lbl.configure(text=f"Voltage:  {text}  V")
        elif mode == "CURR":
            self.status_meas_curr_lbl.configure(text=f"Current:  {text}  A")

    def _set_entry_enabled(self, entry, enabled):
        entry.configure(
            state="normal" if enabled else "disabled",
            fg_color=C["input_bg"] if enabled else "#2d2d3a",
            text_color=C["text"] if enabled else C["text2"])

    def _on_wave_change(self, _=None):
        wave = self.wave_var.get()
        if wave == "CSV Custom (untested)":
            self.csv_frame.pack(fill="x", padx=14, pady=(0, 6), after=self.wave_combo)
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
                self.pts_entry.insert(0, str(min(len(self.csv_points), MAX_TOTAL_POINTS)))
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
            self.csv_points = points
            self.csv_name = os.path.basename(path)
            shown_points = min(len(points), MAX_TOTAL_POINTS)
            self.csv_lbl.configure(text=f"{self.csv_name} ({shown_points} pts)")
            self.pts_entry.configure(state="normal")
            self.pts_entry.delete(0, "end")
            self.pts_entry.insert(0, str(shown_points))
            self._set_entry_enabled(self.pts_entry, False)
            if len(points) > MAX_TOTAL_POINTS:
                self.log(
                    f"Loaded CSV {self.csv_name}; using first {MAX_TOTAL_POINTS} points.",
                    "warn")
            else:
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

    def _read_signed_limit_pair(self, pos_entry, neg_entry, name, max_abs):
        pos = abs(float(pos_entry.get().strip()))
        neg = -abs(float(neg_entry.get().strip()))
        if pos <= 0 or neg >= 0:
            raise ValueError(f"{name} limits must be nonzero.")
        if pos > max_abs or abs(neg) > max_abs:
            raise ValueError(f"{name} limits must be within +/-{max_abs:.1f}.")
        self._normalize_limit_entry_text(pos_entry, pos)
        self._normalize_limit_entry_text(neg_entry, neg)
        return pos, neg

    def _get_device_limits_from_ui(self, show_error=False):
        try:
            voltage_limits = self._read_signed_limit_pair(
                self.soft_volt_pos_limit_entry,
                self.soft_volt_neg_limit_entry,
                "Voltage compliance / limit",
                BOP_MAX_VOLTAGE)
            current_limits = self._read_signed_limit_pair(
                self.soft_curr_pos_limit_entry,
                self.soft_curr_neg_limit_entry,
                "Current limit",
                BOP_MAX_CURRENT)
            return voltage_limits, current_limits
        except Exception as exc:
            if show_error:
                messagebox.showerror(
                    "Device Limits",
                    str(exc) if str(exc) else
                    "Voltage and current limits must be valid signed numbers.")
            return None

    def _get_request_limits(self, req):
        req = req or {}
        voltage_limits = req.get(
            "voltage_limits",
            req.get("voltage_compliance", KepcoController.default_voltage_limits()))
        current_limits = req.get(
            "current_limits",
            req.get("current_limit", KepcoController.default_current_limits()))
        return (
            KepcoController.limit_pair(
                voltage_limits,
                DEFAULT_POSITIVE_VOLTAGE_COMPLIANCE,
                DEFAULT_NEGATIVE_VOLTAGE_COMPLIANCE),
            KepcoController.limit_pair(
                current_limits,
                DEFAULT_POSITIVE_CURRENT_LIMIT,
                DEFAULT_NEGATIVE_CURRENT_LIMIT),
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
        limited_req["voltage_limits"] = limits["VOLT"]
        limited_req["current_limits"] = limits["CURR"]
        return limited_req

    def _log_scpi_sequence(self, label, cmds):
        self._log_safe(f"{label}: {'; '.join(cmds)}", "info")

    def _apply_device_limits(self, mode, voltage_compliance, current_limit):
        mode = (mode or "VOLT").upper()
        try:
            cmds = self.kepco._limit_setup_cmds(
                mode, voltage_compliance, current_limit)
        except ValueError as exc:
            return False, str(exc)
        self._log_scpi_sequence(
            f"Applying {mode} complementary limit setup", cmds)
        return self.kepco.send_sequence(
            cmds, label=f"{mode} complementary limit setup")

    def _safe_prepare_output(self, mode, initial_setpoint=0.0,
                             voltage_compliance=None, current_limit=None,
                             label="Safe output prepare"):
        mode = (mode or "VOLT").upper()
        if voltage_compliance is None:
            voltage_compliance = KepcoController.default_voltage_limits()
        if current_limit is None:
            current_limit = KepcoController.default_current_limits()
        if mode not in ("VOLT", "CURR"):
            return False, f"Unsupported FUNC:MODE '{mode}'"

        cmds = [
            "VOLT:MODE FIX",
            "CURR:MODE FIX",
            f"FUNC:MODE {mode}",
            f"{mode}:RANG 1",
        ]
        initial = KepcoController.format_scpi_value(initial_setpoint)
        if mode == "CURR":
            cmds.extend(KepcoController.signed_limit_cmds(
                "VOLT", voltage_compliance))
            cmds.append(f"CURR {initial}")
        else:
            cmds.extend(KepcoController.signed_limit_cmds(
                "CURR", current_limit))
            cmds.append(f"VOLT {initial}")
        self._log_scpi_sequence(label, cmds)
        return self.kepco.send_sequence(cmds, label=label)

    def _get_software_limits(self, show_error=False):
        limits = self._get_device_limits_from_ui(show_error=show_error)
        if not limits:
            return None
        voltage_limits, current_limits = limits
        return {"VOLT": voltage_limits, "CURR": current_limits}

    def _set_software_limit(self, mode):
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return
        unit = "V" if mode == "VOLT" else "A"
        pos_limit, neg_limit = limits[mode]
        if not self._man_require_conn():
            self.log(
                f"{mode} limits staged locally at "
                f"{neg_limit:.4f} to {pos_limit:.4f} {unit}; "
                "connect to send it to the device.",
                "warn")
            return

        mode_resp = self.kepco.send("FUNC:MODE?", query=True)
        active_mode = KepcoController._normalize_func_mode(mode_resp)
        if not active_mode:
            self.log(
                "Limit command not sent; could not confirm device control mode.",
                "err")
            if not self.kepco.connected:
                self._handle_comm_failure("query control mode for limit set")
            return

        if active_mode != self.control_mode_var.get().upper():
            self.current_control_mode = active_mode
            self.control_mode_var.set(active_mode)
            self._update_mode_buttons(active_mode)

        is_complementary = (
            (active_mode == "CURR" and mode == "VOLT")
            or (active_mode == "VOLT" and mode == "CURR")
        )
        if not is_complementary:
            self.log(
                f"{mode} is the active output channel in {active_mode} mode; "
                "this field is staged as a UI/software limit only.",
                "warn")
            return

        cmds = KepcoController.signed_limit_cmds(mode, limits[mode])
        self.log(f"Sending device limit command(s): {'; '.join(cmds)}", "info")
        ok, msg = self.kepco.send_sequence(
            cmds, label=f"{mode} device limit command(s)")
        self.log(
            f"{mode} device limit set to {neg_limit:.4f} to {pos_limit:.4f} {unit}"
            if ok else f"Failed to send {mode} limit command(s): {msg}",
            "ok" if ok else "err")
        if ok:
            self._schedule_status_poll(100)
        else:
            self._handle_comm_failure(f"set {mode} limit")

    def _check_interlock(self, mode, points, context):
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return False
        return self._check_points_within_limits(mode, points, context, limits)

    def _check_points_within_limits(self, mode, points, context, limits):
        pos_limit, neg_limit = limits[mode]
        high = max(float(point) for point in points)
        low = min(float(point) for point in points)
        unit = "V" if mode == "VOLT" else "A"
        if high > pos_limit + 1e-12 or low < neg_limit - 1e-12:
            messagebox.showerror(
                "Software Interlock",
                f"{context} exceeds the configured {mode.lower()} limit.\n\n"
                f"Requested range: {low:.4f} to {high:.4f} {unit}\n"
                f"Limit range: {neg_limit:.4f} to {pos_limit:.4f} {unit}")
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
            "csv_name": None, "first_chunk_primed": False,
        }

    def _build_csv_request(self, mode):
        if not self.csv_points:
            messagebox.showerror("Input Error", "Load a CSV file first.")
            return None
        freq = self._read_float(self.freq_entry, "frequency")
        loop = self._read_int(self.loop_entry, "loop count")
        if freq is None or loop is None or loop < 0:
            if loop is not None and loop < 0:
                messagebox.showerror("Input Error", "Loop count must be 0 or greater.")
            return None
        point_count = min(len(self.csv_points), MAX_TOTAL_POINTS)
        actual, dwell, actual_freq, warns = WaveformGen.calculate_timing(freq, point_count)
        if actual == 0:
            messagebox.showerror("Input Error", "\n".join(warns))
            return None
        points = list(self.csv_points[:actual])
        if len(points) < 2 or not self._check_interlock(mode, points, "CSV waveform"):
            if len(points) < 2:
                messagebox.showerror("Input Error", "CSV must contain at least 2 points.")
            return None
        if len(self.csv_points) > MAX_TOTAL_POINTS:
            warns.append(f"CSV truncated to {MAX_TOTAL_POINTS} points.")
        lines = [
            f"Points: {len(points)} ({math.ceil(len(points) / MAX_LIST_POINTS)} chunk(s))",
            f"Dwell: {dwell * 1000:.4f} ms",
            f"Actual frequency: {actual_freq:.4f} Hz",
        ]
        if len(points) > MAX_LIST_POINTS:
            lines.append("Waveforms over 1000 points stream in chunks when output is ON.")
        lines.extend([f"Warning: {warning}" for warning in warns])
        self._set_timing_lines(lines)
        return {
            "wave": "CSV Custom (untested)", "mode": mode, "kind": "LIST",
            "amplitude": None, "offset": None,
            "point_count": len(points), "loop": loop, "dwell": dwell,
            "actual_frequency": actual_freq,
            "points": points, "plot_points": points,
            "csv_name": self.csv_name, "first_chunk_primed": False,
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
        pts = min(pts, MAX_TOTAL_POINTS)
        actual, dwell, actual_freq, warns = WaveformGen.calculate_timing(freq, pts)
        if actual == 0:
            messagebox.showerror("Input Error", "\n".join(warns))
            return None
        points = WaveformGen.generate(self.wave_var.get(), actual, amp, offset)
        if not self._check_interlock(mode, points, f"{self.wave_var.get()} waveform"):
            return None
        lines = [
            f"Points: {len(points)} ({math.ceil(len(points) / MAX_LIST_POINTS)} chunk(s))",
            f"Dwell: {dwell * 1000:.4f} ms",
            f"Actual frequency: {actual_freq:.4f} Hz",
        ]
        if len(points) > MAX_LIST_POINTS:
            lines.append("Waveforms over 1000 points stream in chunks when output is ON.")
        lines.extend([f"Warning: {warning}" for warning in warns])
        self._set_timing_lines(lines)
        return {
            "wave": self.wave_var.get(), "mode": mode, "kind": "LIST",
            "amplitude": amp, "offset": offset,
            "point_count": len(points), "loop": loop, "dwell": dwell,
            "actual_frequency": actual_freq,
            "points": points, "plot_points": points,
            "csv_name": None, "first_chunk_primed": False,
        }

    def _read_waveform_request(self):
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
        req["voltage_limits"] = limits["VOLT"]
        req["current_limits"] = limits["CURR"]
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

    def _man_require_conn(self):
        if not self.kepco.connected:
            self.log("Not connected - connect first.", "warn")
            return False
        return True

    def _select_control_mode(self, mode):
        mode = mode.upper()
        self.current_control_mode = mode
        self.control_mode_var.set(mode)
        self._update_mode_buttons(mode)
        if not self.kepco.connected:
            self.log(f"Control mode preset to {mode}", "info")
            return
        limits = self._get_software_limits(show_error=True)
        if not limits:
            return
        if self.current_output_on:
            ok, msg = self._safe_prepare_output(
                mode,
                initial_setpoint=0.0,
                voltage_compliance=limits["VOLT"],
                current_limit=limits["CURR"],
                label=f"Manual {mode} mode safe prepare")
        else:
            ok, msg = self._apply_device_limits(
                mode, limits["VOLT"], limits["CURR"])
        self.log(
            f"Control mode -> {mode}" if ok else f"Failed to set control mode: {msg}",
            "ok" if ok else "err")
        if ok:
            self._schedule_status_poll(100)
        else:
            self._handle_comm_failure("set control mode")

    def _man_set_range(self):
        if not self._man_require_conn():
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
            cmds = [f"{mode}:RANG:AUTO OFF", f"{mode}:RANG 0"]
            label = "Quarter Scale"
        ok = True
        for cmd in cmds:
            ok = bool(self.kepco.send(cmd))
            if not ok:
                break
        self.log(
            f"{mode} range -> {label}" if ok else "Failed to set range",
            "ok" if ok else "err")
        if ok:
            self._schedule_status_poll(100)
        else:
            self._handle_comm_failure("set range")

    def _man_reset(self):
        if not self._man_require_conn():
            return
        if self.sequence_active or self._upload_in_flight:
            messagebox.showwarning(
                "Busy",
                "Stop the active upload/stream before resetting the device.")
            return
        ok = self.kepco.send("*RST")
        if ok:
            self.stop_event.set()
            self.sequence_active = False
            self.is_running = False
            self.current_control_mode = "VOLT"
            self.control_mode_var.set("VOLT")
            self._update_mode_buttons("VOLT")
            self._reset_live_status()
            self._reset_uploaded_state()
            self.log("Device reset (*RST)", "ok")
            self._schedule_status_poll(150)
        else:
            self.log("Reset failed", "err")
            self._handle_comm_failure("reset")

    def _man_send_scpi(self):
        if not self._man_require_conn():
            return
        cmd = self.scpi_entry.get().strip()
        if not cmd:
            return
        self._man_exec_scpi_command(cmd)
        self.scpi_entry.delete(0, "end")

    def _man_exec_scpi_command(self, cmd):
        is_query = cmd.rstrip().endswith("?")
        ts = time.strftime("%H:%M:%S")
        self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
        if is_query:
            resp = self.kepco.send(cmd, query=True)
            self.scpi_resp.insert("end", f"[{ts}] < {resp or '(no response)'}\n")
            if resp is None:
                self._handle_comm_failure(f"SCPI query '{cmd}'")
        else:
            ok = self.kepco.send(cmd)
            self.scpi_resp.insert("end", f"[{ts}] {'OK' if ok else 'FAILED'}\n")
            if not ok:
                self._handle_comm_failure(f"SCPI command '{cmd}'")
        self.scpi_resp.see("end")
        self.log(f"SCPI: {cmd}", "info")
        self._schedule_status_poll(150)

    def _man_send_preset(self, cmd):
        if not self._man_require_conn():
            return
        self.scpi_entry.delete(0, "end")
        self.scpi_entry.insert(0, cmd)
        self._man_exec_scpi_command(cmd)

    def _man_clear_scpi(self):
        self.scpi_resp.delete("1.0", "end")
        self.log("SCPI console cleared", "info")

    def _man_health_check(self):
        if not self._man_require_conn():
            return
        threading.Thread(target=self._man_health_check_worker, daemon=True).start()

    def _man_health_check_worker(self):
        ts = time.strftime("%H:%M:%S")
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

        self._call_on_ui(lambda: self._man_health_check_done(ts, results))

    def _man_health_check_done(self, ts, results):
        self.scpi_resp.insert("end", f"[{ts}] ==== Health Check ====\n")
        missing = False
        for cmd, resp in results:
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.insert("end", f"[{ts}] < {resp or '(no response)'}\n")
            if resp is None:
                missing = True
        self.scpi_resp.insert("end", f"[{ts}] =====================\n")
        self.scpi_resp.see("end")
        self.log("Manual health check complete", "ok")
        if missing:
            self._handle_comm_failure("health check")
        else:
            self._schedule_status_poll(150)

    def _schedule_status_poll(self, delay_ms=1000):
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None
        if self._status_poll_enabled:
            self._status_poll_timer = self.root.after(delay_ms, self._status_poll_tick)

    def _start_status_polling(self):
        self._status_poll_enabled = True
        self._status_poll_paused = False
        self._schedule_status_poll(100)

    def _stop_status_polling(self):
        self._status_poll_enabled = False
        self._status_poll_paused = False
        self._status_poll_in_flight = False
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None

    def _pause_status_polling(self):
        self._status_poll_paused = True
        if self._status_poll_timer:
            try:
                self.root.after_cancel(self._status_poll_timer)
            except Exception:
                pass
            self._status_poll_timer = None

    def _resume_status_polling(self):
        self._status_poll_paused = False
        if self._status_poll_enabled:
            self._schedule_status_poll(100)

    def _wait_for_status_poll_idle(self, timeout=2.0):
        """Block worker threads until an in-flight poll cycle fully unwinds."""
        deadline = time.time() + timeout
        while self._status_poll_in_flight and time.time() < deadline:
            time.sleep(0.05)
        return not self._status_poll_in_flight

    def _status_poll_tick(self):
        self._status_poll_timer = None
        if not self._status_poll_enabled or self._status_poll_paused or not self.kepco.connected:
            return
        if self._status_poll_in_flight:
            self._schedule_status_poll(250)
            return
        self._status_poll_in_flight = True
        threading.Thread(target=self._status_poll_worker, daemon=True).start()

    def _status_poll_worker(self):
        v = self.kepco.send("MEAS:VOLT?", query=True)
        c = self.kepco.send("MEAS:CURR?", query=True)
        outp = self.kepco.send("OUTP?", query=True)
        mode = self.kepco.send("FUNC:MODE?", query=True)
        self._call_on_ui(lambda: self._status_poll_done(v, c, outp, mode))

    def _status_poll_done(self, v, c, outp, mode):
        self._status_poll_in_flight = False
        if any(item is None for item in (v, c, outp, mode)):
            self._handle_comm_failure("status polling")
            return

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
            self._schedule_status_poll(1000)

    def _apply_live_status(self, v, c, outp, mode):
        try:
            v_str = f"{float(v):.4f}"
        except Exception:
            v_str = str(v).strip() or "---.----"
        try:
            c_str = f"{float(c):.4f}"
        except Exception:
            c_str = str(c).strip() or "---.----"
        out_text = str(outp).strip().upper()
        is_on = out_text in ("1", "ON")
        mode_text = str(mode).strip().upper()
        if mode_text == "0":
            mode_text = "VOLT"
        elif mode_text == "1":
            mode_text = "CURR"
        if mode_text not in ("VOLT", "CURR"):
            mode_text = None

        self.status_meas_volt_lbl.configure(text=f"Voltage:  {v_str}  V")
        self.status_meas_curr_lbl.configure(text=f"Current:  {c_str}  A")
        self._set_status_output_display(is_on)
        self._set_status_mode_display(mode_text)

        if mode_text:
            self.current_control_mode = mode_text
            self.control_mode_var.set(mode_text)
            self._update_mode_buttons(mode_text)

        if not self.sequence_active and not self._output_toggle_in_flight:
            self._set_output_ui_state(is_on)
        else:
            self.current_output_on = is_on
            self._set_status_output_display(is_on)

        self._refresh_live_measurement_warning()
        self._update_output_controls()

    def _upload_waveform(self):
        if self._upload_in_flight:
            return
        if self.sequence_active:
            messagebox.showwarning(
                "Waveform Running",
                "Turn output off before uploading a new multi-chunk streamed waveform.")
            return
        if not self.kepco.connected:
            messagebox.showerror("Error", "Connect to a device first.")
            return
        req = self._read_waveform_request()
        if not req:
            return

        self.preview_points = req["plot_points"]
        self._update_preview_plot(req["plot_points"])
        self._upload_in_flight = True
        self.is_running = True
        self.prog_lbl.configure(text="Uploading...")
        self.progress.set(0)
        self._pause_status_polling()
        self._update_output_controls()
        threading.Thread(target=self._upload_request_worker, args=(req,), daemon=True).start()

    def _upload_request_worker(self, req):
        start_sequence = False
        try:
            self._wait_for_status_poll_idle()
            if req["kind"] == "DC":
                ok, msg = self._apply_dc_request(req)
            elif req["point_count"] <= MAX_LIST_POINTS:
                ok, msg = self._upload_single_chunk_request(req)
            else:
                ok, msg, start_sequence = self._prime_multi_chunk_request(req)
        except Exception as exc:
            ok = False
            msg = str(exc)
        self._call_on_ui(lambda: self._upload_request_done(req, ok, msg, start_sequence))

    def _apply_dc_request(self, req):
        self._call_on_ui(lambda: self.progress.set(0.5))
        mode = req["mode"]
        value = req["amplitude"]
        voltage_compliance, current_limit = self._get_request_limits(req)
        prev_req = self.uploaded_request or {}
        live_dc_update = (
            self.current_output_on
            and prev_req.get("kind") == "DC"
            and prev_req.get("mode") == mode
        )

        setpoint_cmd = (
            f"{mode} {KepcoController.format_scpi_value(value)}")

        if live_dc_update:
            ok, msg = self._apply_device_limits(
                mode, voltage_compliance, current_limit)
            if not ok:
                return False, f"DC limit setup failed: {msg}"
            self._log_scpi_sequence("DC live setpoint update", [setpoint_cmd])
            ok, msg = self.kepco.send_sequence(
                [setpoint_cmd], label="DC live setpoint update")
            if not ok:
                return False, msg
        else:
            ok, msg = self._safe_prepare_output(
                mode,
                initial_setpoint=0.0,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                label="DC fixed-output safe prepare")
            if not ok:
                return False, msg
            if self.current_output_on:
                self._log_scpi_sequence(
                    "DC live handoff setpoint update", [setpoint_cmd])
                ok, msg = self.kepco.send_sequence(
                    [setpoint_cmd], label="DC live handoff setpoint update")
                if not ok:
                    return False, msg

        self._call_on_ui(lambda: self.progress.set(1.0))
        unit = "V" if mode == "VOLT" else "A"
        if live_dc_update:
            return True, f"DC setpoint updated live to {value:.4f} {unit}"
        if self.current_output_on:
            return True, f"DC setpoint applied live at {value:.4f} {unit}"
        return True, f"DC setpoint staged at {value:.4f} {unit}"

    def _upload_single_chunk_request(self, req):
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
        if not ok:
            return False, msg
        if self.current_output_on:
            count = 0 if req["loop"] == 0 else max(req["loop"], 1)
            ok, run_msg = self.kepco.run_list(
                req["mode"],
                count=count,
                enable_output=False,
                voltage_compliance=voltage_compliance,
                current_limit=current_limit,
                apply_limit_setup=False)
            if not ok:
                return False, f"Upload succeeded but live re-arm failed: {run_msg}"
            return True, f"{msg}; applied without toggling output"
        return True, msg

    def _prime_multi_chunk_request(self, req):
        voltage_compliance, current_limit = self._get_request_limits(req)
        chunks = [
            req["points"][i:i + MAX_LIST_POINTS]
            for i in range(0, len(req["points"]), MAX_LIST_POINTS)
        ]

        def progress_cb(sent, total):
            pct = sent / max(total, 1)
            self._call_on_ui(lambda p=pct: self.progress.set(p))
            self._call_on_ui(
                lambda s=sent, t=total: self.prog_lbl.configure(
                    text=f"Priming chunk 1/{len(chunks)}... {s}/{t} pts"))

        ok, msg = self.kepco.upload_list_chunk(
            chunks[0], req["dwell"], req["mode"],
            progress_cb=progress_cb,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit)
        if not ok:
            return False, msg, False
        req["first_chunk_primed"] = True
        if self.current_output_on:
            return True, f"{msg}; continuing streamed execution without toggling output", True
        return True, f"{msg}; remaining {len(chunks) - 1} chunk(s) staged for output-on streaming", False

    def _upload_request_done(self, req, ok, msg, start_sequence):
        self._upload_in_flight = False
        prev_req = self.uploaded_request or {}
        self.uploaded_request = req if ok else self.uploaded_request
        if ok:
            self.uploaded_waveform_ready = True
            self._refresh_live_measurement_warning()
            self._refresh_uploaded_status_panel()
            if (
                self.current_output_on
                and prev_req.get("kind") == "DC"
                and prev_req.get("mode") == req["mode"]
                and req["kind"] == "LIST"
                and req.get("offset") is not None
            ):
                self._measurement_guard = {
                    "mode": req["mode"],
                    "previous_value": float(prev_req.get("amplitude", 0.0)),
                    "expected_value": float(req["offset"]),
                }
                self._set_live_measurement_axis(req["mode"], req["offset"])
            self.log(msg, "ok")
            if start_sequence:
                started, start_msg = self._begin_uploaded_sequence(
                    req,
                    output_already_on=self.current_output_on,
                    skip_first_upload=req.get("first_chunk_primed", False),
                )
                if started:
                    self.log(start_msg, "info")
                    return
                self.log(start_msg, "err")
            self.prog_lbl.configure(
                text="Setpoint staged" if req["kind"] == "DC" else "Uploaded")
            self.progress.set(1.0)
        else:
            self.log(f"Upload failed: {msg}", "err")
            self.progress.set(0)
            self.prog_lbl.configure(text="Upload failed")
            if not self.kepco.connected:
                self._handle_comm_failure("upload")

        self.is_running = False
        self._resume_status_polling()
        self._update_output_controls()
        self._schedule_status_poll(100)

    def _begin_uploaded_sequence(self, req, output_already_on=False, skip_first_upload=False):
        if not self.kepco.connected:
            return False, "Connect to a device first."
        if self.sequence_active:
            return False, "A streamed waveform is already active."

        self.stop_event.clear()
        self.sequence_active = True
        self.is_running = True
        self._output_toggle_in_flight = False
        self._pause_status_polling()
        self._update_output_controls()
        if not output_already_on:
            self._set_output_ui_state(True)
        self.prog_lbl.configure(text="Streaming...")
        self.progress.set(0)
        req["first_chunk_primed"] = False
        threading.Thread(
            target=self._sequence_worker,
            args=(req, output_already_on, skip_first_upload),
            daemon=True).start()
        return True, "Streaming multi-chunk waveform."

    def _sequence_worker(self, req, output_already_on=False, skip_first_upload=False):
        ok = True
        final_msg = "Waveform sequence complete."
        stopped = False
        forever = req["loop"] == 0
        iteration = 0
        try:
            self._wait_for_status_poll_idle()
            chunks = [
                req["points"][i:i + MAX_LIST_POINTS]
                for i in range(0, len(req["points"]), MAX_LIST_POINTS)
            ]
            mode = req["mode"]
            voltage_compliance, current_limit = self._get_request_limits(req)

            while not self.stop_event.is_set():
                iteration += 1
                if not forever and iteration > req["loop"]:
                    break

                for chunk_idx, chunk in enumerate(chunks):
                    if self.stop_event.is_set():
                        stopped = True
                        break

                    self._call_on_ui(
                        lambda pts=req["plot_points"], idx=chunk_idx:
                            self._update_status_plot(pts, chunk_idx=idx))

                    need_upload = not (skip_first_upload and iteration == 1 and chunk_idx == 0)
                    if need_upload:
                        def progress_cb(sent, total, ci=chunk_idx, nc=len(chunks)):
                            pct = sent / max(total, 1)
                            self._call_on_ui(lambda p=pct: self.progress.set(p))
                            self._call_on_ui(
                                lambda c=ci, n=nc, s=sent, t=total:
                                    self.prog_lbl.configure(
                                        text=f"Uploading chunk {c + 1}/{n}... {s}/{t} pts"))

                        ok, msg = self.kepco.upload_list_chunk(
                            chunk, req["dwell"], mode,
                            progress_cb=progress_cb,
                            voltage_compliance=voltage_compliance,
                            current_limit=current_limit)
                        if not ok:
                            final_msg = f"Chunk {chunk_idx + 1} upload failed: {msg}"
                            break

                    enable_output = not output_already_on
                    ok, msg = self.kepco.run_list(
                        mode,
                        count=1,
                        enable_output=enable_output,
                        voltage_compliance=voltage_compliance,
                        current_limit=current_limit,
                        apply_limit_setup=enable_output)
                    if not ok:
                        final_msg = f"Chunk {chunk_idx + 1} run failed: {msg}"
                        break

                    output_already_on = True
                    self._call_on_ui(lambda: self._set_output_ui_state(True))
                    self._call_on_ui(
                        lambda c=chunk_idx, n=len(chunks), i=iteration:
                            self.prog_lbl.configure(
                                text=f"Running chunk {c + 1}/{n} (loop {i})"))

                    wait_time = len(chunk) * req["dwell"] + 0.10
                    end_time = time.time() + wait_time
                    while time.time() < end_time:
                        if self.stop_event.is_set():
                            stopped = True
                            break
                        time.sleep(0.05)
                    if stopped:
                        break

                    self._call_on_ui(
                        lambda p=(chunk_idx + 1) / max(len(chunks), 1): self.progress.set(p))

                skip_first_upload = False
                if not ok or stopped:
                    break
                if forever:
                    continue

            if forever and not stopped and ok:
                final_msg = "Streamed waveform stopped."
        except Exception as exc:
            ok = False
            final_msg = str(exc)

        stop_ok = True
        if output_already_on:
            stop_ok, stop_msg = self.kepco.stop(base_mode=req["mode"])
            if not stop_ok:
                ok = False
                final_msg = stop_msg
        self._call_on_ui(
            lambda: self._sequence_done(req, ok, final_msg, stopped, stop_ok))

    def _sequence_done(self, req, ok, msg, stopped, stop_ok):
        self.sequence_active = False
        self.is_running = False
        self._output_toggle_in_flight = False
        if self.uploaded_request:
            self.uploaded_request["first_chunk_primed"] = False

        self._set_output_ui_state(False)
        self.progress.set(0)
        self.prog_lbl.configure(text="Idle")
        self._update_status_plot(req["plot_points"])

        if ok:
            self.log("Waveform stream stopped." if stopped else msg, "ok")
        else:
            self.log(msg, "err")
            if not self.kepco.connected:
                self._handle_comm_failure("waveform streaming")

        self._resume_status_polling()
        self._update_output_controls()
        self._schedule_status_poll(100)

    def _toggle_output(self):
        if self._output_toggle_in_flight:
            return
        target_on = not self.current_output_on
        req = self.uploaded_request

        if not self.kepco.connected:
            self._set_output_ui_state(False)
            self.log("Not connected - connect first.", "warn")
            return
        if target_on and not self.uploaded_waveform_ready:
            self._set_output_ui_state(False)
            self.log("Upload a waveform before enabling output.", "warn")
            return
        if target_on:
            req = self._request_with_latest_ui_limits(req)
            if not req:
                self._set_output_ui_state(False)
                return
        if not target_on and self.sequence_active:
            self._output_toggle_in_flight = True
            self.output_toggle_btn.configure(state="disabled")
            self.log("Stopping streamed waveform...", "info")
            self.stop_event.set()
            return
        if target_on and req and req["kind"] == "LIST" and req["point_count"] > MAX_LIST_POINTS:
            started, msg = self._begin_uploaded_sequence(
                req, output_already_on=False,
                skip_first_upload=req.get("first_chunk_primed", False))
            if not started:
                self._set_output_ui_state(False)
                self.log(msg, "err")
            else:
                self.log(msg, "info")
            return

        self._output_toggle_in_flight = True
        self.output_toggle_btn.configure(state="disabled")
        self._pause_status_polling()
        threading.Thread(
            target=self._output_toggle_worker,
            args=(target_on, req),
            daemon=True).start()

    def _enable_dc_output(self, req):
        mode = req["mode"]
        value = req["amplitude"]
        voltage_compliance, current_limit = self._get_request_limits(req)
        ok, msg = self._safe_prepare_output(
            mode,
            initial_setpoint=0.0,
            voltage_compliance=voltage_compliance,
            current_limit=current_limit,
            label="DC safe setup before OUTP ON")
        if not ok:
            return False, msg

        setpoint_cmd = (
            f"{mode} {KepcoController.format_scpi_value(value)}")
        cmds = ["OUTP ON", setpoint_cmd]
        self._log_scpi_sequence("DC enable and apply setpoint", cmds)
        ok, msg = self.kepco.send_sequence(
            cmds, label="DC enable and apply setpoint")
        if not ok:
            return False, msg
        unit = "V" if mode == "VOLT" else "A"
        return True, f"Output ON; {mode} setpoint {value:.4f} {unit}"

    def _output_toggle_worker(self, target_on, req):
        try:
            self._wait_for_status_poll_idle()
            mode = (req or {}).get("mode") or self.current_control_mode
            if target_on:
                if req["kind"] == "DC":
                    ok, msg = self._enable_dc_output(req)
                else:
                    count = 0 if req["loop"] == 0 else max(req["loop"], 1)
                    voltage_compliance, current_limit = self._get_request_limits(req)
                    ok, msg = self.kepco.run_list(
                        mode,
                        count=count,
                        enable_output=True,
                        voltage_compliance=voltage_compliance,
                        current_limit=current_limit)
            else:
                if req and req["kind"] == "LIST":
                    ok, msg = self.kepco.stop(base_mode=mode)
                else:
                    ok = bool(self.kepco.send("OUTP OFF"))
                    msg = "Output OFF" if ok else "Failed to turn output OFF"
        except Exception as exc:
            ok = False
            msg = str(exc)
        self._call_on_ui(lambda: self._output_toggle_done(target_on, ok, msg))

    def _output_toggle_done(self, target_on, ok, msg):
        self._output_toggle_in_flight = False
        if ok:
            self._set_output_ui_state(target_on)
            self.log(msg, "ok")
            if target_on:
                self.prog_lbl.configure(text="Output enabled")
            else:
                self.prog_lbl.configure(text="Idle")
                self.progress.set(0)
            self._resume_status_polling()
            self._update_output_controls()
            self._schedule_status_poll(100)
            return
        self._set_output_ui_state(not target_on)
        self.log(msg, "err")
        self._resume_status_polling()
        self._update_output_controls()
        if not self.kepco.connected:
            self._handle_comm_failure("output toggle")

    def _start_scan(self):
        self.scan_btn.configure(state="disabled", text="Scanning...")
        self.log(
            "Scanning local subnet for Kepco devices "
            "(Telnet 5024 first, fallback 5025)...",
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
        self.scan_btn.configure(state="normal", text="Scan Network")
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
        if self._connect_in_flight:
            return

        if not self.kepco.connected:
            ip = self.ip_var.get().strip()
            self.log(f"Connect requested for {ip}", "info")
            self._connect_in_flight = True
            self.conn_btn.configure(state="disabled", text="Connecting...")
            threading.Thread(target=self._connect_worker, args=(ip,), daemon=True).start()
        else:
            if self.sequence_active or self._upload_in_flight:
                messagebox.showwarning(
                    "Waveform Busy",
                    "Wait for the active upload/stream to finish before disconnecting.")
                return
            self.log("Disconnect requested", "info")
            self._connect_in_flight = True
            self.conn_btn.configure(state="disabled", text="Disconnecting...")
            threading.Thread(target=self._disconnect_worker, daemon=True).start()

    def _connect_worker(self, ip):
        ok, msg = self.kepco.connect(ip, validate_identity=True)
        idn = self.kepco.last_identity or None
        self._call_on_ui(lambda: self._connect_done(ok, msg, ip, idn))

    def _connect_done(self, ok, msg, ip, idn):
        self._connect_in_flight = False
        self.conn_btn.configure(state="normal")
        if ok:
            self._set_connected_state(True, idn or "Unknown device")
            self._reset_live_status()
            self._reset_uploaded_state()
            self.log(
                f"Connected to {ip} via {self.kepco.transport} "
                f"({self.kepco.port}): {idn or 'Unknown device'}",
                "ok")
            self._start_status_polling()
        else:
            self.kepco.disconnect()
            self._set_connected_state(False)
            self._reset_live_status()
            self._reset_uploaded_state()
            self.log(f"Connection failed: {msg}", "err")

    def _disconnect_worker(self):
        self._wait_for_status_poll_idle()
        ok, err_msg = self._safe_output_off_before_disconnect()
        if ok:
            self.kepco.disconnect()
        self._call_on_ui(lambda: self._disconnect_done(ok, err_msg))

    def _safe_output_off_before_disconnect(self):
        if not self.kepco.connected:
            return True, ""

        def _parse_num(raw):
            try:
                return float(str(raw).strip())
            except Exception:
                return None

        base_mode = (
            self.uploaded_request["mode"]
            if self.uploaded_request else self.current_control_mode
        )

        for attempt in range(2):
            errors = []
            ok_stop, stop_msg = self.kepco.stop(base_mode=base_mode)
            if not ok_stop:
                errors.append(f"stop failed ({stop_msg})")

            for cmd, desc in [
                ("VOLT 0", "set voltage to 0V"),
                ("CURR 0", "set current to 0A"),
                ("OUTP OFF", "turn output OFF"),
            ]:
                if not self.kepco.send(cmd):
                    err = self.kepco.last_error or "send failed"
                    errors.append(f"could not {desc} ({err})")

            outp = (self.kepco.send("OUTP?", query=True) or "").strip().upper()
            v = _parse_num(self.kepco.send("VOLT?", query=True))
            c = _parse_num(self.kepco.send("CURR?", query=True))
            outp_ok = outp in ("0", "OFF")
            zero_ok = (
                v is not None and c is not None
                and abs(v) <= 0.05 and abs(c) <= 0.05
            )

            if outp_ok and zero_ok:
                return True, ""

            errors.append(
                f"verification failed (OUTP?='{outp}', VOLT?='{v}', CURR?='{c}')")
            if attempt == 0:
                time.sleep(0.1)
            else:
                return False, "; ".join(errors)

        return False, "safety verification failed"

    def _disconnect_done(self, ok, err_msg):
        self._connect_in_flight = False
        self.conn_btn.configure(state="normal")
        if not ok:
            self._set_connected_state(True, self.idn_lbl.cget("text"))
            self.log(f"Disconnect blocked by safety interlock: {err_msg}", "err")
            messagebox.showerror(
                "Safety Interlock",
                "Disconnect blocked.\n"
                "Output could not be verified OFF at 0V/0A.\n"
                f"Details: {err_msg}")
            return

        self._stop_status_polling()
        self.stop_event.set()
        self.sequence_active = False
        self.is_running = False
        self._set_connected_state(False)
        self._reset_live_status()
        self._reset_uploaded_state()
        self.log("Disconnected.", "info")

    def _on_close(self):
        self.stop_event.set()
        if self.kepco.connected:
            ok, err_msg = self._safe_output_off_before_disconnect()
            if not ok:
                self.log(f"Close blocked by safety interlock: {err_msg}", "err")
                messagebox.showerror(
                    "Safety Interlock",
                    "Close blocked.\n"
                    "Output could not be verified OFF at 0V/0A.\n"
                    f"Details: {err_msg}")
                return
            self.kepco.disconnect()
        self._stop_status_polling()
        self._stop_data_collection()
        self.log("Application closed.", "info")
        self._close_log_file()
        self._stop_ui_dispatcher()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


# ===========================================================================
if __name__ == "__main__":
    DashboardApp().run()
