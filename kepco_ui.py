#!/usr/bin/env python3
"""
Kepco BIT 802E Waveform Generator — High Performance Edition

Material-design UI with real-time waveform preview, chunk-send
indication, auto-discovery, and optimized multi-list upload.

Hardware Constraints (BIT 802E manual):
  - Max 1000 list points per upload (1002 technically)
  - Dwell time: 0.0005 s (500 µs) to 10 s
  - For >1000 points: sequential multi-list upload required
  - Use VOLT:RANG 1 / CURR:RANG 1 to avoid quarter-scale transients
"""

import socket
import math
import csv
import threading
import time
import ipaddress
import queue as _queue
from tkinter import messagebox, filedialog

# ── GUI + plotting ──────────────────────────────────────────────────────────
import customtkinter as ctk

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.lines as mlines
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ── Constants ───────────────────────────────────────────────────────────────
MIN_DWELL        = 0.0005    # 500 µs – hardware minimum
MAX_DWELL        = 10.0      # hardware maximum
MAX_LIST_POINTS  = 1000      # per single LIST upload
MAX_TOTAL_POINTS = 4000      # 4 × 1000 chunks
TELNET_PORT      = 5024      # manual 2.4.2 / 4.5: Telnet first
SCPI_SOCKET_PORT = 5025      # alternate direct socket endpoint
DISCOVERY_TIMEOUT = 0.25
CHUNK_CMD_LIMIT  = 200       # safe margin for 253-byte SCPI buffer
SCPI_CMD_GAP     = 0.035     # > 25ms spec throughput (PAR 1.2.2)
LIST_VALUES_PER_CMD = 10     # manual examples show max 11 (PAR B.45/B.31)
RECV_TIMEOUT     = 3.0       # socket recv timeout for queries

# ── Material colour palette ─────────────────────────────────────────────────
C = dict(
    bg="#121212", surface="#1e1e2e", card="#2a2a3c",
    primary="#7c3aed", primary_h="#6d28d9",
    green="#10b981", red="#ef4444", amber="#f59e0b",
    text="#e2e8f0", text2="#94a3b8", border="#3f3f5c",
    input_bg="#363650", graph_bg="#161625",
    chunk_colors=["#818cf8", "#34d399", "#fb923c", "#f472b6"],
    sent="#f472b6",
)


# ═══════════════════════════════════════════════════════════════════════════
#  SCPI Controller  (hardened for real BIT 802E hardware)
# ═══════════════════════════════════════════════════════════════════════════
class KepcoController:
    """Thread-safe SCPI control for a Kepco BIT 802E.

    Protocol notes (BIT 802E manual):
      - PAR 2.4.2 / 4.5: Telnet-first on port 5024, socket fallback 5025
      - PAR 1.2.2: connection throughput ~25 ms per command
      - PAR 4.5.2: *WAI / *OPC? to ensure command completion
      - 253-byte input buffer limit per SCPI message
      - List: max 1002 steps, dwell 500 µs … 10 s

    Design:
      - Every non-query command sleeps SCPI_CMD_GAP (35 ms) *inside* the
        lock so no other thread can violate the pacing constraint.
      - *OPC? sync is used only at key checkpoints (after LIST:CLE, after
        all values sent, after DWEL) — NOT after every single LIST:VOLT.
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
        self._lock = threading.Lock()

    # ── connect / disconnect ───────────────────────────────────────────────
    def connect(self, ip, port=None):
        attempts = [(port, "CUSTOM")] if port is not None else [
            (TELNET_PORT, "TELNET"),
            (SCPI_SOCKET_PORT, "SOCKET"),
        ]
        last_err = ""
        for target_port, transport in attempts:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
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
                return True, f"Connected via {transport} ({target_port})"
            except Exception as e:
                last_err = str(e)
                try:
                    s.close()
                except Exception:
                    pass
        self.connected = False
        self.last_error = last_err
        return False, last_err

    def disconnect(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        self.sock = None
        self.connected = False

    def _safe_reconnect(self):
        if not self.ip:
            return False
        ok, _ = self.connect(self.ip, self.port)
        return ok

    # ── Telnet IAC filtering ──────────────────────────────────────────────
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

    # ── socket helpers ─────────────────────────────────────────────────────
    def _drain_echo(self):
        """Quick non-blocking drain of Telnet echo after every send_cmd.

        The BIT 802E Telnet server echoes every command back verbatim.
        If these echo bytes are never read they accumulate in the card's
        tiny TCP send buffer (~253 bytes, PAR B.2).  When that buffer
        fills the card blocks trying to echo and can no longer read new
        commands → deadlock / freeze.

        This is intentionally very short — just long enough to pick up
        a single echo line that is already in-flight.
        """
        prev = self.sock.gettimeout()
        try:
            self.sock.settimeout(0.02)          # 20 ms
            try:
                self.sock.recv(1024)
            except (socket.timeout, OSError):
                pass
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
                    line = line.strip()
                    if not line:
                        continue
                    if echo and line == echo:
                        continue          # discard echo
                    return line
                # Only echo / empty lines so far — keep the tail
                raw = trailing.encode("ascii", errors="ignore")
                if len(raw) > 8192:
                    break
            # Timeout — check anything left in buffer
            if raw:
                text = raw.decode("ascii", errors="ignore").strip()
                if text and not (echo and text == echo):
                    return text
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

    # ── SCPI primitive: command (no response) ──────────────────────────────
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
                self.sock.sendall((cmd + "\n").encode("ascii"))
                time.sleep(SCPI_CMD_GAP)
                self._drain_echo()          # consume Telnet echo
                return True
            except Exception as e:
                self.last_error = str(e)
                self.disconnect()
                return None

    # ── SCPI primitive: query (expects response) ──────────────────────────
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
                self.sock.sendall((cmd + "\n").encode("ascii"))
                resp = self._recv_response(sent_cmd=cmd, timeout=timeout)
                if resp is None:
                    self.last_error = f"No response to '{cmd}'"
                return resp
            except Exception as e:
                self.last_error = str(e)
                self.disconnect()
                return None

    # ── backward-compat wrapper (used by Manual Override callbacks) ────────
    def send(self, cmd, query=False, post_delay=0.0):
        if query:
            return self.send_query(cmd)
        return self.send_cmd(cmd)

    # ── synchronization helpers ────────────────────────────────────────────
    def sync(self):
        """Ensure all pending operations complete before next command.

        Sends *WAI (Wait-to-Continue, PAR A.17) which blocks the device's
        command processor until all pending operations finish.  Unlike
        *OPC? this is a *command* (no response expected) so it cannot
        time-out waiting for a reply — far more reliable on real
        hardware via Telnet.
        """
        return self.send_cmd("*WAI") is not None

    def drain_errors(self):
        """Read and return all queued SYST:ERR entries (stops at '0,…')."""
        errors = []
        for _ in range(20):
            resp = self.send_query("SYST:ERR?")
            if resp is None:
                break
            resp = resp.strip()
            if resp.startswith("0") or "No error" in resp:
                break
            errors.append(resp)
        return errors

    def identity(self):
        return self.send_query("*IDN?")

    # ── List upload (single chunk ≤ 1000 pts) ─────────────────────────────
    def upload_list_chunk(self, points, dwell, mode="VOLT",
                          progress_cb=None):
        """Upload one chunk (≤ 1000 points) with paced writes + verification.

        Strategy (follows manual Figure B-2 order):
          1. Setup: FUNC:MODE, RANG, LIST:CLE, *WAI, LIST:DWEL
          2. Values: send LIST:{mode} batches of ≤ 20 values each,
             each followed only by the mandatory 35 ms gap
          3. Verify: *WAI → LIST:{mode}:POIN? → SYST:ERR?

        Key change from previous revision: *OPC? is NOT used anywhere
        in the upload path.  The manual (PAR A.17) recommends *WAI for
        sequential command synchronization — it blocks the device's
        command processor (no response to time-out on).

        progress_cb(sent, total) is called after each batch if provided.
        """
        if not self.connected and not self._safe_reconnect():
            return False, "Not connected"
        if not points:
            return False, "Empty point list"
        if len(points) > MAX_LIST_POINTS:
            return False, f"Chunk exceeds {MAX_LIST_POINTS} points"

        try:
            # ── Phase 1: Setup (order follows manual Figure B-2) ──
            #   FUNC:MODE → RANG → LIST:CLE → *WAI → LIST:DWEL
            # NOTE: *CLS is intentionally NOT sent here — the manual
            # examples never use it for list operations, and it forces
            # the card to "operation complete idle" which can confuse
            # subsequent synchronisation on some firmware revisions.
            setup_cmds = [
                f"FUNC:MODE {mode}",
                f"{mode}:RANG 1",         # full-scale (PAR 4.5.1.2)
                "LIST:CLE",
                "*WAI",                   # wait for LIST:CLE (PAR A.17)
                f"LIST:DWEL {dwell:.6f}", # dwell BEFORE values (manual order)
            ]
            for cmd in setup_cmds:
                if self.send_cmd(cmd) is None:
                    return False, f"Setup '{cmd}' failed: {self.last_error}"

            # ── Phase 2: Send list values ──
            prefix = f"LIST:{mode} "
            total = len(points)
            sent = 0
            buf = []

            def _fmt(v):
                """Compact value format — matches manual's integer style."""
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

            # ── Phase 3: Verify ──
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

            errors = self.drain_errors()
            if errors:
                return False, f"Device errors: {'; '.join(errors)}"

            return True, (
                f"{total} pts @ {dwell*1000:.3f} ms/step (verified)")

        except Exception as e:
            return False, str(e)

    # ── Run / Stop ─────────────────────────────────────────────────────────
    def run_list(self, mode="VOLT", count=1):
        """Start LIST execution: COUNT → OUTP ON → {mode}:MODE LIST.

        Follows the manual's Figure B-2 sequence exactly.
        """
        try:
            for cmd in [
                f"LIST:COUN {count}",
                "OUTP ON",
                f"{mode}:MODE LIST",
            ]:
                if self.send_cmd(cmd) is None:
                    return False, f"Run '{cmd}' failed: {self.last_error}"
            return True, "Running"
        except Exception as e:
            return False, str(e)

    def stop(self):
        """Stop LIST, return to safe fixed-output state."""
        try:
            for cmd in [
                "VOLT:MODE FIX",
                "CURR:MODE FIX",
                "OUTP OFF",
                "FUNC:MODE VOLT",
            ]:
                if self.send_cmd(cmd) is None:
                    return False, f"Stop '{cmd}' failed: {self.last_error}"
            return True, "Stopped"
        except Exception as e:
            return False, str(e)


# ═══════════════════════════════════════════════════════════════════════════
#  Network Discovery
# ═══════════════════════════════════════════════════════════════════════════
class Discovery:
    """Scan a /24 subnet for Kepco devices (Telnet 5024 first, then 5025)."""

    @staticmethod
    def _probe(ip_str, timeout=DISCOVERY_TIMEOUT):
        for port in (TELNET_PORT, SCPI_SOCKET_PORT):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(timeout)
                s.connect((ip_str, port))
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


# ═══════════════════════════════════════════════════════════════════════════
#  Waveform Mathematics
# ═══════════════════════════════════════════════════════════════════════════
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
                f"Dwell {ideal_dwell*1e6:.1f} µs < min 500 µs "
                f"→ reduced to {max_pts} pts"
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


# ═══════════════════════════════════════════════════════════════════════════
#  Application  (Material-themed, customtkinter)
# ═══════════════════════════════════════════════════════════════════════════
class App:
    def __init__(self):
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.root = ctk.CTk()
        self.root.title("Kepco BIT 802E — Waveform Generator")
        self.root.geometry("1160x870")
        self.root.minsize(980, 750)

        self.kepco = KepcoController()
        self.csv_points = None
        self.current_points = []
        self.is_running = False
        self.stop_event = threading.Event()

        self._build_ui()
        self._update_graph()

    # ──────────────────────────────────────────────────────────────────────
    #  UI construction
    # ──────────────────────────────────────────────────────────────────────
    def _build_ui(self):
        # ═══ Top: connection bar ═══
        conn = ctk.CTkFrame(self.root, corner_radius=10)
        conn.pack(fill="x", padx=12, pady=(10, 4))

        ctk.CTkLabel(conn, text="IP Address:",
                     font=ctk.CTkFont(size=13)).pack(side="left", padx=(14, 4))
        self.ip_var = ctk.StringVar(value="192.168.50.10")
        self.ip_combo = ctk.CTkComboBox(
            conn, variable=self.ip_var,
            values=["192.168.50.10"], width=200,
            font=ctk.CTkFont(size=13))
        self.ip_combo.pack(side="left", padx=4)

        self.scan_btn = ctk.CTkButton(
            conn, text="⟳  Scan Network", width=140,
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

        self.status_lbl = ctk.CTkLabel(conn, text="●  Disconnected",
                                       text_color=C["red"],
                                       font=ctk.CTkFont(size=13))
        self.status_lbl.pack(side="left", padx=14)

        self.idn_lbl = ctk.CTkLabel(conn, text="", text_color=C["text2"],
                                    font=ctk.CTkFont(size=11, slant="italic"))
        self.idn_lbl.pack(side="right", padx=14)

        # ═══ Middle: Tabbed interface ═══
        self.tabview = ctk.CTkTabview(self.root, corner_radius=12)
        self.tabview.pack(fill="both", expand=True, padx=12, pady=4)

        wf_tab = self.tabview.add("🔊  Waveform Generator")
        man_tab = self.tabview.add("🔧  Manual Override")

        # ── Waveform Tab ──
        wf_inner = ctk.CTkFrame(wf_tab, fg_color="transparent")
        wf_inner.pack(fill="both", expand=True)

        cfg = ctk.CTkFrame(wf_inner, width=290, corner_radius=12)
        cfg.pack(side="left", fill="y", padx=(0, 6))
        cfg.pack_propagate(False)

        ctk.CTkLabel(cfg, text="Waveform Configuration",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(
            padx=14, pady=(14, 10))

        # Waveform type
        self._lbl(cfg, "Waveform Type")
        self.wave_var = ctk.StringVar(value="Sine")
        self.wave_combo = ctk.CTkComboBox(
            cfg, variable=self.wave_var,
            values=["Sine", "Square", "Triangle", "Sawtooth", "CSV Custom"],
            command=self._on_wave_change)
        self.wave_combo.pack(fill="x", padx=14, pady=(0, 6))

        # CSV row (initially hidden)
        self.csv_frame = ctk.CTkFrame(cfg, fg_color="transparent")
        self.csv_btn = ctk.CTkButton(
            self.csv_frame, text="📂 Load CSV", width=120,
            command=self._load_csv,
            fg_color="#374151", hover_color="#4b5563")
        self.csv_btn.pack(side="left", padx=(0, 8))
        self.csv_lbl = ctk.CTkLabel(self.csv_frame, text="No file",
                                    text_color=C["text2"],
                                    font=ctk.CTkFont(size=11))
        self.csv_lbl.pack(side="left")

        # Numeric fields
        self._lbl(cfg, "Frequency (Hz)")
        self.freq_entry = ctk.CTkEntry(cfg, placeholder_text="40.0")
        self.freq_entry.insert(0, "40.0")
        self.freq_entry.pack(fill="x", padx=14, pady=(0, 6))

        self._lbl(cfg, "Amplitude (V / A)")
        self.amp_entry = ctk.CTkEntry(cfg, placeholder_text="10.0")
        self.amp_entry.insert(0, "10.0")
        self.amp_entry.pack(fill="x", padx=14, pady=(0, 6))

        self._lbl(cfg, "Offset (V / A)")
        self.off_entry = ctk.CTkEntry(cfg, placeholder_text="0.0")
        self.off_entry.insert(0, "0.0")
        self.off_entry.pack(fill="x", padx=14, pady=(0, 6))

        self._lbl(cfg, "Total Points (max 4000)")
        self.pts_entry = ctk.CTkEntry(cfg, placeholder_text="1000")
        self.pts_entry.insert(0, "1000")
        self.pts_entry.pack(fill="x", padx=14, pady=(0, 6))

        self._lbl(cfg, "Loop Count (0 = infinite)")
        self.loop_entry = ctk.CTkEntry(cfg, placeholder_text="0")
        self.loop_entry.insert(0, "0")
        self.loop_entry.pack(fill="x", padx=14, pady=(0, 6))

        self._lbl(cfg, "Output Mode")
        mode_f = ctk.CTkFrame(cfg, fg_color="transparent")
        mode_f.pack(fill="x", padx=14, pady=(0, 8))
        self.mode_var = ctk.StringVar(value="VOLT")
        ctk.CTkRadioButton(mode_f, text="Voltage",
                           variable=self.mode_var, value="VOLT").pack(
            side="left", padx=(0, 18))
        ctk.CTkRadioButton(mode_f, text="Current",
                           variable=self.mode_var, value="CURR").pack(
            side="left")

        # Preview
        ctk.CTkButton(cfg, text="Preview Waveform",
                      command=self._preview,
                      fg_color="#374151", hover_color="#4b5563",
                      font=ctk.CTkFont(size=12)).pack(
            fill="x", padx=14, pady=(6, 4))

        # Timing info
        self.timing_lbl = ctk.CTkLabel(
            cfg, text="", text_color=C["amber"],
            font=ctk.CTkFont(size=11), wraplength=260, justify="left")
        self.timing_lbl.pack(fill="x", padx=14, pady=(4, 10))

        # ── Right: graph ──
        graph_outer = ctk.CTkFrame(wf_inner, corner_radius=12)
        graph_outer.pack(side="left", fill="both", expand=True)

        self.fig = Figure(figsize=(7, 4), dpi=100, facecolor=C["graph_bg"])
        self.ax = self.fig.add_subplot(111)
        self._style_ax()
        self.canvas = FigureCanvasTkAgg(self.fig, master=graph_outer)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=6, pady=6)

        # ── Manual Override Tab ──
        self._build_manual_tab(man_tab)

        # ═══ Bottom: controls + progress ═══
        bot = ctk.CTkFrame(self.root, corner_radius=10)
        bot.pack(fill="x", padx=12, pady=(4, 4))

        self.run_btn = ctk.CTkButton(
            bot, text="▶  Upload & Run", width=170,
            command=self._run, fg_color=C["green"], hover_color="#059669",
            text_color="#000", font=ctk.CTkFont(size=14, weight="bold"))
        self.run_btn.pack(side="left", padx=(14, 8), pady=10)

        self.stop_btn = ctk.CTkButton(
            bot, text="■  Stop", width=110,
            command=self._stop, fg_color=C["red"], hover_color="#dc2626",
            font=ctk.CTkFont(size=14, weight="bold"))
        self.stop_btn.pack(side="left", padx=8, pady=10)

        self.prog_lbl = ctk.CTkLabel(bot, text="Idle",
                                     text_color=C["text2"],
                                     font=ctk.CTkFont(size=12))
        self.prog_lbl.pack(side="left", padx=20)

        self.progress = ctk.CTkProgressBar(bot, width=220)
        self.progress.pack(side="left", padx=8, pady=10)
        self.progress.set(0)

        # ═══ Log ═══
        log_wrap = ctk.CTkFrame(self.root, corner_radius=10)
        log_wrap.pack(fill="both", padx=12, pady=(0, 10))

        self.log_text = ctk.CTkTextbox(
            log_wrap, height=120,
            font=ctk.CTkFont(family="Consolas", size=11),
            activate_scrollbars=True)
        self.log_text.pack(fill="both", padx=6, pady=6, expand=True)

    # ── helpers ────────────────────────────────────────────────────────────
    @staticmethod
    def _lbl(parent, text):
        ctk.CTkLabel(parent, text=text, text_color=C["text2"],
                     font=ctk.CTkFont(size=12)).pack(
            anchor="w", padx=14, pady=(6, 1))

    def log(self, msg, tag="info"):
        ts = time.strftime("%H:%M:%S")
        sym = {"info": "ℹ", "ok": "✓", "warn": "⚠", "err": "✗"}.get(tag, "·")
        self.log_text.insert("end", f"[{ts}] {sym}  {msg}\n")
        self.log_text.see("end")

    # ──────────────────────────────────────────────────────────────────────
    #  Manual Override Tab
    # ──────────────────────────────────────────────────────────────────────
    def _build_manual_tab(self, parent):
        outer = ctk.CTkFrame(parent, fg_color="transparent")
        outer.pack(fill="both", expand=True)

        # ── Left column: Output & Set Values ──
        left = ctk.CTkScrollableFrame(outer, width=310, corner_radius=12)
        left.pack(side="left", fill="y", padx=(0, 6), pady=0)

        ctk.CTkLabel(left, text="Output Control",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(
            padx=14, pady=(10, 8))

        # Output ON / OFF
        out_row = ctk.CTkFrame(left, fg_color="transparent")
        out_row.pack(fill="x", padx=14, pady=(0, 8))
        ctk.CTkLabel(out_row, text="Output:",
                     font=ctk.CTkFont(size=13)).pack(side="left")
        self.man_outp_var = ctk.StringVar(value="OFF")
        self.man_outp_switch = ctk.CTkSwitch(
            out_row, text="", variable=self.man_outp_var,
            onvalue="ON", offvalue="OFF",
            command=self._man_toggle_output,
            progress_color=C["green"])
        self.man_outp_switch.pack(side="left", padx=8)
        self.man_outp_lbl = ctk.CTkLabel(
            out_row, text="OFF", text_color=C["red"],
            font=ctk.CTkFont(size=13, weight="bold"))
        self.man_outp_lbl.pack(side="left")

        # Operating Mode
        self._lbl(left, "Operating Mode")
        man_mode_row = ctk.CTkFrame(left, fg_color="transparent")
        man_mode_row.pack(fill="x", padx=14, pady=(0, 8))
        self.man_mode_var = ctk.StringVar(value="VOLT")
        ctk.CTkRadioButton(man_mode_row, text="Voltage",
                           variable=self.man_mode_var, value="VOLT").pack(
            side="left", padx=(0, 12))
        ctk.CTkRadioButton(man_mode_row, text="Current",
                           variable=self.man_mode_var, value="CURR").pack(
            side="left", padx=(0, 12))
        ctk.CTkButton(man_mode_row, text="Set", width=60,
                      command=self._man_set_mode,
                      fg_color=C["primary"],
                      hover_color=C["primary_h"]).pack(side="left")

        ctk.CTkFrame(left, height=2, fg_color=C["border"]).pack(
            fill="x", padx=14, pady=8)

        # ── Set Values ──
        ctk.CTkLabel(left, text="Set Values",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(
            padx=14, pady=(4, 2))
        ctk.CTkLabel(left, text="In Voltage mode: VOLT = output, CURR = limit\n"
                     "In Current mode: CURR = output, VOLT = limit",
                     text_color=C["text2"],
                     font=ctk.CTkFont(size=10), justify="left").pack(
            anchor="w", padx=14, pady=(0, 6))

        # Voltage
        v_row = ctk.CTkFrame(left, fg_color="transparent")
        v_row.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkLabel(v_row, text="Voltage (V):",
                     font=ctk.CTkFont(size=12), width=100).pack(side="left")
        self.man_volt_entry = ctk.CTkEntry(v_row, width=110,
                                           placeholder_text="0.0")
        self.man_volt_entry.insert(0, "0.0")
        self.man_volt_entry.pack(side="left", padx=4)
        ctk.CTkButton(v_row, text="Set", width=60,
                      command=self._man_set_voltage,
                      fg_color="#374151",
                      hover_color="#4b5563").pack(side="left", padx=4)

        # Current
        c_row = ctk.CTkFrame(left, fg_color="transparent")
        c_row.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkLabel(c_row, text="Current (A):",
                     font=ctk.CTkFont(size=12), width=100).pack(side="left")
        self.man_curr_entry = ctk.CTkEntry(c_row, width=110,
                                           placeholder_text="0.0")
        self.man_curr_entry.insert(0, "0.0")
        self.man_curr_entry.pack(side="left", padx=4)
        ctk.CTkButton(c_row, text="Set", width=60,
                      command=self._man_set_current,
                      fg_color="#374151",
                      hover_color="#4b5563").pack(side="left", padx=4)

        ctk.CTkFrame(left, height=2, fg_color=C["border"]).pack(
            fill="x", padx=14, pady=8)

        # ── Range ──
        ctk.CTkLabel(left, text="Range Control",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(
            padx=14, pady=(4, 2))
        ctk.CTkLabel(left, text="Full-scale avoids quarter-scale transients",
                     text_color=C["text2"],
                     font=ctk.CTkFont(size=10)).pack(
            anchor="w", padx=14, pady=(0, 6))

        rng_row = ctk.CTkFrame(left, fg_color="transparent")
        rng_row.pack(fill="x", padx=14, pady=(0, 4))
        self.man_range_var = ctk.StringVar(value="Auto")
        ctk.CTkComboBox(rng_row, variable=self.man_range_var,
                        values=["Auto", "Full Scale", "Quarter Scale"],
                        width=150).pack(side="left", padx=(0, 8))
        ctk.CTkButton(rng_row, text="Set", width=60,
                      command=self._man_set_range,
                      fg_color="#374151",
                      hover_color="#4b5563").pack(side="left")

        ctk.CTkFrame(left, height=2, fg_color=C["border"]).pack(
            fill="x", padx=14, pady=8)

        # Reset
        ctk.CTkButton(left, text="⟲  Reset Device (*RST)", width=220,
                      command=self._man_reset,
                      fg_color=C["red"], hover_color="#dc2626",
                      font=ctk.CTkFont(size=13, weight="bold")).pack(
            padx=14, pady=(4, 12))

        # ── Right column: Measurements + SCPI Console ──
        right = ctk.CTkScrollableFrame(outer, corner_radius=12)
        right.pack(side="left", fill="both", expand=True)

        ctk.CTkLabel(right, text="Live Measurements",
                     font=ctk.CTkFont(size=15, weight="bold")).pack(
            padx=14, pady=(14, 8))

        meas_card = ctk.CTkFrame(right, corner_radius=10,
                                 fg_color=C["graph_bg"])
        meas_card.pack(fill="x", padx=14, pady=(0, 6))

        self.meas_volt_lbl = ctk.CTkLabel(
            meas_card, text="Voltage:   — — —  V",
            font=ctk.CTkFont(family="Consolas", size=22),
            text_color="#60a5fa")
        self.meas_volt_lbl.pack(padx=20, pady=(16, 4))

        self.meas_curr_lbl = ctk.CTkLabel(
            meas_card, text="Current:   — — —  A",
            font=ctk.CTkFont(family="Consolas", size=22),
            text_color="#34d399")
        self.meas_curr_lbl.pack(padx=20, pady=(4, 4))

        self.meas_mode_lbl = ctk.CTkLabel(
            meas_card, text="Mode:  — — —",
            font=ctk.CTkFont(family="Consolas", size=14),
            text_color=C["text2"])
        self.meas_mode_lbl.pack(padx=20, pady=(2, 12))

        meas_ctrl = ctk.CTkFrame(right, fg_color="transparent")
        meas_ctrl.pack(fill="x", padx=14, pady=(0, 8))
        self.auto_meas_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(meas_ctrl, text="Auto refresh (1 s)",
                        variable=self.auto_meas_var,
                        command=self._man_toggle_auto_meas).pack(
            side="left", padx=(0, 12))
        ctk.CTkButton(meas_ctrl, text="Refresh Now", width=120,
                      command=self._man_measure,
                      fg_color="#374151",
                      hover_color="#4b5563").pack(side="left")

        ctk.CTkFrame(right, height=2, fg_color=C["border"]).pack(
            fill="x", padx=14, pady=8)

        # Manual Command Console
        ctk.CTkLabel(right, text="Manual Command Console",
                     font=ctk.CTkFont(size=14, weight="bold")).pack(
            anchor="w", padx=14, pady=(4, 6))
        ctk.CTkLabel(
            right,
            text="Enter any SCPI command/query, or use quick commands below.",
            text_color=C["text2"],
            font=ctk.CTkFont(size=10)).pack(anchor="w", padx=14, pady=(0, 6))

        scpi_row = ctk.CTkFrame(right, fg_color="transparent")
        scpi_row.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkLabel(scpi_row, text="CMD:",
                     font=ctk.CTkFont(family="Consolas", size=12)).pack(
            side="left", padx=(0, 4))
        self.scpi_entry = ctk.CTkEntry(
            scpi_row, placeholder_text="e.g.  *IDN?  or  VOLT 5.0",
            font=ctk.CTkFont(family="Consolas", size=12))
        self.scpi_entry.pack(side="left", fill="x", expand=True, padx=4)
        self.scpi_entry.bind("<Return>", lambda e: self._man_send_scpi())
        ctk.CTkButton(scpi_row, text="Send ▶", width=80,
                      command=self._man_send_scpi,
                      fg_color=C["primary"],
                      hover_color=C["primary_h"]).pack(side="left", padx=4)

        quick_row = ctk.CTkFrame(right, fg_color="transparent")
        quick_row.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkButton(quick_row, text="*IDN?", width=72,
                      command=lambda: self._man_send_preset("*IDN?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="SYST:ERR?", width=92,
                      command=lambda: self._man_send_preset("SYST:ERR?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="OUTP?", width=72,
                      command=lambda: self._man_send_preset("OUTP?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="MEAS:VOLT?", width=102,
                      command=lambda: self._man_send_preset("MEAS:VOLT?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row, text="MEAS:CURR?", width=102,
                      command=lambda: self._man_send_preset("MEAS:CURR?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left")

        quick_row2 = ctk.CTkFrame(right, fg_color="transparent")
        quick_row2.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkButton(quick_row2, text="*OPC?", width=72,
                      command=lambda: self._man_send_preset("*OPC?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row2, text="FUNC:MODE?", width=102,
                      command=lambda: self._man_send_preset("FUNC:MODE?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row2, text="LIST:VOLT:POIN?", width=124,
                      command=lambda: self._man_send_preset("LIST:VOLT:POIN?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left", padx=(0, 6))
        ctk.CTkButton(quick_row2, text="LIST:CURR:POIN?", width=124,
                      command=lambda: self._man_send_preset("LIST:CURR:POIN?"),
                      fg_color="#374151", hover_color="#4b5563").pack(
            side="left")

        scpi_ctrl = ctk.CTkFrame(right, fg_color="transparent")
        scpi_ctrl.pack(fill="x", padx=14, pady=(0, 4))
        ctk.CTkButton(scpi_ctrl, text="Health Check", width=120,
                  command=self._man_health_check,
                  fg_color="#374151",
                  hover_color="#4b5563").pack(side="left", padx=(0, 8))
        ctk.CTkButton(scpi_ctrl, text="Clear Console", width=120,
                  command=self._man_clear_scpi,
                  fg_color="#374151",
                  hover_color="#4b5563").pack(side="left")

        self.scpi_resp = ctk.CTkTextbox(
            right, height=120,
            font=ctk.CTkFont(family="Consolas", size=11),
            activate_scrollbars=True)
        self.scpi_resp.pack(fill="both", padx=14, pady=(4, 14), expand=True)

        self._meas_timer = None

    # ──────────────────────────────────────────────────────────────────────
    #  Manual Override callbacks
    # ──────────────────────────────────────────────────────────────────────
    def _man_require_conn(self):
        if not self.kepco.connected:
            self.log("Not connected — connect first.", "warn")
            return False
        return True

    def _man_toggle_output(self):
        if not self._man_require_conn():
            return
        state = self.man_outp_var.get()
        ok = self.kepco.send(f"OUTP {state}")
        if ok:
            on = state == "ON"
            self.man_outp_lbl.configure(
                text=state, text_color=C["green"] if on else C["red"])
            self.log(f"Output → {state}", "ok")
        else:
            self.log("Failed to set output state", "err")

    def _man_set_mode(self):
        if not self._man_require_conn():
            return
        mode = self.man_mode_var.get()
        ok = self.kepco.send(f"FUNC:MODE {mode}")
        self.log(f"Mode → {mode}" if ok else "Failed to set mode",
                 "ok" if ok else "err")

    def _man_set_voltage(self):
        if not self._man_require_conn():
            return
        try:
            val = float(self.man_volt_entry.get())
        except ValueError:
            self.log("Invalid voltage value", "err")
            return
        ok = self.kepco.send(f"VOLT {val:.4f}")
        self.log(f"VOLT → {val:.4f} V" if ok else "Failed to set voltage",
                 "ok" if ok else "err")

    def _man_set_current(self):
        if not self._man_require_conn():
            return
        try:
            val = float(self.man_curr_entry.get())
        except ValueError:
            self.log("Invalid current value", "err")
            return
        ok = self.kepco.send(f"CURR {val:.4f}")
        self.log(f"CURR → {val:.4f} A" if ok else "Failed to set current",
                 "ok" if ok else "err")

    def _man_set_range(self):
        if not self._man_require_conn():
            return
        choice = self.man_range_var.get()
        mode = self.man_mode_var.get()
        if choice == "Auto":
            self.kepco.send(f"{mode}:RANG:AUTO ON")
            self.log(f"{mode} range → Auto", "ok")
        elif choice == "Full Scale":
            self.kepco.send(f"{mode}:RANG:AUTO OFF")
            self.kepco.send(f"{mode}:RANG 1")
            self.log(f"{mode} range → Full Scale", "ok")
        else:
            self.kepco.send(f"{mode}:RANG:AUTO OFF")
            self.kepco.send(f"{mode}:RANG 0")
            self.log(f"{mode} range → Quarter Scale", "ok")

    def _man_reset(self):
        if not self._man_require_conn():
            return
        ok = self.kepco.send("*RST")
        if ok:
            self.man_outp_var.set("OFF")
            self.man_outp_switch.deselect()
            self.man_outp_lbl.configure(text="OFF", text_color=C["red"])
            self.man_mode_var.set("VOLT")
            self.log("Device reset (*RST)", "ok")
        else:
            self.log("Reset failed", "err")

    def _man_measure(self):
        if not self.kepco.connected:
            return
        v = self.kepco.send("MEAS:VOLT?", query=True)
        c = self.kepco.send("MEAS:CURR?", query=True)
        m = self.kepco.send("FUNC:MODE?", query=True)
        try:
            v_str = f"{float(v):.4f}" if v else "— — —"
        except (ValueError, TypeError):
            v_str = v or "— — —"
        try:
            c_str = f"{float(c):.4f}" if c else "— — —"
        except (ValueError, TypeError):
            c_str = c or "— — —"
        self.meas_volt_lbl.configure(text=f"Voltage:  {v_str}  V")
        self.meas_curr_lbl.configure(text=f"Current:  {c_str}  A")
        self.meas_mode_lbl.configure(text=f"Mode:  {m or '— — —'}")

    def _man_toggle_auto_meas(self):
        if self.auto_meas_var.get():
            self._man_auto_meas_tick()
        else:
            if self._meas_timer:
                self.root.after_cancel(self._meas_timer)
                self._meas_timer = None

    def _man_auto_meas_tick(self):
        if self.auto_meas_var.get() and self.kepco.connected:
            self._man_measure()
            self._meas_timer = self.root.after(1000, self._man_auto_meas_tick)
        else:
            self._meas_timer = None

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
        if is_query:
            resp = self.kepco.send(cmd, query=True)
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.insert("end",
                f"[{ts}] < {resp or '(no response)'}\n")
        else:
            ok = self.kepco.send(cmd)
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.insert("end",
                f"[{ts}] {'✓ OK' if ok else '✗ Failed'}\n")
        self.scpi_resp.see("end")
        self.log(f"SCPI: {cmd}", "info")

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
        checks = [
            "*IDN?",
            "FUNC:MODE?",
            "OUTP?",
            "LIST:VOLT:POIN?",
            "LIST:CURR:POIN?",
            "SYST:ERR?",
            "*ESR?",
        ]
        ts = time.strftime("%H:%M:%S")
        self.scpi_resp.insert("end", f"[{ts}] ==== Health Check ====\n")
        for cmd in checks:
            resp = self.kepco.send(cmd, query=True)
            self.scpi_resp.insert("end", f"[{ts}] > {cmd}\n")
            self.scpi_resp.insert("end", f"[{ts}] < {resp or '(no response)'}\n")
        self.scpi_resp.insert("end", f"[{ts}] =====================\n")
        self.scpi_resp.see("end")
        self.log("Manual health check complete", "ok")

    # ──────────────────────────────────────────────────────────────────────
    #  Graph
    # ──────────────────────────────────────────────────────────────────────
    def _style_ax(self):
        self.ax.set_facecolor(C["graph_bg"])
        for sp in self.ax.spines.values():
            sp.set_color(C["border"])
        self.ax.tick_params(colors=C["text2"], labelsize=9)
        self.ax.xaxis.label.set_color(C["text2"])
        self.ax.yaxis.label.set_color(C["text2"])
        self.ax.grid(True, color="#2a2a40", linewidth=0.5, alpha=0.6)

    def _update_graph(self, points=None, chunk_idx=-1):
        self.ax.clear()
        self._style_ax()
        self.ax.set_xlabel("Sample Index")
        self.ax.set_ylabel("Amplitude (V / A)")

        if not points:
            self.ax.set_title("No waveform — configure & preview",
                              color=C["text2"], fontsize=11)
            self.canvas.draw_idle()
            return

        n = len(points)
        chunk_sz = MAX_LIST_POINTS
        chunks = [points[i:i + chunk_sz] for i in range(0, n, chunk_sz)]
        nc = len(chunks)
        colors = C["chunk_colors"]

        for ci, ck in enumerate(chunks):
            start = ci * chunk_sz
            xs = list(range(start, start + len(ck)))
            col = colors[ci % len(colors)]
            lw, alpha = 1.2, 1.0

            if chunk_idx >= 0:               # sending mode
                if ci < chunk_idx:            # already sent → dim
                    alpha = 0.30
                elif ci == chunk_idx:         # currently sending → bold pink
                    lw, col = 2.8, C["sent"]
                else:                         # not yet sent
                    alpha = 0.45

            self.ax.plot(xs, ck, color=col, linewidth=lw, alpha=alpha)

        # chunk boundary lines
        for ci in range(1, nc):
            self.ax.axvline(ci * chunk_sz, color=C["border"],
                            linestyle="--", linewidth=0.7, alpha=0.6)

        # legend (idle mode only)
        if chunk_idx < 0 and nc > 1:
            handles = [mlines.Line2D(
                [], [], color=colors[i % len(colors)], linewidth=2,
                label=f"Chunk {i+1}  ({len(chunks[i])} pts)")
                for i in range(nc)]
            self.ax.legend(handles=handles, fontsize=8, loc="upper right",
                           facecolor=C["card"], edgecolor=C["border"],
                           labelcolor=C["text2"])

        title = (f"Waveform — {n} points, {nc} chunk(s)"
                 if chunk_idx < 0
                 else f"Sending chunk {chunk_idx+1} / {nc} …")
        self.ax.set_title(title, color=C["text"], fontsize=11)
        self.fig.tight_layout(pad=1.5)
        self.canvas.draw_idle()

    # ──────────────────────────────────────────────────────────────────────
    #  Auto-discovery
    # ──────────────────────────────────────────────────────────────────────
    def _start_scan(self):
        self.scan_btn.configure(state="disabled", text="Scanning…")
        self.log(
            "Scanning local subnet for Kepco devices "
            "(Telnet 5024 first, fallback 5025)…")
        ip = self.ip_var.get().strip()
        base = ".".join(ip.split(".")[:3]) + ".0" if ip else "192.168.50.0"

        def done(results):
            self.root.after(0, lambda: self._scan_done(results))

        def prog(d, t):
            self.root.after(0, lambda: self.progress.set(d / t))

        threading.Thread(target=Discovery.scan_subnet,
                         args=(base, done, prog), daemon=True).start()

    def _scan_done(self, results):
        self.scan_btn.configure(state="normal", text="⟳  Scan Network")
        self.progress.set(0)
        if results:
            ips = [r[0] for r in results]
            self.ip_combo.configure(values=ips)
            self.ip_var.set(ips[0])
            for ip, idn in results:
                self.log(f"Found: {ip}  →  {idn}", "ok")
        else:
            self.log("No Kepco devices found on subnet.", "warn")

    # ──────────────────────────────────────────────────────────────────────
    #  Connection
    # ──────────────────────────────────────────────────────────────────────
    def _safe_disconnect_sequence(self):
        """Force safe state before disconnecting from device.

        Returns True only when all required safety commands succeed.
        """
        steps = [
            ("VOLT 0", "set voltage to 0V"),
            ("CURR 0", "set current to 0A"),
            ("OUTP OFF", "turn output OFF"),
        ]
        for cmd, desc in steps:
            ok = self.kepco.send(cmd)
            if not ok:
                err = self.kepco.last_error or "send failed"
                self.log(
                    f"Safety interlock: failed to {desc} before disconnect ({err})",
                    "err")
                messagebox.showerror(
                    "Safety Interlock",
                    "Disconnect blocked.\n"
                    f"Could not {desc} before disconnecting.\n"
                    "Check connection and retry.")
                return False

        self.man_outp_var.set("OFF")
        self.man_outp_switch.deselect()
        self.man_outp_lbl.configure(text="OFF", text_color=C["red"])
        self.man_volt_entry.delete(0, "end")
        self.man_volt_entry.insert(0, "0.0")
        self.man_curr_entry.delete(0, "end")
        self.man_curr_entry.insert(0, "0.0")
        self.log("Safety interlock: VOLT/CURR set to 0 and output turned OFF.", "ok")
        return True

    def _toggle_connect(self):
        if not self.kepco.connected:
            ip = self.ip_var.get().strip()
            ok, msg = self.kepco.connect(ip)
            if ok:
                self.conn_btn.configure(text="Disconnect",
                                        fg_color=C["red"],
                                        hover_color="#dc2626")
                self.status_lbl.configure(text="●  Connected",
                                          text_color=C["green"])
                idn = self.kepco.identity() or "Unknown device"
                self.idn_lbl.configure(text=idn)
                self.log(
                    f"Connected to {ip} via {self.kepco.transport} "
                    f"({self.kepco.port}):  {idn}", "ok")
            else:
                self.log(f"Connection failed: {msg}", "err")
        else:
            if not self._safe_disconnect_sequence():
                return
            self.kepco.disconnect()
            self.conn_btn.configure(text="Connect",
                                    fg_color=C["primary"],
                                    hover_color=C["primary_h"])
            self.status_lbl.configure(text="●  Disconnected",
                                      text_color=C["red"])
            self.idn_lbl.configure(text="")
            self.log("Disconnected.")

    # ──────────────────────────────────────────────────────────────────────
    #  CSV
    # ──────────────────────────────────────────────────────────────────────
    def _on_wave_change(self, _=None):
        if self.wave_var.get() == "CSV Custom":
            self.csv_frame.pack(fill="x", padx=14, pady=(0, 6),
                                after=self.wave_combo)
        else:
            self.csv_frame.pack_forget()

    def _load_csv(self):
        path = filedialog.askopenfilename(
            filetypes=[("CSV", "*.csv"), ("All", "*.*")])
        if not path:
            return
        try:
            with open(path, "r") as f:
                self.csv_points = [float(x)
                                   for row in csv.reader(f)
                                   for x in row if x.strip()]
            name = path.rsplit("/", 1)[-1]
            self.csv_lbl.configure(text=f"{name} ({len(self.csv_points)} pts)")
            self.log(f"Loaded CSV: {name} → {len(self.csv_points)} points", "ok")
        except Exception as e:
            from tkinter import messagebox
            messagebox.showerror("CSV Error", str(e))

    # ──────────────────────────────────────────────────────────────────────
    #  Parameter reading & waveform generation
    # ──────────────────────────────────────────────────────────────────────
    def _read_params(self):
        from tkinter import messagebox
        try:
            freq = float(self.freq_entry.get())
            amp  = float(self.amp_entry.get())
            off  = float(self.off_entry.get())
            pts  = int(self.pts_entry.get())
            loop = int(self.loop_entry.get())
        except ValueError:
            messagebox.showerror("Input Error", "Invalid numeric input.")
            return None
        if pts < 2:
            messagebox.showerror("Input Error", "Need ≥ 2 points.")
            return None
        if pts > MAX_TOTAL_POINTS:
            pts = MAX_TOTAL_POINTS
        return dict(freq=freq, amp=amp, offset=off,
                    points=pts, loop=loop,
                    wave=self.wave_var.get(), mode=self.mode_var.get())

    def _generate_points(self, p):
        """Build the waveform, respecting hardware timing constraints."""
        from tkinter import messagebox

        actual, dwell, actual_freq, warns = WaveformGen.calculate_timing(
            p["freq"], p["points"])
        if actual == 0:
            messagebox.showerror("Error", "\n".join(warns))
            return None, None, None

        if p["wave"] == "CSV Custom":
            if not self.csv_points:
                messagebox.showerror("Error", "Load a CSV file first.")
                return None, None, None
            pts = self.csv_points[:actual]
            actual = len(pts)
            dwell = (1.0 / p["freq"]) / actual if p["freq"] > 0 else MIN_DWELL
            if dwell < MIN_DWELL:
                dwell = MIN_DWELL
                actual_freq = 1.0 / (actual * dwell)
                warns.append(f"CSV dwell clamped to min {MIN_DWELL*1e6:.0f} µs")
        else:
            pts = WaveformGen.generate(p["wave"], actual, p["amp"], p["offset"])

        nc = math.ceil(len(pts) / MAX_LIST_POINTS)
        total_time = len(pts) * dwell
        info = [
            f"Points: {len(pts)}   ({nc} chunk{'s' if nc > 1 else ''}  ×  "
            f"{min(len(pts), MAX_LIST_POINTS)})",
            f"Dwell:  {dwell*1000:.4f} ms   ({dwell*1e6:.1f} µs)",
            f"Actual freq: {actual_freq:.4f} Hz",
            f"Period:  {total_time*1000:.3f} ms",
        ]
        if warns:
            info += [f"⚠ {w}" for w in warns]
        self.timing_lbl.configure(text="\n".join(info))
        return pts, dwell, warns

    def _preview(self):
        p = self._read_params()
        if not p:
            return
        pts, dwell, warns = self._generate_points(p)
        if pts is None:
            return
        self.current_points = pts
        self._update_graph(pts)
        self.log(f"Preview: {len(pts)} pts, dwell={dwell*1000:.4f} ms,"
                 f" {math.ceil(len(pts)/MAX_LIST_POINTS)} chunk(s)")

    # ──────────────────────────────────────────────────────────────────────
    #  Upload & Run  (chunked, background thread)
    # ──────────────────────────────────────────────────────────────────────
    def _pause_auto_measure(self):
        """Pause auto-measure so it doesn't compete for the SCPI bus."""
        self._saved_auto_meas = self.auto_meas_var.get()
        if self._saved_auto_meas:
            self.auto_meas_var.set(False)
            if self._meas_timer:
                self.root.after_cancel(self._meas_timer)
                self._meas_timer = None

    def _resume_auto_measure(self):
        """Restore auto-measure to its previous state."""
        if getattr(self, "_saved_auto_meas", False):
            self.auto_meas_var.set(True)
            self._man_auto_meas_tick()

    def _run(self):
        from tkinter import messagebox
        if self.is_running:
            self.log("Already running.", "warn")
            return
        if not self.kepco.connected:
            messagebox.showerror("Error", "Connect to a device first.")
            return

        p = self._read_params()
        if not p:
            return
        pts, dwell, _ = self._generate_points(p)
        if pts is None:
            return

        self.current_points = pts
        self.stop_event.clear()
        self.is_running = True
        self.run_btn.configure(state="disabled")
        self._pause_auto_measure()

        threading.Thread(
            target=self._upload_thread,
            args=(pts, dwell, p["mode"], p["loop"]),
            daemon=True).start()

    def _upload_thread(self, points, dwell, mode, loop_count):
        """Upload waveform in ≤ 1000-point chunks, run each sequentially.

        Each chunk is uploaded with paced writes (35 ms gap, no *OPC? spam),
        then verified with LIST:{mode}:POIN? and SYST:ERR? before running.
        """
        try:
            chunks = [points[i:i + MAX_LIST_POINTS]
                      for i in range(0, len(points), MAX_LIST_POINTS)]
            nc = len(chunks)
            loops = max(loop_count, 1) if loop_count > 0 else 0
            forever = loops == 0

            self._log_safe(
                f"Upload: {len(points)} pts → {nc} chunk(s), "
                f"dwell={dwell*1000:.4f} ms, "
                f"loops={'∞' if forever else loops}")

            def _progress_cb(sent, total):
                """Called by upload_list_chunk after each value batch."""
                pct = sent / max(total, 1)
                self.root.after(0, lambda p=pct: self.progress.set(p))
                self.root.after(0, lambda s=sent, t=total:
                    self.prog_lbl.configure(
                        text=f"Uploading… {s}/{t} pts"))

            if nc == 1:
                # ── single chunk: upload → verify → run ──
                self._ui_chunk(0, points)
                self.root.after(0, lambda: self.prog_lbl.configure(
                    text="Uploading…"))

                ok, msg = self.kepco.upload_list_chunk(
                    chunks[0], dwell, mode, progress_cb=_progress_cb)
                if not ok:
                    self._log_safe(f"Upload error: {msg}", "err")
                    return
                self._log_safe(f"Uploaded: {msg}", "ok")

                ok, msg = self.kepco.run_list(mode, loops)
                self._log_safe(f"Run: {msg}", "ok" if ok else "err")
                self._ui_chunk(-1, points)
                self.root.after(0, lambda: self.progress.set(1.0))
                self.root.after(0, lambda: self.prog_lbl.configure(
                    text="Running…"))
            else:
                # ── multi-chunk: upload→run→wait each, repeat ──
                iters = loops if loops > 0 else 1
                it = 0
                while not self.stop_event.is_set():
                    it += 1
                    if not forever and it > iters:
                        break

                    for ci, ck in enumerate(chunks):
                        if self.stop_event.is_set():
                            break

                        self._ui_chunk(ci, points)
                        il = "∞" if forever else f"{it}/{iters}"
                        self.root.after(0, lambda c=ci, n=nc, l=il:
                            self.prog_lbl.configure(
                                text=f"Chunk {c+1}/{n} — loop {l}"))

                        # Upload this chunk (with per-batch progress)
                        ok, msg = self.kepco.upload_list_chunk(
                            ck, dwell, mode, progress_cb=_progress_cb)
                        if not ok:
                            self._log_safe(
                                f"Chunk {ci+1} upload failed: {msg}", "err")
                            return
                        self._log_safe(
                            f"Chunk {ci+1}/{nc}: {msg}", "ok")

                        # Run this chunk once
                        ok, msg = self.kepco.run_list(mode, count=1)
                        if not ok:
                            self._log_safe(
                                f"Chunk {ci+1} run failed: {msg}", "err")
                            return

                        # Wait for chunk to finish executing + margin
                        wait = len(ck) * dwell + 0.10
                        elapsed = 0.0
                        while elapsed < wait and not self.stop_event.is_set():
                            time.sleep(min(0.05, wait - elapsed))
                            elapsed += 0.05

                        pct = (ci + 1) / nc
                        self.root.after(0, lambda p=pct: self.progress.set(p))

                    if not self.stop_event.is_set():
                        self._log_safe(f"Completed iteration {it}", "ok")

                self._ui_chunk(-1, points)

            self.root.after(0, lambda: self.prog_lbl.configure(text="Done"))
            self.root.after(0, lambda: self.progress.set(1.0))
            self._log_safe("Waveform sequence complete.", "ok")

        except Exception as e:
            self._log_safe(f"Error: {e}", "err")
        finally:
            self.is_running = False
            self.root.after(0, lambda: self.run_btn.configure(state="normal"))
            self.root.after(0, lambda: self._resume_auto_measure())

    def _ui_chunk(self, idx, pts):
        """Thread-safe graph update."""
        self.root.after(0, lambda: self._update_graph(pts, chunk_idx=idx))

    def _log_safe(self, msg, tag="info"):
        self.root.after(0, lambda: self.log(msg, tag))

    # ──────────────────────────────────────────────────────────────────────
    #  Stop
    # ──────────────────────────────────────────────────────────────────────
    def _stop(self):
        self.stop_event.set()
        if self.kepco.connected:
            ok, msg = self.kepco.stop()
            self.log(f"Stop: {msg}", "ok" if ok else "err")
        self.is_running = False
        self.run_btn.configure(state="normal")
        self.prog_lbl.configure(text="Idle")
        self.progress.set(0)
        self._resume_auto_measure()
        if self.current_points:
            self._update_graph(self.current_points)

    # ──────────────────────────────────────────────────────────────────────
    def run(self):
        self.root.mainloop()


# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    App().run()
