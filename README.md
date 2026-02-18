# Kepco BIT 802E — Waveform Generator (High‑Performance)

A Material-style GUI to generate, preview and upload waveform LISTs to a Kepco BIT‑802E / BOP power supply. Includes a hardened SCPI controller (Telnet + direct socket), chunked list upload with pacing and verification, network auto-discovery, and a full-featured simulator for offline testing.

✅ Key features
- Realistic, hardware-aware waveform generation and timing (dwell constraints enforced)
- Chunked LIST upload (≤ 1000 pts per chunk) with paced writes and verification
- Robust SCPI over Telnet (IAC filtering) + socket fallback
- Live waveform preview, manual SCPI console, auto-discovery on /24 subnets
- Built-in simulator (`kepco_simulator.py`) for development and testing
- Safety interlocks (sets outputs to 0 and turns OFF before disconnect)

Files of interest
- `kepco_ui.py` — main GUI application (modern CustomTkinter UI)
- `kepco_simulator.py` — device simulator (Telnet 5024 / socket 5025)
- `requirements.txt` — runtime dependencies
- `802E opr-r2 operating manual.md` — reference manual included in workspace

Hardware constraints (implemented)
- Minimum dwell: 500 µs (MIN_DWELL)
- Maximum dwell: 10 s (MAX_DWELL)
- Max points per LIST upload: 1000 (MAX_LIST_POINTS)
- Telnet port: 5024; SCPI socket fallback: 5025

Quickstart — run locally
1. Create and activate a virtualenv (recommended):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. (Optional) Start simulator for local testing:

   ```bash
   python3 kepco_simulator.py
   # simulator listens on 0.0.0.0:5024 (Telnet) and :5025 (socket)
   ```

3. Start the UI:

   ```bash
   python3 kepco_ui.py
   ```

4. In the UI set the IP (use `127.0.0.1` for the local simulator or the device IP), Connect → Preview → Upload & Run.

CSV format
- Any CSV with numeric values is accepted. Values are flattened row-by-row and parsed as floats.
- If a CSV has more points than allowed, it will be truncated to hardware limits (max 4000 total / 1000 per chunk).

Usage notes & safety
- The app enforces device pacing (≈35 ms gap) and consumes Telnet echoes to avoid device deadlocks.
- The app issues safety commands (VOLT 0 / CURR 0 / OUTP OFF) before disconnecting — do not bypass.
- For multi-chunk waveforms, the UI uploads chunks sequentially and runs each chunk once (or loops if configured).

Troubleshooting
- Connection failing? Check firewall and that the device answers on port 5024 (Telnet) or 5025 (SCPI socket).
- If you see frozen commands on real hardware, ensure Telnet echoes are being drained and the device firmware is compatible with the manual's timing constraints.
- Use the simulator (`kepco_simulator.py`) to reproduce behavior without hardware.

Developer notes & recommended improvements
- Well implemented: robust SCPI handling (IAC stripping, echo drain), paced writes, and verification via `LIST:...:POIN?` + `SYST:ERR?`.
- Recommended next steps:
  1. Add unit tests for `WaveformGen`, `KepcoController` (IAC stripping & pacing), and `Discovery.scan_subnet`.
  2. Split GUI and controller for easier unit testing and add type annotations across modules.
  3. Add CI (GitHub Actions) with linting and test runs.
  4. Add a LICENSE and CONTRIBUTING.md and provide a packaged entry point (console_scripts).

Known limitations
- No automated unit tests in the repository yet.
- No license file included (add one if you plan to open-source).
- GUI is Tk-based (CustomTkinter) — headless operation requires separate CLI utilities.

Contributing
- Fork → develop on a feature branch → open a PR. Add tests for new behaviour and keep UI/controller separation in mind.