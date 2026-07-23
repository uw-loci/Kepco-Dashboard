# Kepco BIT 802E Dashboard

Desktop GUI for configuring, previewing, uploading, and running DC setpoints or LIST waveforms on a Kepco BIT 802E / BOP power supply. The app is built with CustomTkinter and Matplotlib, talks SCPI over TCP, and is designed around a staged workflow: connect, configure, preview, upload, then enable output.

## What The App Does

- Connects to a Kepco by IP, preferring the direct raw SCPI socket on `5025` and using Telnet on `5024` only as fallback, then verifies a complete device-state snapshot before enabling control
- Scans the selected `/24` subnet and validates candidate devices with `*IDN?`
- Generates `DC`, `Sine`, `Square`, `Triangle`, `Sawtooth`, and CSV-based waveforms
- Previews waveform shape locally before sending anything to hardware
- Uploads LIST data in verified chunks that fit the BIT 802E command and point limits
- Supports voltage (`VOLT`) and current (`CURR`) control modes with one absolute limit value for each channel
- Provides manual SCPI controls, quick diagnostic queries, range control, health check, and reset
- Shows the uploaded waveform, live output state, control mode, and voltage/current readback only while device communication is verified
- Monitors DC current-mode setpoints against live current and temperature-adjusted voltage expectations
- Optionally records live readback samples to CSV
- Logs session and communication activity to `logs/`
- Attempts a safe shutdown on disconnect or close by stopping output and verifying `OFF` at approximately `0 V` and `0 A`

## Main Files

- `kepco_ui.py`: main desktop application, SCPI controller, waveform generation, discovery, and UI orchestration
- `solenoid_temperature_reader.py`: reads latest solenoid temperatures from EBEAM WebMonitor JSONL logs for DC current monitoring
- `requirements.txt`: Python dependencies
- `docs/802e_manual.md`: device reference material
- [`docs/KEPCO LAN IP Configuration Steps 2026-3-15-CMov.pdf`](docs/KEPCO%20LAN%20IP%20Configuration%20Steps%202026-3-15-CMov.pdf): Kepco LAN/IP setup reference for configuring network access to the supply

The main code paths in `kepco_ui.py` are organized around four classes:

- `DashboardApp`: builds the UI and coordinates preview, upload, output control, status polling, logging, and data collection
- `KepcoController`: owns SCPI transport, command pacing, LIST upload/run/stop behavior, and disconnect safety actions
- `Discovery`: scans a `/24` subnet for devices that respond like Kepco/BOP/BIT hardware
- `WaveformGen`: calculates dwell timing and generates waveform point lists

## Getting Started
New Kepco devices must be configured for LAN/IP access before the dashboard can connect to them. Follow the steps in the [Kepco LAN/IP configuration guide](docs/KEPCO%20LAN%20IP%20Configuration%20Steps%202026-3-15-CMov.pdf) before using **Scan Network** or **Connect** with a new supply.


Create a virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start the dashboard:

```powershell
python kepco_ui.py
```

## Typical Workflow

1. Enter a device IP or use **Scan Network**.
2. Click **Connect** and wait for the device identity and state verification to complete. Controls remain locked until the dashboard reports **Connected**.
3. In **Manual Override**, select `VOLT` or `CURR` mode and adjust V/I limits if needed.
4. In **Waveform Generator**, choose a waveform and enter frequency, amplitude, offset, point count, and loop count.
5. Click **Preview Waveform** to validate the waveform locally.
6. Click **Upload** to stage the DC setpoint or LIST waveform on the device.
7. Use **Enable Output** to run the uploaded output.
8. Use **Disable Output**, **Disconnect**, or close the app when finished.

Enabling output remains locked until a waveform or DC setpoint has been uploaded. If the app connects to a Kepco that is already reporting output ON, the output control remains available so the operator can disable it without uploading a waveform first.

## Waveforms And Uploads

`DC` uses fixed `VOLT` or `CURR` commands and does not use LIST mode.

Generated AC waveforms and CSV waveforms use LIST mode. A single LIST upload is limited to `1000` points, so larger waveforms are staged as multiple chunks. The app supports up to `4000` total points and streams multi-chunk waveforms chunk-by-chunk while output is enabled.

CSV mode reads numeric cells from the selected file, flattens them into one point list, requires at least two points, and uses the selected frequency to calculate dwell. If the file contains more than `4000` values, only the first `4000` are used.

## Limits And Safety

Implemented hardware and software limits:

- Minimum dwell: `0.0005 s`
- Maximum dwell: `10.0 s`
- Maximum LIST points per upload chunk: `1000`
- Maximum staged waveform points: `4000`
- BOP voltage rating used by the UI limit check: `+/-100 V`
- BOP current rating used by the UI limit check: `+/-2 A`
- Default absolute voltage limit: `40 V`
- Default absolute current limit: `2 A`

Before preview or upload, the app validates the requested waveform symmetrically against the configured absolute limit for the selected control mode. On disconnect or application close, it stops LIST mode, zeros only the active fixed-output channel, sends `OUTP OFF`, and verifies `OUTP?`, `MEAS:VOLT?`, and `MEAS:CURR?`. This preserves the complementary hardware limit and avoids programming the inactive channel during shutdown. If verification fails, disconnect or close is blocked so the operator can resolve the unsafe state.

The BIT 802E exposes one complementary hardware-limit channel, not independent positive and negative SCPI limit registers. The dashboard therefore accepts one absolute voltage limit and one absolute current limit. Each row shows the currently committed dashboard value in the left display box and accepts a pending value in the adjacent entry; **Set** commits it after local staging or successful device programming. Each committed value defines a symmetric software interlock (`-limit` through `+limit`) and is sent only when it is the complementary hardware channel: `VOLT <limit>` in current mode or `CURR <limit>` in voltage mode. The BOP front-panel screwdriver adjustments remain the independent physical polarity limits described by the manual.

## Communication State And Verification

The dashboard distinguishes a live TCP socket from a trustworthy device state. Its communication states are `DISCONNECTED`, `CONNECTING`, `VERIFYING`, `HEALTHY`, `DEGRADED`, and `FAULTED`. `HEALTHY` is the final verified state that unlocks normal controls.

A successful TCP connection enters `VERIFYING`; neither the socket nor a `*IDN?` reply alone enables normal controls. The health gate requires a valid Kepco `*IDN?` reply, an exact `*OPC?` reply of `1`, and three complete valid status snapshots. Telnet sessions additionally require `SYST:REM?` to report remote mode; when necessary the controller sends `SYST:REM 1` and verifies it before enabling control. All three snapshots must report the same output state and control mode before the dashboard transitions to `HEALTHY`. While this runs, the UI displays **Connected — Verifying Device State** and keeps control buttons disabled. Query-specific validators reject nonnumeric/nonfinite values, invalid output or mode tokens, voltage above `110 V`, and current above `2.2 A`; a response such as `33 A` is rejected rather than displayed as a valid readback.

The four poll replies are treated as one atomic snapshot: no voltage, current, output, or mode field is updated until every reply passes its query-specific validator. Polling validates each reply before issuing the next query: a timeout, missing reply, malformed token, or impossible value aborts that poll immediately, so later queries cannot consume a delayed response and become shifted. If a status response fails, communication enters `DEGRADED`. Output state is displayed as **Output: UNKNOWN** with the instruction **Verify the KEPCO before interacting with the load**; normal controls and polling are locked, and the rejected snapshot is not written to data collection. A lost dashboard session never implies the physical BOP output is off. Use **Recover** to create a fresh socket session and repeat full verification. If recovery cannot verify the state, the dashboard enters `FAULTED` and requires operator action.

## Status, Logs, And Data Collection

While communication is verified (`HEALTHY`), the dashboard polls:

- `MEAS:VOLT?`
- `MEAS:CURR?`
- `OUTP?`
- `FUNC:MODE?`

During steady-state operation, the next four-query status snapshot is scheduled
1 second after the previous snapshot completes. Connect and operator
transactions can still request an immediate refresh.

The status panel also shows the latest solenoid 1 and solenoid 2 temperatures read from the EBEAM WebMonitor log directory:

```text
~/EBEAM_dashboard/EBEAM-Dashboard-WMLogs/webMonitor_log_*.txt
```

The newest valid JSONL status entry is used. If the file cannot be found, cannot be parsed, or no longer updates, the live console shows a WebMonitor warning. WebMonitor values are polled every `3 s`; values older than `10 s` are marked stale.

The on-screen **Event Log** starts at a compact height. Use **Expand Log**
or press `Alt+L` to enlarge it vertically; the expanded height follows the
window size. While expanded, the preview and active/uploaded waveform graphs
are condensed so the log receives its requested space on low-resolution
displays. Use **Collapse Log** or `Alt+L` again to restore the compact log and
normal graph sizes.

## DC Current Monitoring

The **DC Monitor Thresholds** controls set percentage tolerances for the DC current monitor. Their dark read-only boxes show the committed symmetric tolerances as `+/-value`; each vertically stacked threshold has its own pending entry and **Set** button. The default voltage and current tolerances are both `5%`.

The monitor is active only during this stage:

- The app is connected to the Kepco
- Output is enabled
- The uploaded/staged request is `DC`
- The selected uploaded control mode is `CURR`
- The device status poll reports `CURR` mode

In every other stage, including disconnected, uploaded-but-output-off, voltage-mode DC, LIST waveform upload, LIST waveform output, AC waveform streaming, and output transitions, the voltage and current monitor lines are set to inactive.

When active, the current monitor compares measured current against the uploaded DC current setpoint using the configured current tolerance. The voltage monitor calculates the expected supply voltage from the current setpoint and the two WebMonitor solenoid temperatures:

```text
expected voltage = Iset * (20.95 + 0.0470 * (solenoid_1_temp + solenoid_2_temp))
```

Measured voltage is then compared with that expected voltage using the configured voltage tolerance. If either temperature, the setpoint, or the live voltage is unavailable, the voltage monitor reports that it cannot compute the expected voltage. These monitor messages are indicators in the live console; they do not replace the upload-time limit checks or the disconnect/close safety interlock.

Session logs are written to:

```text
logs/kepco_dashboard_date_YYYY-MM-DD_HHMMSS.log
```

The session file retains all application and communication diagnostics, including high-frequency polling traffic. The bottom event panel intentionally shows only successful events, warnings, errors, and critical errors so hardware actions are not displaced by SCPI polling. Each visible line includes an `INFO`, `WARNING`, `ERROR`, or `CRITICAL ERROR` level.

When **Collect data** is enabled, readback samples are written to:

```text
logs/kepco_readback_collection_date_YYYY-MM-DD_HHMMSS.csv
```

The data collection CSV includes timestamp, elapsed seconds, readback voltage, readback current, output state, and mode. Only accepted, verified status snapshots are recorded.

## Communication Notes

One dedicated worker thread owns the SCPI socket. Polling, DC and LIST transactions, connection/recovery, and safe disconnect all enter the same FIFO queue; nested controller calls execute directly on that owner so a complete four-query status snapshot cannot be interleaved with commands from another workflow. One monotonic-time throttle enforces at least a `35 ms` gap between every SCPI transmission, including command-to-command, command-to-query, query-to-command, and query-to-query transitions. Time spent waiting for a query response counts toward the interval, so slow replies do not incur an additional fixed delay. Normal SCPI queries use a `6 s` receive timeout. Raw port `5025` responses are accepted only after a complete LF-terminated frame (including CRLF) has arrived; fragmented bytes remain in a persistent receive buffer and are never accepted as a timeout tail. A missing/incomplete frame retires the socket immediately and is not retried on that session. Telnet port `5024` uses separate stateful IAC, prompt, and echo parsing, requires verified remote mode before becoming healthy, and treats EOF or connection errors during echo draining as transport loss. Low-level send functions never reconnect; they abort the active transaction when no trusted socket is available. Reconnection is performed only by the high-level **Connect**/**Recover** workflow, which creates a fresh session and runs the full verification gate as one queued transaction. The log records communication-state transitions, recovery attempts, verification snapshots, and rejected response reasons. Uploads verify accepted LIST point count and check the device error queue after the transfer.

Network discovery and active control are mutually exclusive. The dashboard disables scanning while connecting or connected so it cannot open a second competing socket to the same Kepco.

Dependent DC state changes do not rely on the `35 ms` pacing delay. If a mode transition is actually needed, the controller sends `FUNC:MODE`, waits with `*WAI`, and verifies `FUNC:MODE?`; it changes the selected source to `MODE FIX` only when necessary, waits again, and verifies the source `MODE?`. Initial DC/LIST staging then follows the manual's optimized sequence: lock the active source to full scale with `RANG 1`, stage the operating parameter at zero, and program the one complementary limit to its desired absolute maximum. For DC output enable, the requested operating setpoint is programmed and verified while output remains off, then `OUTP ON` is sent. Later same-mode DC updates change only the operating parameter; the complementary limit is rewritten only when the operator changes it. Multi-chunk LIST streaming similarly establishes the complementary limit with the first staged chunk and preserves it across subsequent chunk uploads. Live DC mode changes are rejected until output is disabled. Polling is paused for DC staging, mode/range/limit changes, output transitions, reset/manual writes, recovery, and disconnect. It resumes after a verified postflight snapshot or, when the socket remains healthy, after logging a command rejection. BIT `-221,"Settings conflict"` entries are retained as contextual advisory warnings and do not block an otherwise verified transaction. Other cleanly received SCPI rejections fail the requested action and are logged without being misclassified as communication degradation. Transport framing failures, timeouts, invalid responses, and socket failures enter `DEGRADED` and require fresh-socket recovery.

Changing between voltage and current control clears any staged waveform and
locks output enable until a new waveform is uploaded in the selected mode.
Successful output transitions are logged explicitly as `Output ON` or
`Output OFF` rather than as a generic command result.

Programmed setpoint and limit queries are compared using BIT-resolution-aware tolerances rather than exact decimal equality. The active main channel uses the manual's 15 magnitude bits; the complementary limit channel uses 12 bits. A two-LSB floor accommodates the documented calibrated readback behavior, while differences beyond that floor (or the existing relative tolerance, whichever is larger) still reject the transaction. Accepted quantized values and their tolerances are recorded in the communication log.

Preview is local-only. Upload, output toggle, manual SCPI commands, status polling, and disconnect safety checks are the paths that communicate with the device.

## Maintenance Notes

- Route device communication through `KepcoController.send_cmd`, `send_query`, `send_sequence`, or `run_transaction`; those helpers enforce socket-owner serialization, command pacing, Telnet echo handling, and verified-state/recovery behavior.
- Worker threads must update the GUI through `DashboardApp._call_on_ui`. Direct Tkinter updates from background threads can destabilize the UI.
- DC transactions invalidate any in-flight poll generation and remain paused until their queued postflight verification succeeds. LIST upload/streaming also pauses periodic polling; disconnect and recovery keep it paused until the session is either safely closed or fully reverified.
- Waveforms over `1000` points are not resident on the device all at once. The first chunk is primed, then `_sequence_worker` uploads and runs subsequent chunks while streaming.
- The voltage and current limit entries are absolute magnitudes. The active output channel is checked symmetrically against its magnitude in software, and only the complementary channel is programmed on the BIT when appropriate.
- Data collection is driven by status polling, so samples pause whenever polling is paused for uploads, output transitions, streaming, or disconnect safety checks.

## Troubleshooting

- When connecting the Kepco to a new laptop/device, set a manual IP address for the Ethernet adapter on the same subnet. See step 7 in the [Kepco LAN/IP configuration guide](docs/KEPCO%20LAN%20IP%20Configuration%20Steps%202026-3-15-CMov.pdf).
- If connection fails, confirm the IP address and check ports `5024` and `5025`.
- Before deployment, verify raw socket port `5025` behavior on the installed BIT 802E firmware; Telnet `5024` remains the compatibility fallback.
- If the dashboard shows **Communication degraded**, do not trust displayed output/readback values. Use **Recover**; if it reaches **Communication faulted**, investigate the link/device and reconnect after operator action.
- A logged `-221,"Settings conflict"` is advisory. The dashboard records the transaction that produced it and continues when the requested live state verifies successfully.
- Other device command errors abort and log the requested action without forcing communication recovery. Verify the requested hardware state before trying a corrective command.
- If scan finds nothing, enter an IP in the expected subnet first; scan uses that `/24`.
- If upload is rejected, check waveform point count, dwell warnings, and the configured V/I limits.
- If output is OFF and the output control is locked, upload a waveform or DC setpoint before enabling it. If the connected Kepco is already reporting output ON, use **Disable Output** without uploading a waveform first.
- Review the on-screen log or the saved session log for SCPI-level details.

## Known Limitations

- Automated tests cover transport framing, socket-owner serialization, atomic snapshots, command-error behavior, output-control gating, and the manual-recommended DC command sequence. Hardware-in-the-loop endurance testing is still required on the target BIT 802E firmware.
- CSV mode is labeled `untested` in the UI and should be validated on target hardware.
- BIT 802E readback can be inaccurate while LIST-driven AC output is active; the UI shows a warning during that state.
- Higher frequency requests may reduce the requested point count because dwell cannot go below `0.0005 s`.
