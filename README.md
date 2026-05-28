# Kepco BIT 802E Dashboard

Desktop GUI for configuring, previewing, uploading, and running DC setpoints or LIST waveforms on a Kepco BIT 802E / BOP power supply. The app is built with CustomTkinter and Matplotlib, talks SCPI over TCP, and is designed around a staged workflow: connect, configure, preview, upload, then enable output.

## What The App Does

- Connects to a Kepco by IP, trying Telnet on `5024` first and direct SCPI socket on `5025` second
- Scans the selected `/24` subnet and validates candidate devices with `*IDN?`
- Generates `DC`, `Sine`, `Square`, `Triangle`, `Sawtooth`, and CSV-based waveforms
- Previews waveform shape locally before sending anything to hardware
- Uploads LIST data in verified chunks that fit the BIT 802E command and point limits
- Supports voltage (`VOLT`) and current (`CURR`) control modes with signed software limits
- Provides manual SCPI controls, quick diagnostic queries, range control, health check, and reset
- Shows the uploaded waveform, live output state, control mode, and voltage/current readback
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
2. Click **Connect** and confirm the device identity appears.
3. In **Manual Override**, select `VOLT` or `CURR` mode and adjust V/I limits if needed.
4. In **Waveform Generator**, choose a waveform and enter frequency, amplitude, offset, point count, and loop count.
5. Click **Preview Waveform** to validate the waveform locally.
6. Click **Upload** to stage the DC setpoint or LIST waveform on the device.
7. Use **Enable Output** to run the uploaded output.
8. Use **Disable Output**, **Disconnect**, or close the app when finished.

The output control remains locked until a waveform or DC setpoint has been uploaded.

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
- Default voltage compliance entries: `+20 V` and `-20 V`
- Default current limit entries: `+2 A` and `-2 A`

Before preview or upload, the app validates the requested waveform against the configured signed limit range for the selected control mode. On disconnect or application close, it stops LIST mode, sets `VOLT 0`, sets `CURR 0`, sends `OUTP OFF`, and verifies `OUTP?`, `VOLT?`, and `CURR?`. If verification fails, disconnect or close is blocked so the operator can resolve the unsafe state.

## Status, Logs, And Data Collection

While connected, the dashboard polls:

- `MEAS:VOLT?`
- `MEAS:CURR?`
- `OUTP?`
- `FUNC:MODE?`

The status panel also shows the latest solenoid 1 and solenoid 2 temperatures read from the EBEAM WebMonitor log directory:

```text
~/EBEAM_dashboard/EBEAM-Dashboard-WMLogs/webMonitor_log_*.txt
```

The newest valid JSONL status entry is used. If the file cannot be found, cannot be parsed, or no longer updates, the live console shows a WebMonitor warning. WebMonitor values are polled every `3 s`; values older than `10 s` are marked stale.

## DC Current Monitoring

The **DC Monitor Thresholds** controls set percentage tolerances for the DC current monitor. The default voltage and current tolerances are both `5%`.

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

When **Collect data** is enabled, readback samples are written to:

```text
logs/kepco_readback_collection_date_YYYY-MM-DD_HHMMSS.csv
```

The data collection CSV includes timestamp, elapsed seconds, readback voltage, readback current, output state, and mode.

## Communication Notes

The controller uses paced SCPI writes with a `35 ms` gap between non-query commands. Telnet echo and negotiation bytes are drained so query responses are not confused with echoed commands. Uploads verify accepted LIST point count and check the device error queue after the transfer.

Preview is local-only. Upload, output toggle, manual SCPI commands, status polling, and disconnect safety checks are the paths that communicate with the device.

## Maintenance Notes

- Route device communication through `KepcoController.send_cmd`, `send_query`, or `send_sequence`; those helpers enforce locking, command pacing, Telnet echo handling, and reconnect behavior.
- Worker threads must update the GUI through `DashboardApp._call_on_ui`. Direct Tkinter updates from background threads can destabilize the UI.
- Upload, output-toggle, and streaming paths pause status polling, wait for any in-flight poll to finish, then resume polling. Disconnect waits for any active poll before running the safety sequence and stops polling after disconnect.
- Waveforms over `1000` points are not resident on the device all at once. The first chunk is primed, then `_sequence_worker` uploads and runs subsequent chunks while streaming.
- V/I limit entries serve both as software interlocks and as device limit inputs. The active output channel is checked in software; the complementary channel is sent to the device as the compliance/current limit when appropriate.
- Data collection is driven by status polling, so samples pause whenever polling is paused for uploads, output transitions, streaming, or disconnect safety checks.

## Troubleshooting

- When connecting the Kepco to a new laptop/device, set a manual IP address for the Ethernet adapter on the same subnet. See step 7 in the [Kepco LAN/IP configuration guide](docs/KEPCO%20LAN%20IP%20Configuration%20Steps%202026-3-15-CMov.pdf).
- If connection fails, confirm the IP address and check ports `5024` and `5025`.
- If scan finds nothing, enter an IP in the expected subnet first; scan uses that `/24`.
- If upload is rejected, check waveform point count, dwell warnings, and the configured V/I limits.
- If output is locked, upload a waveform or DC setpoint first.
- Review the on-screen log or the saved session log for SCPI-level details.

## Known Limitations

- The repository does not currently include automated tests.
- CSV mode is labeled `untested` in the UI and should be validated on target hardware.
- BIT 802E readback can be inaccurate while LIST-driven AC output is active; the UI shows a warning during that state.
- Higher frequency requests may reduce the requested point count because dwell cannot go below `0.0005 s`.
