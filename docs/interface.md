# Kepco-Dashboard Interface Documentation

This document provides an overview of the public interfaces and major components in the `Kepco-Dashboard` project. It is intended for developers who want to understand the architecture, extend the application, or integrate with other tools.

---

## Table of Contents

1. [Overview](#overview)
2. [Core Modules](#core-modules)
   - [kepco_ui.py](#kepcouipy)
   - [kepco_simulator.py](#kepco_simulatorpy)
3. [Classes & Interfaces](#classes--interfaces)
   - [KepcoController](#kepcocontroller)
   - [Discovery](#discovery)
   - [WaveformGen](#waveformgen)
   - [App](#app)
4. [Constants and Configuration](#constants-and-configuration)
5. [Error Handling and Logging](#error-handling-and-logging)
6. [Extensibility Points](#extensibility-points)
7. [UI Structure & Callbacks](#ui-structure--callbacks)
8. [Running and Deployment](#running-and-deployment)
9. [Appendices](#appendices)
   - [Terminology](#terminology)
   - [Hardware Limits](#hardware-limits)

---

## Overview

The application provides a Material-design GUI for generating and uploading waveforms to a Kepco BIT 802E waveform generator over a network connection. It includes network scanning, waveform math, and a manual SCPI console.

## Core Modules

### `kepco_ui.py`
Contains the main UI application and core logic. It defines the controller for Kepco hardware, discovery utilities, waveform generation and the `App` class which orchestrates the GUI. This is the entry point for users running the desktop application.

### `kepco_simulator.py`
Provides a comprehensive software simulator emulating the BIT 802E hardware and its Telnet/SCPI interface. Useful for development and testing without actual equipment. The module implements a `KepcoDevice` model along with a customtkinter dashboard and network listeners on ports 5024/5025.


## Classes & Interfaces

### `KepcoController`
- Thread-safe SCPI communication with hardware.
- Public methods: `connect`, `disconnect`, `send_cmd`, `send_query`, `upload_list_chunk`, `run_list`, `stop`, etc.
- Usage example: create instance, call `connect(ip)`, `upload_list_chunk(points, dwell)`.

### `Discovery`
- Static utilities for scanning a `/24` subnet.
- `scan_subnet(base_ip, callback=None, progress_cb=None)`.

### `WaveformGen`
- Static math helpers.
- `calculate_timing(freq, total_points)` returns adjusted points, dwell, warnings.
- `generate(wave_type, n, amplitude, offset)` returns point list.

### `KepcoDevice` (Simulator)
- Represents the full internal state of a simulated BIT 802E device.
- Methods mirror SCPI commands and manage internal registers, list data, execution thread, measurement noise, etc.
- Used by `kepco_simulator.py` to provide network listeners and a GUI dashboard. Not part of the main application but useful for testing.

### `App`
- Main GUI class.
- Public method: `run()`.
- Internal helpers for event callbacks, waveform preview, upload, manual override UI.

## Constants and Configuration
A number of module-level constants control hardware limits and UI behaviour. Key values include:

- `MIN_DWELL`, `MAX_DWELL` – minimum/maximum dwell times for list points.
- `MAX_LIST_POINTS`, `MAX_TOTAL_POINTS` – chunking limits imposed by hardware.
- `TELNET_PORT`, `SCPI_SOCKET_PORT` – TCP ports used for device communication.
- `DISCOVERY_TIMEOUT`, `CHUNK_CMD_LIMIT`, `SCPI_CMD_GAP` – networking timing constants.

Values are defined in `kepco_ui.py` at the top of the file and mirrored in the simulator where applicable.

## Error Handling and Logging

- `KepcoController` tracks the last communication error in `last_error`. Many methods return `(ok, msg)` tuples where `ok` is a boolean and `msg` describes success or error.
- The GUI `App.log()` method timestamps messages and prefixes them with symbols; it supports tags (`info`, `ok`, `warn`, `err`) for colouring.
- The simulator also provides a log callback to record commands and responses.
- GUI errors are surfaced via `messagebox.showerror` for user-facing prompts.

## Extensibility Points
Describe where to hook additional waveforms, UI changes, add simulator support.

## UI Structure & Callbacks
The main window is organised into three vertical regions:

1. **Connection bar** – top frame with IP entry, scan button, connect/disconnect button, status label and identity label. Methods: `_start_scan`, `_toggle_connect`.
2. **Tabbed interface** – center portion with two tabs:
   - **Waveform Generator tab** – configuration panel on left, waveform preview graph on right. Controls for waveform type, parameters, preview, timing info. Callbacks: `_on_wave_change`, `_load_csv`, `_preview`, `_run`.
   - **Manual Override tab** – controls for direct SCPI commands, output on/off, set voltage/current, range, measurement display, SCPI console. Callbacks prefixed with `_man_` and include `_man_toggle_output`, `_man_set_voltage`, etc.
3. **Bottom controls** – upload/run and stop buttons, progress bar/message, log textbox.

Additional UI functions handle updating the graph (`_update_graph`), managing auto-measurement timers, and safe disconnect sequencing (`_safe_disconnect_sequence`).

Throughout the code, `root.after` is used to schedule UI updates from background threads.

## Running and Deployment
Instructions: dependencies from `requirements.txt`, run `python kepco_ui.py`.

## Appendices
Terminology and hardware limits from constants.

---

*End of interface framework.*