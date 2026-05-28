"""Read solenoid temperatures from EBEAM dashboard WebMonitor JSONL logs."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Union


WEBMONITOR_LOG_PATTERN = "webMonitor_log_*.txt"
EBEAM_DASHBOARD_DIR = "EBEAM_dashboard"
WEBMONITOR_LOG_DIR = "EBEAM-Dashboard-WMLogs"
WEBMONITOR_TAIL_CHUNK_BYTES = 64 * 1024
WEBMONITOR_TAIL_SCAN_BYTES = 2 * 1024 * 1024

TemperatureValue = Optional[Union[float, str]]


@dataclass(frozen=True)
class SolenoidTemperatureSnapshot:
    solenoid_1: TemperatureValue = None
    solenoid_2: TemperatureValue = None
    timestamp: Optional[str] = None
    source_path: Optional[str] = None
    error: Optional[str] = None


class WebMonitorSolenoidTemperatureReader:
    """Read the most recent solenoid temperature values from EBEAM JSONL logs."""

    def __init__(self, log_dir: Optional[Union[str, Path]] = None):
        self.log_dir = Path(log_dir) if log_dir is not None else self.default_log_dir()

    @staticmethod
    def default_log_dir() -> Path:
        """Match the EBEAM dashboard's WebMonitor log directory."""
        return Path.home() / EBEAM_DASHBOARD_DIR / WEBMONITOR_LOG_DIR

    def read_latest(self) -> SolenoidTemperatureSnapshot:
        """Return solenoid 1/2 values from the newest valid WebMonitor log entry."""
        try:
            log_files = self._find_log_files_newest_first()
            if not log_files:
                return SolenoidTemperatureSnapshot(
                    error=f"No {WEBMONITOR_LOG_PATTERN} files found in {self.log_dir}"
                )

            for log_file in log_files:
                try:
                    if log_file.stat().st_size == 0:
                        continue
                except OSError:
                    continue

                entry = self._read_last_valid_entry(log_file)
                if entry is None:
                    continue

                temperatures = self._extract_temperatures(entry)
                if temperatures is None:
                    continue

                timestamp = entry.get("timestamp")
                if not isinstance(timestamp, str):
                    timestamp = None

                return SolenoidTemperatureSnapshot(
                    solenoid_1=self._normalize_temperature_value(
                        self._get_temperature(temperatures, "1")
                    ),
                    solenoid_2=self._normalize_temperature_value(
                        self._get_temperature(temperatures, "2")
                    ),
                    timestamp=timestamp,
                    source_path=str(log_file),
                )

            return SolenoidTemperatureSnapshot(
                error=(
                    "No valid JSON status entry found in the newest "
                    f"{WEBMONITOR_TAIL_SCAN_BYTES} bytes of any "
                    f"{WEBMONITOR_LOG_PATTERN} file in {self.log_dir}"
                )
            )
        except OSError as exc:
            return SolenoidTemperatureSnapshot(
                error=f"Unable to read WebMonitor log directory {self.log_dir}: {exc}"
            )

    def _find_log_files_newest_first(self) -> List[Path]:
        if not self.log_dir.exists() or not self.log_dir.is_dir():
            return []

        candidates = []
        for path in self.log_dir.glob(WEBMONITOR_LOG_PATTERN):
            try:
                if path.is_file():
                    stat = path.stat()
                    candidates.append((stat.st_mtime, path.name, path))
            except OSError:
                continue

        if not candidates:
            return []

        candidates.sort(reverse=True)
        return [candidate[2] for candidate in candidates]

    def _read_last_valid_entry(self, path: Path) -> Optional[dict]:
        try:
            with path.open("rb") as handle:
                handle.seek(0, 2)
                file_size = handle.tell()
                bytes_remaining = min(file_size, WEBMONITOR_TAIL_SCAN_BYTES)
                position = file_size
                pending_prefix = b""

                while position > 0 and bytes_remaining > 0:
                    read_size = min(
                        WEBMONITOR_TAIL_CHUNK_BYTES,
                        position,
                        bytes_remaining,
                    )
                    position -= read_size
                    bytes_remaining -= read_size
                    handle.seek(position)
                    chunk = handle.read(read_size)
                    lines = (chunk + pending_prefix).splitlines()

                    if position > 0 and lines:
                        pending_prefix = lines[0]
                        lines = lines[1:]
                    else:
                        pending_prefix = b""

                    for raw_line in reversed(lines):
                        entry = self._parse_status_line(raw_line)
                        if entry is not None:
                            return entry
        except OSError:
            return None

        return None

    def _parse_status_line(self, raw_line: bytes) -> Optional[dict]:
        try:
            line = raw_line.decode("utf-8").strip()
        except UnicodeDecodeError:
            return None

        if not line:
            return None

        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            return None

        if isinstance(entry, dict) and self._extract_temperatures(entry) is not None:
            return entry

        return None

    @staticmethod
    def _extract_temperatures(entry: dict) -> Optional[dict]:
        status = entry.get("status")
        if not isinstance(status, dict):
            return None
        temperatures = status.get("temperatures")
        if not isinstance(temperatures, dict):
            return None
        return temperatures

    @staticmethod
    def _get_temperature(temperatures: dict, key: str) -> Any:
        if key in temperatures:
            return temperatures[key]
        try:
            return temperatures[int(key)]
        except (KeyError, ValueError):
            return None

    @staticmethod
    def _normalize_temperature_value(value: Any) -> TemperatureValue:
        if value is None:
            return None

        if isinstance(value, bool):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            value = value.strip()
            if not value or value.lower() in {"none", "null", "nan"}:
                return None
            try:
                return float(value)
            except ValueError:
                return value

        return None
