"""Read solenoid temperatures from EBEAM dashboard WebMonitor JSONL logs."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union


WEBMONITOR_LOG_PATTERN = "webMonitor_log_*.txt"
EBEAM_DASHBOARD_DIR = "EBEAM_dashboard"
WEBMONITOR_LOG_DIR = "EBEAM-Dashboard-WMLogs"

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
            log_file = self._find_latest_log_file()
            if log_file is None:
                return SolenoidTemperatureSnapshot(
                    error=f"No {WEBMONITOR_LOG_PATTERN} files found in {self.log_dir}"
                )

            try:
                if log_file.stat().st_size == 0:
                    return SolenoidTemperatureSnapshot(
                        source_path=str(log_file),
                        error=f"Newest WebMonitor log is empty: {log_file}",
                    )
            except OSError as exc:
                return SolenoidTemperatureSnapshot(
                    source_path=str(log_file),
                    error=f"Unable to inspect WebMonitor log {log_file}: {exc}",
                )

            entry = self._read_last_valid_entry(log_file)
            if entry is None:
                return SolenoidTemperatureSnapshot(
                    source_path=str(log_file),
                    error=f"No valid JSON status entry found in {log_file}",
                )

            temperatures = self._extract_temperatures(entry)
            if temperatures is None:
                return SolenoidTemperatureSnapshot(
                    source_path=str(log_file),
                    error=f"Latest valid JSON entry has no status.temperatures object: {log_file}",
                )

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
        except OSError as exc:
            return SolenoidTemperatureSnapshot(
                error=f"Unable to read WebMonitor log directory {self.log_dir}: {exc}"
            )

    def _find_latest_log_file(self) -> Optional[Path]:
        if not self.log_dir.exists() or not self.log_dir.is_dir():
            return None

        candidates = []
        for path in self.log_dir.glob(WEBMONITOR_LOG_PATTERN):
            try:
                if path.is_file():
                    stat = path.stat()
                    candidates.append((stat.st_mtime, path.name, path))
            except OSError:
                continue

        if not candidates:
            return None

        candidates.sort(reverse=True)
        return candidates[0][2]

    def _read_last_valid_entry(self, path: Path) -> Optional[dict]:
        try:
            with path.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
        except (OSError, UnicodeDecodeError):
            return None

        for raw_line in reversed(lines):
            line = raw_line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
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
