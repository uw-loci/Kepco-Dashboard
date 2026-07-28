"""Read solenoid temperatures from EBEAM dashboard Data Log JSONL files."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Union


DATALOG_FILE_PATTERN = "datalog_*.txt"
EBEAM_DASHBOARD_DIR = "EBEAM_dashboard"
DATALOG_DIR = "EBEAM-Dashboard-Datalogs"
DATALOG_TAIL_CHUNK_BYTES = 64 * 1024
DATALOG_TAIL_SCAN_BYTES = 2 * 1024 * 1024

TemperatureValue = Optional[Union[float, str]]


@dataclass(frozen=True)
class SolenoidTemperatureSnapshot:
    solenoid_1: TemperatureValue = None
    solenoid_2: TemperatureValue = None
    timestamp: Optional[str] = None
    source_path: Optional[str] = None
    error: Optional[str] = None


class DatalogSolenoidTemperatureReader:
    """Read the most recent solenoid temperature values from EBEAM Data Logs."""

    def __init__(self, datalog_dir: Optional[Union[str, Path]] = None):
        self.datalog_dir = (
            Path(datalog_dir)
            if datalog_dir is not None
            else self.default_datalog_dir()
        )

    @staticmethod
    def default_datalog_dir() -> Path:
        """Match the EBEAM dashboard's Data Log directory."""
        return Path.home() / EBEAM_DASHBOARD_DIR / DATALOG_DIR

    def read_latest(self) -> SolenoidTemperatureSnapshot:
        """Return solenoid 1/2 values from the newest valid Data Log entry."""
        try:
            datalog_files = self._find_datalog_files_newest_first()
            if not datalog_files:
                return SolenoidTemperatureSnapshot(
                    error=(
                        f"No {DATALOG_FILE_PATTERN} files found in "
                        f"{self.datalog_dir}"
                    )
                )

            for datalog_file in datalog_files:
                try:
                    if datalog_file.stat().st_size == 0:
                        continue
                except OSError:
                    continue

                entry = self._read_last_valid_entry(datalog_file)
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
                    source_path=str(datalog_file),
                )

            return SolenoidTemperatureSnapshot(
                error=(
                    "No valid JSON status entry found in the newest "
                    f"{DATALOG_TAIL_SCAN_BYTES} bytes of any "
                    f"{DATALOG_FILE_PATTERN} file in {self.datalog_dir}"
                )
            )
        except OSError as exc:
            return SolenoidTemperatureSnapshot(
                error=f"Unable to read Data Log directory {self.datalog_dir}: {exc}"
            )

    def _find_datalog_files_newest_first(self) -> List[Path]:
        if not self.datalog_dir.exists() or not self.datalog_dir.is_dir():
            return []

        candidates = []
        for path in self.datalog_dir.glob(DATALOG_FILE_PATTERN):
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
                bytes_remaining = min(file_size, DATALOG_TAIL_SCAN_BYTES)
                position = file_size
                pending_prefix = b""

                while position > 0 and bytes_remaining > 0:
                    read_size = min(
                        DATALOG_TAIL_CHUNK_BYTES,
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
