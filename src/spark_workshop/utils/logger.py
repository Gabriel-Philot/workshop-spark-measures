"""Small workshop logger with deterministic, submit-friendly output."""

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo


class Logger:
    levels = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]
    colors = {
        "CRITICAL": "\033[1;41m",
        "ERROR": "\033[1;31m",
        "WARNING": "\033[1;33m",
        "INFO": "\033[1;36m",
        "DEBUG": "\033[1;32m",
    }

    def __init__(self, level: str = "INFO", timezone: str = "UTC"):
        self.timezone = ZoneInfo(timezone)
        self.current_level = self.levels.index("INFO")
        self.set_level(level)

    def set_level(self, level: str) -> None:
        normalized = level.upper()
        if normalized not in self.levels:
            raise ValueError(
                f"Invalid log level '{level}'. Expected one of {self.levels}"
            )
        self.current_level = self.levels.index(normalized)

    def _emit(self, level: str, message: str) -> str | None:
        if self.levels.index(level) > self.current_level:
            return None
        timestamp = datetime.now(self.timezone).strftime("%Y-%m-%d %H:%M:%S%z")
        label = f"[{level}]"
        if sys.stdout.isatty() and "NO_COLOR" not in os.environ:
            label = f"{self.colors.get(level, '')}{label}\033[0m"
        rendered = f"[{timestamp}] {label} {message}"
        print(rendered, flush=True)
        return rendered

    def debug(self, message: str) -> str | None:
        return self._emit("DEBUG", message)

    def info(self, message: str) -> str | None:
        return self._emit("INFO", message)

    def warning(self, message: str) -> str | None:
        return self._emit("WARNING", message)

    def error(self, message: str) -> str | None:
        return self._emit("ERROR", message)

    def critical(self, message: str) -> str | None:
        return self._emit("CRITICAL", message)


logger = Logger(
    level=os.environ.get("SPARK_WORKSHOP_LOG_LEVEL", "INFO"),
    timezone=os.environ.get("SPARK_WORKSHOP_TIMEZONE", "UTC"),
)
