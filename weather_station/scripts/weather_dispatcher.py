#!/usr/bin/env python3
"""
weather_dispatcher.py

Purpose:
- For R&D: start the DriverManager, print snapshots at a fixed loop rate, and exit cleanly on Ctrl+C.
- No networking, no logging, no systemd. Pure console validation.
"""

from __future__ import annotations

import time
import signal
from typing import Optional

from driver_manager import DriverManager


class WeatherDispatcher:
    def __init__(self, *, loop_hz: float = 1.0, debug: bool = True) -> None:
        self.loop_hz = float(loop_hz)
        self.debug = debug

        self.driver_manager = DriverManager(debug=debug, manager_loop_hz=5.0)

        self._stop = False

        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

    def _handle_stop(self, signum, frame) -> None:
        if self.debug:
            print(f"\nWeatherDispatcher: received signal {signum}, shutting down.")
        self._stop = True

    def start(self) -> None:
        if self.debug:
            print("WeatherDispatcher.start(): starting DriverManager...")

        self.driver_manager.start()

        if self.debug:
            print("WeatherDispatcher.start(): started.")

        period = 1.0 / self.loop_hz if self.loop_hz > 0 else 1.0

        while not self._stop:
            snap = self.driver_manager.get_snapshot()
            print(snap)
            time.sleep(period)

    def stop(self) -> None:
        if self.debug:
            print("WeatherDispatcher.stop(): stopping...")

        self.driver_manager.stop()

        if self.debug:
            print("WeatherDispatcher.stop(): stopped.")


if __name__ == "__main__":
    dispatcher = WeatherDispatcher(loop_hz=1.0, debug=True)
    try:
        dispatcher.start()
    finally:
        dispatcher.stop()
