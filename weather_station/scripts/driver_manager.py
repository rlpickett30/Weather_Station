#!/usr/bin/env python3
"""
driver_manager.py

Purpose:
- Own and start/stop all device drivers and managers.
- Provide a single snapshot interface for the dispatcher.

This version is refactored to match the new GPSPPSManager signature:
- No pps_device keyword argument.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from GPS_v3_driver import GPSv3Driver
from GPS_PPS_management import GPSPPSManager, GPSGateConfig


class DriverManager:
    def __init__(
        self,
        *,
        debug: bool = False,
        gps_loop_hz: float = 5.0,
        manager_loop_hz: float = 5.0,
        gate_config: Optional[GPSGateConfig] = None,
    ) -> None:
        self.debug = debug

        self.gps_driver = GPSv3Driver(update_hz=1.0)
        self.gps_pps_manager = GPSPPSManager(
            self.gps_driver,
            gate=gate_config or GPSGateConfig(pps_lock_pulses_required=5),
            loop_hz=manager_loop_hz,
        )

        self._started = False

    def start(self) -> None:
        if self._started:
            return

        if self.debug:
            print("DriverManager.start(): starting GPS driver...")

        self.gps_driver.start()

        if self.debug:
            print("DriverManager.start(): GPS driver started.")
            print("DriverManager.start(): starting GPS/PPS manager...")

        self.gps_pps_manager.start()

        if self.debug:
            print("DriverManager.start(): GPS/PPS manager started.")
            print("DriverManager.start(): complete.")

        self._started = True

    def stop(self) -> None:
        if not self._started:
            return

        if self.debug:
            print("DriverManager.stop(): stopping...")

        try:
            self.gps_pps_manager.stop()
        finally:
            self.gps_driver.stop()

        if self.debug:
            print("DriverManager.stop(): complete.")

        self._started = False

    def get_snapshot(self) -> Dict[str, Any]:
        """
        Returns a single dict suitable for the dispatcher.
        """
        return self.gps_pps_manager.get_output_snapshot()
