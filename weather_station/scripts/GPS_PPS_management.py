#!/usr/bin/env python3
"""
GPS_PPS_management.py

Purpose:
- Monitor Linux PPS via /sys/class/pps/* and establish a stable "pps_locked" state.
- Fuse PPS timing with GPS UTC time to compute node_time_s (seconds since midnight UTC).
- Preserve GPS position fields independently of PPS time selection.
- Provide a single get_output_snapshot() dict for downstream modules.

Design choices:
- PPS presence is autodetected via /sys/class/pps.
- PPS lock requires N pulses (default 5) to be conservative.
- PPS alone does not provide absolute UTC; we only declare time_source="pps" once we have a valid UTC anchor
  and can compute a stable offset.
"""

from __future__ import annotations

import calendar
import os
import glob
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

# Import your driver class name exactly as you have it.
from GPS_v3_driver import GPSv3Driver


@dataclass
class GPSGateConfig:
    """
    Conservative defaults.
    You can loosen these later if you want faster state transitions.
    """
    pps_lock_pulses_required: int = 5
    gps_fix_required_for_position: bool = True  # position validity requires GPS fix
    gps_time_required_for_pps_time: bool = True  # PPS time_source requires gps utc_valid
    max_gps_stale_s: float = 5.0  # if no NMEA seen recently, gps_link_ok becomes False


class SysfsPPSReader:
    """
    Reads PPS edges using sysfs assert timestamps, e.g.:
      /sys/class/pps/pps0/assert  -> "1766780046.098052833#346"

    We track the sequence number after '#'. If it increments, we saw a new PPS pulse.
    """

    def __init__(self, pps_sysfs_dir: str):
        self.pps_sysfs_dir = pps_sysfs_dir
        self.assert_path = os.path.join(pps_sysfs_dir, "assert")

        if not os.path.exists(self.assert_path):
            raise FileNotFoundError(f"Missing PPS assert path: {self.assert_path}")

        self._last_seq: Optional[int] = None
        self._last_assert_str: Optional[str] = None

    @staticmethod
    def _parse_assert_line(line: str) -> tuple[Optional[float], Optional[int]]:
        """
        Example:
          "1766780046.098052833#346"
        Returns:
          (timestamp_float, seq_int)
        """
        line = line.strip()
        if not line:
            return None, None

        if "#" in line:
            left, right = line.split("#", 1)
            try:
                ts = float(left)
            except Exception:
                ts = None
            try:
                seq = int(right)
            except Exception:
                seq = None
            return ts, seq

        # If format changes (rare), we still try a float parse.
        try:
            return float(line), None
        except Exception:
            return None, None

    def read_assert(self) -> tuple[Optional[float], Optional[int], str]:
        raw = open(self.assert_path, "r", encoding="utf-8").read().strip()
        ts, seq = self._parse_assert_line(raw)
        return ts, seq, raw

    def poll_edge(self) -> tuple[bool, Optional[float], Optional[int]]:
        """
        Returns:
          (edge_detected, ts, seq)
        """
        ts, seq, raw = self.read_assert()

        # Primary: sequence increments
        if seq is not None:
            if self._last_seq is None:
                self._last_seq = seq
                self._last_assert_str = raw
                return False, ts, seq
            if seq != self._last_seq:
                self._last_seq = seq
                self._last_assert_str = raw
                return True, ts, seq
            return False, ts, seq

        # Fallback: raw string changes
        if self._last_assert_str is None:
            self._last_assert_str = raw
            return False, ts, seq
        if raw != self._last_assert_str:
            self._last_assert_str = raw
            return True, ts, seq

        return False, ts, seq


def _seconds_since_midnight_utc_from_struct_time(st: time.struct_time) -> int:
    return int(st.tm_hour) * 3600 + int(st.tm_min) * 60 + int(st.tm_sec)


class GPSPPSManager:
    """
    Threaded manager:
    - Watches PPS via sysfs
    - Watches GPS snapshot for link/time/position
    - Establishes PPS lock and UTC offset
    - Emits a unified output dict
    """

    def __init__(
        self,
        gps_driver: GPSv3Driver,
        *,
        gate: Optional[GPSGateConfig] = None,
        loop_hz: float = 5.0,
    ) -> None:
        if loop_hz <= 0:
            raise ValueError("loop_hz must be > 0")

        self.gps = gps_driver
        self.gate = gate or GPSGateConfig()
        self.loop_hz = loop_hz

        self._pps_reader: Optional[SysfsPPSReader] = None
        self._pps_present: bool = False
        self._pps_locked: bool = False
        self._pps_lock_count: int = 0
        self._pps_last_event_mono: Optional[float] = None

        # Time offset logic:
        # We convert PPS edge (kernel timestamp) to "seconds since midnight UTC"
        # using GPS UTC time as an anchor.
        self._offset_s: float = 0.0
        self._offset_valid: bool = False

        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started: bool = False

        self._lock = threading.Lock()
        self._output: Dict[str, Any] = {
            "status": "init",
            "time_source": "pi_clock",      # "pi_clock" or "pps"
            "pps_present": False,
            "pps_locked": False,
            "gps_link_ok": False,
            "gps_valid": False,             # position validity
            "gps_time_valid": False,        # utc_valid from GPS driver
            "lat": None,
            "lon": None,
            "alt_m": None,
            "sats": None,
            "hdop": None,
            "node_time_s": None,            # seconds since midnight UTC
        }

    # ---------- Lifecycle ----------

    def start(self) -> None:
        if self._started:
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run, name="GPSPPSManager", daemon=True)
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        self._thread = None
        self._started = False

    # ---------- Public interface ----------

    def get_output_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._output)

    # ---------- Internals ----------

    @staticmethod
    def _discover_pps_sysfs_dir() -> Optional[str]:
        candidates = sorted(glob.glob("/sys/class/pps/pps*"))
        for c in candidates:
            if os.path.isdir(c) and os.path.exists(os.path.join(c, "assert")):
                return c
        return None

    def _ensure_pps_reader(self) -> None:
        """
        Create PPS reader if possible, and update presence flag.
        """
        if self._pps_reader is not None:
            # still present?
            if os.path.exists(self._pps_reader.assert_path):
                self._pps_present = True
                return
            # disappeared
            self._pps_reader = None
            self._pps_present = False
            self._pps_locked = False
            self._pps_lock_count = 0
            self._offset_valid = False

        sysfs_dir = self._discover_pps_sysfs_dir()
        if sysfs_dir is None:
            self._pps_present = False
            return

        try:
            self._pps_reader = SysfsPPSReader(sysfs_dir)
            self._pps_present = True
        except Exception:
            self._pps_reader = None
            self._pps_present = False

    def _update_from_gps(self) -> Dict[str, Any]:
        """
        Pull a GPS snapshot and compute:
        - gps_link_ok
        - gps_time_valid
        - gps_valid (position)
        """
        snap = self.gps.get_snapshot() if hasattr(self.gps, "get_snapshot") else {}

        now_mono = time.monotonic()

        last_rx = snap.get("last_nmea_rx_monotonic", None)
        gps_link_ok = False
        if isinstance(last_rx, (int, float)):
            gps_link_ok = (now_mono - float(last_rx)) <= float(self.gate.max_gps_stale_s)

        gps_time_valid = bool(snap.get("utc_valid", False))
        utc_dt = snap.get("utc_datetime", None)

        has_fix = bool(snap.get("has_fix", False))
        lat = snap.get("lat_dd", None)
        lon = snap.get("lon_dd", None)

        gps_valid = False
        if self.gate.gps_fix_required_for_position:
            gps_valid = has_fix and (lat is not None) and (lon is not None)
        else:
            gps_valid = (lat is not None) and (lon is not None)

        out = {
            "gps_link_ok": gps_link_ok,
            "gps_time_valid": gps_time_valid,
            "utc_datetime": utc_dt,
            "gps_valid": gps_valid,
            "lat": lat if gps_valid else None,
            "lon": lon if gps_valid else None,
            "alt_m": snap.get("alt_m", None) if gps_valid else snap.get("alt_m", None),
            "sats": snap.get("satellites", None),
            "hdop": snap.get("hdop", None),
        }
        return out

    def _update_offset_using_gps(self, pps_edge_ts: float, utc_dt: time.struct_time) -> None:
        """
        Establish offset:
          node_time_s ≈ pps_edge_ts + offset_s
        where:
          node_time_s is seconds since midnight UTC.

        pps_edge_ts is a kernel timestamp in seconds since the Unix epoch (float).
        utc_dt is GPS UTC datetime (struct_time).
        """
        # Convert GPS utc datetime to epoch seconds (UTC)
        # timegm uses UTC (unlike mktime).
        epoch_utc = calendar.timegm(utc_dt)

        # seconds since midnight for that utc_dt
        ssm = _seconds_since_midnight_utc_from_struct_time(utc_dt)

        # epoch_utc corresponds to that utc_dt (rounded to second). We align PPS edge to that second.
        # Derive offset so that pps_edge_ts maps to seconds since midnight.
        # If PPS edge corresponds to "this second boundary", pps_edge_ts should be close to epoch_utc.
        # offset = ssm - (pps_edge_ts - epoch_utc)
        # -> node_time = pps_edge_ts + (ssm - (pps_edge_ts - epoch_utc)) = ssm + epoch_utc
        # That is wrong. We need node_time as seconds since midnight, not epoch.

        # Correct:
        # If pps_edge_ts ~= epoch_utc + frac, then epoch_utc maps to ssm at that moment.
        # So:
        #   ssm ~= (pps_edge_ts - epoch_utc) + ???  (not right either)
        # The simplest stable approach:
        #   Use pps_edge_ts to represent "epoch seconds" precisely at edge,
        #   then compute seconds since midnight from that epoch seconds.
        # That avoids offsets entirely, BUT depends on correct epoch-to-UTC conversion.
        #
        # We will therefore set offset as:
        #   offset_s = (seconds_since_midnight(epoch_at_edge)) - pps_edge_ts
        # using GPS-provided epoch time as trust anchor.
        #
        # For alignment, assume the PPS edge corresponds to the top of the second for utc_dt.
        # We'll use epoch_utc as the integer second, and replace its fractional part with the PPS fractional part.
        frac = pps_edge_ts - int(pps_edge_ts)
        epoch_at_edge = float(epoch_utc) + frac

        # seconds since midnight at that epoch second boundary:
        ssm_at_edge = _seconds_since_midnight_utc_from_struct_time(utc_dt)

        # offset maps pps_edge_ts -> ssm_at_edge
        self._offset_s = float(ssm_at_edge) - float(pps_edge_ts)
        self._offset_valid = True

    def _compute_node_time_s(self, *, use_pps: bool, pps_edge_ts: Optional[float], utc_dt: Optional[time.struct_time]) -> Optional[int]:
        """
        node_time_s = seconds since midnight UTC
        """
        if use_pps and self._offset_valid and pps_edge_ts is not None:
            return int(pps_edge_ts + self._offset_s)

        # Fallbacks:
        # 1) GPS UTC time if valid
        if utc_dt is not None:
            return _seconds_since_midnight_utc_from_struct_time(utc_dt)

        # 2) System UTC time
        now_utc = time.gmtime()
        return _seconds_since_midnight_utc_from_struct_time(now_utc)

    def _run(self) -> None:
        period = 1.0 / float(self.loop_hz)

        while not self._stop_evt.is_set():
            try:
                self._ensure_pps_reader()

                gps_info = self._update_from_gps()
                gps_link_ok = gps_info["gps_link_ok"]
                gps_valid = gps_info["gps_valid"]
                gps_time_valid = gps_info["gps_time_valid"]
                utc_dt = gps_info.get("utc_datetime", None)

                # PPS polling
                pps_edge = False
                pps_ts = None

                if self._pps_present and self._pps_reader is not None:
                    pps_edge, pps_ts, _seq = self._pps_reader.poll_edge()
                    if pps_edge:
                        self._pps_last_event_mono = time.monotonic()
                        self._pps_lock_count += 1
                        if self._pps_lock_count >= int(self.gate.pps_lock_pulses_required):
                            self._pps_locked = True

                        # If we have GPS UTC, update offset right when we see PPS edges.
                        if isinstance(pps_ts, (int, float)) and utc_dt is not None and gps_time_valid:
                            if self.gate.gps_time_required_for_pps_time:
                                self._update_offset_using_gps(float(pps_ts), utc_dt)
                            else:
                                # Allow offset even if utc_valid false (not recommended)
                                self._update_offset_using_gps(float(pps_ts), utc_dt)

                else:
                    # PPS not present
                    self._pps_locked = False
                    self._pps_lock_count = 0
                    self._offset_valid = False

                # If PPS edges stop for too long, drop lock
                if self._pps_locked and self._pps_last_event_mono is not None:
                    if (time.monotonic() - self._pps_last_event_mono) > 2.5:
                        self._pps_locked = False
                        self._pps_lock_count = 0
                        self._offset_valid = False

                # Decide time source:
                # We only declare time_source="pps" if PPS is locked AND we have a valid UTC anchor (offset valid).
                use_pps_time = bool(self._pps_present and self._pps_locked and self._offset_valid)
                time_source = "pps" if use_pps_time else "pi_clock"

                node_time_s = self._compute_node_time_s(
                    use_pps=use_pps_time,
                    pps_edge_ts=float(pps_ts) if isinstance(pps_ts, (int, float)) else None,
                    utc_dt=utc_dt if (utc_dt is not None and gps_time_valid) else None,
                )

                with self._lock:
                    self._output.update({
                        "status": "ok",
                        "time_source": time_source,
                        "pps_present": bool(self._pps_present),
                        "pps_locked": bool(self._pps_locked),
                        "gps_link_ok": bool(gps_link_ok),
                        "gps_valid": bool(gps_valid),
                        "gps_time_valid": bool(gps_time_valid),
                        # Preserve position independently of time source
                        "lat": gps_info["lat"],
                        "lon": gps_info["lon"],
                        "alt_m": gps_info["alt_m"],
                        "sats": gps_info["sats"],
                        "hdop": gps_info["hdop"],
                        "node_time_s": node_time_s,
                    })

            except Exception as e:
                with self._lock:
                    self._output.update({
                        "status": "error",
                        "time_source": "pi_clock",
                        "pps_present": bool(self._pps_present),
                        "pps_locked": False,
                        "gps_link_ok": False,
                        "gps_valid": False,
                        "gps_time_valid": False,
                        "node_time_s": None,
                        "last_error": repr(e),
                    })

            time.sleep(period)
