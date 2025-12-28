# -*- coding: utf-8 -*-

# send_over_wifi.py

"""
Created on Fri Nov 28 18:55:00 2025

@author: Lee Pickett

This module is responsible for pushing completed event dictionaries from the
Backyard Acoustic Monitor node to another machine over Wi-Fi.

It is intentionally small and transport-focused: dispatcher.py should hand it a
fully-formed event dict, and this module will take care of serializing and
sending that event to the configured destination.

Version 1 transport:
- UDP + JSON
    - Events are serialized to UTF-8 JSON.
    - Sent as a single datagram to DEST_HOST:DEST_PORT.
    - No acknowledgements or retries; this is a simple fire-and-forget channel
      for local network testing and logging on a PC.

Future versions:
- Optional HTTP POST transport for more reliable delivery.
- Simple retry / backoff logic.
- TLS or shared-secret signing if you ever want basic security.
- Integration with a dedicated server or gateway process.

In six months:
If you are wondering “where do we actually put events on the wire and send
them to my PC or gateway?”, this is the file. Other code should just call
send_event(event_dict) and let this module handle the transport details.
"""

from __future__ import annotations

import json
import logging
import socket
from typing import Any, Dict

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("birdstation.send_over_wifi")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# You can safely edit these in your own copy.
# DEST_HOST should be the IP or hostname of the machine that will receive events.
# DEST_PORT is the UDP port that your receiver script will bind to.
DEST_HOST: str = "192.168.1.192"  # ← replace with your PC / gateway IP
DEST_PORT: int = 50555            # ← replace with your chosen UDP port

# Reserved for future expansion (e.g., "udp" vs "http").
PROTOCOL: str = "udp"


# ---------------------------------------------------------------------------
# Core send logic
# ---------------------------------------------------------------------------

def _send_udp(payload: bytes) -> None:
    """
    Low-level UDP sender for already-encoded payloads.
    """
    logger.debug("Preparing to send UDP packet to %s:%s (len=%d bytes)",
                 DEST_HOST, DEST_PORT, len(payload))
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sent = sock.sendto(payload, (DEST_HOST, DEST_PORT))
        logger.debug("UDP send successful, bytes sent: %d", sent)
    finally:
        sock.close()


def send_event(event: Dict[str, Any]) -> None:
    """
    Serialize an event dict to JSON and send it over Wi-Fi using the configured
    transport (currently UDP only).

    Args:
        event: A JSON-serializable dictionary representing the event.

    Raises:
        ValueError: If PROTOCOL is set to an unsupported value.
        OSError: For underlying socket/network errors.
    """
    logger.debug("send_event() called with event: %s", event)

    # Serialize to JSON.
    try:
        payload_str = json.dumps(event, ensure_ascii=False)
    except (TypeError, ValueError) as exc:
        logger.error("Failed to serialize event to JSON: %s", exc)
        raise

    payload_bytes = payload_str.encode("utf-8")
    logger.debug("Event serialized to %d bytes of JSON.", len(payload_bytes))

    # Dispatch based on protocol.
    if PROTOCOL.lower() == "udp":
        _send_udp(payload_bytes)
    else:
        logger.error("Unsupported PROTOCOL '%s' in send_over_wifi.py", PROTOCOL)
        raise ValueError(f"Unsupported PROTOCOL '{PROTOCOL}'")


# ---------------------------------------------------------------------------
# Standalone debug/demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("Running send_over_wifi.py as a script for debug/demo.")
    logger.info("Destination is %s:%s over %s", DEST_HOST, DEST_PORT, PROTOCOL.upper())

    # Minimal example event for testing.
    example_event: Dict[str, Any] = {
        "event_id": "TEST_EVENT_0001",
        "node_id": "yard_station_1",
        "timestamp_utc": "2025-11-28T18:55:00Z",
        "local_time": "2025-11-28T11:55:00-07:00",
        "weather": None,
        "bird": {
            "bird_id": "AMRO",
            "species_name": "American Robin",
            "confidence": 0.99,
        },
        "model": {
            "model_file": "/path/to/BirdNET_GLOBAL_6K_V2.4_Model",
            "model_dir": "/path/to",
        },
    }

    try:
        send_event(example_event)
        logger.info("Example event sent successfully.")
    except Exception as exc:
        logger.exception("Failed to send example event: %s", exc)
