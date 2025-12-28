# node_database.py

import json
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional

class NodeDatabase:
    """
    Handles offline storage of events on the node and flushing them
    to the server once Wi-Fi/UDP is available again.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS queued_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def queue_event(self, event: Dict[str, Any]) -> None:
        """
        Store an event locally when the node cannot reach the server.
        """
        conn = self._connect()
        cur = conn.cursor()

        created_at = datetime.utcnow().isoformat()
        event_type = event.get("event_type", "unknown")
        payload_json = json.dumps(event)

        cur.execute(
            """
            INSERT INTO queued_events (created_at, event_type, payload_json, sent)
            VALUES (?, ?, ?, 0)
            """,
            (created_at, event_type, payload_json),
        )
        conn.commit()
        conn.close()

    def flush_pending(self, send_func) -> None:
        """
        Attempt to send all unsent events using the given send function.

        send_func(event_dict) must:
          - send the event to the server (over UDP, etc.)
          - raise an exception or return False on failure
        """
        conn = self._connect()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, payload_json
            FROM queued_events
            WHERE sent = 0
            ORDER BY id ASC
            """
        )
        rows = cur.fetchall()

        if not rows:
            conn.close()
            return

        for row_id, payload_json in rows:
            event = json.loads(payload_json)

            try:
                ok = send_func(event)
                if ok is False:
                    # Treat explicit False as a failure
                    break
            except Exception:
                # Network still down, stop trying for now
                break

            # If we reach here, send succeeded; mark as sent
            cur.execute(
                "UPDATE queued_events SET sent = 1 WHERE id = ?",
                (row_id,),
            )

        conn.commit()
        conn.close()

    def has_pending(self) -> bool:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM queued_events WHERE sent = 0 LIMIT 1")
        row = cur.fetchone()
        conn.close()
        return row is not None
