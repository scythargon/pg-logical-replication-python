import json
import threading
import time
from typing import Any, Callable, Dict, Optional

import psycopg2
import psycopg2.extras

from .models import Wal2JsonOutput


class LogicalReplicationClient:
    def __init__(
        self,
        dsn: str,
        slot_name: str,
        plugin: str = "wal2json",
        plugin_options: Optional[Dict[str, Any]] = None,
    ):
        self.dsn = dsn
        self.slot_name = slot_name
        self.plugin = plugin
        self.plugin_options = plugin_options or {}
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._replication_conn: Optional[psycopg2.extensions.connection] = None
        self._callback: Optional[Callable[[str, Wal2JsonOutput], None]] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._start_lsn: Optional[int] = None

    def create_slot(self) -> None:
        """Create a new replication slot if it doesn't exist."""
        self._conn = psycopg2.connect(self.dsn)
        try:
            # First check if slot exists
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pg_replication_slots WHERE slot_name = %s",
                    (self.slot_name,)
                )
                result = cur.fetchall()
                
                if not result:
                    print(f"Creating replication slot {self.slot_name}")
                    cur.execute(
                        f"SELECT * FROM pg_create_logical_replication_slot('{self.slot_name}', '{self.plugin}')"
                    )
                    slot_info = cur.fetchone()
                    print(f"Created replication slot: {slot_info}")
                    # Parse LSN value from slot_info[1] which is in format "0/199EB170"
                    lsn_parts = slot_info[1].split('/')
                    self._start_lsn = (int(lsn_parts[0], 16) << 32) | int(lsn_parts[1], 16)
                else:
                    print(f"Replication slot {self.slot_name} already exists")
                
        except psycopg2.Error as e:
            if "already exists" not in str(e):
                raise
        finally:
            self._conn.close()
            self._conn = None

    def _replication_worker(self, callback: Callable[[str, Wal2JsonOutput], None]) -> None:
        """Worker thread for handling replication messages."""
        try:
            # Create replication connection
            self._replication_conn = psycopg2.connect(
                self.dsn,
                connection_factory=psycopg2.extras.LogicalReplicationConnection
            )
            
            # Create replication cursor
            with self._replication_conn.cursor() as cur:
                # Start replication
                plugin_options = {
                    key.replace("-", "_"): str(value)  # Convert keys to use underscores
                    for key, value in self.plugin_options.items()
                }
                print(f"Starting replication with options: {plugin_options}")
                cur.start_replication(
                    slot_name=self.slot_name,
                    options=plugin_options,
                    decode=True,
                    status_interval=10,
                    start_lsn=self._start_lsn
                )
                
                while self._running:
                    msg = cur.read_message()
                    if msg is None:
                        print("No message received, sleeping...")
                        time.sleep(0.1)
                        continue
                    
                    try:
                        if msg.payload:
                            data = msg.payload
                            print(f"Received raw data: {data}")
                            
                            try:
                                parsed_data = json.loads(data)
                                print(f"Parsed data: {parsed_data}")
                                wal_output = Wal2JsonOutput.from_dict(parsed_data)
                                if callback:
                                    callback(msg.data_start, wal_output)
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON: {e}")
                                continue
                            
                            # Send feedback
                            print(f"Sending feedback with LSN: {msg.data_start}")
                            cur.send_feedback(
                                write_lsn=msg.data_start,
                                flush_lsn=msg.data_start,
                                apply_lsn=msg.data_start,
                                force=True
                            )
                        else:
                            print(f"Message received but no payload: {msg}")
                        
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        if not self._running:
                            break
                        time.sleep(0.1)
                        
        except Exception as e:
            print(f"Error in replication: {e}")
            raise
        finally:
            if self._replication_conn:
                self._replication_conn.close()
                self._replication_conn = None

    def start(self, callback: Callable[[str, Wal2JsonOutput], None]) -> None:
        """Start the replication stream."""
        self._callback = callback
        self._running = True
        
        # Start replication in a separate thread
        self._thread = threading.Thread(
            target=self._replication_worker,
            args=(callback,)
        )
        self._thread.daemon = True
        self._thread.start()

    def stop(self) -> None:
        """Stop the replication stream."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
