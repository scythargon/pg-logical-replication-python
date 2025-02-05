import os
import time
from typing import Any, Dict, List

import psycopg2
import pytest

from pg_logical_replication.client import LogicalReplicationClient
from pg_logical_replication.models import Wal2JsonOutput

# Get DSN directly if provided, otherwise build from components
TEST_DSN = os.getenv("TEST_DSN")
if not TEST_DSN:
    # Test configuration for local development
    TEST_CONFIG = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "54320")),
        "user": "postgres",
        "password": "postgrespw",
        "database": "playground",
    }
    TEST_DSN = f"postgresql://{TEST_CONFIG['user']}:{TEST_CONFIG['password']}@{TEST_CONFIG['host']}:{TEST_CONFIG['port']}/{TEST_CONFIG['database']}"


@pytest.fixture(scope="session")
def pg_client():
    conn = psycopg2.connect(TEST_DSN)
    yield conn
    conn.close()

@pytest.fixture(scope="session", autouse=True)
def setup_database(pg_client):
    with pg_client.cursor() as cur:
        # Create users table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            PRIMARY KEY(id),
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            email VARCHAR(1000),
            phone VARCHAR(1000),
            deleted boolean NOT NULL DEFAULT false,
            created timestamp with time zone NOT NULL DEFAULT NOW()
        );
        """)
        
        # Set REPLICA IDENTITY to FULL for proper UPDATE tracking
        cur.execute("ALTER TABLE users REPLICA IDENTITY FULL;")
        
        # Insert initial test data
        cur.execute("""
        INSERT INTO users(firstname, lastname, email, phone)
        SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT) 
        FROM generate_series(1, 100);
        """)
        
    pg_client.commit()


def test_insert_and_delete():
    changes: List[Dict[str, Any]] = []

    def on_change(lsn: str, output: Wal2JsonOutput):
        print(f"Received change at LSN {lsn}: {output}")
        for change in output.change:
            changes.append({
                "kind": change.kind,
                "schema": change.schema,
                "table": change.table,
                "data": dict(zip(change.columnnames, change.columnvalues))
            })

    # Create a test connection first to ensure the database is ready
    test_conn = psycopg2.connect(TEST_DSN)

    try:
        # Clean up any existing replication slots and clean the table
        with test_conn.cursor() as cur:
            cur.execute("""
                DO $$
                BEGIN
                    PERFORM pg_drop_replication_slot(slot_name)
                    FROM pg_replication_slots
                    WHERE slot_name = 'slot_wal2json_test';
                EXCEPTION WHEN OTHERS THEN
                    NULL;
                END $$;
            """)
            # Clean the table before starting the test
            cur.execute("DELETE FROM users")
            test_conn.commit()

        client = LogicalReplicationClient(
            dsn=TEST_DSN,
            slot_name="slot_wal2json_test",
            plugin="wal2json",
            plugin_options={}
        )

        # Create replication slot
        client.create_slot()

        # Check slot status
        with test_conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = 'slot_wal2json_test'")
            print(f"Slot status: {cur.fetchone()}")

        # Start replication
        client.start(on_change)

        # Wait a bit for replication to start
        time.sleep(2)

        # Insert test data and remember the IDs
        inserted_ids = []
        with test_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users(firstname, lastname, email, phone)
                SELECT md5(random()::text), md5(random()::text),
                       md5(random()::text), md5(random()::text)
                FROM generate_series(1, 5)
                RETURNING id
            """)
            inserted_ids = [row[0] for row in cur.fetchall()]
            test_conn.commit()

        # Wait for changes to be processed
        time.sleep(2)

        # Verify inserts
        insert_count = len([c for c in changes if c["kind"] == "insert"])
        assert insert_count == 5, f"Expected 5 inserts, got {insert_count}"

        # Delete only the data we inserted
        with test_conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = ANY(%s)", (inserted_ids,))
            test_conn.commit()

        # Wait for changes to be processed
        time.sleep(2)

        # Verify deletes
        delete_count = len([c for c in changes if c["kind"] == "delete"])
        assert delete_count == 5, f"Expected 5 deletes, got {delete_count}"

        # Clean up
        client.stop()
    finally:
        test_conn.close()


def test_update():
    changes: List[Dict[str, Any]] = []

    def on_change(lsn: str, output: Wal2JsonOutput):
        print(f"Received data: {output}")  # Print raw output for debugging
        print(f"Received change at LSN {lsn}: {output}")
        for change in output.change:
            changes.append({
                "kind": change.kind,
                "schema": change.schema,
                "table": change.table,
                "data": dict(zip(change.columnnames, change.columnvalues))
            })

    # Create a test connection first to ensure the database is ready
    test_conn = psycopg2.connect(TEST_DSN)

    try:
        # Clean up any existing replication slots and clean the table
        with test_conn.cursor() as cur:
            cur.execute("""
                DO $$
                BEGIN
                    PERFORM pg_drop_replication_slot(slot_name)
                    FROM pg_replication_slots
                    WHERE slot_name = 'slot_wal2json_test';
                EXCEPTION WHEN OTHERS THEN
                    NULL;
                END $$;
            """)
            # Clean the table before starting the test
            cur.execute("DELETE FROM users")
            test_conn.commit()

        # First insert some test data and remember the IDs
        inserted_ids = []
        with test_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users(firstname, lastname, email, phone)
                SELECT md5(random()::text), md5(random()::text),
                       md5(random()::text), md5(random()::text)
                FROM generate_series(1, 10)
                RETURNING id
            """)
            inserted_ids = [row[0] for row in cur.fetchall()]
            test_conn.commit()

        client = LogicalReplicationClient(
            dsn=TEST_DSN,
            slot_name="slot_wal2json_test",
            plugin="wal2json",
            plugin_options={}
        )

        # Create replication slot
        client.create_slot()

        # Check slot status
        with test_conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = 'slot_wal2json_test'")
            print(f"Slot status: {cur.fetchone()}")

        # Start replication
        client.start(on_change)

        # Wait a bit for replication to start
        time.sleep(2)

        # Update test data
        with test_conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                SET firstname = md5(random()::text)
                WHERE id = ANY(%s)
            """, (inserted_ids,))
            test_conn.commit()

        # Wait for changes to be processed
        time.sleep(2)

        # Verify updates
        update_count = len([c for c in changes if c["kind"] == "update"])
        assert update_count == 10, f"Expected 10 updates, got {update_count}"

        # Clean up
        client.stop()
    finally:
        test_conn.close()
