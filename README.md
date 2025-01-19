# PostgreSQL Logical Replication Client for Python

A lightweight Python client for PostgreSQL logical replication using psycopg2 and wal2json plugin. Enables basic Change Data Capture (CDC) by streaming database changes through the replication protocol. Supports PostgreSQL 17.

## Key Features

- Basic logical replication support with wal2json plugin
- Simple synchronous implementation using psycopg2
- Type hints and dataclasses for better code completion
- Example test suite with Docker environment

## Requirements

- Python 3.12+
- PostgreSQL with wal2json plugin installed
- Poetry for dependency management

## Installation

```bash
poetry add pg-logical-replication
```

## Usage

```python
from pg_logical_replication.client import LogicalReplicationClient

def main():
    # Callback function to process changes
    def on_change(lsn, output):
        for change in output.change:
            print(f"LSN: {lsn}, Change: {change}")

    # Create client
    client = LogicalReplicationClient(
        dsn="postgresql://user:password@localhost:5432/dbname",
        slot_name="my_slot",
        plugin="wal2json",
        plugin_options={"include-timestamp": True}
    )

    # Create replication slot if needed
    client.create_slot()

    # Start replication
    client.start(on_change)

if __name__ == "__main__":
    main()
```

## Table Configuration

For UPDATE operations to be properly captured, tables must have REPLICA IDENTITY set to FULL. You can set this using:

```sql
ALTER TABLE your_table REPLICA IDENTITY FULL;
```

Without this setting, UPDATE operations may not include all necessary column values in the replication stream.

## Development

1. Clone the repository
2. Install dependencies:
   ```bash
   poetry install
   ```

3. Run tests:
   ```bash
   poetry run pytest
   ```

## Testing Infrastructure

The project includes a Docker-based testing infrastructure that sets up PostgreSQL with the wal2json plugin. To run the tests:

1. Start the test database:
   ```bash
   cd infra/docker-pg-logical-replication
   docker compose up -d
   ```

2. Run the tests:
   ```bash
   poetry run pytest
   ```

The test infrastructure automatically configures the necessary REPLICA IDENTITY settings for the test tables.

## License

MIT
