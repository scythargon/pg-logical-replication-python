FROM postgres:17

RUN apt-get update && \
    apt-get install -y postgresql-17-wal2json postgresql-17-decoderbufs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ["./postgresql.conf", "/etc/postgresql/postgresql.conf"]
COPY ["./docker-entrypoint-initdb.d/*", "/docker-entrypoint-initdb.d/"]
