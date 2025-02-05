FROM postgres:14

RUN apt-get update && \
    apt-get install -y postgresql-14-wal2json postgresql-14-decoderbufs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ["./postgresql.conf", "/etc/postgresql/postgresql.conf"]
COPY ["./docker-entrypoint-initdb.d/*", "/docker-entrypoint-initdb.d/"]
