FROM postgres:13

COPY init.sql /docker-entrypoint-initdb.d/

RUN chmod a+r /docker-entrypoint-initdb.d/*

RUN mkdir -p /data/csv_imports && chown -R postgres:postgres /data/csv_imports

