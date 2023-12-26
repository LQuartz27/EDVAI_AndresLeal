/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password edvai \
--table clientes_vw \
--m 1 \
--target-dir /sqoop/ingest/clientes \
--as-parquetfile --delete-target-dir