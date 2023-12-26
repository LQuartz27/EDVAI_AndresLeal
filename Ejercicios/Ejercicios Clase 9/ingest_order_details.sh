/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password edvai \
--table order_details \
--columns "order_id, unit_price, quantity, discount" \
--m 1 \
--target-dir /sqoop/ingest/order_details \
--as-parquetfile --delete-target-dir