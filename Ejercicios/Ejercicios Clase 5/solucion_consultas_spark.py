# CARGAR EL DF
df = spark.read.parquet("/nifi/*.parquet")

# IMPRIMIENDO EL SCHEMA DEL DF -> COLUMNAS Y DATA TYPES
df.printSchema()

"""
root
 |-- VendorID: long (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: double (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: double (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: long (nullable = true)
 |-- DOLocationID: long (nullable = true)
 |-- payment_type: long (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- airport_fee: double (nullable = true)
"""

# CREANDO VISTA UNA VISTA TEMPORAL -> SOLO DISPONIBLE DURANTE LA SPARK SESSION QUE ESTÁ CORRIENDO
df.createOrReplaceTempView("yellow_tripdata")

#3.1) Mostrar los resultados siguientes
# a. VendorId Integer
# b. Tpep_pickup_datetime date
# c. Total_amount double
# d. Donde el total (total_amount sea menor a 10 dólares)
spark.sql("select VendorID, cast(tpep_pickup_datetime as date), total_amount from yellow_tripdata WHERE total_amount < 10").show()

"""
+--------+--------------------+------------+
|VendorID|tpep_pickup_datetime|total_amount|
+--------+--------------------+------------+
|       1|          2020-12-31|         4.3|
|       2|          2020-12-31|         8.3|
|       2|          2020-12-31|        9.96|
|       2|          2020-12-31|         9.3|
|       2|          2020-12-31|         5.8|
|       1|          2020-12-31|         0.0|
|       1|          2020-12-31|         9.3|
|       2|          2020-12-31|         9.8|
|       2|          2020-12-31|         8.8|
|       2|          2020-12-31|        9.96|
|       2|          2020-12-31|         9.3|
|       2|          2020-12-31|         7.8|
|       1|          2020-12-31|        9.55|
|       2|          2020-12-31|         4.8|
|       1|          2020-12-31|         9.8|
|       2|          2020-12-31|         8.8|
|       1|          2020-12-31|         7.8|
|       2|          2020-12-31|        9.36|
|       2|          2020-12-31|         8.3|
|       1|          2020-12-31|         9.3|
+--------+--------------------+------------+
only showing top 20 rows
"""

# 3.2) Mostrar los 10 días que más se recaudó dinero (tpep_pickup_datetime, total
# amount)
spark.sql("select cast(tpep_pickup_datetime as date), SUM(total_amount) AS recaudo from yellow_tripdata GROUP BY cast(tpep_pickup_datetime as date) ORDER BY SUM(total_amount) DESC LIMIT 10").show()

"""
+--------------------+-----------------+
|tpep_pickup_datetime|          recaudo|
+--------------------+-----------------+
|          2021-01-28|961322.5600002451|
|          2021-01-22|942205.9300002148|
|          2021-01-29|937373.5100002222|
|          2021-01-21|932444.4500002082|
|          2021-01-15|931628.1900002063|
|          2021-01-14|926664.0400001821|
|          2021-01-27|  895259.87000017|
|          2021-01-19|890581.4500001629|
|          2021-01-07|887670.1600001527|
|          2021-01-08| 878002.730000146|
"""

# 3.3) Mostrar los 10 viajes que menos dinero recaudó en viajes mayores a 10 millas
# (trip_distance, total_amount)
spark.sql("select trip_distance, total_amount AS recaudo from yellow_tripdata WHERE trip_distance > 10 ORDER BY total_amount ASC LIMIT 10").show()

"""
+-------------+-------+
|trip_distance|recaudo|
+-------------+-------+
|        12.68| -252.3|
|        34.35|-176.42|
|        14.75| -152.8|
|        33.96|-127.92|
|         29.1| -119.3|
|        26.94| -111.3|
|        20.08| -107.8|
|        19.55| -102.8|
|        19.16| -90.55|
|        25.83| -88.54|
+-------------+-------+
"""

# 3.4) Mostrar los viajes de más de dos pasajeros que hayan pagado con tarjeta de
# crédito (mostrar solo las columnas trip_distance y tpep_pickup_datetime)
spark.sql("SELECT trip_distance, cast(tpep_pickup_datetime as date) FROM yellow_tripdata WHERE passenger_count >= 2 AND payment_type = 1").show()

"""
+-------------+--------------------+
|trip_distance|tpep_pickup_datetime|
+-------------+--------------------+
|          2.7|          2020-12-31|
|         6.11|          2020-12-31|
|         1.21|          2020-12-31|
|          1.7|          2020-12-31|
|         1.16|          2020-12-31|
|         3.15|          2020-12-31|
|         0.64|          2020-12-31|
|        10.74|          2020-12-31|
|         2.01|          2020-12-31|
|         3.45|          2020-12-31|
|         2.85|          2020-12-31|
|         1.68|          2020-12-31|
|         0.77|          2020-12-31|
|         0.52|          2020-12-31|
|          0.4|          2020-12-31|
|         1.05|          2020-12-31|
|         5.85|          2020-12-31|
|          3.7|          2020-12-31|
|        16.54|          2020-12-31|
|          4.0|          2020-12-31|
+-------------+--------------------+
only showing top 20 rows
"""

# 3.5) Mostrar los 7 viajes con mayor propina en distancias mayores a 10 millas (mostrar
# campos tpep_pickup_datetime, trip_distance, passenger_count, tip_amount)
spark.sql("SELECT cast(tpep_pickup_datetime as date), trip_distance, passenger_count, tip_amount FROM yellow_tripdata WHERE trip_distance > 10 ORDER BY tip_amount DESC LIMIT 7").show()

"""
+--------------------+-------------+---------------+----------+
|tpep_pickup_datetime|trip_distance|passenger_count|tip_amount|
+--------------------+-------------+---------------+----------+
|          2021-01-20|        427.7|            1.0|   1140.44|
|          2021-01-03|        267.7|            1.0|     369.4|
|          2021-01-12|        326.1|            0.0|    192.61|
|          2021-01-19|        260.5|            1.0|    149.03|
|          2021-01-31|         11.1|            0.0|     100.0|
|          2021-01-01|        14.86|            2.0|      99.0|
|          2021-01-18|         13.0|            0.0|      90.0|
+--------------------+-------------+---------------+----------+
"""

# 3.6) Mostrar para cada uno de los valores de RateCodeID, el monto total y el monto
# promedio. Excluir los viajes en donde RateCodeID es ‘Group Ride’
spark.sql("SELECT RatecodeID, SUM(total_amount), AVG(total_amount) FROM yellow_tripdata WHERE RateCodeID <> '6' GROUP BY RatecodeID ").show()

"""
+----------+--------------------+------------------+
|RatecodeID|   sum(total_amount)| avg(total_amount)|
+----------+--------------------+------------------+
|       1.0|1.9496468430212937E7|15.606626116946773|
|       4.0|   90039.93000000082| 74.90842762063296|
|       3.0|   67363.26000000043| 78.69539719626219|
|       2.0|   973635.4700000732| 65.52937609369182|
|      99.0|  1748.0699999999997| 48.55749999999999|
|       5.0|  255075.08999999086|48.939963545662096|
+----------+--------------------+------------------+
"""