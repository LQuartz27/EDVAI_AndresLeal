from pyspark.sql.functions import col

# Reading the file without inferring the schema

df = spark.read.options(header=True).csv("yellow_tripdata_2021-01.csv")

df.printSchema()

"""
root
 |-- VendorID: string (nullable = true)
 |-- tpep_pickup_datetime: string (nullable = true)
 |-- tpep_dropoff_datetime: string (nullable = true)
 |-- passenger_count: string (nullable = true)
 |-- trip_distance: string (nullable = true)
 |-- RatecodeID: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: string (nullable = true)
 |-- DOLocationID: string (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: string (nullable = true)
 |-- extra: string (nullable = true)
 |-- mta_tax: string (nullable = true)
 |-- tip_amount: string (nullable = true)
 |-- tolls_amount: string (nullable = true)
 |-- improvement_surcharge: string (nullable = true)
 |-- total_amount: string (nullable = true)
 |-- congestion_surcharge: string (nullable = true)
"""

# Reading the file inferring the schema
df2 = spark.read.options(header=True, inferSchema=True).csv("yellow_tripdata_2021-01.csv")

"""
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: string (nullable = true)
 |-- tpep_dropoff_datetime: string (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
"""

################################################################################
# 5 SELECTING THE DATA TO INSERT INTO PAYMENTS TABLE
################################################################################

payments_df = df.select(col("VendorID").cast("int"), col("tpep_pickup_datetime").cast("timestamp"), col("payment_type").cast("int"), col("total_amount").cast("double") )

payments_df.printSchema()

"""
root
 |-- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- total_amount: double (nullable = true)
"""

# Showing distinct values for payment types
payments_df.select("payment_type").distinct().show()

"""
+------------+
|payment_type|
+------------+
|        null|
|           1|
|           3|
|           4|
|           2|
+------------+
"""

# Selecting only payments with credit card - We know credit card = 1, 
#from the data dictionary in https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

payments_df_credit_card = payments_df.filter(col("payment_type")==1)

payments_df_credit_card.show(10)
"""
+--------+--------------------+------------+------------+
|VendorID|tpep_pickup_datetime|payment_type|total_amount|
+--------+--------------------+------------+------------+
|       1| 2021-01-01 00:43:30|           1|       51.95|
|       1| 2021-01-01 00:15:48|           1|       36.35|
|       2| 2021-01-01 00:31:49|           1|       24.36|
|       1| 2021-01-01 00:16:29|           1|       14.15|
|       1| 2021-01-01 00:26:12|           1|       18.95|
|       2| 2021-01-01 00:15:52|           1|        24.3|
|       2| 2021-01-01 00:46:36|           1|       10.79|
|       2| 2021-01-01 00:31:06|           1|       14.16|
|       2| 2021-01-01 00:17:48|           1|        10.3|
|       2| 2021-01-01 00:33:38|           1|       12.09|
+--------+--------------------+------------+------------+
"""

# Comparing dataframes sizes

# All payments
print((payments_df.count(), len(payments_df.columns)))
"""(1369765, 4)"""

# Credit card payments
print((payments_df_credit_card.count(), len(payments_df_credit_card.columns)))
"""(934473, 4)"""

# Inserting credit card records into Hive table, called payments
payments_df_credit_card.write.insertInto("tripdata_andres_leal_db.payments")

################################################################################
# 6 SELECTING THE DATA TO INSERT INTO PASSENGERS TABLE
################################################################################

passengers_df = df.select(col("tpep_pickup_datetime").cast("timestamp"), col("passenger_count").cast("int"), col("total_amount").cast("double") )

passengers_df.show(10)
"""
+--------------------+---------------+------------+
|tpep_pickup_datetime|passenger_count|total_amount|
+--------------------+---------------+------------+
| 2021-01-01 00:30:10|              1|        11.8|
| 2021-01-01 00:51:20|              1|         4.3|
| 2021-01-01 00:43:30|              1|       51.95|
| 2021-01-01 00:15:48|              0|       36.35|
| 2021-01-01 00:31:49|              1|       24.36|
| 2021-01-01 00:16:29|              1|       14.15|
| 2021-01-01 00:00:28|              1|        17.3|
| 2021-01-01 00:12:29|              1|        21.8|
| 2021-01-01 00:39:16|              1|        28.8|
| 2021-01-01 00:26:12|              2|       18.95|
+--------------------+---------------+------------+
"""

# Selecting only the desired records
passengers_df_condition = passengers_df.filter( ( col("passenger_count") > 2 ) & ( col("total_amount") > 8 ) )

passengers_df_condition.show(10)
"""
+--------------------+---------------+------------+
|tpep_pickup_datetime|passenger_count|total_amount|
+--------------------+---------------+------------+
| 2021-01-01 00:15:52|              3|        24.3|
| 2021-01-01 00:31:06|              5|       14.16|
| 2021-01-01 00:42:11|              5|         8.3|
| 2021-01-01 00:43:41|              3|         9.3|
| 2021-01-01 00:34:37|              4|        18.3|
| 2021-01-01 00:06:08|              4|        13.3|
| 2021-01-01 00:19:57|              3|        40.3|
| 2021-01-01 00:28:07|              5|        14.8|
| 2021-01-01 00:08:04|              3|       18.59|
| 2021-01-01 00:22:02|              3|       13.56|
+--------------------+---------------+------------+
"""

# Inserting the desired records into the Passengers Hive table
passengers_df_condition.write.insertInto("tripdata_andres_leal_db.passengers")

################################################################################
# 7 SELECTING THE DATA TO INSERT INTO TOLLS TABLE
################################################################################

tolls_df = df.select(col("tpep_pickup_datetime").cast("timestamp"), col("passenger_count").cast("int"), col("tolls_amount").cast("double"), col("total_amount").cast("double") )

tolls_df.printSchema()
"""
root
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)
"""

tolls_df.show(10)
"""
+--------------------+---------------+------------+------------+
|tpep_pickup_datetime|passenger_count|tolls_amount|total_amount|
+--------------------+---------------+------------+------------+
| 2021-01-01 00:30:10|              1|         0.0|        11.8|
| 2021-01-01 00:51:20|              1|         0.0|         4.3|
| 2021-01-01 00:43:30|              1|         0.0|       51.95|
| 2021-01-01 00:15:48|              0|         0.0|       36.35|
| 2021-01-01 00:31:49|              1|         0.0|       24.36|
| 2021-01-01 00:16:29|              1|         0.0|       14.15|
| 2021-01-01 00:00:28|              1|         0.0|        17.3|
| 2021-01-01 00:12:29|              1|         0.0|        21.8|
| 2021-01-01 00:39:16|              1|         0.0|        28.8|
| 2021-01-01 00:26:12|              2|         0.0|       18.95|
+--------------------+---------------+------------+------------+
"""

# Selecting only the desired records
tolls_df_condition = tolls_df.filter( (col("tolls_amount") > 0.1) & (col("passenger_count") > 1) )

tolls_df_condition.show(10)
"""
+--------------------+---------------+------------+------------+
|tpep_pickup_datetime|passenger_count|tolls_amount|total_amount|
+--------------------+---------------+------------+------------+
| 2021-01-01 00:10:46|              2|        6.12|       33.92|
| 2021-01-01 00:37:40|              2|        6.12|       59.42|
| 2021-01-01 00:07:26|              2|        6.12|       35.92|
| 2021-01-01 00:16:22|              6|        6.12|        40.1|
| 2021-01-01 00:18:47|              3|        6.12|        54.0|
| 2021-01-01 00:14:05|              2|         2.8|        34.1|
| 2021-01-01 01:30:07|              4|        6.12|       61.42|
| 2021-01-01 01:04:32|              4|        6.12|       51.42|
| 2021-01-01 01:42:43|              2|       11.75|       12.05|
| 2021-01-01 01:22:03|              6|        6.12|       71.42|
+--------------------+---------------+------------+------------+
"""

# Inserting the desired records into the Tolls Hive table
tolls_df_condition.write.insertInto("tripdata_andres_leal_db.tolls")

################################################################################
# 8 SELECTING THE DATA TO INSERT INTO CONGESTION TABLE
################################################################################

congestion_df = df.select( col("tpep_pickup_datetime").cast("timestamp"), col("passenger_count").cast("int"), col("congestion_surcharge").cast("double"), col("total_amount").cast("double") )

congestion_df.printSchema()
"""
root
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- congestion_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
"""

congestion_df.show(10)
"""
+--------------------+---------------+--------------------+------------+
|tpep_pickup_datetime|passenger_count|congestion_surcharge|total_amount|
+--------------------+---------------+--------------------+------------+
| 2021-01-01 00:30:10|              1|                 2.5|        11.8|
| 2021-01-01 00:51:20|              1|                 0.0|         4.3|
| 2021-01-01 00:43:30|              1|                 0.0|       51.95|
| 2021-01-01 00:15:48|              0|                 0.0|       36.35|
| 2021-01-01 00:31:49|              1|                 2.5|       24.36|
| 2021-01-01 00:16:29|              1|                 2.5|       14.15|
| 2021-01-01 00:00:28|              1|                 0.0|        17.3|
| 2021-01-01 00:12:29|              1|                 2.5|        21.8|
| 2021-01-01 00:39:16|              1|                 0.0|        28.8|
| 2021-01-01 00:26:12|              2|                 2.5|       18.95|
+--------------------+---------------+--------------------+------------+
"""

# Selecting only the desired records
congestion_df_condition = congestion_df.filter( (col("tpep_pickup_datetime") > '2021-01-01 00:00') & (col("tpep_pickup_datetime") < '2021-01-02 00:00') )

congestion_df_condition.show(10)
"""
+--------------------+---------------+--------------------+------------+
|tpep_pickup_datetime|passenger_count|congestion_surcharge|total_amount|
+--------------------+---------------+--------------------+------------+
| 2021-01-01 00:30:10|              1|                 2.5|        11.8|
| 2021-01-01 00:51:20|              1|                 0.0|         4.3|
| 2021-01-01 00:43:30|              1|                 0.0|       51.95|
| 2021-01-01 00:15:48|              0|                 0.0|       36.35|
| 2021-01-01 00:31:49|              1|                 2.5|       24.36|
| 2021-01-01 00:16:29|              1|                 2.5|       14.15|
| 2021-01-01 00:00:28|              1|                 0.0|        17.3|
| 2021-01-01 00:12:29|              1|                 2.5|        21.8|
| 2021-01-01 00:39:16|              1|                 0.0|        28.8|
| 2021-01-01 00:26:12|              2|                 2.5|       18.95|
+--------------------+---------------+--------------------+------------+
"""
# Inserting the desired records into the Congestion Hive table
congestion_df_condition.write.insertInto("tripdata_andres_leal_db.congestion")

################################################################################
# 9 SELECTING THE DATA TO INSERT INTO DISTANCE TABLE
################################################################################

distance_df = df.select( col("tpep_pickup_datetime").cast("timestamp"), col("passenger_count").cast("int"), col("trip_distance").cast("double"), col("total_amount").cast("double") )

# Selecting only the desired records
distance_df_condition = distance_df.filter( (col("tpep_pickup_datetime") > '2020-12-31 00:00') & (col("tpep_pickup_datetime") < '2021-01-01 00:00') & (col("passenger_count")==1) & (col("trip_distance")>15) )

distance_df_condition.show(10)
"""
+--------------------+---------------+-------------+------------+
|tpep_pickup_datetime|passenger_count|trip_distance|total_amount|
+--------------------+---------------+-------------+------------+
| 2020-12-31 21:40:20|              1|        17.96|        53.3|
+--------------------+---------------+-------------+------------+
"""

# Inserting the desired records into the Congestion Hive table
distance_df_condition.write.insertInto("tripdata_andres_leal_db.distance")