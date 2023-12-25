from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)


## Cargamos los archivos a dataframes
# df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv")
df_1 = spark.read.parquet("hdfs://172.17.0.2:9000/ingest/yellow_trip_data_2021-01.parquet")
df_2 = spark.read.parquet("hdfs://172.17.0.2:9000/ingest/yellow_trip_data_2021-02.parquet")

# Unimos los dataframes en uno solo

df_union = df_1.union(df_2)

##creamos una vista del DF
df_union.createOrReplaceTempView("union_vw")

##Filtramos el DF quedandonos solamente con viajes que tuvieron como inicio o destino aeropuertos, que hayan pagado con dinero
filtered_df = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount, total_amount from union_vw where payment_type = 2 and airport_fee is not NULL")

##opcional: si queremos ver la info que quedo filtrada####
#filtered_df.show(5)

##Creamos una vista con la data filtrada###
filtered_df.createOrReplaceTempView("tripdata_vista_filtrada")

##insertamos el DF filtrado en la tabla tripdata_table
hc.sql("insert into tripdata_andres_leal_db.airport_trips SELECT * FROM tripdata_vista_filtrada;")