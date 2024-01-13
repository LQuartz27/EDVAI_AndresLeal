from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, lit
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

## Cargamos los archivos a dataframes
df_1 = spark.read.options(header=True, inferSchema=True, delimiter=';').csv("hdfs://172.17.0.2:9000/ingest/2021_informe_ministerio.csv")
df_2 = spark.read.options(header=True, inferSchema=True, delimiter=';').csv("hdfs://172.17.0.2:9000/ingest/2022_informe_ministerio.csv")

# Unimos los dataframes en uno solo
df_union = df_1.union(df_2)

# Cambiamos los nombres y casteamos a los tipos de dato requeridos
df_union = df_union.select(to_date(col("Fecha"), "dd/MM/yyyy").alias("fecha"), 
                            col("`Hora UTC`").alias("horaUTC"), 
                            col("`Clase de Vuelo (todos los vuelos)`").alias("clase_de_vuelo"),
                            col("`Clasificación Vuelo`").alias("clasificacion_de_vuelo"),
                            col("`Tipo de Movimiento`").alias("tipo_de_movimiento"),
                            col("Aeropuerto").alias("aeropuerto"),
                            col("`Origen / Destino`").alias("origen_destino"),
                            col("`Aerolinea Nombre`").alias("aerolinea_nombre"),
                            col("Aeronave").alias("aeronave"),
                            coalesce(col("Pasajeros").cast("int"), lit(0)).alias("pasajeros")
                           ) 

# Conservamos únicamente los vuelos domésticos
# print(df_union.select("clasificacion_de_vuelo").distinct().show())
df_union = df_union.filter(col("clasificacion_de_vuelo").isin("Domestico","Doméstico"))

# Insertamos el DF con los tipos corregidos en la tabla del datawarehouse
df_union.write.insertInto("ejercicio1_db.vuelos")