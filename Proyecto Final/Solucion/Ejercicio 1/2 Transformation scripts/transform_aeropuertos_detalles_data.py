from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, coalesce, lit
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)


## Cargamos los archivos a dataframes
df = spark.read.options(header=True, inferSchema=True, delimiter=';').csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

to_drop_columns = ("inhab", "fir",)

# Eliminamos las columnas que no necesitamos
df = df.drop(*to_drop_columns)

print((df.count(), len(df.columns)))

# Cmabiamos los nombres y casteamos a los tipos de dato requeridos
df = df.select( 
                col("local").alias("aeropuerto"), 
                col("oaci").alias("oac"),
                col("iata"),
                col("tipo"),
                col("denominacion"),
                col("coordenadas"),
                col("latitud").cast("string"),
                col("longitud").cast("string"),
                col("elev").cast("float").alias("elev"),
                col("uom_elev"),
                col("ref"),
                coalesce(col("distancia_ref"), lit(0)).cast("float").alias("distancia_ref"),
                col("direccion_ref"),
                col("condicion"),
                col("control"),
                col("region"),
                col("uso"),
                col("trafico"),
                col("sna"),
                col("concesionado"),
                col("provincia")
            )

# Insertamos el DF con los tipos corregidos en la tabla del datawarehouse
df.write.insertInto("ejercicio1_db.aeropuertos_detalles")