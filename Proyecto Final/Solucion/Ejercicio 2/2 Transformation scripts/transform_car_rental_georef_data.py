from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, lit, lower
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

## Cargamos los archivos a dataframes
car_rental_df = spark.read.options(header=True, inferSchema=True, delimiter=',').csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
georef_df = spark.read.options(header=True, inferSchema=True, delimiter=';').csv("hdfs://172.17.0.2:9000/ingest/georef_usa.csv")

# Limpiamos los nombres de las columnas de los DataFrames
for col in car_rental_df.columns:
    new_col = col.replace(' ', '_').replace('.','_')
    car_rental_df = car_rental_df.withColumnRenamed(col, new_col)

for col in georef_df.columns:
    new_col = col.replace(' ', '_').replace('.','_')
    georef_df = georef_df.withColumnRenamed(col, new_col)

georef_df = georef_df.withColumnRenamed("Iso_3166-3_Area_Code", "Area_Code")\
                     .withColumnRenamed("United_States_Postal_Service_state_abbreviation", "State_Abbreviation")\
                     .withColumnRenamed("Official_Code_State", "Code_State")\
                     .withColumnRenamed("Official_Name_State", "Name_State")

# Casteamos la columna rating a entero
car_rental_df = car_rental_df.withColumn("rating", car_rental_df.rating.cast("int"))

# Pasamos todos los strings a minusculas en la columna fuelType
car_rental_df = car_rental_df.withColumn("fuelType", lower(car_rental_df['fuelType']))

# Cruzamos las tablas y seleccionamos las columnas de interés
car_rental_df.createOrReplaceTempView("car_rental_vw")
georef_df.createOrReplaceTempView("georef_vw")

join_df = spark.sql(
    """
    SELECT 
    DISTINCT
    cr.fuelType,
    cr.rating,
    cr.renterTripsTaken,
    cr.reviewCount,
    cr.location_city as city,
    g.Name_State as state_name,
    cr.owner_id,
    cr.rate_daily,
    cr.vehicle_make as make,
    cr.vehicle_model as model,
    cr.vehicle_year as year
    FROM 
    car_rental_vw cr
    LEFT JOIN georef_vw g
      ON cr.location_state = g.State_Abbreviation
    WHERE cr.location_state <> 'TX'
    """
)

# Insertamos el resultado en nuestra tabla previamente creada
join_df.write.insertInto("car_rental_db.car_rental_analytics")