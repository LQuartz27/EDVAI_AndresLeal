from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

# CARGAR EL DF Y CREAMOS LA VISTA TEMPORAL EN SPARK PARA SU MANIPULACION CON SQL
# df = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/clientes/*.parquet")
df = spark.read.options(header=True, inferSchema=True).csv("hdfs://172.17.0.2:9000/nifi/titanic.csv")

# Remover las columnas SibSp y Parch
df = df.drop("SibSp", "Parch")

df.createOrReplaceTempView("v_df")

# Por cada fila calcular el promedio de edad de los hombres en caso que sea
# hombre y promedio de edad de las mujeres en caso que sea mujer

# Si el valor de cabina en nulo, dejarlo en 0 (cero)

select_stmnt = """
SELECT 
PassengerId,
Survived,
Pclass,
Name,
Sex,
Age,
Ticket,
Fare,
Embarked,
CASE 
  WHEN Sex = 'male' THEN   (SELECT AVG(Age) FROM v_df WHERE Sex = 'male')
  WHEN Sex = 'female' THEN (SELECT AVG(Age) FROM v_df WHERE Sex = 'female')
END as sex_avg_age,
COALESCE(Cabin, 0) AS Cabin
FROM v_df v
"""

final_df = spark.sql(select_stmnt)
final_df = final_df.withColumn("Age", final_df.Age.cast('int'))

final_df.createOrReplaceTempView("v_final_df")

# INSERTAMOS LA DATA PROCESADA EN LA TABLA DESEADA
hive_insert_stmnt = f'insert into tripdata_andres_leal_db.titanic_transformed select * from v_final_df;'

hc.sql(hive_insert_stmnt)
