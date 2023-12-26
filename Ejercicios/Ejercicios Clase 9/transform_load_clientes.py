from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

# CARGAR EL DF Y CREAMOS LA VISTA TEMPORAL EN SPARK PARA SU MANIPULACION CON SQL
df = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/clientes/*.parquet")

df.createOrReplaceTempView("v_df")

select_best_clients = """
select * from v_df
where productos_vendidos > (select avg(productos_vendidos) from v_df)
"""

# best_clients_df = spark.sql(select_best_clients)
# best_clients_df.createOrReplaceTempView("best_clients_vw")

# INSERTAMOS LA DATA PROCESADA EN LA TABLA DESEADA

hive_insert_stmnt = f'insert into northwind_analytics.products_sold {select_best_clients};'

hc.sql(hive_insert_stmnt)