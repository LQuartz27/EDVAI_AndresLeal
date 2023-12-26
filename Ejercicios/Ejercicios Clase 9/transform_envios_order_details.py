from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

# CARGAMOS LOS DF, Y CREAMOS VISTAS TEMPORALES EN SPARK PARA SU MANIPULACION CON SQL
envios_df = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/envios/*.parquet")
order_details_df = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/order_details/*.parquet")

# CONVERTIMOS LA COLUMNA DE FECHA, QUE VIENE COMO STRING A TIPO FECHA
envios_df = envios_df.select(envios_df.order_id, envios_df.shipped_date.cast("date"), envios_df.company_name, envios_df.phone)

envios_df.createOrReplaceTempView("v_envios")
order_details_df.createOrReplaceTempView("v_od")

# SQL STMNT PARA REALIZAR LOS CALCULOS REQUERIDOS
select_orders = """
select 
v_envios.*,
unit_price * (1-discount) as unit_price_discount,
quantity,
(unit_price * (1-discount) ) * quantity as total_price
from v_envios
left join v_od
on v_envios.order_id = v_od.order_id 
where discount > 0
"""

# INSERTAMOS LA DATA PROCESADA EN LA TABLA DESEADA
hive_insert_stmnt = f'insert into northwind_analytics.products_sent {select_orders};'

hc.sql(hive_insert_stmnt)