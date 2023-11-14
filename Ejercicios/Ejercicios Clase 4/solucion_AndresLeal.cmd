rem                         PRACTICA SQOOP


rem Mostrar las tablas de la base de datos northwind

sqoop list-tables --connect jdbc:postgresql://172.17.0.4:5432/northwind --username postgres --password edvai

rem Mostrar los clientes de Argentina

sqoop eval --connect jdbc:postgresql://172.17.0.4:5432/northwind --username postgres --password edvai --query "select * from customers where country = 'Argentina'"

rem Importar un archivo .parquet que contenga toda la tabla orders. Luego ingestar el
rem archivo a HDFS (carpeta /sqoop/ingest)

sqoop import --connect jdbc:postgresql://172.17.0.4:5432/northwind --username postgres --password edvai --table orders --m 1 --target-dir /sqoop/ingest/exercise/import_from_table --as-parquetfile --delete-target-dir

rem Importar un archivo .parquet que contenga solo los productos con mas 20 unidades en
rem stock, de la tabla Products . Luego ingestar el archivo a HDFS (carpeta ingest)

sqoop import --connect jdbc:postgresql://172.17.0.4:5432/northwind --username postgres --password edvai --query "select * from products p where units_in_stock > 20 AND \$CONDITIONS" --m 1 --target-dir /sqoop/ingest/exercise/import_from_query --as-parquetfile --delete-target-dir

rem ####################################################################################
rem ####################################################################################
rem ####################################################################################

rem                         PRACTICA SQOOP

@REM 1) En el shell de Nifi, crear un script .sh que descargue el archivo starwars.csv al directorio
@REM    /home/nifi/ingest (crearlo si es necesario). 

@REM Ejecutarlo con ./home/nifi/ingest/ingest.sh
@REM https://github.com/fpineyro/homework-0/blob/master/starwars.csv

./landing_nifi.sh

@REM 2) Usando procesos en Nifi:

@REM a) tomar el archivo starwars.csv desde el directorio /home/nifi/ingest.

@REM b) Mover el archivo starwars.csv desde el directorio anterior, a /home/nifi/bucket
@REM    (crear el directorio si es necesario)

@REM c) Tomar nuevamente el archivo, ahora desde /home/nifi/bucket

@REM d) Ingestarlo en HDFS/nifi (si es necesario, crear el directorio con hdfs dfs -mkdir
@REM    /nifi )
   
