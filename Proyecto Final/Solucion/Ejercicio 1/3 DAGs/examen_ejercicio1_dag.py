from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='examen_ejercicio1_dag',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['Examen Final'],
    params={"example_key": "example_value"},
) as dag:

    inicia_proceso = DummyOperator(
        task_id='inicia_proceso',
    )

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    with TaskGroup("Ingest", tooltip="Tasks for Ingestion of Airports/Trips data") as ingestion_section:

        ingest_aeropuerto_detalles = BashOperator(
            task_id='ingest_aeropuerto_detalles',
            bash_command='/usr/bin/sh /home/hadoop/scripts/examen/ingest_aeropuerto_detalles.sh ',
        )

        ingest_informe_ministerio_2021 = BashOperator(
            task_id='ingest_informe_ministerio_2021',
            bash_command='/usr/bin/sh /home/hadoop/scripts/examen/ingest_informe_ministerio_2021.sh ',
        )

        ingest_informe_ministerio_2022 = BashOperator(
            task_id='ingest_informe_ministerio_2022',
            bash_command='/usr/bin/sh /home/hadoop/scripts/examen/ingest_informe_ministerio_2022.sh ',
        )

    with TaskGroup("Processing", tooltip="Tasks for Processing Airports/Trips data") as processing_section:
        
        transform_aeropuertos_detalles_data = BashOperator(
            task_id='transform_aeropuertos_detalles_data',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen/transform_aeropuertos_detalles_data.py ',
        )

        transform_informe_ministerio_data = BashOperator(
        task_id='transform_informe_ministerio_data',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen/transform_informe_ministerio_data.py ',
    )

    inicia_proceso >> ingestion_section >>  processing_section >> finaliza_proceso
    

if __name__ == "__main__":
    dag.cli()