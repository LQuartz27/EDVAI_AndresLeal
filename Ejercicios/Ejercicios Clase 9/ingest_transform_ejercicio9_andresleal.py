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
    dag_id='ingest-transform-andresleal-ejercicio-9',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform','load','ejercicio 9'],
    params={"example_key": "example_value"},
) as dag:

    inicia_proceso = DummyOperator(
        task_id='inicia_proceso',
    )

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    with TaskGroup("Ingest", tooltip="Tasks for Ingestion from northwind") as ingestion_section:
        ingest_clientes = BashOperator(
            task_id='ingest_clientes',
            bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_clientes.sh ',
        )

        ingest_envios = BashOperator(
            task_id='ingest_envios',
            bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_envios.sh ',
        )

        ingest_order_details = BashOperator(
            task_id='ingest_order_details',
            bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_order_details.sh ',
        )

    with TaskGroup("Processing", tooltip="Tasks for Processing northwind data") as processing_section:
        
        transform_load_clientes = BashOperator(
            task_id='transform_load_clientes',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_load_clientes.py ',
        )

        transform_envios_order_details = BashOperator(
        task_id='transform_envios_order_details',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_envios_order_details.py ',
    )

    inicia_proceso >> ingestion_section >>  processing_section >> finaliza_proceso
    

if __name__ == "__main__":
    dag.cli()