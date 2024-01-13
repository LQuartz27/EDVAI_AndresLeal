from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='examen_ej2_dag_padre',
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

        ingest_car_rental_data = BashOperator(
            task_id='ingest_car_rental_data',
            bash_command='/usr/bin/sh /home/hadoop/scripts/examen/car_rental_data.sh ',
        )

        ingest_georef_usa = BashOperator(
            task_id='georef_usa_data',
            bash_command='/usr/bin/sh /home/hadoop/scripts/examen/georef_usa.sh ',
        )

    trigger = TriggerDagRunOperator(
        task_id="ejecutar_dag_hijo",
        trigger_dag_id="examen_ej2_dag_hijo",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )

    inicia_proceso >> ingestion_section >> trigger >> finaliza_proceso


if __name__ == "__main__":
    dag.cli()