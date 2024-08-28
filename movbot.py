from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )

from kafka import KafkaProducer
from json import dumps

with DAG(
        'mov_bot',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(seconds=3),
        'max_active_tasks': 3,
        'max_active_runs': 1,
        },

    description='pyspark',
    #schedule=timedelta(days=1),
    
    schedule="@yearly",
    start_date=datetime(2015, 1, 1),
    end_date=datetime(2024, 1, 1),    
    
    catchup=True,
    tags=['pyspark', 'movie', 'json', 'dynamic'],
) as dag:


    def send_kafka_message(year):
        producer = KafkaProducer(
            bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
            value_serializer=lambda x: dumps(x).encode('utf-8')
    )

        message = {'sender': '[bot]', 'message': f'[Airflow] {year}년의 영화목록데이터 추출 작업이 완료되었습니다.', 'end': False, 'timestamp': datetime.now().isoformat()}
        producer.send('team2', value=message)
        producer.flush()


    get_data = BashOperator(
        task_id='get.data',
        bash_command='python /home/oddsummer/teamproj/dags/py/movlist.py {{ execution_date.year }}',
    )

    
    get_flatten = BashOperator(
    task_id='data.flatten',
    bash_command="""
            $SPARK_HOME/bin/spark-submit /home/oddsummer/teamproj/dags/py/flatten.py
        """
        )


    send_message = PythonOperator(
        task_id='send.kafka.message',
        python_callable=send_kafka_message,
        op_args=['{{ execution_date.year }}'],
    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    
    task_start >> get_data >> get_flatten >> send_message >> task_end
