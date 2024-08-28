from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps

def send_kafka_message():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    
    message = {'sender': '[SYSTEM]', 'message': '미팅시간 5분전 입니다', 'end': False}
    producer.send('chatroom', value=message)
    producer.flush()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_reminder',
    default_args=default_args,
    description='Sends a reminder message to the chatroom at 9:30 AM',
    schedule_interval='30 9 * * *',
    catchup=False,
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

send_message_task = PythonOperator(
    task_id='send_kafka_message',
    python_callable=send_kafka_message,
    dag=dag,
)

start_task >> send_message_task >> end_task

