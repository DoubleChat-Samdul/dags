from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
from json import dumps
from datetime import datetime

def send_kafka_message():
    producer = KafkaProducer(
        bootstrap_servers=['ec2-43-203-210-250.ap-northeast-2.compute.amazonaws.com:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    
    message = {'sender': '[bot]', 'message': '미팅시간 5분 전입니다', 'end': False, 'timestamp': datetime.now().isoformat()}
    producer.send('team2', value=message)
    producer.flush()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'send_reminder',
    default_args=default_args,
    description='Sends a reminder message to the chatroom at 9:30 AM',
    schedule_interval='30 00 * * *',
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

