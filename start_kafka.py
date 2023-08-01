# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'RELACE WITH YOUR OWN NAME',
    'start_date': days_ago(1),
    'email': ['RELACE WITH YOUR OWN EMAIL'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    dag_id='start-kafka',
    default_args=default_args,
    description='start kafka',
    schedule_interval='12 17 * * *',
)

start_zoo = BashOperator(
    task_id='start_zoo',
    bash_command='../kafka_2.12-2.8.0/bin/zookeeper-server-start.sh ../kafka_2.12-2.8.0/config/zookeeper.properties',
    dag=dag,
)

start_server = BashOperator(
    task_id='start_server',
    bash_command='../kafka_2.12-2.8.0/bin/kafka-server-start.sh ../kafka_2.12-2.8.0/config/config/server.properties',
    dag=dag,
)


check_topic = BashOperator(
    task_id='check_topic',
    bash_command='../kafka_2.12-2.8.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092',
    dag=dag,
)


start_zoo >> start_server >> check_topic


