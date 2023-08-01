# streamingDataAirflow

This repo contains codes that can perform the following and use Airflow to schedule:

streaming data from an open source weather API -> transform data -> use Kafka to process the streaming data -> post data to Power BI dashboard

You need to set up the following values before running the script:

\# for the `kafka_dashboard_airflow.py` script

cur_url = #"REPLACE THIS STRING with your own API to retrieve current weather data"

ago24h_url = #"REPLACE THIS STRING with your own API to retrieve weather data from 24 hours ago"

endpoint = #"REPLACE THIS STRING with your own API for powerBI"

\# for the `start_kafka.py` script, define your own path

'owner': 'RELACE WITH YOUR OWN NAME'

'email': ['REPLACE WITH YOUR OWN EMAIL']

\# Replace the ".." with yout own path

bash_command='../kafka_2.12-2.8.0/bin/zookeeper-server-start.sh ../kafka_2.12-2.8.0/config/zookeeper.properties'

bash_command='../kafka_2.12-2.8.0/bin/kafka-server-start.sh ../kafka_2.12-2.8.0/config/config/server.properties'

bash_command='../kafka_2.12-2.8.0/bin/kafka-topics.sh --list --bootstrap-server localhost:9092'

