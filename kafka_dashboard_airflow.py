#!/usr/bin/env python 
import requests
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic, KafkaAdminClient, ConfigResource, ConfigResourceType

from datetime import datetime, timedelta
import schedule
import time
import pytz

#get data and publish at specific time at different intervals

def get_data_cur(cur_headers, cur_url):
    
    response_cur = requests.get(cur_url, headers=cur_headers)
    message_current_js = response_cur.json()
    cur_time = message_current_js['data']['time']
    
    print(f'current time: {cur_time}')
    
    return message_current_js, cur_time

def get_data_24Ago(ago24h_headers, ago24h_url, cur_time):
    
    start_time = datetime.now(tz=pytz.utc) - timedelta(minutes=1435)
    end_time = start_time + timedelta(minutes=2)
    
    start_time_formatted = start_time.isoformat("T").split(".")[0] + "Z"
    end_time_formatted = end_time.isoformat("T").split(".")[0] + "Z"
    
    payload = {
            "location": "austin",
            "fields": ["temperature"],
            "units": "metric",
            "timesteps": ["1m"],
            "startTime": start_time_formatted,
            "endTime": end_time_formatted
            }
    
    response = requests.post(ago24h_url, json=payload, headers=ago24h_headers)
    message_24hAgo_js = response.json()
    
    print(f'24hAgo time: {start_time_formatted}')
    
    return message_24hAgo_js

def publish_data(producer, topic, message):
    
    producer.send(topic, message)

def consume_data(consumer):
    
    message_list = []
    
    for msg in consumer:
        print(msg.value.decode("utf-8"))
        message_list.append(msg.value.decode("utf-8"))
    
    return message_list

def analyze_cur_data(message_cur_list):
    
    assert(len(message_cur_list)==1)
    
    message = message_cur_list[0]
    message_js = json.loads(message)
    
    cur_time = message_js['data']["time"]
    cur_temperature = message_js['data']['values']['temperature']
    cur_temperature = round((cur_temperature * 9/5 + 32),2)
    
    return cur_time, cur_temperature

def analyze_24hAgo_data(message_24hAgo_list):
    
    assert(len(message_24hAgo_list)==1)
    
    message = message_24hAgo_list[0]
    message_js = json.loads(message)
    
    Ago24h_temperature = message_js['data']['timelines'][0]['intervals'][0]['values']['temperature']
    Ago24h_temperature = round((Ago24h_temperature * 9/5 + 32),2)
    
    return Ago24h_temperature
    

def send_to_dashboard(cur_time, cur_temperature, Ago24h_temperature, headers, endpoint):
    
    payload = [{
                "time" : cur_time,
                "current temperature" : cur_temperature,
                "24h ago temperature": Ago24h_temperature
                }]
    
    response = requests.post(endpoint, json=payload, headers=headers)
    print(f'message from dashboard: {response.text}')
    print(f'curent tmp: {cur_temperature}')
    print(f'24h ago temperature: {Ago24h_temperature}')

if __name__ == "__main__":
    
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer_current = KafkaConsumer("current", 
                                bootstrap_servers=["localhost:9092"],
                                auto_offset_reset='earliest', 
                                group_id='current', 
                                consumer_timeout_ms=1000)
    
    consumer_24hAgo = KafkaConsumer("24hAgo", 
                                bootstrap_servers=["localhost:9092"],
                                auto_offset_reset='earliest', 
                                group_id='24h', 
                                consumer_timeout_ms=1000)
    
    #weather api
    cur_headers = {"accept": "application/json"}
    cur_url = #"REPLACE THIS STRING with your own API to retrieve current weather data"
    
    ago24h_headers = {
                    "accept": "application/json",
                    "Accept-Encoding": "gzip",
                    "content-type": "application/json"
                    }
    ago24h_url = #"REPLACE THIS STRING with your own API to retrieve weather data from 24 hours ago"
    
    #dashboard api
    endpoint = #"REPLACE THIS STRING with your own API for powerBI"
    headers_dashboard = {
                        "content-type": "application/json"
                        }
                    
    message_current_js, cur_time = get_data_cur(cur_headers, cur_url)
    message_24hAgo_js = get_data_24Ago(ago24h_headers, ago24h_url, cur_time)
    
    publish_data(producer, "current", message_current_js)
    print('published current data')
    
    publish_data(producer, "24hAgo", message_24hAgo_js)
    print('published 24h ago data')
    
    message_cur_list = consume_data(consumer_current)
    print('consumed current data')
    
    message_24hAgo_list = consume_data(consumer_24hAgo)
    print('consumed 24h ago data')
    
    cur_time, cur_temperature = analyze_cur_data(message_cur_list)
    Ago24h_temperature = analyze_24hAgo_data(message_24hAgo_list)
    
    send_to_dashboard(cur_time, cur_temperature, Ago24h_temperature, headers_dashboard, endpoint)
    















