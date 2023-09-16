# This is a sample Python script.
import json
import logging
import uuid
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: json.dumps(k).encode('utf-8'),  # Serialize the key as JSON
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the value as JSON
)
kafka_topic = 'monitor'

scheduler = BlockingScheduler()


def send_topic_message(ms, status):
    monitor_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    message = {
        "ms-name": ms,
        "monitor-timestamp": monitor_timestamp,
        "ms-status": status
    }
    key = str(uuid.uuid4())
    producer.send(kafka_topic, key=key, value=message)
    producer.flush()
    logger.info(f'Published Monitor Topic for ms: {ms} with status: {status}')


def call_api_endpoint():
    logger.info("Hi")
    call_command()
    call_command_processor()


def call_command():
    url = "http://localhost:5000/health"
    # Make the API request

    response = requests.get(url)
    if response.status_code == 200:
        logger.info("API call command successful")
        send_topic_message("Command", "OK")
        # Process the API response data as needed
        # ...
    else:
        logger.info(f"Failed to call API. Status code: {response.status_code}")
        send_topic_message("Command", "FAILED")


def call_command_processor():
    url = "http://localhost:5001/health"
    # Make the API request

    response = requests.get(url)
    if response.status_code == 200:
        logger.info("API call command processor successful")
        send_topic_message("Command Processor", "OK")
        # Process the API response data as needed
        # ...
    else:
        logger.info(f"Failed to call API. Status code: {response.status_code}")
        send_topic_message("Command Processor", "FAILED")


logging.basicConfig(filename='monitorlogs.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


logger = logging.getLogger('Monitor')
if __name__ == '__main__':
    job = scheduler.add_job(call_api_endpoint, 'interval', minutes=1)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
