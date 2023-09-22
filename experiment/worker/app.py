from confluent_kafka import Consumer, KafkaError
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql
import json
import time

# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
    'enable.auto.commit': 'false'
}

db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',  
    'port': '5432', 
}


topic_source = "questions_processor_failed"
# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([topic_source])  # Replace 'your_topic' with your Kafka topic name

producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),  # Serialize the key as JSON
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the value as JSON
)


# Function to check if the database is up
def is_database_up():
    try:
        conn = psycopg2.connect(**db_config)
        conn.close()
        return True
    except Exception as e:
        print(f'Database is not up: {str(e)}')
        return False


# Continuously poll for new messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed
    if msg is None:
        continue

    # Periodically check if the database is up (every 5 seconds)
    while not is_database_up():
        time.sleep(5)

    print("Database is up and running.")

    if msg.error():
        # Handle any Kafka errors
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            print('Reached end of partition')
        else:
            print(f'Error: {msg.error()}')
    else:
        # Print the received message
        print(f'Received message: {msg.value().decode("utf-8")}')

        # Parse the received JSON message (assuming it's in JSON format)
        try:
            json_message = json.loads(msg.value().decode('utf-8'))
            id_user = json_message.get('id_user')
            id_question = json_message.get('id_question')
            name = json_message.get('name')
            questionnaire_type = json_message.get('questionnaire_type')
            respuesta = json_message.get('respuesta')
            create_timestamp = json_message.get('create_timestamp')

            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            insert_query = sql.SQL("""
                INSERT INTO questionnaire (id_user, id_question, name, type_questionnaire, respuesta, create_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (id_user, id_question, name, questionnaire_type, respuesta,create_timestamp))
            conn.commit()
            cursor.close()
            conn.close()
            print("question saved")

            # Attempt to process the message
            # Add your processing logic here

            # If processing succeeds, commit the offset
            print("commit message ")
            consumer.commit()
        except (psycopg2.OperationalError, psycopg2.Error) as db_error:
                    print(f'Database error: {db_error}')
                    message = {
                        "id_user": id_user,
                        "name": name,
                        "questionnaire_type": questionnaire_type,
                        "id_question": id_question,
                        "respuesta": respuesta,
                        "create_timestamp": create_timestamp
                    }
                    key = f'{id_user}+{id_question}'
                    producer.send(topic_source, key=key, value=message)
                    producer.flush()
                    print("Message sent to Kafka topic.")
        except Exception as e:
            # Handle the exception (e.g., log it)
            print(f'Error processing message: {str(e)}')
            continue
            # Optionally, you can choose not to commit the offset
            # to mark the message as unprocessed
            # This will cause the same message to be retrieved again
            # when the consumer restarts
            # consumer.close()  # Close the consumer to avoid committing

# Close the Kafka consumer
consumer.close()
