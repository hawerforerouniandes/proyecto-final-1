from confluent_kafka import Consumer, KafkaError
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql
import json
from flask import Flask, request

# Create a Flask app
app = Flask(__name__)

# Define a health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return 'pong', 200  # Respond with 'pong' and HTTP 200 status code

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)



# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
}


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: json.dumps(k).encode('utf-8'),  # Serialize the key as JSON
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the value as JSON
)

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# PostgreSQL database connection configuration
db_config = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',  
    'port': '5432', 
}

topic_source = 'questions_processor'
topic_error  = 'questions_processor_failed'

# Subscribe to the Kafka topic
consumer.subscribe([topic_source])  # Replace 'demo' with your Kafka topic name

# PostgreSQL connection


# Continuously poll for new messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed

    if msg is None:
        continue

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

            # Save the message to the PostgreSQL table
            try:
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
            except psycopg2.OperationalError or psycopg2.Error  as db_error:
                print(f'Database error: {db_error}')
                # Publish the message to another Kafka topic in case of database error
                message = {
                    "id_user": id_user,
                    "name": name,
                    "questionnaire_type": questionnaire_type,
                    "id_question": id_question,
                    "respuesta": respuesta,
                    "create_timestamp": create_timestamp
                }
                key = f'{id_user}+{id_question}'
                producer.send(topic_error, key=key, value=message)
                producer.flush()
                print("Message sent to Kafka topic.")
            finally:
                conn.close()
        except Exception as e:
            print(f'Error parsing or saving message: {str(e)}')

# Close the Kafka consumer and PostgreSQL connection
consumer.close()
conn.close()
