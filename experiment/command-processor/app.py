from confluent_kafka import Consumer, KafkaError
from kafka import KafkaProducer
import psycopg2
from psycopg2 import sql
import json
import http.server
import socketserver
import threading

# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
}

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
topic_error = 'questions_processor_failed'

# Subscribe to the Kafka topic
consumer.subscribe([topic_source])

# Define a custom request handler that extends the BaseHTTPRequestHandler
class MyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'pong')
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

# Create a socket server with the custom request handler
def run_http_server():
    with socketserver.TCPServer(('', 5001), MyHandler) as httpd:
        print('Server started on port 5001...')
        httpd.serve_forever()

# Define a function to process Kafka messages
def process_kafka_messages():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=lambda k: json.dumps(k).encode('utf-8'),  # Serialize the key as JSON
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the value as JSON
    )

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print(f'Error: {msg.error()}')
        else:
            print(f'Received message: {msg.value().decode("utf-8")}')

            try:
                json_message = json.loads(msg.value().decode('utf-8'))
                id_user = json_message.get('id_user')
                id_question = json_message.get('id_question')
                name = json_message.get('name')
                questionnaire_type = json_message.get('questionnaire_type')
                respuesta = json_message.get('respuesta')
                create_timestamp = json_message.get('create_timestamp')

                try:
                    conn = psycopg2.connect(**db_config)
                    cursor = conn.cursor()
                    insert_query = sql.SQL("""
                        INSERT INTO questionnaire (id_user, id_question, name, type_questionnaire, respuesta, create_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """)
                    cursor.execute(insert_query, (id_user, id_question, name, questionnaire_type, respuesta, create_timestamp))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    print("question saved")
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
                    producer.send(topic_error, key=key, value=message)
                    producer.flush()
                    print("Message sent to Kafka topic.")
                finally:
                    conn.close()
            except Exception as e:
                print(f'Error parsing or saving message: {str(e)}')

# Create threads for the HTTP server and Kafka consumer
http_server_thread = threading.Thread(target=run_http_server)
kafka_consumer_thread = threading.Thread(target=process_kafka_messages)

# Start the threads
http_server_thread.start()
kafka_consumer_thread.start()

# Wait for the threads to finish
http_server_thread.join()
kafka_consumer_thread.join()
