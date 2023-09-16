from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2 import sql
import json

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


# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe(['questions_processor_failed'])  # Replace 'your_topic' with your Kafka topic name

# Continuously poll for new messages
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed
    print("poll messages")

    if msg is None:
        print("There is no messages")
        break

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
        except Exception as e:
            # Handle the exception (e.g., log it)
            print(f'Error processing message: {str(e)}')
            consumer.close()
            break
            # Optionally, you can choose not to commit the offset
            # to mark the message as unprocessed
            # This will cause the same message to be retrieved again
            # when the consumer restarts
            # consumer.close()  # Close the consumer to avoid committing

# Close the Kafka consumer
consumer.close()
