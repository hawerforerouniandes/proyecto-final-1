from confluent_kafka import Consumer, KafkaError

# Define Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'my-consumer-group',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe(['demo'])  # Replace 'demo' with your Kafka topic name

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

# Close the Kafka consumer
consumer.close()
