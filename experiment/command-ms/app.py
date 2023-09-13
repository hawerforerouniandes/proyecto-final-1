from flask import Flask, request, jsonify
from kafka import KafkaProducer
import logging  # Import the logging module

app = Flask(__name__)
# Set the logging level to DEBUG for the entire application
logging.basicConfig(level=logging.DEBUG)

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Replace with your Kafka broker list
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Define the Kafka topic where messages will be emitted
kafka_topic = 'demo'  # Replace with your Kafka topic name

@app.route('/greet', methods=['POST'])
def send_message():
    try:
        data = request.get_json()
        name = data['name']
        message = data['message']

        # Emit the message to the Kafka topic
        producer.send(kafka_topic, message)
        producer.flush()

        return jsonify({'status': 'success', 'message': 'Message sent to Kafka topic.'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
