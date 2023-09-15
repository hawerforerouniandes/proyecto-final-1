from flask import Flask, request, jsonify
from kafka import KafkaProducer
import logging  # Import the logging module

app = Flask(__name__)
# Set the logging level to DEBUG for the entire application
logging.basicConfig(level=logging.DEBUG)

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Replace with your Kafka broker list
    key_serializer=lambda k: str(k).encode('utf-8'),  # Serialize the key as UTF-8 string
    value_serializer=lambda v: str(v).encode('utf-8')  # Serialize the value as UTF-8 string
)

# Define the Kafka topic where messages will be emitted
kafka_topic = 'questions_processor'  # Replace with your Kafka topic name

@app.route('/question', methods=['POST'])
def send_message():
    try:
        data = request.get_json()
        id_user = data.get('id_user')
        id_question = data.get('id_question')

        # Construct the message payload as a dictionary
        message = {
            'id_user': id_user,
            'name': data.get('name'),
            'questionnaire_type': data.get('questionnaire_type'),
            'id_question': id_question,
            'respuesta': data.get('respuesta')
        }

        # Emit the message to the Kafka topic with the specified key
        key = f'{id_user}+{id_question}'
        producer.send(kafka_topic, key=key, value=message)
        producer.flush()

        return jsonify({'status': 'success', 'message': 'Message sent to Kafka topic.'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return 'pong', 200  # Respond with 'pong' and HTTP 200 status code
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
