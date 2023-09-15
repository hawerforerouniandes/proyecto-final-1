from flask import Flask, request, jsonify
from kafka import KafkaProducer
import logging
from datetime import datetime
import json

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    key_serializer=lambda k: json.dumps(k).encode('utf-8'),  # Serialize the key as JSON
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the value as JSON
)

kafka_topic = 'questions_processor'

@app.route('/question', methods=['POST'])
def send_message():
    try:
        data = request.get_json()
        id_user = data.get('id_user')
        id_question = data.get('id_question')

        create_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        message = {
            "id_user": id_user,
            "name": data.get('name'),
            "questionnaire_type": data.get('questionnaire_type'),
            "id_question": id_question,
            "respuesta": data.get('respuesta'),
            "create_timestamp": create_timestamp
        }

        key = f'{id_user}+{id_question}'
        producer.send(kafka_topic, key=key, value=message)
        producer.flush()

        return jsonify({'status': 'success', 'message': 'Message sent to Kafka topic.'})

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return 'pong', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
