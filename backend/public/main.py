from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_restful import Api

from src.view.public_user_view import VistaPing

app = Flask(__name__)

app.config['PROPAGATE_EXCEPTIONS'] = True

api = Api(app)
api.add_resource(VistaPing, "/public/ping")


cors = CORS(app)
app_context = app.app_context()
app_context.push()
jwt = JWTManager(app)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3005)
