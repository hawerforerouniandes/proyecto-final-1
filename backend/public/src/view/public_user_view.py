import os

from flask_restful import Resource
from flask import request
import requests


class VistaPing(Resource): 
    def get(self):
        return "Pong"

