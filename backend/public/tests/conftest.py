import pytest
from main import app as main_app

@pytest.fixture
def app():
    app_mock = main_app
    return app_mock

@pytest.fixture()
def client(app):
    return app.test_client()