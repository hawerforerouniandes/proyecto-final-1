
def test_ping(client):
        response_valid = client.get("/public/ping")
        assert response_valid.json == "Pong"