import requests
import json
import time
import traceback
from kafka import KafkaProducer

# Twitch API credentials
CLIENT_ID = "exdwulqs8817ahx29j1oz9mgnv8tgq"
CLIENT_SECRET = "ua49cfqafynbywqd9y05m2dc1krl8l"
TOPIC = "twitch_streams"

# Get access token
def get_access_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    response = requests.post(url, params=params)
    if response.ok:
        return response.json()["access_token"]
    else:
        raise Exception("Failed to obtain access token: " + response.text)

ACCESS_TOKEN = get_access_token()

HEADERS = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Optional: test Kafka connection
try:
    test_msg = {"test": "Kafka connection"}
    producer.send(TOPIC, test_msg)
    producer.flush()
    print(f"[TEST] Sent test message: {test_msg}")
except Exception as e:
    print("Kafka connection test failed!")
    traceback.print_exc()
    exit(1)

# Fetch Twitch streams
def fetch_streams():
    url = "https://api.twitch.tv/helix/streams?first=5"
    response = requests.get(url, headers=HEADERS)
    if response.ok:
        return response.json().get("data", [])
    else:
        print("Failed to fetch streams:", response.text)
        return []

# Set to True for one-time test run (not a loop)
TEST_MODE = False

def run_stream_producer():
    while True:
        try:
            streams = fetch_streams()
            for stream in streams:
                payload = {
                    "user_name": stream["user_name"],
                    "game_name": stream.get("game_name", "unknown"),
                    "viewer_count": stream["viewer_count"],
                    "started_at": stream["started_at"]
                }
                producer.send(TOPIC, payload)
                producer.flush()
                print(f"✅ Sent: {payload}")
            
            if TEST_MODE:
                print("✅ Test mode: exiting after one batch.")
                break

            time.sleep(10)
        except Exception as e:
            print("❌ Error in producer loop:")
            traceback.print_exc()
            break

# Start producing
run_stream_producer()
