import requests, json, os, time
time.sleep(10)  # Wait for Kafka to be ready
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY", "Paris")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")

while True:
    try:
        resp = requests.get(
            f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        )
        if resp.status_code == 200:
            data = resp.json()
            producer.send(TOPIC, data)
            print(f"Sent weather data for {CITY}")
        else:
            print(f"Error {resp.status_code} fetching weather data")
    except Exception as e:
        print("Error:", e)
    time.sleep(600)  # every 10 min
