import os
import json
from kafka import KafkaConsumer
import time

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")
GROUP_ID = os.getenv("GROUP_ID", "weather-consumer-group")

def main():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",  # lit les messages existants
                enable_auto_commit=True
            )

            print(f"✅ Consumer started, listening on topic '{TOPIC}'")

            for message in consumer:
                data = message.value
                city = data.get("name")
                temp = data.get("main", {}).get("temp")
                weather = data.get("weather", [{}])[0].get("description")
                print(f"🌤️ {city}: {temp}°C, {weather}")

        except Exception as e:
            print("⚠️ Consumer error:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
