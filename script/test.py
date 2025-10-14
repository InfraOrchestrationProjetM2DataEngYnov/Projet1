# import
import os 
import json
import time 
import requests
from kafka import KafkaProducer

# configuation
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY", "Fonte de Angeão")
KAFKA = os.getenv("KAFKA", "kafka:8080")
TOPIC = os.getenv("KAFKA_TOPIC", "NOM_TOPICMESCOUILLES")

# Recup le temps
def get_weather(city):
    """Récupère la météo pour une ville via OpenWeatherMap"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()