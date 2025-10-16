import requests, json, os, time
time.sleep(30)  # Wait for Kafka to be ready
from kafka import KafkaProducer
import requests, json, os, time
from datetime import datetime, timezone, timedelta

producer = KafkaProducer(
    bootstrap_servers=[os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY", "Paris")
TOPIC = os.getenv("TOPIC_NAME", "weather-api")

BASE_URL = "http://api.openweathermap.org/data/2.5"
GEO_URL = "http://api.openweathermap.org/geo/1.0"
ONECALL3_URL = "https://api.openweathermap.org/data/3.0"
HISTORY_URL = "http://history.openweathermap.org/data/2.5"


def get_coordinates(city):
    """
    Récupère la latitude et la longitude d'une ville.
    Exemple :
        get_coordinates("Paris") 
        get_coordinates("Paris,FR") 

    """
    url = f"{GEO_URL}/direct?q={city}&limit=1&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    if not data:
        raise ValueError(f"Ville '{city}' introuvable. Vérifie l'orthographe ou ajoute le code pays (ex: Paris,FR).")

    name = data[0].get("name", "")
    country = data[0].get("country", "")

    # Si le résultat ne contient pas de pays ou si c’est un nom générique (comme “France”), on le rejette
    if not country or len(country) != 2:
        raise ValueError(f"'{city}' ne semble pas être une ville valide. Entrez une ville précise (ex: Paris,FR).")

    return data[0]['lat'], data[0]['lon'], name



# Recup le temps
def get_weather(city):
    """Récupère la météo pour une ville via OpenWeatherMap"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_forecast(city, days=5):
    """ Récupère les prévisions météo (jusqu’à 5 jours)"""
    lat, lon, name = get_coordinates(city)
    url = f"{BASE_URL}/forecast?lat={lat}&lon={lon}&appid={API_KEY}&units=metric&lang=fr"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_air_pollution(city):
    """Récupère la qualité de l’air pour une ville"""
    lat, lon, name = get_coordinates(city)
    url = f"{BASE_URL}/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_precipitations(city):
    """Récupère les précipitations récentes (1h/3h) pluie et neige pour une ville.

    Retourne un dict avec les clés: rain_1h, rain_3h, snow_1h, snow_3h (valeurs en mm).
    """
    data = get_weather(city)
    rain = data.get("rain", {}) or {}
    snow = data.get("snow", {}) or {}

    return {
        "city": data.get("name"),
        "rain_1h": float(rain.get("1h", 0.0)),
        "rain_3h": float(rain.get("3h", 0.0)),
        "snow_1h": float(snow.get("1h", 0.0)),
        "snow_3h": float(snow.get("3h", 0.0)),
        "units": "mm",
        "timestamp_utc": datetime.fromtimestamp(data.get("dt", 0), tz=timezone.utc).isoformat() if data.get("dt") else None,
    }


def get_sun_info(city):
    """Récupère les informations Soleil (lever/coucher) pour une ville.

    Retourne un dict avec sunrise/sunset en UTC et heure locale (selon offset fourni par l'API).
    """
    data = get_weather(city)
    tz_offset = int(data.get("timezone", 0) or 0)
    sys_info = data.get("sys", {}) or {}

    sunrise_ts = sys_info.get("sunrise")
    sunset_ts = sys_info.get("sunset")

    sunrise_utc = datetime.fromtimestamp(sunrise_ts, tz=timezone.utc) if sunrise_ts else None
    sunset_utc = datetime.fromtimestamp(sunset_ts, tz=timezone.utc) if sunset_ts else None

    sunrise_local = sunrise_utc + timedelta(seconds=tz_offset) if sunrise_utc else None
    sunset_local = sunset_utc + timedelta(seconds=tz_offset) if sunset_utc else None

    day_length_seconds = int((sunset_utc - sunrise_utc).total_seconds()) if (sunrise_utc and sunset_utc) else None

    return {
        "city": data.get("name"),
        "timezone_offset_seconds": tz_offset,
        "sunrise_utc": sunrise_utc.isoformat() if sunrise_utc else None,
        "sunset_utc": sunset_utc.isoformat() if sunset_utc else None,
        "sunrise_local": sunrise_local.isoformat() if sunrise_local else None,
        "sunset_local": sunset_local.isoformat() if sunset_local else None,
        "day_length_seconds": day_length_seconds,
    }

while True:
    try:
        sun_info = get_sun_info(CITY)
        precipations_info = get_precipitations(CITY)
        air_pollution = get_air_pollution(CITY)
        forecast = get_forecast(CITY)
        weather = get_weather(CITY)
        data = {
            'sun_info':sun_info,
            'precipitations_info':precipations_info,
            'air_pollution':air_pollution,
            'forecast':forecast,
            'weather':weather
        }
        producer.send(TOPIC,data)
        producer.flush()
        print(f"Sent data {data} for {CITY}")
        # # resp = requests.get(
        # #     f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        # # )
        # if resp.status_code == 200:
        #     data = resp.json()
        #     producer.send(TOPIC, data)
        #     producer.flush()
        #     print(f"Sent weather data for {CITY}")
        # else:
        #     print(f"Error {resp.status_code} fetching weather data")
    except Exception as e:
        print("Error:", e)
    time.sleep(600)  # every 10 min