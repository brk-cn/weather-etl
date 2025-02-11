import pandas as pd
import requests
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(city):
    """Fetch weather data from OpenWeather API for a given city."""
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    
    print(f"Failed to fetch weather data for {city}: {response.status_code}")
    return None


def transform_weather(data):
    """Transform raw weather API response into a structured DataFrame."""
    if not data:
        return None

    transformed_data = {
        "City": data["name"],
        "Temperature (°C)": data["main"]["temp"],
        "Feels Like (°C)": data["main"]["feels_like"],
        "Humidity (%)": data["main"]["humidity"],
        "Weather": data["weather"][0]["main"],
        "Weather Description": data["weather"][0]["description"],
        "Wind Speed (m/s)": data["wind"]["speed"],
        "Timestamp": pd.to_datetime("now").strftime("%Y-%m-%d %H:%M:%S"),
    }

    return pd.DataFrame([transformed_data])


def create_db():
    """Create SQLite database and weather table if it does not exist."""
    conn = sqlite3.connect("weather_data.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            temperature REAL,
            feels_like REAL,
            humidity INTEGER,
            weather TEXT,
            weather_description TEXT,
            wind_speed REAL,
            timestamp DATETIME
        )
    """)
    
    conn.commit()
    conn.close()


def save_to_db(df):
    """Save transformed weather data to SQLite database."""
    conn = sqlite3.connect("weather_data.db")
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO weather (
                city, temperature, feels_like, humidity, weather, weather_description, wind_speed, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["City"],
            row["Temperature (°C)"],
            row["Feels Like (°C)"],
            row["Humidity (%)"],
            row["Weather"],
            row["Weather Description"],
            row["Wind Speed (m/s)"],
            row["Timestamp"],
        ))

    conn.commit()
    conn.close()
    print("Data saved to SQLite database!")