import requests
import time
from typing import Dict
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import os
import tqdm

load_dotenv()

# Global timestamp tracker
_last_request_time = [0.0]  # Mutable to keep state across calls


def _fetch_latlong(conn):
    query = """
        SELECT DISTINCT *
        FROM (
            SELECT 
                start_station_latitude AS latitude,
                start_station_longitude AS longitude
            FROM bronze.bike_trips
            UNION ALL
            SELECT 
                end_station_latitude AS latitude,
                end_station_longitude AS longitude
            FROM bronze.bike_trips
        ) AS locations;
    """
    locations = pd.read_sql_query(query, conn)

    return locations



def reverse_geocode(lat: float, lon: float) -> Dict:

    min_interval = 0.8
    now = time.time()
    elapsed = now - _last_request_time[0]
    
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)

    url = f"{os.getenv('REVERSE_GEOCODE_API_URL')}"
    querystring = {
        "lat": str(lat),
        "lon": str(lon),
        "format": "json",
        "addressdetails": "1",
        "accept-language": "en",
        "namedetails": "0"
    }
    headers = {
        "x-rapidapi-host": "forward-reverse-geocoding.p.rapidapi.com",
        "x-rapidapi-key": "0d3fb22caamshacbf495674a4023p16229fjsncc89dfa953a4"
    }

    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        response.raise_for_status()
        _last_request_time[0] = time.time()
        address = response.json().get("address",  "")
        address_id = response.json().get("place_id",  "")
        display_name = response.json().get("display_name",  "")
        output = {
            "location_id": address_id,
            "latitude": lat,
            "longitude": lon,
            "highway": address.get("highway", ""),
            "road": address.get("road",  ""),
            "neighbourhood": address.get("neighbourhood",  ""),
            "suburb": address.get("suburb",  ""),
            "city": address.get("city",  ""),
            "state": address.get("state",  ""),
            "postcode": address.get("postcode",  ""),
            "country": address.get("country",  ""),
            "display_name": display_name
        }

        return output
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "lat": lat, "lon": lon}

def commit_row(row, conn):   
    with conn:
        with conn.cursor() as cur:
            columns = list(row.keys())
            values = [row[col] for col in columns]
            placeholders = ", ".join(["%s"] * len(columns))
            col_names = ", ".join(columns)

            cur.execute(
                f"""
                    INSERT INTO bronze.locations ({col_names}) VALUES ({placeholders})
                    ON CONFLICT (latitude, longitude) DO NOTHING
                """,
                values
            )


def enrich_locations(conn):
    print("Enriching locations...")
    locations = _fetch_latlong(conn)
    print(f"Found {len(locations)} locations to enrich")
    for location in tqdm.tqdm(locations.values):
        lat, long = location
        if not(int(lat) == 0 and int(long) == 0):
            commit_row(reverse_geocode(lat, long), conn)
    