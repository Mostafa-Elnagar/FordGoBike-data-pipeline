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

def _fetch_enriched_locations(conn):
    query = """
        SELECT latitude, longitude
        FROM bronze.locations
    """
    locations = pd.read_sql_query(query, conn)
    return locations


def reverse_geocode(lat: float, lon: float, api_key) -> Dict:

    min_interval = 0.05
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
        "x-rapidapi-host": f"{os.getenv('GEOCODE_API_HOST')}",
        "x-rapidapi-key": api_key
    }

    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        response.raise_for_status()
        _last_request_time[0] = time.time()
        address = response.json().get("address", None)
        address_id = response.json().get("place_id",  None)
        display_name = response.json().get("display_name",  None)
        output = {
            "location_id": address_id,
            "latitude": lat,
            "longitude": lon,
            "highway": address.get("highway", None),
            "road": address.get("road",  None),
            "neighbourhood": address.get("neighbourhood",  None),
            "suburb": address.get("suburb",  None),
            "city": address.get("city",  None),
            "state": address.get("state",  None),
            "postcode": address.get("postcode",  None),
            "country": address.get("country",  None),
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
    enriched_locations = _fetch_enriched_locations(conn)
    insert_count = 0
    api_keys = [os.getenv(f'GEOCODE_API_KEY{i}') for i in range(1, int(os.getenv('GEOCODE_KEY_COUNT')) + 1)]
    print(f"Found {len(locations)} locations to enrich")
    for location in tqdm.tqdm(locations.values):
        lat, long = location
        if not(int(lat) == 0 and int(long) == 0):
            if not any((enriched_locations['latitude'] == lat) & (enriched_locations['longitude'] == long)):
                commit_row(reverse_geocode(lat, long, api_keys[(insert_count) % len(api_keys)]), conn)
                insert_count += 1
    print(f"Enriched {insert_count} locations")
    