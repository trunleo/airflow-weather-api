import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
from random import randint
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import logging

# Function to get coordinates using geopy
def geolocator() -> Nominatim:
    return Nominatim(user_agent="thailand_provinces")

def location_geocode(geolocator, district: str, attempts: int = 1, max_attempts: int = 5):
    try:
        location = geolocator.geocode(f"{district}, Thailand")
        if location:
            print(f"Geocoded {district}: ({location.latitude}, {location.longitude})")
            return location.latitude, location.longitude
        else:
            print(f"Could not geocode district: {district}")
            return 0, 0
    except GeocoderTimedOut:
        if attempts < max_attempts:
            print(f"Timeout occurred. Retrying {attempts}/{max_attempts} for district: {district}")
            return location_geocode(geolocator, district, attempts + 1, max_attempts)
        else:
            print(f"Geocoding timed out for district: {district}")
            return 0, 0
    except Exception as e:
        print(f"Error geocoding district {district}: {e}")
        return 0, 0