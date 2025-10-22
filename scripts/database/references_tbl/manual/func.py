import requests
import numpy as np
import pandas as pd
import json
import psycopg2
from geopy.geocoders import Nominatim
from pandas import DataFrame

# Save DataFrame to CSV
def save_df_to_csv(df: pd.DataFrame, file_path: str):
    try:
        df.to_csv(file_path, index=False)
        print(f"DataFrame saved to {file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
# Get station data from API
def get_station_data_from_api() -> dict:
    url = "http://data.tmd.go.th/api/Station/V1/"
    params_str = {"uid": "demo", "ukey": "demokey", "format": "json"}
    
    try:
        response = requests.request(
            method="GET",
            data="",
            url=url,
            params=params_str,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return {}



# Define province-to-region mapping
region_map = {
    "Northern": {
        "Provinces": {
            "Chiang Mai", "Chiang Rai", "Lamphun", "Lampang", "Phayao",
            "Nan", "Phrae", "Mae Hong Son", "Uttaradit", "Tak",
            "Sukhothai", "Phitsanulok", "Phichit", "Kamphaeng Phet"
        },
        "id": "RE1"
    },
    "Northeastern": {
        "Provinces": {
            "Nakhon Ratchasima", "Buri Ram", "Surin", "Si Sa Ket", "Ubon Ratchathani",
            "Yasothon", "Amnat Charoen", "Roi Et", "Maha Sarakham", "Kalasin",
            "Khon Kaen", "Chaiyaphum", "Nakhon Phanom", "Sakon Nakhon",
            "Mukdahan", "Loei", "Nong Bua Lam Phu", "Udon Thani", "Nong Khai", "Bueng Kan"
        },
        "id": "RE2"
    },
    "Central": {
        "Provinces": {
            "Bangkok", "Nakhon Pathom", "Nonthaburi", "Pathum Thani",
            "Phra Nakhon Si Ayutthaya", "Lop Buri", "Sing Buri", "Ang Thong",
            "Saraburi", "Suphan Buri", "Chainat", "Nakhon Sawan", "Uthai Thani"
        },
        "id": "RE3"
    },
    "Eastern": {
        "Provinces": {
            "Chon Buri", "Rayong", "Chanthaburi", "Trat", "Prachin Buri", "Sa Kaeo", "Chachoengsao"
        },
        "id": "RE4"
    },
    "Southern": {
        "Provinces": {
            "Chumphon", "Ranong", "Surat Thani", "Phangnga", "Phuket",
            "Krabi", "Nakhon Si Thammarat", "Phatthalung", "Songkhla",
            "Satun", "Trang", "Narathiwat", "Pattani", "Yala"
        },
        "id": "RE5"
    }
}
# Create location table
def load_location_data(excel_path: str) -> pd.DataFrame:
    try:
        location_data = pd.read_excel(excel_path)
        return location_data
    except Exception as e:
        print(f"Error loading location data: {e}")
        return pd.DataFrame()

def handle_location_data(excel_path: str = './data/location_data.xlsx') -> DataFrame:

    df = pd.read_excel(excel_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df_province = df[['province', 'province_(thai)']].drop_duplicates().reset_index(drop=True)
    df_province['id'] = ['PROV' + str(i) for i in range(1, len(df_province) + 1)]
    df['province_id'] = df['province'].map(df_province.set_index('province')['id'])
    df_district = df[['district', 'postal_code', 'district_(thai)']].drop_duplicates().reset_index(drop=True)
    df_district['id'] = ['DIST' + str(i) for i in range(1, len(df_district) + 1)]
    df['district_id'] = df[['district','postal_code']].apply(lambda x: df_district.set_index(['district', 'postal_code']).loc[tuple(x)]['id'], axis=1)
    df['subdistrict_id'] = ['SUBDIST' + str(i) for i in range(1, len(df) + 1)]
    
    return df
    
# Function to map province to region_id
def map_region_id(province_name: str) -> str:
    for provinces in region_map.values():
        if province_name in provinces["Provinces"]:
            return provinces["id"]
    return ""

# Find the nearest coordinate for each province
def find_nearest_coordinates(location_df: pd.DataFrame, coordinates_df: pd.DataFrame) -> pd.DataFrame:
    location_df = location_df.copy()
    location_df['coordinate_id'] = np.nan

    for idx, location in location_df.iterrows():
        if pd.notna(location['latitude']) and pd.notna(location['longitude']):
            # Calculate distances to all coordinates
            lat_diff = coordinates_df['latitude'] - location['latitude']
            lon_diff = coordinates_df['longitude'] - location['longitude']
            distances = lat_diff**2 + lon_diff**2
            # Find the index of the nearest coordinate
            nearest_idx = distances.idxmin()
            # Assign the coordinate ID to the location
            location_df.at[idx, 'coordinate_id'] = coordinates_df.at[nearest_idx, 'id']

    return location_df
