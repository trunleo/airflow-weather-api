from .func import *
from .geolocation import *
from datetime import datetime
import pandas as pd

geolocator = geolocator()
# Get coordinates from json file
def adding_date_cols(df: pd.DataFrame) -> pd.DataFrame:
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['created_at'] = current_time
    df['updated_at'] = current_time
    return df

def process_coordinates_tbl(json_path, path_str: str = 'coordinates.csv') -> pd.DataFrame:
    with open(json_path, 'r') as file:
        data = json.load(file)
    
    coordinates = pd.DataFrame(data['coordinates'])
    # transpose the DataFrame
    coordinates = coordinates.T
    # Create a new column 'coordinate_id' based on the index
    coordinates['id'] = "COOR" + coordinates.index
    # Reorder columns to have 'coordinate_id' first
    cols = ['id'] + [col for col in coordinates.columns if col != 'id']
    coordinates = coordinates[cols]
    # remove province column
    coordinates.drop(columns=['province'], inplace=True)
    # Convert to proper data types
    coordinates['id'] = coordinates['id'].astype(str)
    coordinates['latitude'] = coordinates['latitude'].astype(float).round(6)
    coordinates['longitude'] = coordinates['longitude'].astype(float).round(6)
    coordinates = adding_date_cols(coordinates)
    save_df_to_csv(coordinates, path_str)
    return coordinates

def process_station_tbl(data: dict, province_df: pd.DataFrame, path_str: str = 'stations.csv') -> pd.DataFrame:
    if not data or 'Station' not in data:
        print("No station data available.")
        return pd.DataFrame()
    
    stations = data['Station']
    
    # Convert to DataFrame
    stations_df = pd.DataFrame(stations)
    
    # Convert latitude and longitude to numeric, coerce errors to NaN
    stations_df['Latitude'] = pd.to_numeric(stations_df['Latitude'], errors='coerce')
    stations_df['Longitude'] = pd.to_numeric(stations_df['Longitude'], errors='coerce')
    
    # Keep only relevant columns
    stations_df = stations_df[['StationID', 'WmoCode', 'Latitude', 'Longitude', 'StationNameThai', 'StationNameEnglish','Province']]
    
    # Rename columns for clarity
    stations_df.rename(columns={
        'StationID': 'id',
        'WmoCode': 'wmo_station_number',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
        'StationNameThai': 'name_th',
        'StationNameEnglish': 'name_en',
        'Province': 'province_th'
    }, inplace=True)
    
    # Create null columns for future data
    stations_df['province_id'] = stations_df['Province'].map(
        province_df.set_index('name_th')['id']
    )
    stations_df.drop(columns=['Province'], inplace=True)
    stations_df = adding_date_cols(stations_df)
    save_df_to_csv(stations_df, path_str)
    return stations_df

# Create region table
def process_region_tbl(path_str: str = 'regions.csv') -> pd.DataFrame: 
    region_df = pd.DataFrame({
    'id': ['RE1', 'RE2', 'RE3', 'RE4', 'RE5'],
    'name_en': ['Northern', 'Northeastern', 'Central', 'East', 'Southern'],
    'name_th': ['ภาคเหนือ', 'ภาคตะวันออกเฉียงเหนือ', 'ภาคกลาง', 'ภาคตะวันออก', 'ภาคใต้']
    })
    region_df = adding_date_cols(region_df)
    save_df_to_csv(region_df, path_str)
    return region_df

def process_province_tbl(location_data: DataFrame, coordinates_df: pd.DataFrame, path_str: str = 'provinces.csv') -> pd.DataFrame:
    provinces_cols = ['province_id', 'postal_code', 'province_(thai)', 'province']
    prov_info = location_data[provinces_cols]
    prov_info.rename(columns={
        'province_id': 'id',
        'postal_code': 'postal_code',
        'province': 'name_en',
        'province_(thai)': 'name_th'}, inplace=True)
    prov_info_grouped = prov_info.groupby(['id','name_th','name_en'])['postal_code'].apply(list).reset_index()
    for index, row in prov_info_grouped.iterrows():
        latitude, longitude = location_geocode(geolocator, row['name_en'])
        prov_info_grouped.at[index, 'latitude'] = latitude
        prov_info_grouped.at[index, 'longitude'] = longitude
    # Group by province id to get postal code list
    prov_info_grouped['region_id'] = prov_info_grouped['name_en'].apply(map_region_id)
    province_df = find_nearest_coordinates(prov_info_grouped, coordinates_df)
    province_df = adding_date_cols(province_df)
    save_df_to_csv(province_df, path_str)
    return province_df

def process_district_tbl(location_data: DataFrame, coordinates_df: pd.DataFrame, path_str: str = 'districts.csv') -> pd.DataFrame:
    # DISTRICT
    district_cols = ['district_id', 'postal_code', 'district_(thai)', 'district', 'province_id']
    district_info = location_data[district_cols]
    district_info.rename(columns={
        'district_id': 'district_id',
        'postal_code': 'postal_code',
        'district_(thai)': 'name_th',
        'district': 'name_en',
        'province_id': 'province_id'
    }, inplace=True)
    district_info.drop_duplicates(subset=['district_id'], inplace=True)
    for index, row in district_info.iterrows():
        latitude, longitude = location_geocode(geolocator, row['name_en'])
        district_info.at[index, 'latitude'] = latitude
        district_info.at[index, 'longitude'] = longitude
    # Manually fix coordinates with 0,0
    adding_districts = {
        'rachak-sinlapakhom': (17.266817, 103.002874),
        'Thung Khao Luangกิ่' : (16.0205189, 103.840045),
        'Nong Bunnak': (14.7358946, 102.3937099),
        'Khoa Khitchakut': (12.966577, 102.0128883) 
    }

    for district, (lat, lon) in adding_districts.items():
        district_info.loc[district_info['name_en'] == district, 'latitude'] = lat
        district_info.loc[district_info['name_en'] == district, 'longitude'] = lon

    # Group by province id to get postal code list
    district_df = find_nearest_coordinates(district_info, coordinates_df)
    district_df = adding_date_cols(district_df)

    save_df_to_csv(district_df, path_str)
    return district_df

def process_subdistrict_tbl(location_df: DataFrame, district_df: DataFrame, province_df: DataFrame, coordinates_df: DataFrame, path_str: str = 'subdistricts.csv') -> pd.DataFrame:
    subdistrict_columns = [
        'province_id',
        'district_id',
        'subdistrict_id',
        'postal_code',
        'subdistrict',
        'subdistrict_(thai)',
        'latitude',
        'longitude',
        ]
    subdistrict_df = location_df[subdistrict_columns]
    subdistrict_df.rename(columns={
        'province_id': 'province_id',
        'district_id': 'district_id',
        'subdistrict_id': 'id',
        'postal_code': 'postal_code',
        'subdistrict': 'name_en',
        'subdistrict_(thai)': 'name_th',
        'latitude': 'latitude',
        'longitude': 'longitude'}, inplace=True)
    subdistrict_df['full_address_eng'] = subdistrict_df['name_en'] + ', ' + district_df.set_index('id').loc[subdistrict_df['district_id'], 'name_en'].values + ', ' + province_df.set_index('id').loc[subdistrict_df['province_id'], 'name_en'].values + ', Thailand'
    subdistrict_df['full_address_th'] = subdistrict_df['name_th'] + ', ' + district_df.set_index('id').loc[subdistrict_df['district_id'], 'name_th'].values + ', ' + province_df.set_index('id').loc[subdistrict_df['province_id'], 'name_th'].values + ', ประเทศไทย'
    district_df = find_nearest_coordinates(subdistrict_df, coordinates_df)
    province_df = adding_date_cols(subdistrict_df)
    save_df_to_csv(province_df, path_str)

    return subdistrict_df