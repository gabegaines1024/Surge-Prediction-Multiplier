import pandas as pd
import requests_cache
from retry_requests import retry
import openmeteo_requests

def fetch_nyc_weather(start_date, end_date):
    # Setup API client with cache and retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7128, "longitude": -74.0060,
        "start_date": start_date, "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation"]
    }
    
    responses = openmeteo.weather_api(url, params=params)
    res = responses[0]
    
    # Process hourly data
    hourly = res.Hourly()
    temp_data = hourly.Variables(0).ValuesAsNumpy()
    precip_data = hourly.Variables(1).ValuesAsNumpy()
    
    # Create date range matching the data length
    data = {
        "Time_Bin": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            periods=len(temp_data)
        ),
        "temp": temp_data,
        "precip": precip_data
    }
    
    df_weather = pd.DataFrame(data)
    
    # Resample to 15min to match Taxi Data
    df_weather = df_weather.set_index('Time_Bin').resample('15min').ffill().reset_index()
    return df_weather