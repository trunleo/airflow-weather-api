from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import date, datetime

@dataclass
class WeatherForecast:
    """Single weather forecast record data model"""
    coordinate_id: int
    date: date
    hour: int
    tc: Optional[float] = None  # Temperature (°C)
    rh: Optional[float] = None  # Relative humidity (%)
    slp: Optional[float] = None  # Sea-level pressure (hPa)
    rain: Optional[float] = None  # Rainfall (mm)
    
    # Wind speed at different pressure levels
    ws_10m: Optional[float] = None
    ws_925: Optional[float] = None
    ws_850: Optional[float] = None
    ws_700: Optional[float] = None
    ws_500: Optional[float] = None
    ws_200: Optional[float] = None
    
    # Wind direction at different pressure levels
    wd_10m: Optional[float] = None
    wd_925: Optional[float] = None
    wd_850: Optional[float] = None
    wd_700: Optional[float] = None
    wd_500: Optional[float] = None
    wd_200: Optional[float] = None
    
    # Cloud cover
    cloudlow: Optional[float] = None
    cloudmed: Optional[float] = None
    cloudhigh: Optional[float] = None
    
    cond: Optional[int] = None  # Weather condition code
    wdfn: Optional[str] = None  # Wind direction full name
    
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class WeatherForecastResponse:
    """Weather API response model"""
    forecasts: List[WeatherForecast]
    total_count: int
    
    @classmethod
    def from_api_response(cls, raw_data: Dict[str, Any]) -> 'WeatherForecastResponse':
        """Create response from API raw data"""
        forecasts = []
        for item in raw_data.get('data', []):
            forecast = WeatherForecast(
                coordinate_id=item.get('coordinate_id'),
                date=datetime.strptime(item['date'], '%Y-%m-%d').date(),
                hour=item.get('hour'),
                tc=item.get('temperature'),
                rh=item.get('humidity'),
                slp=item.get('pressure'),
                rain=item.get('rainfall'),
                cloudlow=item.get('cloud_low'),
                cloudmed=item.get('cloud_medium'),
                cloudhigh=item.get('cloud_high'),
                cond=item.get('condition_code'),
                wdfn=item.get('wind_direction_name'),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            # Handle wind data
            wind_levels = ['10m', '925', '850', '700', '500', '200']
            for level in wind_levels:
                setattr(forecast, f'ws_{level}', item.get(f'wind_speed_{level}'))
                setattr(forecast, f'wd_{level}', item.get(f'wind_direction_{level}'))
            
            forecasts.append(forecast)
        
        return cls(
            forecasts=forecasts,
            total_count=len(forecasts)
        )