from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Index, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Coordinate(Base):
    """Coordinate table model"""
    __tablename__ = 'coordinates'
    
    id = Column(Integer, primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    location_name = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    forecasts = relationship("WeatherForecast", back_populates="coordinate")

class WeatherForecast(Base):
    """Weather forecast table model"""
    __tablename__ = 'forecasts'
    
    id = Column(Integer, primary_key=True)
    coordinate_id = Column(Integer, ForeignKey('coordinates.id'), nullable=False)
    date = Column(Date, nullable=False)
    hour = Column(Integer, nullable=False)
    tc = Column(Float)  # Temperature (°C)
    rh = Column(Float)  # Relative humidity (%)
    slp = Column(Float)  # Sea-level pressure (hPa)
    rain = Column(Float)  # Rainfall (mm)
    
    # Wind speed at different pressure levels
    ws_10m = Column(Float)
    ws_925 = Column(Float)
    ws_850 = Column(Float)
    ws_700 = Column(Float)
    ws_500 = Column(Float)
    ws_200 = Column(Float)
    
    # Wind direction at different pressure levels
    wd_10m = Column(Float)
    wd_925 = Column(Float)
    wd_850 = Column(Float)
    wd_700 = Column(Float)
    wd_500 = Column(Float)
    wd_200 = Column(Float)
    
    # Cloud cover
    cloudlow = Column(Float)  # Low cloud cover (%)
    cloudmed = Column(Float)  # Medium cloud cover (%)
    cloudhigh = Column(Float)  # High cloud cover (%)
    
    cond = Column(Integer)  # Weather condition code
    wdfn = Column(String(50))  # Wind direction full name
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    coordinate = relationship("Coordinate", back_populates="forecasts")
    
    # Indexes
    __table_args__ = (
        Index('idx_coordinate_date_hour', 'coordinate_id', 'date', 'hour'),
    )