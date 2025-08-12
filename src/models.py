"""
Data models for the GPS tracking system.
Contains SQLAlchemy models and Pydantic models.
"""

from pydantic import BaseModel
from sqlalchemy import Column, String, DateTime, Boolean, BigInteger, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
WaybillsBase = declarative_base()

# SQLAlchemy Models
class DeviceVehicleMapping(Base):
    __tablename__ = "device_vehicle_mapping"
    __table_args__ = {'schema': 'atlas_app'}
    vehicle_no = Column(Text, index=True)
    device_id = Column(Text, index=True, primary_key=True)

class RoutePolyline(Base):
    __tablename__ = "route_polylines"
    __table_args__ = {'schema': 'atlas_app'}
    
    route_id = Column(BigInteger, primary_key=True)
    polyline = Column(Text)
    merchant_operating_city_id = Column(Text, primary_key=True)

# Waybills database models
class Waybill(Base):
    __tablename__ = "waybills"
    waybill_id = Column(BigInteger, primary_key=True)
    schedule_id = Column(BigInteger)
    schedule_trip_id = Column(BigInteger)
    deleted = Column(Boolean, nullable=False, default=False)
    schedule_no = Column(Text)
    schedule_trip_name = Column(Text)
    schedule_type = Column(Text)
    service_type = Column(Text)
    updated_at = Column(DateTime)
    status = Column(Text)
    vehicle_no = Column(Text)

class BusSchedule(Base):
    __tablename__ = "bus_schedule"
    
    schedule_id = Column(BigInteger, primary_key=True)
    deleted = Column(Boolean, nullable=False, default=False)
    route_code = Column(Text)
    status = Column(Text)
    route_id = Column(BigInteger, nullable=False)

class BusScheduleTripDetail(Base):
    __tablename__ = "bus_schedule_trip_detail"
    
    schedule_trip_detail_id = Column(BigInteger, primary_key=True)
    schedule_trip_id = Column(BigInteger)
    deleted = Column(Boolean, nullable=False, default=False)
    route_number_id = Column(BigInteger, nullable=False)

# Pydantic Models
class FleetInfo(BaseModel):
    """Pydantic model for fleet information returned by get_fleet_info function"""
    vehicle_no: str
    device_id: str
    route_id: str