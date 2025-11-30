# app/models/census.py
from sqlalchemy import Column, Integer, String, Float
from geoalchemy2 import Geometry
from app.core.database import Base

class CensusTract(Base):
    __tablename__ = "census_tracts"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True, nullable=False)
    population = Column(Integer, default=0)
    
    # Geometria (SRID 4326 = GPS/WGS84)
    geom = Column(Geometry("POLYGON", srid=4326, spatial_index=True), nullable=False)