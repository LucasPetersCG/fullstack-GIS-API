# backend/app/models/city.py
from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from app.core.database import Base

class City(Base):
    __tablename__ = "cities"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    uf = Column(String, nullable=False)
    
    # Dados Demográficos
    population = Column(Integer, default=0)
    
    # --- NOVOS CAMPOS ECONÔMICOS ---
    pib_total = Column(Float, nullable=True)       # PIB (Mil Reais)
    pib_per_capita = Column(Float, nullable=True)  # PIB per Capita (Reais)
    total_companies = Column(Integer, nullable=True) # CEMPRE: Unidades locais
    total_workers = Column(Integer, nullable=True)   # CEMPRE: Pessoal ocupado
    
    geom = Column(Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=True)

    # Relacionamento: Uma cidade tem vários distritos
    districts = relationship("District", back_populates="city", cascade="all, delete-orphan")

class District(Base):
    __tablename__ = "districts"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city = relationship("City", back_populates="districts")
    
    # Por enquanto sem geometria, pois a API V3 não fornece
    # Futuro: population, geom

class CityCatalog(Base):
    __tablename__ = "city_catalog"
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True)
    name = Column(String, index=True)
    uf = Column(String)