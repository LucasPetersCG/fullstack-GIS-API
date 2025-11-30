# backend/app/models/city.py
from sqlalchemy import Column, Integer, String, Float
from geoalchemy2 import Geometry
from app.core.database import Base

class City(Base):
    """
    Tabela Pesada: Guarda Geometria e Dados dos Municípios importados.
    """
    __tablename__ = "cities"

    id = Column(Integer, primary_key=True, index=True)
    
    # Código IBGE (7 dígitos) - Ex: 3504107
    code = Column(String, unique=True, index=True, nullable=False)
    
    name = Column(String, nullable=False)   # Ex: Atibaia
    uf = Column(String, nullable=False)     # Ex: SP
    
    population = Column(Integer, default=0)
    
    # Geometria do Município inteiro (Polígono/MultiPolígono)
    # Spatial Index garante busca rápida no mapa
    geom = Column(Geometry("MULTIPOLYGON", srid=4326, spatial_index=True), nullable=True)

class CityCatalog(Base):
    """
    Tabela Leve: Apenas para o Autocomplete/Busca.
    Não tem geometria, apenas ID e Nome.
    """
    __tablename__ = "city_catalog"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, index=True) # 3504107
    name = Column(String, index=True) # Atibaia
    uf = Column(String) # SP