# backend/app/schemas/geo.py
from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class FeatureProperties(BaseModel):
    """
    Define QUAIS dados serão enviados para o Frontend.
    Se o campo não estiver aqui, o FastAPI remove ele do JSON.
    """
    code: str
    name: str # <--- OBRIGATÓRIO PARA O TOOLTIP
    uf: Optional[str] = None
    
    population: Optional[int] = 0
    
    # Dados Econômicos
    pib_total: Optional[float] = 0.0
    pib_per_capita: Optional[float] = 0.0
    pib_year: Optional[int] = None
    
    total_companies: Optional[int] = 0
    total_workers: Optional[int] = 0
    companies_year: Optional[int] = None

class Feature(BaseModel):
    type: str = "Feature"
    geometry: Dict[str, Any]
    properties: FeatureProperties # <--- Link com a classe acima

class FeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[Feature]