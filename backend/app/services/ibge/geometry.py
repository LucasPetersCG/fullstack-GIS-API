# backend/app/services/ibge/geometry.py
import httpx
import geopandas as gpd
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class IbgeGeometryService:
    # API Malhas v3 (Municípios) - Fonte Oficial e Estável
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/malhas/municipios"

    async def fetch_city_geom(self, city_code: str) -> gpd.GeoDataFrame:
        """
        Baixa a geometria de UM município específico.
        Ex: city_code = 3504107 (Atibaia)
        """
        url = f"{self.BASE_URL}/{city_code}"
        params = {
            "formato": "application/vnd.geo+json",
            "qualidade": "minima" # Leve para web
        }
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            logger.info(f"🌍 Baixando malha para {city_code}...")
            response = await client.get(url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Erro IBGE Malhas: {response.status_code}")
                return gpd.GeoDataFrame()

            try:
                gdf = gpd.read_file(BytesIO(response.content))
            except Exception as e:
                logger.error(f"Erro ao ler GeoJSON: {e}")
                return gpd.GeoDataFrame()
            
            if gdf.empty:
                return gdf

            # Padronização de CRS (IBGE usa SIRGAS 2000, Web usa WGS84)
            if gdf.crs != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            
            # Correção Topológica
            gdf["geometry"] = gdf["geometry"].buffer(0)
            
            # Garante coluna de código para o merge posterior
            # A API de malhas as vezes não traz o código na properties, então forçamos.
            gdf["code"] = str(city_code)
                
            return gdf