# app/services/ibge/geometry.py
import httpx
import geopandas as gpd
from io import BytesIO
import logging
from shapely.geometry import Polygon

logger = logging.getLogger(__name__)

class IbgeGeometryService:
    """Responsável exclusivamente por buscar geometrias (Malhas/WFS)."""
    
    # Atibaia
    CITY_CODE = "3504107"
    MALHAS_API_URL = f"https://servicodados.ibge.gov.br/api/v3/malhas/{CITY_CODE}"

    def _get_mock_geometry(self) -> gpd.GeoDataFrame:
        """Mock realista: Centro, Alvinópolis e Jd Cerejeiras."""
        logger.warning("⚠️ GEOMETRY BYPASS: Usando Setores Estáticos ⚠️")
        
        # Coordenadas aproximadas reais
        p1 = Polygon([(-46.553, -23.112), (-46.550, -23.112), (-46.550, -23.115), (-46.553, -23.115), (-46.553, -23.112)])
        p2 = Polygon([(-46.560, -23.120), (-46.555, -23.120), (-46.555, -23.125), (-46.560, -23.125), (-46.560, -23.120)])
        p3 = Polygon([(-46.570, -23.130), (-46.565, -23.130), (-46.565, -23.135), (-46.570, -23.135), (-46.570, -23.130)])

        return gpd.GeoDataFrame([
            {"code": "3504107001", "geometry": p1},
            {"code": "3504107002", "geometry": p2},
            {"code": "3504107003", "geometry": p3},
        ], crs="EPSG:4326")

    async def fetch_tracts(self) -> gpd.GeoDataFrame:
        """Tenta baixar via API REST, fallback para Mock."""
        params = {"formato": "application/vnd.geo+json", "qualidade": "minima", "intrarregiao": "setor_censitario"}
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                logger.info(f"Downloading Geometry: {self.MALHAS_API_URL}")
                response = await client.get(self.MALHAS_API_URL, params=params)
                
                if response.status_code == 200:
                    gdf = gpd.read_file(BytesIO(response.content))
                    if not gdf.empty:
                        # Padronização e Limpeza
                        if gdf.crs != "EPSG:4326": gdf = gdf.to_crs("EPSG:4326")
                        gdf["geometry"] = gdf["geometry"].buffer(0)
                        
                        # Normaliza nome da coluna
                        for col in ["CD_SETOR", "codarea", "CD_GEOCODI"]:
                            if col in gdf.columns:
                                gdf = gdf.rename(columns={col: "code"})
                                break
                        
                        # Filtra colunas
                        return gdf[["code", "geometry"]]
                
                logger.error(f"Geometry API Failed: {response.status_code}")
                return self._get_mock_geometry()

        except Exception as e:
            logger.error(f"Geometry Error: {e}")
            return self._get_mock_geometry()