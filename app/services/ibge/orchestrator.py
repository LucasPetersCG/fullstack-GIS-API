# app/services/ibge/orchestrator.py
import logging
import geopandas as gpd
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.ibge.geometry import IbgeGeometryService
from app.services.ibge.demographics import IbgeDemographicsService
from app.repositories.census_repository import CensusRepository # Novo import

logger = logging.getLogger(__name__)

class IbgeEtlOrchestrator:
    def __init__(self, db: AsyncSession = None):
        # Agora aceitamos uma sessão de banco opcional
        self.db = db
        self.geo_service = IbgeGeometryService()
        self.demo_service = IbgeDemographicsService()

    async def _extract_transform(self) -> gpd.GeoDataFrame:
        """Método interno que faz apenas o E e o T do ETL."""
        logger.info("🔄 Iniciando Pipeline de ETL...")
        
        gdf = await self.geo_service.fetch_tracts()
        df_stats = await self.demo_service.fetch_population()
        
        logger.info(f"Merging: {len(gdf)} setores + {len(df_stats)} registros.")
        
        gdf_final = gdf.merge(df_stats, on="code", how="left")
        gdf_final["population"] = gdf_final["population"].fillna(0)
        
        return gdf_final

    async def get_consolidated_data_json(self) -> str:
        """Retorna JSON direto (apenas para visualização rápida)."""
        gdf = await self._extract_transform()
        return gdf.to_json()

    async def sync_database(self):
        """Executa o ETL completo e salva no banco (Load)."""
        if not self.db:
            raise ValueError("Database session required for sync.")
        
        # 1. Extract & Transform
        gdf = await self._extract_transform()
        
        # 2. Load (Repository)
        repo = CensusRepository(self.db)
        await repo.save_tracts(gdf)
        
        return {"status": "success", "imported": len(gdf)}