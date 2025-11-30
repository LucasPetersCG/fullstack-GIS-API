# backend/app/services/ibge/orchestrator.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.ibge.geometry import IbgeGeometryService
from app.services.ibge.demographics import IbgeDemographicsService
from app.repositories.city_repository import CityRepository

logger = logging.getLogger(__name__)

class IbgeEtlOrchestrator:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.geo_service = IbgeGeometryService()
        self.demo_service = IbgeDemographicsService()
        self.repo = CityRepository(db)

    async def sync_catalog(self):
        """
        Baixa a lista de TODOS os municípios do Brasil e salva no banco.
        Operação pesada (5.570 registros), deve ser feita esporadicamente.
        """
        logger.info("🔄 Iniciando sincronização do Catálogo de Cidades...")
        cities_list = await self.demo_service.fetch_all_cities_catalog()
        
        if not cities_list:
            raise ValueError("Falha ao baixar catálogo do IBGE.")
            
        await self.repo.update_catalog(cities_list)
        return {"status": "success", "total": len(cities_list)}

    async def import_city(self, city_code: str):
        """
        ETL On-Demand: Baixa Geometria e População de UMA cidade e salva.
        """
        logger.info(f"🔄 Importando cidade {city_code}...")
        
        # 1. Busca Paralela (Idealmente), aqui faremos sequencial por simplicidade
        gdf = await self.geo_service.fetch_city_geom(city_code)
        population = await self.demo_service.fetch_city_population(city_code)
        
        if gdf.empty:
            raise ValueError(f"Geometria não encontrada para cidade {city_code}")

        # 2. Persistência
        await self.repo.save_city(gdf, population)
        
        return {
            "status": "success", 
            "city": gdf["code"].iloc[0], 
            "population": population
        }