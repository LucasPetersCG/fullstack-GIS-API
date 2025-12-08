# backend/app/services/ibge/orchestrator.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.ibge.geometry import IbgeGeometryService
from app.services.ibge.demographics import IbgeDemographicsService
from app.repositories.city_repository import CityRepository
from app.services.ibge.economics import IbgeEconomicsService 
from app.services.ibge.topology import IbgeTopologyService

logger = logging.getLogger(__name__)

class IbgeEtlOrchestrator:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.geo_service = IbgeGeometryService()
        self.demo_service = IbgeDemographicsService()
        self.eco_service = IbgeEconomicsService() 
        self.topo_service = IbgeTopologyService() 
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
        logger.info(f"🚀 Iniciando ETL Profundo para {city_code}...")
        
        # 1. Dados Básicos (Geo + Pop)
        gdf = await self.geo_service.fetch_city_geom(city_code)
        population = await self.demo_service.fetch_city_population(city_code)
        
        if gdf.empty:
            raise ValueError(f"Cidade {city_code} não encontrada na malha.")

        # 2. Dados Econômicos (PIB + Empresas)
        logger.info("💰 Buscando indicadores econômicos...")
        pib_total = await self.eco_service.fetch_pib(city_code)
        company_stats = await self.eco_service.fetch_companies_stats(city_code)
        
        # Cálculo de Derivados
        pib_per_capita = (pib_total * 1000) / population if population > 0 else 0
        
        # 3. Topologia (Distritos)
        logger.info("🏘️ Mapeando distritos...")
        districts_list = await self.topo_service.fetch_districts(city_code)
        
        # 4. Preparar DTO para Persistência
        city_data = {
            "population": population,
            "pib_total": pib_total,
            "pib_per_capita": pib_per_capita,
            "total_companies": company_stats["total_companies"],
            "total_workers": company_stats["total_workers"]
        }
        
        # Salva tudo
        await self.repo.save_full_city_data(gdf, city_data, districts_list)
        
        return {
            "status": "success", 
            "city": city_code, 
            "indicators": city_data,
            "districts_count": len(districts_list)
        }