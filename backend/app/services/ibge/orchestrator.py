# backend/app/services/ibge/orchestrator.py
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.ibge.geometry import IbgeGeometryService
from app.services.ibge.demographics import IbgeDemographicsService
from app.services.ibge.economics import IbgeEconomicsService
from app.services.ibge.topology import IbgeTopologyService
from app.repositories.city_repository import CityRepository

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
        """Sincroniza lista de cidades."""
        logger.info("🔄 Iniciando sincronização do Catálogo...")
        cities_list = await self.demo_service.fetch_all_cities_catalog()
        
        if not cities_list:
            logger.warning("⚠️ Catálogo vazio ou erro de conexão. Abortando.")
            return {"status": "warning", "message": "Falha na conexão com IBGE."}
            
        await self.repo.update_catalog(cities_list)
        return {"status": "success", "total": len(cities_list)}

    async def import_city(self, city_code: str):
        logger.info(f"🚀 Iniciando ETL para {city_code}...")
        
        # 1. Busca Dados
        gdf = await self.geo_service.fetch_city_geom(city_code)
        
        # Busca Nome Oficial
        details = await self.demo_service.fetch_city_details(city_code)
        city_name = details.get("name", "Nome não encontrado") # Nome Seguro
        city_uf = details.get("uf", "BR")
        
        population = await self.demo_service.fetch_city_population(city_code)
        
        if gdf.empty:
            raise ValueError(f"Geometria não encontrada.")

        # 2. Economia
        pib_total, pib_year = await self.eco_service.fetch_pib(city_code)
        company_stats = await self.eco_service.fetch_companies_stats(city_code)
        
        # Derivados
        pib_per_capita = (pib_total * 1000) / population if population > 0 else 0
        
        # 3. Topologia
        districts_list = await self.topo_service.fetch_districts(city_code)
        
        # 4. DTO explícito (Nome vai AQUI, não no GDF)
        city_data = {
            "name": city_name,  # <--- CORREÇÃO PRINCIPAL
            "uf": city_uf,
            "population": population,
            "pib_total": pib_total,
            "pib_per_capita": pib_per_capita,
            "pib_year": pib_year,
            "total_companies": company_stats["total_companies"],
            "total_workers": company_stats["total_workers"],
            "companies_year": company_stats["year"]
        }
        
        await self.repo.save_full_city_data(gdf, city_data, districts_list)
        
        return {"status": "success", "city": city_name, "data": city_data}