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
        """Importa dados de uma cidade específica."""
        logger.info(f"🚀 Iniciando ETL para {city_code}...")
        
        # 1. Dados Básicos e Nome Oficial
        gdf = await self.geo_service.fetch_city_geom(city_code)
        
        # Busca detalhes para garantir nome oficial correto (evitar "undefined")
        details = await self.demo_service.fetch_city_details(city_code)
        city_name = details.get("name", "Desconhecido")
        
        population = await self.demo_service.fetch_city_population(city_code)
        
        if gdf.empty:
            raise ValueError(f"Cidade {city_code} não encontrada na malha.")

        # Injeta metadados no GeoDataFrame para o Repository usar
        gdf["NM_MUN"] = city_name
        gdf["SIGLA_UF"] = details.get("uf", "BR")

        # 2. Dados Econômicos (PIB + Empresas)
        # O fetch_pib retorna (valor, ano)
        pib_total, pib_year = await self.eco_service.fetch_pib(city_code)
        
        # O fetch_companies retorna dict com chaves 'total_companies', 'year', etc
        company_stats = await self.eco_service.fetch_companies_stats(city_code)
        
        # Cálculo de Derivados
        pib_per_capita = 0.0
        if population > 0:
            pib_per_capita = (pib_total * 1000) / population
        
        # 3. Topologia (Distritos)
        districts_list = await self.topo_service.fetch_districts(city_code)
        
        # 4. Montar DTO para Persistência
        city_data = {
            "population": population,
            "pib_total": pib_total,
            "pib_per_capita": pib_per_capita,
            "pib_year": pib_year,
            "total_companies": company_stats.get("total_companies", 0),
            "total_workers": company_stats.get("total_workers", 0),
            "companies_year": company_stats.get("year", 0)
        }
        
        await self.repo.save_full_city_data(gdf, city_data, districts_list)
        
        return {"status": "success", "city": city_name, "data": city_data}