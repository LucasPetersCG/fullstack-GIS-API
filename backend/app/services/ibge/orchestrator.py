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
        
        # 1. Dados Básicos
        gdf = await self.geo_service.fetch_city_geom(city_code)
        details = await self.demo_service.fetch_city_details(city_code)
        city_name = details["name"]
        
        population = await self.demo_service.fetch_city_population(city_code)
        
        if gdf.empty:
            raise ValueError(f"Cidade {city_code} não encontrada.")

        # Injeta metadados no GDF
        gdf["NM_MUN"] = city_name
        gdf["SIGLA_UF"] = details["uf"]

        # 2. Dados Econômicos (Desempacota a tupla valor, ano)
        logger.info("💰 Buscando indicadores econômicos...")
        pib_total, pib_year = await self.eco_service.fetch_pib(city_code)
        company_stats = await self.eco_service.fetch_companies_stats(city_code)
        
        # Log para debug
        logger.info(f"Dados encontrados: PIB ({pib_year}): {pib_total}, Empresas ({company_stats['year']}): {company_stats['total_companies']}")

        # Cálculo de Derivados
        pib_per_capita = (pib_total * 1000) / population if population > 0 else 0
        
        # 3. Topologia
        districts_list = await self.topo_service.fetch_districts(city_code)
        
        # 4. Persistência
        city_data = {
            "population": population,
            "pib_total": pib_total,
            "pib_per_capita": pib_per_capita,
            "pib_year": int(pib_year) if pib_year else None,
            "total_companies": company_stats["total_companies"],
            "total_workers": company_stats["total_workers"],
            "companies_year": company_stats["year"]
        }
        
        await self.repo.save_full_city_data(gdf, city_data, districts_list)
        
        # Retorna metadados extras para o Frontend (Via resposta do Import)
        return {"status": "success", "city": gdf["NM_MUN"].iloc[0], "data": city_data}