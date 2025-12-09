# backend/app/repositories/city_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.postgresql import insert
from app.models.city import City, CityCatalog, District
import geopandas as gpd
import json
import logging

logger = logging.getLogger(__name__)

class CityRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def save_full_city_data(self, gdf: gpd.GeoDataFrame, data: dict, districts: list):
        """Salva Cidade Completa + Lista de Distritos."""
        if gdf.empty: return

        row = gdf.iloc[0]
        city_code = str(row["code"])
        
        # Prioriza o nome vindo da API de Localidades (injetado no GDF pelo Orchestrator)
        city_name = row.get("NM_MUN") or "Desconhecido"
        
        logger.info(f"💾 Persistindo {city_name} (Pop: {data['population']}, PIB Ano: {data['pib_year']})...")

        # Casting de Geometria (Garante MultiPolygon)
        from shapely.geometry import Polygon, MultiPolygon
        geom = row["geometry"]
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom])
        wkt = geom.wkt 

        # Dicionário explícito para evitar erros de "Unconsumed column"
        values_data = {
            "code": city_code,
            "name": city_name,
            "uf": row.get("SIGLA_UF", "BR"),
            "geom": wkt,
            "population": data["population"],
            "pib_total": data["pib_total"],
            "pib_per_capita": data["pib_per_capita"],
            "pib_year": data["pib_year"],
            "total_companies": data["total_companies"],
            "total_workers": data["total_workers"],
            "companies_year": data["companies_year"]
        }

        # Upsert
        stmt = insert(City).values(**values_data).on_conflict_do_update(
            index_elements=['code'],
            set_={
                "name": values_data["name"],
                "geom": values_data["geom"],
                "population": values_data["population"],
                "pib_total": values_data["pib_total"],
                "pib_per_capita": values_data["pib_per_capita"],
                "pib_year": values_data["pib_year"],
                "total_companies": values_data["total_companies"],
                "total_workers": values_data["total_workers"],
                "companies_year": values_data["companies_year"]
            }
        )
        
        result = await self.db.execute(stmt.returning(City.id))
        city_id = result.scalar()
        
        # Atualizar Distritos
        if districts and city_id:
            await self.db.execute(delete(District).where(District.city_id == city_id))
            districts_to_insert = []
            for d in districts:
                districts_to_insert.append({
                    "code": str(d["id"]),
                    "name": d["nome"],
                    "city_id": city_id
                })
            if districts_to_insert:
                await self.db.execute(insert(District), districts_to_insert)
        
        await self.db.commit()

    async def update_catalog(self, cities_list: list):
        if not cities_list: return
        await self.db.execute(delete(CityCatalog))
        await self.db.execute(insert(CityCatalog), cities_list)
        await self.db.commit()

    async def get_all_features(self):
        """
        Retorna GeoJSON com TODOS os dados para o mapa.
        Seleciona explicitamente colunas de ano e totais.
        """
        stmt = select(
            City.code, City.name, City.uf,
            City.population, 
            City.pib_per_capita, City.pib_year,
            City.total_companies, City.total_workers, City.companies_year,
            func.ST_AsGeoJSON(City.geom).label("geojson")
        )
        result = await self.db.execute(stmt)
        
        features = []
        for row in result.all():
            features.append({
                "type": "Feature",
                "geometry": json.loads(row.geojson),
                "properties": {
                    "code": row.code,
                    "name": row.name, # Resolve o "Undefined"
                    "uf": row.uf,
                    "population": row.population,
                    "pib_per_capita": row.pib_per_capita or 0,
                    "pib_year": row.pib_year,
                    "total_companies": row.total_companies or 0,
                    "total_workers": row.total_workers or 0,
                    "companies_year": row.companies_year
                }
            })
        return features
        
    async def list_catalog(self, search: str = None):
        stmt = select(CityCatalog.code, CityCatalog.name, CityCatalog.uf)
        if search:
            stmt = stmt.where(CityCatalog.name.ilike(f"%{search}%"))
        stmt = stmt.limit(10)
        result = await self.db.execute(stmt)
        return result.all()