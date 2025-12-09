from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, func
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
        if gdf.empty: return

        row = gdf.iloc[0]
        city_code = str(row["code"])
        
        # CORREÇÃO: Pega o nome do dicionário de dados (Fonte de Verdade)
        city_name = data.get("name") 
        if not city_name:
            city_name = "Desconhecido" # Fallback final
        
        logger.info(f"💾 Persistindo {city_name}...")

        # Geometria
        from shapely.geometry import Polygon, MultiPolygon
        geom = row["geometry"]
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom])
        wkt = geom.wkt 

        # Upsert Limpo
        stmt = insert(City).values(
            code=city_code,
            name=city_name, # Agora vem certo
            uf=data.get("uf", "BR"),
            geom=wkt,
            population=data["population"],
            pib_total=data["pib_total"],
            pib_per_capita=data["pib_per_capita"],
            pib_year=data["pib_year"],
            total_companies=data["total_companies"],
            total_workers=data["total_workers"],
            companies_year=data["companies_year"]
        ).on_conflict_do_update(
            index_elements=['code'],
            set_={
                "name": city_name, # Atualiza o nome se mudar
                "geom": wkt,
                "population": data["population"],
                "pib_total": data["pib_total"],
                "pib_per_capita": data["pib_per_capita"],
                "pib_year": data["pib_year"],
                "total_companies": data["total_companies"],
                "total_workers": data["total_workers"],
                "companies_year": data["companies_year"]
            }
        )
        
        result = await self.db.execute(stmt.returning(City.id))
        city_id = result.scalar()
        
        # Distritos
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

    async def get_all_features(self):
        """Retorna GeoJSON completo para o mapa."""
        stmt = select(
            City.code, City.name, City.uf,
            City.population, City.pib_total, City.pib_per_capita, City.pib_year,
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
                    "name": row.name, # AQUI ESTÁ A CHAVE DO SUCESSO
                    "uf": row.uf,
                    "population": row.population,
                    "pib_per_capita": row.pib_per_capita,
                    "pib_year": row.pib_year,
                    "total_companies": row.total_companies,
                    "total_workers": row.total_workers,
                    "companies_year": row.companies_year
                }
            })
        return features

    async def update_catalog(self, cities_list: list):
        if not cities_list: return
        await self.db.execute(delete(CityCatalog))
        await self.db.execute(insert(CityCatalog), cities_list)
        await self.db.commit()

    async def list_catalog(self, search: str = None):
        stmt = select(CityCatalog.code, CityCatalog.name, CityCatalog.uf)
        if search:
            stmt = stmt.where(CityCatalog.name.ilike(f"%{search}%"))
        stmt = stmt.limit(10)
        result = await self.db.execute(stmt)
        return result.all()