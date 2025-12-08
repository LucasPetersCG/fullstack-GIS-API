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
        
        # CORREÇÃO: Definição explícita das variáveis
        city_code = str(row["code"])
        
        # Lógica de Nome: Prioriza o nome vindo do Orchestrator (data['city']) se existir, 
        # senão pega do shapefile, senão desconhecido.
        # Mas note que no orchestrator passamos data={population...}. O nome não está dentro de data.
        # O nome está no row['NM_MUN'] ou podemos ter passado no data?
        # Vamos confiar no row.get("NM_MUN") ou row.get("name") que o geometry.py padronizou.
        city_name = row.get("NM_MUN") or row.get("name") or "Desconhecido"
        
        logger.info(f"💾 Persistindo {city_name} ({city_code})...")

        # Casting de Geometria
        from shapely.geometry import Polygon, MultiPolygon
        geom = row["geometry"]
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom])
        wkt = geom.wkt 

        # Upsert
        stmt = insert(City).values(
            code=city_code,
            name=city_name,
            uf=row.get("SIGLA_UF", "BR"),
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
                "name": city_name,
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
        logger.info(f"✅ Dados salvos com sucesso.")
        
    async def update_catalog(self, cities_list: list):
        """
        Atualiza o catálogo completo de cidades (Autocomplete).
        Limpa a tabela e insere tudo de novo (Full Refresh).
        """
        if not cities_list:
            return

        logger.info(f"📚 Atualizando catálogo com {len(cities_list)} cidades...")
        
        # Limpa tabela atual
        await self.db.execute(delete(CityCatalog))
        
        # Bulk Insert
        await self.db.execute(
            insert(CityCatalog),
            cities_list
        )
        await self.db.commit()

    async def get_all_features(self):
        """Retorna GeoJSON com TODOS os dados para o mapa."""
        stmt = select(
            City.code, City.name, City.population, 
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
                    "name": row.name, # Isso resolve o "undefined" no tooltip
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
        """Busca simples no catálogo para o frontend."""
        stmt = select(CityCatalog.code, CityCatalog.name, CityCatalog.uf)
        if search:
            # Busca case-insensitive
            stmt = stmt.where(CityCatalog.name.ilike(f"%{search}%"))
        
        stmt = stmt.limit(10) # Retorna só 10 para não travar
        result = await self.db.execute(stmt)
        return result.all()