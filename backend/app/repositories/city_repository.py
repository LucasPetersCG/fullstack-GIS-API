# backend/app/repositories/city_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete
from sqlalchemy.dialects.postgresql import insert
from app.models.city import City, CityCatalog
import geopandas as gpd
import json
import logging

logger = logging.getLogger(__name__)

class CityRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def save_city(self, gdf: gpd.GeoDataFrame, population: int):
        """
        Salva ou Atualiza um Município na tabela 'cities'.
        """
        if gdf.empty:
            return

        # Pega a primeira linha (assumindo que o GeoDataFrame é de uma única cidade)
        row = gdf.iloc[0]
        city_code = str(row["code"])
        
        # Pega o nome do município se a malha trouxer, senão usa padrão
        city_name = row.get("NM_MUN", "Desconhecido")
        
        logger.info(f"💾 Persistindo cidade {city_code} ({population} hab)...")

        # Upsert (Inserir, se existir atualiza)
        stmt = insert(City).values(
            code=city_code,
            name=city_name,
            uf=row.get("SIGLA_UF", "BR"), # API malhas as vezes traz, as vezes não.
            population=population,
            geom=row["geometry"].wkt
        ).on_conflict_do_update(
            index_elements=['code'],
            set_={
                "population": population, 
                "geom": row["geometry"].wkt,
                "name": city_name
            }
        )
        
        await self.db.execute(stmt)
        await self.db.commit()

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
        """Retorna GeoJSON de todas as cidades salvas na tabela 'cities'."""
        stmt = select(
            City.code, City.name, City.population,
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
                    "name": row.name,
                    "population": row.population
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