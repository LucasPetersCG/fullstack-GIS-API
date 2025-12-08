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
        city_name = row.get("NM_MUN", "Desconhecido")
        
        logger.info(f"💾 Persistindo {city_name} (Pop: {data['population']}, PIB: {data['pib_total']})...")

        # --- CORREÇÃO DE GEOMETRIA (CASTING) ---
        from shapely.geometry import Polygon, MultiPolygon
        geom = row["geometry"]
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom]) # Converte para MultiPolygon
            
        wkt = geom.wkt # Agora é garantido ser MULTIPOLYGON(...)
        # ----------------------------------------

        # Upsert na Tabela CITIES
        stmt = insert(City).values(
            code=city_code,
            name=city_name,
            uf=row.get("SIGLA_UF", "BR"),
            geom=wkt,
            # Campos de Dados
            population=data["population"],
            pib_total=data["pib_total"],
            pib_per_capita=data["pib_per_capita"],
            total_companies=data["total_companies"],
            total_workers=data["total_workers"]
        ).on_conflict_do_update(
            index_elements=['code'],
            set_={
                "name": city_name,
                "geom": wkt,
                "population": data["population"],
                "pib_total": data["pib_total"],
                "pib_per_capita": data["pib_per_capita"],
                "total_companies": data["total_companies"],
                "total_workers": data["total_workers"]
            }
        )
        
        # Executa e retorna o ID
        result = await self.db.execute(stmt.returning(City.id))
        city_id = result.scalar()
        
        # 2. Atualizar Distritos
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
        logger.info(f"✅ Cidade {city_name} atualizada com {len(districts)} distritos.")
    
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
        stmt = select(
            City.code, City.name, City.population, 
            City.pib_per_capita, City.total_companies, # Novos campos
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
                    "population": row.population,
                    # Tratamento de nulos para o frontend
                    "pib_per_capita": row.pib_per_capita or 0,
                    "total_companies": row.total_companies or 0
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