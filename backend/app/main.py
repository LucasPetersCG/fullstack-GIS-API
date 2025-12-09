# backend/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Depends, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Imports internos
from app.core.init_db import init_tables # (Se tiver comentado no passo anterior, mantenha comentado)
from app.core.database import get_db
from app.services.ibge.orchestrator import IbgeEtlOrchestrator
from app.repositories.city_repository import CityRepository
from app.schemas.geo import FeatureCollection
from app.api.deps import get_current_user
from app.models.user import User
from app.routers import auth
from app.services.ibge.topology import IbgeTopologyService
from app.services.ibge.cempre_probe import CempreProbeService

@asynccontextmanager
async def lifespan(app: FastAPI):
    # await init_tables() # Mantemos desligado pois usamos Alembic
    yield

app = FastAPI(title="Atibaia Geo-Insights", lifespan=lifespan)

# Autenticação
app.include_router(auth.router, tags=["auth"])

# --- ROTAS DE ADMINISTRAÇÃO (ETL) ---

@app.post("/admin/sync-catalog")
async def sync_catalog(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user) # Requer Login
):
    """Atualiza a lista de 5.570 municípios (Base para o Search)."""
    orchestrator = IbgeEtlOrchestrator(db)
    return await orchestrator.sync_catalog()

@app.post("/cities/import/{city_code}")
async def import_specific_city(
    city_code: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user) # Requer Login
):
    """Baixa dados reais do IBGE para uma cidade e coloca no mapa."""
    orchestrator = IbgeEtlOrchestrator(db)
    try:
        return await orchestrator.import_city(city_code)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

# --- ROTAS PÚBLICAS (LEITURA) ---

@app.get("/cities/search")
async def search_cities(
    q: str = Query(..., min_length=3),
    db: AsyncSession = Depends(get_db)
):
    """Autocomplete: Busca cidades pelo nome no catálogo local."""
    repo = CityRepository(db)
    results = await repo.list_catalog(search=q)
    return [{"code": r.code, "name": r.name, "uf": r.uf} for r in results]

@app.get("/map", response_model=FeatureCollection)
async def get_map_data(db: AsyncSession = Depends(get_db)):
    """Retorna todas as cidades que já foram importadas."""
    repo = CityRepository(db)
    features = await repo.get_all_features()
    return {"type": "FeatureCollection", "features": features}

@app.get("/probe/districts/{city_code}")
async def probe_districts(city_code: str, current_user: User = Depends(get_current_user)):
    """
    Rota de Diagnóstico: Verifica o que o IBGE tem de dados
    para Distritos e Subdistritos desta cidade.
    """
    service = IbgeTopologyService()
    return await service.probe_hierarchy(city_code)

@app.get("/debug/cempre/{city_code}")
async def debug_cempre(city_code: str):
    """
    Rota de Diagnóstico Técnico para a Tabela 1685 (CEMPRE).
    Não requer autenticação (para facilitar o teste rápido).
    """
    probe = CempreProbeService()
    return await probe.run_diagnostic(city_code)

# --- FRONTEND ---
app.mount("/view", StaticFiles(directory="/frontend", html=True), name="frontend")
app.mount("/login", StaticFiles(directory="/frontend", html=True), name="login")

