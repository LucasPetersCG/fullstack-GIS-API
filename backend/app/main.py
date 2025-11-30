# backend/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Depends
from fastapi.staticfiles import StaticFiles
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.init_db import init_tables
from app.core.database import get_db
from app.services.ibge.orchestrator import IbgeEtlOrchestrator
from app.repositories.census_repository import CensusRepository
from app.schemas.geo import FeatureCollection
from app.api.deps import get_current_user
from app.models.user import User
from app.routers import auth


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Executa criação das tabelas ao ligar o servidor
    #await init_tables()
    yield

app = FastAPI(title="Atibaia Geo-Insights", lifespan=lifespan)
app.include_router(auth.router, tags=["auth"])


# --- ROTAS DE API (BACKEND) ---

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "Geo-Insights API is running"}

@app.get("/etl/preview")
async def preview_etl():
    """Visualiza o JSON gerado pelo ETL (sem salvar no banco)."""
    orchestrator = IbgeEtlOrchestrator()
    geojson_data = await orchestrator.get_consolidated_data_json()
    return Response(content=geojson_data, media_type="application/json")

@app.post("/etl/sync")
async def sync_etl(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user) # <--- O CADEADO
):
    """
    Executa o ETL e Salva no Banco.
    REQUER AUTENTICAÇÃO (JWT).
    """
    # (Opcional) Log de quem executou
    print(f"Usuário {current_user.username} iniciou o ETL.")
    
    orchestrator = IbgeEtlOrchestrator(db=db)
    result = await orchestrator.sync_database()
    return result

@app.get("/map", response_model=FeatureCollection)
async def get_map_data(db: AsyncSession = Depends(get_db)):
    """Endpoint consumido pelo Frontend."""
    repo = CensusRepository(db)
    features = await repo.get_all_features()
    return {"type": "FeatureCollection", "features": features}

# --- ROTA DE FRONTEND (MONOREPO) ---
# Montamos a pasta /frontend (do container) na URL /view
app.mount("/view", StaticFiles(directory="/frontend", html=True), name="frontend")