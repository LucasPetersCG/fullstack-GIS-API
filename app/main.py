# app/main.py
from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.database import get_db

app = FastAPI(title="Atibaia Geo-Insights")

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "Geo-Insights API is running"}

@app.get("/db-check")
async def test_database(db: AsyncSession = Depends(get_db)):
    """
    Testa a conexão com o banco e verifica a versão do PostGIS.
    Isso garante que a extensão espacial está ativa.
    """
    try:
        # Executa uma query SQL crua para pedir a versão do PostGIS
        result = await db.execute(text("SELECT postgis_full_version()"))
        version = result.scalar()
        return {
            "status": "success",
            "database": "connected",
            "postgis_version": version
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }