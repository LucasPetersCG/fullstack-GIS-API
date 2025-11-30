# app/core/init_db.py
import logging
from app.core.database import engine, Base
from app.models.city import City, CityCatalog
from app.models.user import User

logger = logging.getLogger(__name__)

async def init_tables():
    """Cria as tabelas no banco de dados ao iniciar."""
    logger.info("⏳ Inicializando tabelas no PostGIS...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Tabelas verificadas/criadas com sucesso!")