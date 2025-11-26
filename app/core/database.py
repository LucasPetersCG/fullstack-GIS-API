# app/core/database.py
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from app.core.config import settings
from typing import AsyncGenerator

# Cria o motor assíncrono. echo=True ajuda a ver o SQL gerado no terminal .
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=True,
    future=True
)

# Fábrica de sessões. expire_on_commit=False é mandatório para async.
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

# Base para nossos Modelos (tabelas) herdarem
Base = declarative_base()

# Dependência para ser usada nas rotas do FastAPI
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()