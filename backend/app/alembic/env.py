import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

# --- IMPORTAÇÕES AJUSTADAS PARA A NOVA ESTRUTURA ---
import sys
import os
# Adiciona o diretório 'app' ao path para o Alembic achar os módulos
sys.path.append(os.getcwd())

from app.core.database import Base
from app.core.config import settings
# Importar TODOS os modelos para o autogenerate funcionar
from app.models.user import User
from app.models.city import City, CityCatalog

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

def run_migrations_offline() -> None:
    url = settings.DATABASE_URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def include_object(object, name, type_, reflected, compare_to):
    # Lista de tabelas do NOSSO sistema (White List)
    # Se a tabela não estiver aqui, o Alembic deve ignorá-la.
    # alembic_version é a tabela interna do próprio alembic.
    my_tables = ["users", "cities", "city_catalog", "alembic_version"]
    
    if type_ == "table":
        # Se a tabela NÃO estiver na nossa lista, IGNORE.
        # Isso protege as tabelas do PostGIS (spatial_ref_sys) e Tiger Geocoder.
        if name not in my_tables:
            return False
            
    return True

def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, include_object=include_object)

    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = settings.DATABASE_URL

    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())