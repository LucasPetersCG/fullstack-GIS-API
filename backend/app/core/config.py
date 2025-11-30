# app/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Atibaia Geo-Insights"
    DATABASE_URL: str

    # NOVAS CONFIGURAÇÕES DE SEGURANÇA
    # Em produção, isso DEVE vir de variáveis de ambiente (.env)
    # Gere uma string aleatória com: openssl rand -hex 32
    SECRET_KEY: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

settings = Settings()