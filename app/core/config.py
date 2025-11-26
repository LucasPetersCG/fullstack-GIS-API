# app/core/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Atibaia Geo-Insights"
    DATABASE_URL: str

    # O Pydantic lê as variáveis de ambiente automaticamente (ex: do docker-compose)
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

settings = Settings()