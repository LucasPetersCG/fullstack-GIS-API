# Dockerfile
FROM python:3.11-slim

# Evita que o Python gere arquivos .pyc e força o buffer de stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependências de sistema necessárias para GeoPandas/GDAL e PostGIS client
# libpq-dev é necessário para o psycopg2/asyncpg
# gdal-bin e libgdal-dev são cruciais para operações espaciais
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements (vamos criar em breve)
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código
COPY . .

# Comando padrão (será sobrescrito pelo docker-compose, mas bom ter)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]