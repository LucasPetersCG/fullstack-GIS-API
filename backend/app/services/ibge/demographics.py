# backend/app/services/ibge/demographics.py
import httpx
import logging
import asyncio
from typing import List, Dict

logger = logging.getLogger(__name__)

class IbgeDemographicsService:
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://sidra.ibge.gov.br/"
    }

    async def fetch_city_population(self, city_code: str) -> int:
        """Busca população total (Censo 2022)."""
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[{city_code}]"
        
        async with httpx.AsyncClient(timeout=10.0, headers=self.HEADERS) as client:
            try:
                response = await client.get(url)
                if response.status_code != 200: return 0
                
                data = response.json()
                if not data: return 0

                val = data[0]["resultados"][0]["series"][0]["serie"]["2022"]
                if val and val.isdigit():
                    return int(val)
                return 0
            except Exception as e:
                logger.error(f"Erro Population: {e}")
                return 0

    async def fetch_all_cities_catalog(self) -> List[Dict]:
        """Baixa catálogo de municípios (Blindado)."""
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        
        async with httpx.AsyncClient(timeout=60.0, headers=self.HEADERS) as client:
            logger.info("📚 Baixando catálogo completo de municípios...")
            try:
                
                response = await client.get(url)
                response.raise_for_status()
                
                raw_data = response.json()
                
            except Exception as e:
                logger.error(f"Erro fatal no catálogo: {e}")
                return []
            
            # Parsing Seguro
            catalog = []
            for item in raw_data:
                try:
                    micro = item.get("microrregiao") or {}
                    meso = micro.get("mesorregiao") or {}
                    uf_obj = meso.get("UF") or {}
                    uf_sigla = uf_obj.get("sigla", "BR")

                    catalog.append({
                        "code": str(item["id"]),
                        "name": item["nome"],
                        "uf": uf_sigla
                    })
                except Exception:
                    continue
            
            logger.info(f"Catálogo processado: {len(catalog)} cidades.")
            return catalog

    async def fetch_city_details(self, city_code: str) -> dict:
        """Busca o nome oficial e UF."""
        url = f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{city_code}"
        async with httpx.AsyncClient(timeout=10.0, headers=self.HEADERS) as client:
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    d = r.json()
                    # Safe Navigation
                    micro = d.get("microrregiao") or {}
                    meso = micro.get("mesorregiao") or {}
                    uf = meso.get("UF") or {}
                    
                    return {
                        "name": d.get("nome"),
                        "uf": uf.get("sigla", "BR")
                    }
            except: pass
        return {"name": "Desconhecido", "uf": "BR"}