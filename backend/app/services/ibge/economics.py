# backend/app/services/ibge/economics.py
import httpx
import logging
import asyncio
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)

class IbgeEconomicsService:
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"
    
    # Adicionado HEADERS para evitar bloqueio WAF no loop
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://sidra.ibge.gov.br/"
    }

    async def _fetch_single_var(self, table: str, variable: str, city_code: str, classification: str = "") -> Tuple[float, int]:
        """
        Busca uma única variável (Ano a Ano).
        """
        years = ["2022", "2021", "2020", "2019"]
        
        async with httpx.AsyncClient(timeout=10.0, headers=self.HEADERS) as client:
            for year in years:
                # Monta URL CORRETA antes de chamar
                url = f"{self.BASE_URL}/{table}/periodos/{year}/variaveis/{variable}?localidades=N6[{city_code}]"
                if classification:
                    url += f"&classificacao={classification}"
                
                # Delay para não tomar Ban
                
                
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200: continue
                    
                    data = resp.json()
                    if not data: continue

                    # Navegação segura
                    item = data[0]
                    res_obj = item.get("resultados", [])
                    if not res_obj: continue
                    
                    series = res_obj[0]["series"][0]["serie"]
                    val_str = list(series.values())[0]
                    
                    if val_str and val_str not in ["-", "...", "X"]:
                        return float(val_str), int(year)
                        
                except Exception:
                    continue
        
        return 0.0, 0

    async def fetch_pib(self, city_code: str) -> Tuple[float, int]:
        """PIB Total (Tabela 5938, Var 37)."""
        logger.info(f"💰 Buscando PIB para {city_code}...")
        return await self._fetch_single_var("5938", "37", city_code)

    async def fetch_companies_stats(self, city_code: str) -> Dict[str, Any]:
        """CEMPRE (Tabela 1685)."""
        logger.info(f"🏢 Buscando Empresas para {city_code}...")
        stats = {"total_companies": 0, "total_workers": 0, "year": 0}
        
        # 1. Busca Empresas (Var 153)
        comp_val, comp_year = await self._fetch_single_var("1685", "153", city_code)
        
        # Fallback com classificação Total se falhar sem
        if comp_val == 0:
             comp_val, comp_year = await self._fetch_single_var("1685", "153", city_code, "12762[0]")

        stats["total_companies"] = int(comp_val)
        stats["year"] = comp_year

        # 2. Busca Pessoal (Var 154)
        work_val, _ = await self._fetch_single_var("1685", "154", city_code)
        if work_val == 0:
             work_val, _ = await self._fetch_single_var("1685", "154", city_code, "12762[0]")
             
        stats["total_workers"] = int(work_val)
        
        return stats