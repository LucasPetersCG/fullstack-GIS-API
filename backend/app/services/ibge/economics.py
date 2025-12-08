# backend/app/services/ibge/economics.py
import httpx
import logging
from typing import Dict, Tuple, Any

logger = logging.getLogger(__name__)

class IbgeEconomicsService:
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

    async def fetch_pib(self, city_code: str) -> Tuple[float, str]:
        """
        Busca PIB Total (Tabela 5938).
        Retorna: (Valor em Mil Reais, Ano de Referência)
        Janela: 2020 a 2025.
        """
        # Solicita os últimos 6 anos possíveis
        url = f"{self.BASE_URL}/5938/periodos/2025|2024|2023|2022|2021|2020/variaveis/37?localidades=N6[{city_code}]"
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(url)
                if resp.status_code != 200: return 0.0, ""
                
                data = resp.json()
                if not data: return 0.0, ""

                series = data[0]["resultados"][0]["series"][0]["serie"]
                
                # Ordena decrescente para pegar o ano mais recente com valor válido
                for ano in sorted(series.keys(), reverse=True):
                    val = series[ano]
                    if val and val not in ["-", "...", "X"]:
                        return float(val), ano
                
                return 0.0, ""
            except Exception as e:
                logger.error(f"Erro PIB: {e}")
                return 0.0, ""

    async def fetch_companies_stats(self, city_code: str) -> Dict[str, Any]:
        """
        Busca dados do CEMPRE (Tabela 1685).
        Retorna: {total_companies, total_workers, year}
        """
        url = f"{self.BASE_URL}/1685/periodos/2025|2024|2023|2022|2021|2020/variaveis/153|154?localidades=N6[{city_code}]&classificacao=12762[0]" 
        
        stats = {"total_companies": 0, "total_workers": 0, "year": ""}
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(url)
                if resp.status_code != 200: return stats
                
                data = resp.json()
                
                # Precisamos encontrar um ano comum onde ambos os dados existam (idealmente)
                # Ou pegamos o mais recente de cada um. Vamos tentar pegar o ano mais recente da primeira variável.
                
                for item in data:
                    var_id = str(item["id"])
                    series = item["resultados"][0]["series"][0]["serie"]
                    
                    for ano in sorted(series.keys(), reverse=True):
                        val = series[ano]
                        if val and val.isdigit():
                            if var_id == "153": # Empresas
                                stats["total_companies"] = int(val)
                                if not stats["year"]: stats["year"] = ano
                            elif var_id == "154": # Trabalhadores
                                stats["total_workers"] = int(val)
                            break # Achou o mais recente desta variável, para.
                
                return stats
            except Exception as e:
                logger.error(f"Erro CEMPRE: {e}")
                return stats