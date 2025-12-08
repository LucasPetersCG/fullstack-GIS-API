# backend/app/services/ibge/economics.py
import httpx
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class IbgeEconomicsService:
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"

    async def fetch_pib(self, city_code: str) -> float:
        """Busca PIB Total (Tabela 5938). Retorna float (Mil Reais)."""
        # Periodo -1 (Último disponível)
        url = f"{self.BASE_URL}/5938/periodos/-1/variaveis/37?localidades=N6[{city_code}]"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                resp = await client.get(url)
                if resp.status_code != 200: return 0.0
                
                data = resp.json()
                if not data: return 0.0

                # Estrutura: data[0]['resultados'][0]['series'][0]['serie']['ANO']
                val_dict = data[0]["resultados"][0]["series"][0]["serie"]
                val = list(val_dict.values())[0] # Pega o valor do ano retornado
                
                return float(val) if val and val not in ["-", "..."] else 0.0
            except Exception as e:
                logger.error(f"Erro PIB: {e}")
                return 0.0

    async def fetch_companies_stats(self, city_code: str) -> Dict[str, int]:
        """
        Busca dados de Empresas (CEMPRE - Tabela 1685).
        Retorna: Total de Unidades Locais e Pessoal Ocupado.
        """
        # Variável 153 (Unidades locais), 154 (Pessoal ocupado total)
        # Classificação 12762[0] = Total geral
        url = f"{self.BASE_URL}/1685/periodos/-1/variaveis/153|154?localidades=N6[{city_code}]&classificacao=12762[0]" 
        
        stats = {"total_companies": 0, "total_workers": 0}
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(url)
                if resp.status_code != 200: return stats
                
                data = resp.json()
                
                for item in data:
                    var_id = item["id"] # "153" ou "154"
                    try:
                        val_dict = item["resultados"][0]["series"][0]["serie"]
                        val_str = list(val_dict.values())[0]
                        
                        if val_str and val_str not in ["-", "..."]:
                            val = int(val_str)
                            if str(var_id) == "153": stats["total_companies"] = val
                            if str(var_id) == "154": stats["total_workers"] = val
                    except:
                        continue
                return stats
            except Exception as e:
                logger.error(f"Erro CEMPRE: {e}")
                return stats