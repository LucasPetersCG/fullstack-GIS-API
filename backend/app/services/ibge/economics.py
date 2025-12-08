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

    async def fetch_companies_stats(self, city_code: str) -> Dict[str, int]:
        """
        Busca dados do CEMPRE (Tabela 1685).
        Varredura: 2025 -> 2019.
        Retorna: {total_companies, total_workers, year}
        """
        # Variáveis: 153 (Unidades locais), 154 (Pessoal ocupado)
        url = f"{self.BASE_URL}/1685/periodos/2025|2024|2023|2022|2021|2020|2019/variaveis/153|154?localidades=N6[{city_code}]&classificacao=12762[0]" 
        
        stats = {"total_companies": 0, "total_workers": 0, "year": 0}
        
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(url)
                if resp.status_code != 200: return stats
                
                data = resp.json()
                # O IBGE retorna uma lista com 2 objetos (um para cada variável)
                # Precisamos achar um ano comum ou o mais recente de cada.
                
                # Mapa auxiliar: { '2021': {'153': 100, '154': 500}, '2020': ... }
                years_data = {}

                for item in data:
                    var_id = str(item["id"])
                    series = item["resultados"][0]["series"][0]["serie"]
                    
                    for ano, val in series.items():
                        if val and val not in ["-", "...", "X"]:
                            if ano not in years_data: years_data[ano] = {}
                            years_data[ano][var_id] = int(val)

                # Agora pega o ano mais recente que tenha AMBOS os dados (ou pelo menos empresas)
                for ano in sorted(years_data.keys(), reverse=True):
                    vals = years_data[ano]
                    # Se tiver empresas (153), usamos esse ano
                    if "153" in vals:
                        stats["total_companies"] = vals.get("153", 0)
                        stats["total_workers"] = vals.get("154", 0)
                        stats["year"] = int(ano)
                        break
                
                return stats
            except Exception as e:
                logger.error(f"Erro CEMPRE: {e}")
                return stats