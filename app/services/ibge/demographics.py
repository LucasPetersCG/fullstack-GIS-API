# app/services/ibge/demographics.py
import httpx
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class IbgeDemographicsService:
    """Responsável exclusivamente por buscar dados censitários (API Agregados)."""
    
    CITY_CODE = "3504107"
    # Tabela 4714 (Pop 2022)
    DATA_API_URL = "https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93"

    def _get_mock_data(self) -> pd.DataFrame:
        """Mock compatível com a geometria."""
        logger.warning("⚠️ DEMOGRAPHICS BYPASS: Usando Dados Estáticos ⚠️")
        return pd.DataFrame([
            {"code": "3504107001", "population": 1500},
            {"code": "3504107002", "population": 3200},
            {"code": "3504107003", "population": 850},
        ])

    async def fetch_population(self) -> pd.DataFrame:
        """Tenta API v3, fallback para Mock."""
        params = {"localidades": f"N15[N6[{self.CITY_CODE}]]", "view": "flat"}
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                logger.info(f"Downloading Data: {self.DATA_API_URL}")
                response = await client.get(self.DATA_API_URL, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    # Parsing simplificado para flat view
                    if data and "resultados" in data[0]:
                        series = data[0]["resultados"][0]["series"]
                        parsed = []
                        for item in series:
                            try:
                                parsed.append({
                                    "code": str(item["localidade"]["id"]),
                                    "population": item["serie"]["2022"]
                                })
                            except KeyError: continue
                        
                        df = pd.DataFrame(parsed)
                        df["population"] = pd.to_numeric(df["population"], errors="coerce").fillna(0)
                        return df

                logger.error(f"Demographics API Failed: {response.status_code}")
                return self._get_mock_data()

        except Exception as e:
            logger.error(f"Demographics Error: {e}")
            return self._get_mock_data()