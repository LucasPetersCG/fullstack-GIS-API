# backend/app/services/ibge/topology.py
import httpx
import logging
from typing import Dict, Any, List # Adicionado List

logger = logging.getLogger(__name__)

class IbgeTopologyService:
    """
    Serviço híbrido:
    1. Produção: Busca hierarquia para persistência (fetch_districts).
    2. Diagnóstico: Sonda capacidades da API (probe_hierarchy).
    """
    
    LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"
    MALHAS_URL = "https://servicodados.ibge.gov.br/api/v3/malhas"

    # --- MÉTODO NOVO (USADO PELO ORCHESTRATOR) ---
    async def fetch_districts(self, city_code: str) -> List[Dict]:
        """
        Retorna lista limpa de distritos para salvar no Banco de Dados.
        Uso: Orchestrator -> Repository
        """
        url = f"{self.LOCALIDADES_URL}/municipios/{city_code}/distritos"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    # Retorna a lista pura: [{'id': '...', 'nome': '...'}]
                    return response.json()
                return []
            except Exception as e:
                logger.error(f"Erro Topologia (Distritos): {e}")
                return []

    # --- MÉTODO ANTIGO (DIAGNÓSTICO / PROBE) ---
    async def probe_hierarchy(self, city_code: str) -> Dict[str, Any]:
        """
        Retorna relatório detalhado de disponibilidade.
        Uso: Rota /probe/districts
        """
        report = {
            "city_code": city_code,
            "districts_count": 0,
            "has_district_geometry": False,
            "details": []
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            # 1. Buscar Distritos
            url_dist = f"{self.LOCALIDADES_URL}/municipios/{city_code}/distritos"
            resp_dist = await client.get(url_dist)
            
            if resp_dist.status_code != 200:
                report["error"] = "Falha ao buscar distritos"
                return report

            districts = resp_dist.json()
            report["districts_count"] = len(districts)

            # 2. Teste de Geometria (Visual)
            if districts:
                first_dist_id = districts[0]["id"]
                url_geom = f"{self.MALHAS_URL}/distritos/{first_dist_id}"
                try:
                    resp_geom = await client.get(url_geom, params={"formato": "application/vnd.geo+json"})
                    if resp_geom.status_code == 200 and resp_geom.content:
                        report["has_district_geometry"] = True
                except Exception:
                    pass

            report["details"] = districts
            return report