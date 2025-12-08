# backend/app/services/ibge/topology.py
import httpx
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class IbgeTopologyService:
    """
    Serviço focado em explorar a hierarquia territorial intra-municipal.
    Objetivo: Listar Distritos/Subdistritos e validar existência de geometria.
    """
    
    LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"
    MALHAS_URL = "https://servicodados.ibge.gov.br/api/v3/malhas"

    async def probe_hierarchy(self, city_code: str) -> Dict[str, Any]:
        """
        Retorna a estrutura completa de distritos/subdistritos e testa
        se o primeiro distrito encontrado possui mapa disponível.
        """
        report = {
            "city_code": city_code,
            "districts_count": 0,
            "subdistricts_count": 0,
            "has_district_geometry": False,
            "details": []
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            # 1. Buscar Distritos
            logger.info(f"🔍 Buscando distritos para {city_code}...")
            url_dist = f"{self.LOCALIDADES_URL}/municipios/{city_code}/distritos"
            resp_dist = await client.get(url_dist)
            
            if resp_dist.status_code != 200:
                report["error"] = "Falha ao buscar distritos"
                return report

            districts = resp_dist.json()
            report["districts_count"] = len(districts)

            # 2. Buscar Subdistritos (Granularidade máxima)
            logger.info(f"🔍 Buscando subdistritos para {city_code}...")
            url_sub = f"{self.LOCALIDADES_URL}/municipios/{city_code}/subdistritos"
            resp_sub = await client.get(url_sub)
            subdistricts = resp_sub.json() if resp_sub.status_code == 200 else []
            report["subdistricts_count"] = len(subdistricts)

            # 3. Teste de Geometria (Visual)
            # Vamos pegar o ID do primeiro distrito e tentar baixar a malha dele.
            if districts:
                first_dist_id = districts[0]["id"]
                dist_name = districts[0]["nome"]
                
                logger.info(f"🧪 Testando geometria para distrito: {dist_name} ({first_dist_id})")
                
                # A API de Malhas aceita IDs de distritos? Vamos testar.
                url_geom = f"{self.MALHAS_URL}/distritos/{first_dist_id}"
                try:
                    resp_geom = await client.get(url_geom, params={"formato": "application/vnd.geo+json"})
                    
                    if resp_geom.status_code == 200 and resp_geom.content:
                        report["has_district_geometry"] = True
                        report["geometry_test_msg"] = f"Sucesso! A API retornou GeoJSON para o distrito {dist_name}."
                    else:
                        report["geometry_test_msg"] = f"Falha ({resp_geom.status_code}). A API de Malhas v3 não retornou mapa para o distrito."
                except Exception as e:
                    report["geometry_test_msg"] = f"Erro de conexão no teste de malha: {str(e)}"

            # Monta o detalhe hierárquico para inspeção
            report["details"] = {
                "districts": [{"id": d["id"], "nome": d["nome"]} for d in districts],
                "subdistricts_sample": [{"id": s["id"], "nome": s["nome"]} for s in subdistricts[:5]] # Amostra
            }

            return report