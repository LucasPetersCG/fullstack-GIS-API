# backend/app/services/ibge/cempre_probe.py
import httpx
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class CempreProbeService:
    BASE_URL = "https://servicodados.ibge.gov.br/api/v3/agregados"
    TABLE_ID = "1685" # CEMPRE

    async def run_diagnostic(self, city_code: str) -> Dict[str, Any]:
        """
        Executa uma bateria de testes na tabela 1685 para entender a falha.
        """
        report = {
            "city_code": city_code,
            "metadata_check": {},
            "test_results": []
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            # --- 1. ANÁLISE DE METADADOS ---
            # Vamos ver o que o IBGE diz que essa tabela tem.
            meta_url = f"{self.BASE_URL}/{self.TABLE_ID}/metadados"
            try:
                resp = await client.get(meta_url)
                if resp.status_code == 200:
                    meta = resp.json()
                    
                    # Extrair Periodos Disponíveis
                    periodos = [p["id"] for p in meta.get("periodos", [])]
                    last_period = periodos[-1] if periodos else "N/A"
                    
                    # Extrair Classificações Exigidas
                    classificacoes = []
                    for c in meta.get("classificacoes", []):
                        cats = [cat["id"] for cat in c.get("categorias", [])[:3]] # Pega 3 exemplos
                        classificacoes.append({
                            "id": c["id"], 
                            "nome": c["nome"],
                            "exemplo_categorias": cats
                        })

                    report["metadata_check"] = {
                        "status": "OK",
                        "ultimos_periodos": periodos[-5:], # Ultimos 5
                        "periodo_recomendado": last_period,
                        "classificacoes_exigidas": classificacoes
                    }
                else:
                    report["metadata_check"] = {"status": "ERRO", "http_code": resp.status_code}
                    return report # Se não tem metadata, nem adianta continuar
            except Exception as e:
                report["metadata_check"] = {"status": "EXCEPTION", "error": str(e)}
                return report

            # --- 2. TESTES DE REQUISIÇÃO ---
            last_period = report["metadata_check"]["periodo_recomendado"]
            
            # Teste A: Consulta Padrão (que está falhando)
            # Classificação 12762[0] (Total)
            url_a = f"{self.BASE_URL}/{self.TABLE_ID}/periodos/{last_period}/variaveis/153|154?localidades=N6[{city_code}]&classificacao=12762[0]"
            await self._probe_url(client, "Teste A (Padrão 12762[0])", url_a, report)

            # Teste B: Consulta Sem Classificação (Para ver se o erro é a classificação)
            url_b = f"{self.BASE_URL}/{self.TABLE_ID}/periodos/{last_period}/variaveis/153|154?localidades=N6[{city_code}]"
            await self._probe_url(client, "Teste B (Sem Classificação)", url_b, report)

            # Teste C: Consulta Nível Brasil (Para ver se o erro é o Município vazio)
            # Se funcionar para BR e falhar para Cidade, é falta de dado local.
            url_c = f"{self.BASE_URL}/{self.TABLE_ID}/periodos/{last_period}/variaveis/153?localidades=BR&classificacao=12762[0]"
            await self._probe_url(client, "Teste C (Nível Brasil)", url_c, report)

        return report

    async def _probe_url(self, client, test_name, url, report):
        try:
            resp = await client.get(url)
            result = {
                "test": test_name,
                "url": url,
                "status_code": resp.status_code,
                "success": resp.status_code == 200,
            }
            
            if resp.status_code != 200:
                # Captura o corpo do erro (pode ser HTML ou JSON de erro)
                result["error_body"] = resp.text[:500] # Primeiros 500 chars
            else:
                data = resp.json()
                if not data:
                    result["data_status"] = "VAZIO []"
                else:
                    # Tenta ver se tem valor
                    try:
                        val = list(data[0]["resultados"][0]["series"][0]["serie"].values())[0]
                        result["data_sample"] = val
                    except:
                        result["data_status"] = "JSON Complexo/Vazio"
            
            report["test_results"].append(result)
        except Exception as e:
            report["test_results"].append({"test": test_name, "error": str(e)})