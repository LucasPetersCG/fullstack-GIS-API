# backend/app/services/ibge/demographics.py
import httpx
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class IbgeDemographicsService:
    
    async def fetch_city_population(self, city_code: str) -> int:
        """
        Busca população total de um município (Censo 2022).
        API Agregados v3 | Tabela 4714 | Var 93
        """
        # N6[{city_code}] -> Nível Município filtrado pelo ID
        url = f"https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[{city_code}]"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            logger.info(f"📊 Baixando dados populacionais para {city_code}...")
            response = await client.get(url)
            
            if response.status_code != 200:
                logger.warning(f"Erro API Dados: {response.status_code}")
                return 0
                
            data = response.json()
            # O retorno é uma lista. Se vier vazia, não tem dados.
            if not data:
                return 0

            try:
                # Parsing do JSON complexo do SIDRA
                # Estrutura típica: [{'resultados': [{'series': [{'serie': {'2022': '12345'}}]}]}]
                val = data[0]["resultados"][0]["series"][0]["serie"]["2022"]
                
                # Trata valores como "..." ou "-"
                if val and val.isdigit():
                    return int(val)
                return 0
            except (KeyError, IndexError, ValueError) as e:
                logger.error(f"Erro parsing dados IBGE: {e}")
                return 0

    async def fetch_all_cities_catalog(self) -> List[Dict]:
            """
            Baixa a lista de TODOS os municípios do Brasil (Nome + ID + UF).
            Versão Blindada contra inconsistências da API do IBGE.
            """
            url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
            
            # Timeout aumentado para 60s (lista é grande)
            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info("📚 Baixando catálogo completo de municípios...")
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                except Exception as e:
                    logger.error(f"Erro de conexão com IBGE: {e}")
                    return [] # Retorna lista vazia em vez de quebrar
                
                raw_data = response.json()
                
                catalog = []
                for item in raw_data:
                    try:
                        # Navegação Segura:
                        # Usa .get() e "or {}" para garantir que nunca tentamos acessar chaves em None
                        # Ex: Se microrregiao for None, usa {}, e o próximo .get falha suavemente
                        micro = item.get("microrregiao") or {}
                        meso = micro.get("mesorregiao") or {}
                        uf_obj = meso.get("UF") or {}
                        
                        # Se falhar tudo, tenta pegar a UF direto (alguns endpoints retornam diferente)
                        # ou define 'BR' como fallback
                        uf_sigla = uf_obj.get("sigla", "BR")

                        catalog.append({
                            "code": str(item["id"]),
                            "name": item["nome"],
                            "uf": uf_sigla
                        })
                    except Exception as e:
                        # Loga o erro mas NÃO PARA O LOOP. Pula apenas essa cidade.
                        logger.warning(f"Ignorando cidade mal formatada ID {item.get('id')}: {e}")
                        continue
                
                logger.info(f"Catálogo processado: {len(catalog)} cidades encontradas.")
                return catalog