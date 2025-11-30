# app/probe_ibge.py
import httpx
import asyncio
import xml.etree.ElementTree as ET

async def probe_services():
    print("🕵️  INICIANDO SONDAGEM DO IBGE...\n")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # --- 1. SONDAGEM DA API DE DADOS (AGREGADOS V3) ---
        print("--- [1] API AGREGADOS v3 (Tabela 4714) ---")
        meta_url = "https://servicodados.ibge.gov.br/api/v3/agregados/4714/metadados"
        
        try:
            r = await client.get(meta_url)
            r.raise_for_status()
            data = r.json()
            
            # Verifica se N15 (Setor Censitário) está listado
            niveis = data.get("niveisTerritoriais", [])
            ids_niveis = [n['id'] for n in niveis]
            
            print(f"Status: ONLINE (200)")
            print(f"Níveis Territoriais Disponíveis: {ids_niveis}")
            
            if "N15" in ids_niveis:
                print("✅ SUCESSO: O nível N15 (Setor) ESTÁ disponível na Tabela 4714.")
            else:
                print("❌ FALHA: O nível N15 NÃO está disponível nesta tabela. (Precisamos achar outra tabela).")
                
        except Exception as e:
            print(f"❌ ERRO FATAL NA API v3: {e}")

        print("\n" + "="*50 + "\n")

        # --- 2. SONDAGEM DO WFS (GEOSERVER) ---
        print("--- [2] GEOSERVER WFS (Camadas 2022) ---")
        wfs_url = "https://geoservicos.ibge.gov.br/geoserver/ows?service=wfs&version=1.1.0&request=GetCapabilities"
        
        try:
            r = await client.get(wfs_url)
            if r.status_code != 200:
                print(f"❌ ERRO HTTP WFS: {r.status_code}")
            else:
                # Parsing Simples do XML para achar camadas com 'Setor' e '2022'
                root = ET.fromstring(r.content)
                # Namespaces do XML do Geoserver são chatos, vamos fazer busca bruta no texto
                # para achar o <Name>...</Name>
                
                print("Buscando camadas que contenham '2022' e 'setor'...")
                found_layers = []
                
                # Iterar sobre todas as tags (forma bruta mas eficaz para namespaces dinâmicos)
                for elem in root.iter():
                    if "Name" in elem.tag and elem.text:
                        name = elem.text
                        if "2022" in name and ("setor" in name.lower() or "sc" in name.lower()):
                            found_layers.append(name)
                
                if found_layers:
                    print(f"✅ CAMADAS ENCONTRADAS: {found_layers}")
                    print("USE UM DESSES NOMES NO SEU IBGE_LOADER.PY!")
                else:
                    print("⚠️ Nenhuma camada óbvia encontrada (O Geoserver mudou os nomes?)")
                    
        except Exception as e:
            print(f"❌ ERRO NO WFS: {e}")

if __name__ == "__main__":
    asyncio.run(probe_services())