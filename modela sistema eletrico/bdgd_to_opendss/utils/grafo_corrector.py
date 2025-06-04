import os
import re
import networkx as nx
import matplotlib.pyplot as plt

# Diretório base
caminho_base = r"C:\TED_MIR\repositorios\TED_MIR_repository\modela sistema eletrico\bdgd_to_opendss\cenarios\cenario_1\001001\run.dss"

# Grafo para armazenar conexões
grafo = nx.Graph()
# Dicionário para armazenar tensões dos transformadores
tensoes_transformadores = {}
pacs_fases_trafos = {}
pacs_fases_seccionadoras = {}
pacs_fases_segmentos = {}

def carregar_linhas_arquivo(caminho):
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return f.readlines()
    except Exception as e:
        print(f"Erro ao abrir o arquivo: {e}")
        return []






def extrair_barra_slack(linhas):
    try:
        pac_fases_slack = {}
        tensao = None

        # Localiza a linha do circuito e pega a tensão na linha seguinte
        for i, linha in enumerate(linhas):
            if "New Object" in linha and "Circuit" in linha:
                if i + 1 < len(linhas):
                    linha_basekv = linhas[i + 1]
                    match_kv = re.search(r"basekv\s*=\s*([\d\.]+)", linha_basekv, re.IGNORECASE)
                    if match_kv:
                        tensao = float(match_kv.group(1))
                break

        # Agora encontra a linha que conecta a SourceBus
        for linha in linhas:
            if "New line" in linha and "bus1" in linha and "bus2" in linha:
                match_bus1 = re.search(r"bus1\s*=\s*([\w\d\.]+)", linha, re.IGNORECASE)
                match_bus2 = re.search(r"bus2\s*=\s*([\w\d\.]+)", linha, re.IGNORECASE)

                if match_bus1 and match_bus2:
                    bus1 = match_bus1.group(1)
                    bus2 = match_bus2.group(1)

                    if bus1.split('.')[0].lower() == "sourcebus":
                        nome_barra = bus2.split('.')[0]
                        fases = bus2.split('.')[1:] if '.' in bus2 else []
                        pac_fases_slack[nome_barra] = {
                            "fases": fases,
                            "tensao": tensao
                        }
                        return pac_fases_slack

    except Exception as e:
        print(f"Erro ao ler barra slack: {e}")

    return None, {}






def extrair_buses_cargas(linhas):
    pac_fases_cargas = {}

    for linha in linhas:
        if "New Load" in linha:
            match = re.search(r"Bus1\s*=\s*([\w\d\.]+)", linha, re.IGNORECASE)
            if match:
                bus_completo = match.group(1)
                partes = bus_completo.split('.')
                pac = partes[0]
                fases = partes[1:] if len(partes) > 1 else []

                # Atualizar só se novo conjunto de fases for maior
                if pac not in pac_fases_cargas:
                    pac_fases_cargas[pac] = fases
                else:
                    if len(fases) > len(pac_fases_cargas[pac]):
                        pac_fases_cargas[pac] = fases

    return pac_fases_cargas






def extrair_tensoes_transformadores(linhas):
    tensoes_transformadores = {}
    pacs_fases_trafos = {}

    for i in range(len(linhas)):
        if "New Transformer" in linhas[i]:
            match_bus1 = re.search(r"wdg\s*=\s*1\s+bus\s*=\s*([\w\d\.]+)", linhas[i + 1], re.IGNORECASE)
            match_kv1 = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 1], re.IGNORECASE)

            match_bus2 = re.search(r"wdg\s*=\s*2\s+bus\s*=\s*([\w\d\.]+)", linhas[i + 2], re.IGNORECASE)
            match_kv2 = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 2], re.IGNORECASE)

            if match_bus1 and match_kv1 and match_bus2 and match_kv2:
                bus1_completo = match_bus1.group(1)
                bus2_completo = match_bus2.group(1)

                pac1 = bus1_completo.split('.')[0]
                fases1 = bus1_completo.split('.')[1:] if '.' in bus1_completo else []

                pac2 = bus2_completo.split('.')[0]
                fases2 = bus2_completo.split('.')[1:] if '.' in bus2_completo else []

                kv1 = float(match_kv1.group(1))
                kv2 = float(match_kv2.group(1))

                # Atualiza tensões (substitui direto)
                tensoes_transformadores[pac1] = kv1
                tensoes_transformadores[pac2] = kv2

                # Função auxiliar para atualizar fases se a nova quantidade for maior
                def atualiza_fases_trafos(pac, novas_fases):
                    if pac not in pacs_fases_trafos:
                        pacs_fases_trafos[pac] = set(novas_fases)
                    else:
                        if len(novas_fases) > len(pacs_fases_trafos[pac]):
                            pacs_fases_trafos[pac] = set(novas_fases)

                atualiza_fases_trafos(pac1, fases1)
                atualiza_fases_trafos(pac2, fases2)

    # Converte sets para listas antes de retornar
    for pac in pacs_fases_trafos:
        pacs_fases_trafos[pac] = list(pacs_fases_trafos[pac])

    return tensoes_transformadores, pacs_fases_trafos




def vincular_segmentos(linhas):
    grafo = nx.Graph()
    pac_fases = {}

    for linha in linhas:
        if re.search(r"New Line", linha, re.IGNORECASE):
            # Extrair bus1 e bus2 (com as fases embutidas)
            match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+(?:\.[\d]+)+)", linha)
            match_bus2 = re.search(r"(?i)bus2\s*=\s*([\w\d]+(?:\.[\d]+)+)", linha)

            if match_bus1 and match_bus2:
                bus1_full = match_bus1.group(1)
                bus2_full = match_bus2.group(1)

                # Extrair o nome base (antes do primeiro ponto) e as fases (depois)
                bus1_parts = bus1_full.split('.')
                bus2_parts = bus2_full.split('.')

                bus1_pac = bus1_parts[0]
                bus2_pac = bus2_parts[0]

                # Fases são o resto depois do nome base
                fases_bus1 = set(bus1_parts[1:])  # conjunto de fases novas
                fases_bus2 = set(bus2_parts[1:])

                # Função auxiliar para atualizar fases se novo conjunto for maior
                def atualiza_fases(pac, novas_fases):
                    if pac not in pac_fases:
                        pac_fases[pac] = novas_fases
                    else:
                        # Atualiza só se novas fases tiverem mais elementos
                        if len(novas_fases) > len(pac_fases[pac]):
                            pac_fases[pac] = novas_fases

                # Atualizar pac_fases
                atualiza_fases(bus1_pac, fases_bus1)
                atualiza_fases(bus2_pac, fases_bus2)

                # Adicionar aresta no grafo
                grafo.add_edge(bus1_pac, bus2_pac)

    # Converte sets para listas antes de retornar
    for pac in pac_fases:
        pac_fases[pac] = list(pac_fases[pac])

    return grafo, pac_fases



tensoes_buses = []
def extrair_transformadores(linhas):
    for i, linha in enumerate(linhas):
        if "New Transformer" in linha and i + 2 < len(linhas):
            bus1_match = re.search(r"bus=([\w\d]+(?:\.[\d]+)*)", linhas[i + 1], re.IGNORECASE)
            kv1_match = re.search(r"kv=([\d\.]+)", linhas[i + 1], re.IGNORECASE)
            bus2_match = re.search(r"bus=([\w\d]+(?:\.[\d]+)*)", linhas[i + 2], re.IGNORECASE)
            kv2_match = re.search(r"kv=([\d\.]+)", linhas[i + 2], re.IGNORECASE)

            if bus1_match and kv1_match and bus2_match and kv2_match:
                bus1 = bus1_match.group(1).split('.')[0]
                bus2 = bus2_match.group(1).split('.')[0]
                kv1 = float(kv1_match.group(1))
                kv2 = float(kv2_match.group(1))
                tensoes_buses[bus1] = kv1
                tensoes_buses[bus2] = kv2
                grafo.add_edge(bus1, bus2)

def propagar_tensoes():
    """
    Propaga a tensão conhecida ao longo do grafo, copiando a tensão do nó de origem para seus vizinhos
    ainda não visitados. Não calcula queda de tensão, apenas propaga valores.
    """
    visitados = set()
    fila = list(tensoes_buses.keys())  # Começa dos buses com tensão conhecida

    while fila:
        bus_atual = fila.pop(0)
        visitados.add(bus_atual)

        for vizinho in grafo.neighbors(bus_atual):
            if vizinho not in visitados:
                if vizinho not in tensoes_buses:
                    tensoes_buses[vizinho] = tensoes_buses[bus_atual]
                fila.append(vizinho)




linhas = carregar_linhas_arquivo(caminho_base)

pac_fases_slack = extrair_barra_slack(linhas)
pac_cargas = extrair_buses_cargas(linhas)


tensoes_transformadores, pacs_fases_trafos = extrair_tensoes_transformadores(linhas)
vincular_segmentos(linhas)

visitados = set(nx.dfs_preorder_nodes(grafo, pac_fases_slack))
cargas_conectadas = pac_cargas.intersection(visitados)
cargas_desconectadas = pac_cargas.difference(visitados)

print("Cargas conectadas:", cargas_conectadas)
print("Cargas desconectadas:", cargas_desconectadas)
print("Tensões dos transformadores:", tensoes_transformadores)

extrair_transformadores(linhas)
vincular_segmentos(linhas)
propagar_tensoes()

print("Tensões dos buses:")
for bus, tensao in tensoes_buses.items():
    print(f"Bus: {bus}, Tensão: {tensao} kV")


