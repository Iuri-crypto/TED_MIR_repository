import os
import re
import networkx as nx
import matplotlib.pyplot as plt

# Diretório base
caminho_base = r"C:\TED_MIR\roda_fluxo_potencia\bdgd_distribuidoras_modelados\bdgd_2023_energisa_mt\alimentadores\010010\run.dss"

# Grafo para armazenar conexões
grafo = nx.Graph()
# Dicionário para armazenar tensões dos transformadores
tensoes_transformadores = {}

def extrair_barra_slack():
    """
    Função para encontrar o bus da barra slack no arquivo DSS principal.
    """
    try:
        with open(caminho_base, "r", encoding="utf-8") as f:
            for linha in f:
                if "New Object" in linha and "Circuit" in linha:
                    match = re.search(r"bus1\s*=\s*([\w\d]+)", linha, re.IGNORECASE)
                    if match:
                        return match.group(1).split('.')[0]
    except Exception as e:
        print(f"Erro ao ler barra slack: {e}")
    return None




def extrair_buses_cargas():
    """
    Função para extrair os buses das cargas a partir do arquivo DSS principal.
    """
    pac_cargas = set()
    try:
        with open(caminho_base, "r", encoding="utf-8") as f:
            for linha in f:
                if "New Load" in linha:
                    match_bus1 = re.search(r"Bus1\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha, re.IGNORECASE)
                    if match_bus1:
                        pac_cargas.add(match_bus1.group(1).split('.')[0])
    except Exception as e:
        print(f"Erro ao extrair buses das cargas: {e}")
    return pac_cargas




def extrair_tensoes_transformadores():
    """
    Função para extrair as tensões primária e secundária dos transformadores
    a partir do arquivo DSS principal e associá-las aos buses.
    """
    try:
        with open(caminho_base, "r", encoding="utf-8") as f:
            linhas = f.readlines()
            for i in range(len(linhas)):
                if "New Transformer" in linhas[i]:
                    match_bus1 = re.search(r"wdg\s*=\s*1\s+bus\s*=\s*([\w\d]+(?:\.[\d]+)*)", linhas[i + 1], re.IGNORECASE)
                    match_kv1  = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 1], re.IGNORECASE)
                    match_bus2 = re.search(r"wdg\s*=\s*2\s+bus\s*=\s*([\w\d]+(?:\.[\d]+)*)", linhas[i + 2], re.IGNORECASE)
                    match_kv2  = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 2], re.IGNORECASE)

                    if match_bus1 and match_kv1 and match_bus2 and match_kv2:
                        bus1 = match_bus1.group(1).split('.')[0]
                        kv1  = float(match_kv1.group(1))
                        bus2 = match_bus2.group(1).split('.')[0]
                        kv2  = float(match_kv2.group(1))

                        tensoes_transformadores[bus1] = kv1
                        tensoes_transformadores[bus2] = kv2
    except Exception as e:
        print(f"Erro ao extrair tensões dos transformadores: {e}")




def vincular_buses():
    """
    Função para criar conexões (arestas) entre os buses com base no arquivo DSS principal.
    """
    try:
        with open(caminho_base, "r", encoding="utf-8") as f:
            for linha in f:
                match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)
                match_bus2 = re.search(r"(?i)bus2\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)

                if match_bus1 and match_bus2:
                    bus1 = match_bus1.group(1).split('.')[0]
                    bus2 = match_bus2.group(1).split('.')[0]
                    grafo.add_edge(bus1, bus2)
    except Exception as e:
        print(f"Erro ao vincular buses: {e}")

                        
                        
                        
                        
                        
# Dicionário para armazenar as tensões dos buses
tensoes_buses = {}

def extrair_transformadores():
    """
    Extrai os transformadores diretamente do arquivo run.dss, associando suas tensões
    aos buses primário e secundário e adicionando a conexão no grafo.
    """
    try:
        with open(caminho_base, "r", encoding="utf-8") as f:
            linhas = f.readlines()
            for i, linha in enumerate(linhas):
                if "New Transformer" in linha:
                    # Tenta capturar os dados nas duas próximas linhas
                    if i + 2 < len(linhas):
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
    except Exception as e:
        print(f"Erro ao extrair transformadores: {e}")

                            
                            
def propagar_tensoes():
    """
    Função para propagar as tensões ao longo do grafo, mantendo a tensão do transformador mais próximo.
    """
    visitados = set()
    fila = list(tensoes_buses.keys())  # Começa dos buses que já têm tensão definida
    
    while fila:
        bus_atual = fila.pop(0)
        visitados.add(bus_atual)
        
        for vizinho in grafo.neighbors(bus_atual):
            if vizinho not in visitados:
                if vizinho not in tensoes_buses:
                    tensoes_buses[vizinho] = tensoes_buses[bus_atual]
                fila.append(vizinho)
                
                

def gerar_grafo():
    """
    Função para gerar o grafo com as conexões.
    """
    # Extrair a barra slack
    barra_slack_bus = extrair_barra_slack()

    # Extrair as cargas
    pac_cargas = extrair_buses_cargas()

    # Extrair tensões dos transformadores
    extrair_tensoes_transformadores()

    # Vincular os buses (conexões)
    vincular_buses()

    # Realiza a busca em profundidade (DFS) a partir da barra slack
    visitados = set(nx.dfs_preorder_nodes(grafo, barra_slack_bus))

    # Cargas conectadas diretamente ou indiretamente à barra slack
    cargas_conectadas = pac_cargas.intersection(visitados)

    # Cargas desconectadas (não atingidas pela DFS)
    cargas_desconectadas = pac_cargas.difference(visitados)




    print("Cargas conectadas:", cargas_conectadas)
    print("Cargas desconectadas:", cargas_desconectadas)
    print("Tensões dos transformadores:", tensoes_transformadores)
    
    
   
    extrair_transformadores()
    vincular_buses()
    propagar_tensoes()
    
    print("Tensões dos buses:")
    for bus, tensao in tensoes_buses.items():
        print(f"Bus: {bus}, Tensão: {tensao} kV")

# Chamar a função principal para gerar o grafo
gerar_grafo()
