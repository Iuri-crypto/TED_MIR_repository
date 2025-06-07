
from tqdm import tqdm  # Certifique-se de instalar: pip install tqdm

import networkx as nx
from networkx.drawing.nx_agraph import to_agraph
import pandas as pd
import re

import matplotlib.pyplot as plt

# Diretório base
caminho_base = r"C:\TED MIDR\TED_MIR_repository\modela sistema eletrico\bdgd_to_opendss\cenarios\cenario_1\001001\run.dss"

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

                    grafo.add_edge(bus1, bus2)

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

                grafo.add_edge(pac1, pac2)

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



tensoes_buses = {}
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



def extrair_linhas_para_dict(linhas_raw):
    linhas_dict = {}
    contador = 0
    for linha in linhas_raw:
        if "New Line" in linha:
            match_bus1 = re.search(r"bus1\s*=\s*([\w\d\.]+)", linha, re.IGNORECASE)
            match_bus2 = re.search(r"bus2\s*=\s*([\w\d\.]+)", linha, re.IGNORECASE)
            match_phases = re.search(r"phases\s*=\s*([\d]+)", linha, re.IGNORECASE)

            if match_bus1 and match_bus2:
                bus1 = match_bus1.group(1).split('.')[0]
                bus2 = match_bus2.group(1).split('.')[0]

                fases = []
                if match_phases:
                    num_fases = int(match_phases.group(1))
                    # A, B, C → 1, 2, 3
                    fases = ['1', '2', '3'][:num_fases]

                linhas_dict[f"Linha_{contador}"] = {
                    "Bus1": bus1,
                    "Bus2": bus2,
                    "Phases": fases
                }
                contador += 1
    return linhas_dict



def ajustar_fases_para_cargas_rapido(grafo, pac_cargas, pac_fases, pacs_fases_trafos, linhas):
    import networkx as nx
    from tqdm import tqdm

    alteracoes = {}

    # Pré-calcular caminhos mínimos dos transformadores para todos os nós
    caminhos_minimos = nx.multi_source_dijkstra_path(grafo, pacs_fases_trafos)

    carga_escolhida = None
    caminho_barramentos_para_carga = []

    for carga, fases_carga in tqdm(pac_cargas.items(), desc="Ajustando fases com propagação"):
        if carga not in grafo.nodes:
            continue

        fases_carga_set = set(fases_carga)

        # Barramentos conectados à carga
        barramentos_conectados = list(grafo.neighbors(carga))
        if not barramentos_conectados:
            continue

        for barramento_inicial in barramentos_conectados:
            if barramento_inicial not in caminhos_minimos:
                continue

            caminho = caminhos_minimos[barramento_inicial]


            houve_alteracao = False

            for barramento in caminho:
                if barramento not in pac_fases:
                    pac_fases[barramento] = []

                fases_atuais = set(pac_fases[barramento])
                if not fases_carga_set.issubset(fases_atuais):
                    novas_fases = fases_atuais.union(fases_carga_set)
                    if barramento not in alteracoes:
                        alteracoes[barramento] = {
                            "antes": list(fases_atuais),
                            "depois": list(novas_fases)
                        }
                    pac_fases[barramento] = list(novas_fases)
                    houve_alteracao = True

            if houve_alteracao and carga_escolhida is None:
                carga_escolhida = carga
                caminho_barramentos_para_carga = caminho
                break  # para a iteração de barramentos

        if carga_escolhida is not None:
            break  # já encontrou uma carga com alteração

    # Atualiza fases das linhas
    for linha_id, linha_info in linhas.items():
        bus1 = linha_info.get('Bus1')
        bus2 = linha_info.get('Bus2')

        fases_bus1 = set(pac_fases.get(bus1, []))
        fases_bus2 = set(pac_fases.get(bus2, []))

        fases_linha = fases_bus1.union(fases_bus2)
        fases_atuais_linha = set(linha_info.get('Phases', []))

        if fases_linha != fases_atuais_linha:
            linhas[linha_id]['Phases'] = list(fases_linha)

    return alteracoes, carga_escolhida, caminho_barramentos_para_carga



def mapear_caminhos_carga_para_trafo(grafo, pac_cargas, pacs_fases_trafos):
    caminhos_cargas = {}

    # Caminhos de todos os nós até os trafos mais próximos
    caminhos_completos = nx.multi_source_dijkstra_path(grafo, pacs_fases_trafos)

    for carga in tqdm(pac_cargas.keys(), desc="Mapeando caminhos carga-trafo"):
        if carga not in grafo.nodes:
            continue

        # Vizinhos conectados à carga
        barramentos_conectados = list(grafo.neighbors(carga))
        if not barramentos_conectados:
            continue

        menor_caminho = None
        menor_tamanho = float("inf")

        for barramento in barramentos_conectados:
            if barramento not in caminhos_completos:
                continue

            caminho = caminhos_completos[barramento]  # já é o caminho do barramento ao trafo
            caminho_completo = [carga] + caminho

            if len(caminho_completo) < menor_tamanho:
                menor_tamanho = len(caminho_completo)
                menor_caminho = caminho_completo

        if menor_caminho:
            caminhos_cargas[carga] = menor_caminho

    return caminhos_cargas





def hierarchy_pos(G, root, width=1.0, vert_gap=0.2, vert_loc=0, xcenter=0.5, pos=None, parent=None):
    """
    Gera posições para plot hierárquico (sem usar pygraphviz).
    """
    if pos is None:
        pos = {root: (xcenter, vert_loc)}
    else:
        pos[root] = (xcenter, vert_loc)
    
    neighbors = list(G.neighbors(root))
    if parent:
        if parent in neighbors:
            neighbors.remove(parent)
    if neighbors:
        dx = width / len(neighbors)
        next_x = xcenter - width/2 - dx/2
        for neighbor in neighbors:
            next_x += dx
            pos = hierarchy_pos(G, neighbor, width=dx, vert_gap=vert_gap,
                                vert_loc=vert_loc - vert_gap, xcenter=next_x,
                                pos=pos, parent=root)
    return pos


def plotar_grafo_radial(grafo, raiz):
    arvore = nx.bfs_tree(grafo, raiz)
    pos = hierarchy_pos(arvore, raiz, width=2.0)  # alargar horizontalmente

    plt.figure(figsize=(30, 15))  # aproveita melhor a tela
    nx.draw(
        arvore,
        pos=pos,
        with_labels=False,
        node_size=5,             # maior que antes
        node_color="skyblue",
        edge_color="red",
        linewidths=0.5,
        alpha=0.9
    )
    plt.axis("off")
    plt.tight_layout()
    plt.show()




linhas = carregar_linhas_arquivo(caminho_base)

pac_fases_slack = extrair_barra_slack(linhas)
pac_cargas = extrair_buses_cargas(linhas)


tensoes_transformadores, pacs_fases_trafos = extrair_tensoes_transformadores(linhas)
grafo, pac_fases = vincular_segmentos(linhas)


caminhos_carga_trafo = mapear_caminhos_carga_para_trafo(grafo, pac_cargas, pacs_fases_trafos)


nó_inicial = list(pac_fases_slack.keys())[0]
visitados = set(nx.dfs_preorder_nodes(grafo, nó_inicial))

cargas_conectadas = set(pac_cargas.keys()).intersection(visitados)
cargas_desconectadas = set(pac_cargas.keys()).difference(visitados)


#plotar_grafo_radial(grafo, nó_inicial)

print("Cargas conectadas:", cargas_conectadas)
print("Cargas desconectadas:", cargas_desconectadas)
print("Tensões dos transformadores:", tensoes_transformadores)

extrair_transformadores(linhas)
vincular_segmentos(linhas)
propagar_tensoes()

print("Tensões dos buses:")
for bus, tensao in tensoes_buses.items():
   print(f"Bus: {bus}, Tensão: {tensao} kV")


linhas_raw = carregar_linhas_arquivo(caminho_base)
linhas_dict = extrair_linhas_para_dict(linhas_raw)


alteracoes, carga_escolhida, caminho_barramentos_para_carga = ajustar_fases_para_cargas_rapido(grafo, pac_cargas, pac_fases, pacs_fases_trafos, linhas_dict)




df_alteracoes = pd.DataFrame([
    {"pac": pac, "fases_antes": info["antes"], "fases_depois": info["depois"]}
    for pac, info in alteracoes.items()
])

# Mostra os barramentos modificados
print("\n=== Barramentos com alteração de fases ===")
for pac, info in alteracoes.items():
    print(f"Barramento: {pac} | Antes: {info['antes']} -> Depois: {info['depois']}")
