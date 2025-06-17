import os
import re
import networkx as nx
import matplotlib.pyplot as plt

def remover_elementos_desconectados(caminho_base):
    def extrair_barras_e_conexoes(linhas):
        grafo = nx.Graph()
        todos_buses = set()
        lista_arestas = []

        for linha in linhas:
            match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)
            match_bus2 = re.search(r"(?i)bus2\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)
            if match_bus1:
                bus1 = match_bus1.group(1).split('.')[0]
                todos_buses.add(bus1)
            if match_bus2:
                bus2 = match_bus2.group(1).split('.')[0]
                todos_buses.add(bus2)
            if match_bus1 and match_bus2:
                grafo.add_edge(bus1, bus2)
                lista_arestas.append((bus1, bus2))

        return grafo, todos_buses, lista_arestas

    def encontrar_barra_slack(linhas):
        for i, linha in enumerate(linhas):
            if "New Object" in linha and "Circuit" in linha:
                for j in range(i + 1, min(i + 5, len(linhas))):
                    match = re.search(r"bus1\s*=\s*([\w\d\.]+)", linhas[j], re.IGNORECASE)
                    if match:
                        return match.group(1).split('.')[0]
        return None

    def eliminar_elementos(linhas, buses_desconectados, pasta, subpasta):
        novas_linhas = []
        i = 0
        while i < len(linhas):
            linha = linhas[i].strip()

            if "New Line." in linha:
                match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+)", linha)
                match_bus2 = re.search(r"(?i)bus2\s*=\s*([\w\d]+)", linha)
                if match_bus1 and match_bus2:
                    bus1 = match_bus1.group(1).split('.')[0]
                    bus2 = match_bus2.group(1).split('.')[0]
                    if bus1 in buses_desconectados and bus2 in buses_desconectados:
                        print(f"[{pasta}/{subpasta}] Removendo LINE entre {bus1} e {bus2}")
                        i += 1
                        continue

            elif "New Load." in linha:
                match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+)", linha)
                if match_bus1:
                    bus = match_bus1.group(1).split('.')[0]
                    if bus in buses_desconectados:
                        print(f"[{pasta}/{subpasta}] Removendo LOAD em {bus}")
                        i += 2
                        continue

            elif "New PVSystem." in linha:
                match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+)", linha)
                if match_bus1:
                    bus = match_bus1.group(1).split('.')[0]
                    if bus in buses_desconectados:
                        print(f"[{pasta}/{subpasta}] Removendo PVSystem em {bus}")
                        i += 4
                        continue

            novas_linhas.append(linhas[i])
            i += 1

        return novas_linhas

    def hierarchy_pos(G, root, width=1.0, vert_gap=0.2, vert_loc=0, xcenter=0.5, pos=None, parent=None):
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

    def plotar_grafo_radial(grafo, raiz, todos_buses, buses_conectados, caminho_saida):
        arvore = nx.bfs_tree(grafo, raiz)
        pos = hierarchy_pos(arvore, raiz, width=2.0)

        plt.figure(figsize=(30, 15))
        node_colors = [
            "lightcoral" if n not in buses_conectados else "skyblue"
            for n in arvore.nodes()
        ]

        nx.draw(
            arvore,
            pos=pos,
            with_labels=False,  # <- Rótulos removidos
            node_size=10,
            node_color=node_colors,
            edge_color="gray",
            linewidths=0.5,
            alpha=0.9,
            font_size=8  # Este argumento será ignorado se with_labels=False
        )
        plt.axis("off")
        plt.tight_layout()
        plt.show()


    for pasta_superior in os.listdir(caminho_base):
        caminho_superior = os.path.join(caminho_base, pasta_superior)
        if not os.path.isdir(caminho_superior):
            continue

        for subpasta in os.listdir(caminho_superior):
            caminho_subpasta = os.path.join(caminho_superior, subpasta)
            if not os.path.isdir(caminho_subpasta):
                continue

            caminho_dss = os.path.join(caminho_subpasta, "run_cargas_agregadas.dss")
            if not os.path.isfile(caminho_dss):
                continue

            print(f"\n🔍 Processando: {pasta_superior}/{subpasta}/run_cargas_agregadas.dss")
            with open(caminho_dss, 'r', encoding='utf-8') as f:
                linhas = f.readlines()

            grafo, todos_buses, lista_arestas = extrair_barras_e_conexoes(linhas)
            barra_slack = encontrar_barra_slack(linhas)

            if not barra_slack or barra_slack not in grafo:
                print(f"⚠️  Barra slack não encontrada ou inválida: {barra_slack}")
                continue

            buses_conectados = set(nx.dfs_preorder_nodes(grafo, barra_slack))
            buses_desconectados = todos_buses - buses_conectados

            # ➕ Plotar grafo ANTES da limpeza usando Matplotlib
            #plotar_grafo_radial(grafo, barra_slack, todos_buses, buses_conectados, caminho_subpasta)

            novas_linhas = eliminar_elementos(linhas, buses_desconectados, pasta_superior, subpasta)

            with open(caminho_dss, 'w', encoding='utf-8') as f:
                f.writelines(novas_linhas)

            print(f"✅ Remoção concluída em: {pasta_superior}/{subpasta}")
