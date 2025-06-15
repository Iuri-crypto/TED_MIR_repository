import os
import re
import networkx as nx

class GrafoDSS:
    def __init__(self, caminho, tabela_tensoes):
        self.caminho_base = caminho
        self.grafo = nx.Graph()
        self.tabela_tensoes = tabela_tensoes
        self.tensoes_transformadores = {}
        self.tensoes_buses = {}

    def extrair_barra_slack(self):
        try:
            with open(self.caminho_base, "r", encoding="utf-8") as f:
                for linha in f:
                    if "New Object" in linha and "Circuit" in linha:
                        match = re.search(r"bus1\s*=\s*([\w\d]+)", linha, re.IGNORECASE)
                        if match:
                            return match.group(1).split('.')[0]
        except Exception as e:
            print(f"Erro ao ler barra slack: {e}")
        return None

    def extrair_buses_cargas(self):
        pac_cargas = set()
        try:
            with open(self.caminho_base, "r", encoding="utf-8") as f:
                for linha in f:
                    if "New Load" in linha:
                        match_bus1 = re.search(r"Bus1\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha, re.IGNORECASE)
                        if match_bus1:
                            pac_cargas.add(match_bus1.group(1).split('.')[0])
        except Exception as e:
            print(f"Erro ao extrair buses das cargas: {e}")
        return pac_cargas

    def extrair_tensoes_transformadores(self):
        try:
            with open(self.caminho_base, "r", encoding="utf-8") as f:
                linhas = f.readlines()
                for i in range(len(linhas)):
                    if "New Transformer" in linhas[i]:
                        match_bus1 = re.search(r"wdg\s*=\s*1\s+bus\s*=\s*([\w\d]+(?:\.[\d]+)*)", linhas[i + 1], re.IGNORECASE)
                        match_kv1 = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 1], re.IGNORECASE)
                        match_bus2 = re.search(r"wdg\s*=\s*2\s+bus\s*=\s*([\w\d]+(?:\.[\d]+)*)", linhas[i + 2], re.IGNORECASE)
                        match_kv2 = re.search(r"kv\s*=\s*([\d\.]+)", linhas[i + 2], re.IGNORECASE)

                        if match_bus1 and match_kv1 and match_bus2 and match_kv2:
                            bus1 = match_bus1.group(1).split('.')[0]
                            kv1 = float(match_kv1.group(1))
                            bus2 = match_bus2.group(1).split('.')[0]
                            kv2 = float(match_kv2.group(1))
                            self.tensoes_transformadores[bus1] = kv1
                            self.tensoes_transformadores[bus2] = kv2
        except Exception as e:
            print(f"Erro ao extrair tensões dos transformadores: {e}")

    def vincular_buses(self):
        try:
            with open(self.caminho_base, "r", encoding="utf-8") as f:
                for linha in f:
                    match_bus1 = re.search(r"(?i)bus1\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)
                    match_bus2 = re.search(r"(?i)bus2\s*=\s*([\w\d]+(?:\.[\d]+)*)", linha)
                    if match_bus1 and match_bus2:
                        bus1 = match_bus1.group(1).split('.')[0]
                        bus2 = match_bus2.group(1).split('.')[0]
                        self.grafo.add_edge(bus1, bus2)
        except Exception as e:
            print(f"Erro ao vincular buses: {e}")

    def extrair_transformadores(self):
        try:
            with open(self.caminho_base, "r", encoding="utf-8") as f:
                linhas = f.readlines()
                for i, linha in enumerate(linhas):
                    if "New Transformer" in linha:
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
                                self.tensoes_buses[bus1] = kv1
                                self.tensoes_buses[bus2] = kv2
                                self.grafo.add_edge(bus1, bus2)
        except Exception as e:
            print(f"Erro ao extrair transformadores: {e}")

    def propagar_tensoes(self):
        visitados = set()
        fila = list(self.tensoes_buses.keys())

        while fila:
            bus_atual = fila.pop(0)
            visitados.add(bus_atual)
            for vizinho in self.grafo.neighbors(bus_atual):
                if vizinho not in visitados:
                    if vizinho not in self.tensoes_buses:
                        self.tensoes_buses[vizinho] = self.tensoes_buses[bus_atual]
                    fila.append(vizinho)



    def encontrar_tensoes_base(self):
        try:
            caminho_raiz = self.caminho_base  # deve ser a pasta principal

            for pasta_superior in os.listdir(caminho_raiz):
                caminho_superior = os.path.join(caminho_raiz, pasta_superior)
                if not os.path.isdir(caminho_superior):
                    continue

                for subpasta in os.listdir(caminho_superior):
                    caminho_subpasta = os.path.join(caminho_superior, subpasta)
                    if not os.path.isdir(caminho_subpasta):
                        continue

                    arquivo_dss = os.path.join(caminho_subpasta, "run_cargas_agregadas.dss")
                    if not os.path.isfile(arquivo_dss):
                        print(f"Arquivo 'run_cargas_agregadas.dss' não encontrado em: {caminho_subpasta}")
                        continue

                    print(f"Processando arquivo: {arquivo_dss}")
                    self.caminho_base = arquivo_dss  # define novo caminho temporário

                    barra_slack_bus = self.extrair_barra_slack()
                    pac_cargas = self.extrair_buses_cargas()
                    self.extrair_tensoes_transformadores()
                    self.vincular_buses()

                    if barra_slack_bus:
                        visitados = set(nx.dfs_preorder_nodes(self.grafo, barra_slack_bus))
                    else:
                        visitados = set()

                    cargas_conectadas = pac_cargas.intersection(visitados)
                    cargas_desconectadas = pac_cargas.difference(visitados)

                    self.extrair_transformadores()
                    self.vincular_buses()
                    self.propagar_tensoes()

                    self.listar_tensoes_presentes_em_subpastas_single(arquivo_dss)

                    # limpa os dados para o próximo ciclo
                    self.grafo.clear()
                    self.tensoes_transformadores.clear()
                    self.tensoes_buses.clear()

        except Exception as e:
            print(f"Erro ao processar pastas: {e}")


    def listar_tensoes_presentes_em_subpastas_single(self, arquivo_dss):
        tensoes_presentes = set()

        for tensao in self.tensoes_buses.values():
            tensao_v = tensao * 1000  # converte kV para V

            correspondentes = [valor for valor in self.tabela_tensoes.values() if abs(tensao_v - valor) < 500]

            if correspondentes:
                tensoes_presentes.add(round(tensao_v))
            else:
                tabela_proxima = min(self.tabela_tensoes.values(), key=lambda x: abs(x - tensao_v))
                tensoes_presentes.add(round(tabela_proxima))

        tensoes_ordenadas = sorted(tensoes_presentes)
        for t in tensoes_ordenadas:
            print(f"{t} V")

        tensoes_kv_formatadas = [f"{t / 1000:.3f}" for t in tensoes_ordenadas]
        linha_voltagebases = f'Set VoltageBases = "{", ".join(tensoes_kv_formatadas)}"\n'
        linha_calcvoltage = 'CalcVoltageBases\n'

        try:
            with open(arquivo_dss, "r", encoding="utf-8") as f:
                linhas = f.readlines()

            # Filtrar todas as linhas que não sejam as linhas de VoltageBases e CalcVoltageBases associadas
            novas_linhas = []
            skip_next = False
            for i, linha in enumerate(linhas):
                if skip_next:
                    skip_next = False
                    continue
                if linha.strip().startswith("Set VoltageBases ="):
                    # pula essa linha e a próxima se for CalcVoltageBases
                    if i + 1 < len(linhas) and linhas[i + 1].strip() == "CalcVoltageBases":
                        skip_next = True
                    continue
                else:
                    novas_linhas.append(linha)

            # Adiciona as linhas atualizadas no fim do arquivo (ou poderia inserir em local fixo)
            novas_linhas.append("\n" + linha_voltagebases)
            novas_linhas.append(linha_calcvoltage)

            with open(arquivo_dss, "w", encoding="utf-8") as f:
                f.writelines(novas_linhas)

        except Exception as e:
            print(f"Erro ao escrever tensões base no arquivo {arquivo_dss}: {e}")
