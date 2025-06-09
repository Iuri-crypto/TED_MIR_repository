import os
import matplotlib.pyplot as plt
import py_dss_interface

# Inicializa a interface com o OpenDSS
dss = py_dss_interface.DSS()

# Caminho raiz
caminho_base = r"C:\modelagens\cenario_2"

# Lista para armazenar alimentadores com 100 iterações
alimentadores_com_100_iteracoes = []

# Percorrer todas as subpastas e encontrar arquivos 'run_cargas_agregadas.dss'
for root, _, files in os.walk(caminho_base):
    for nome_arquivo in files:
        if nome_arquivo.lower() == "run_cargas_agregadas.dss":
            caminho_arquivo = os.path.join(root, nome_arquivo)
            nome_alimentador = os.path.basename(root)

            print(f"Processando: {caminho_arquivo}")

            dss.text("Clear")
            dss.text("Set DefaultBaseFrequency=60")
            dss.text(f"Compile {caminho_arquivo}")
            dss.solution.max_iterations = 100
            dss.solution.max_control_iterations = 100
            dss.solution.solve()

            # Coleta as tensões por nó
            barra_tensao = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag_pu))

            # Filtra somente nós terminando com .1, .2 ou .3
            barra_tensao_filtrada = [
                (nome, vpu) for nome, vpu in barra_tensao if nome.endswith((".1", ".2", ".3"))
            ]

            # Plota gráfico
            if barra_tensao_filtrada:
                tensoes = [vpu for _, vpu in barra_tensao_filtrada]

                plt.figure(figsize=(12, 6))
                plt.plot(tensoes, marker='o')
                plt.title(f"Tensões em pu - {nome_alimentador}")
                plt.xlabel("Índice do nó")
                plt.ylabel("Tensão (pu)")
                plt.grid(True)
                plt.tight_layout()
                plt.show()

            # Verifica número de iterações
            iteracoes = dss.solution.iterations
            print(f"Iterações: {iteracoes}")
            if iteracoes == 100:
                alimentadores_com_100_iteracoes.append(nome_alimentador)

# Exibe resultado final
print("\n--- Alimentadores com 100 iterações ---")
for nome in alimentadores_com_100_iteracoes:
    print(f"- {nome}")
if not alimentadores_com_100_iteracoes:
    print("Nenhum alimentador teve exatamente 100 iterações.")
