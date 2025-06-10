import os
import time
import matplotlib.pyplot as plt
from multiprocessing import Pool
import py_dss_interface
import matplotlib.style as style

# Estilo bonito para os gráficos
style.use("seaborn-v0_8-darkgrid")

def processar_alimentador(caminho_arquivo):
    dss = py_dss_interface.DSS()
    nome_alimentador = os.path.basename(os.path.dirname(caminho_arquivo))
    print(f"Processando: {caminho_arquivo}")

    dss.text("Clear")
    dss.text("Set DefaultBaseFrequency=60")
    dss.text(f"Compile {caminho_arquivo}")

    total_tempo = 0
    iteracoes_ultimas = 0
    dss.solution.max_iterations = 100
    dss.solution.max_control_iterations = 100

    for i in range(288):
        time.sleep(0.02)  # atraso de 20 ms

        # Recompila o DSS no passo 230
        if i == 230:
            print(f"[{nome_alimentador}] Recompilando no passo {i}")
            dss.text("Clear")
            dss.text("Set DefaultBaseFrequency=60")
            dss.text(f"Compile {caminho_arquivo}")

        start = time.time()
        dss.solution.solve()
        fim = time.time()

        total_tempo += (fim - start)
        iteracoes_ultimas = dss.solution.iterations

    barra_tensao = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag_pu))
    barra_tensao_filtrada = [
        (nome, vpu) for nome, vpu in barra_tensao
        if nome.endswith((".1", ".2", ".3")) and vpu > 0
    ]

    barras_problema = [(nome, vpu) for nome, vpu in barra_tensao_filtrada if 0.1 <= vpu <= 0.5]
    if barras_problema:
        print(f"\nBarramentos com tensão entre 0.1 e 0.5 pu em {nome_alimentador}:")
        for nome, vpu in barras_problema:
            print(f"  - {nome}: {vpu:.3f} pu")

    if barra_tensao_filtrada:
        nomes_nos, tensoes = zip(*barra_tensao_filtrada)
        indices = list(range(len(tensoes)))

        plt.figure(figsize=(14, 7))
        plt.plot(indices, tensoes, marker='o', linestyle='-', color='royalblue', label='Tensão por nó')
        plt.axhline(y=1.05, color='green', linestyle='--', linewidth=1, label='Limite Superior (1.05 pu)')
        plt.axhline(y=0.95, color='red', linestyle='--', linewidth=1, label='Limite Inferior (0.95 pu)')
        plt.title(f"Tensões em pu - {nome_alimentador}", fontsize=16)
        plt.xlabel("Índice do nó", fontsize=12)
        plt.ylabel("Tensão (pu)", fontsize=12)
        plt.legend()
        plt.grid(True, linestyle=':', linewidth=0.8)
        plt.tight_layout()
        plt.show()

    print(f"Iterações para {nome_alimentador} na última simulação: {iteracoes_ultimas}")
    print(f"Tempo total para 288 simulações em {nome_alimentador}: {total_tempo:.2f} segundos")

    if iteracoes_ultimas == 100:
        return nome_alimentador
    return None


if __name__ == "__main__":
    caminho_base = r"C:\TED_MIDR\modelagens\cenario_2"
    arquivos_dss = []

    for root, _, files in os.walk(caminho_base):
        for nome_arquivo in files:
            if nome_arquivo.lower() == "run_cargas_agregadas.dss":
                arquivos_dss.append(os.path.join(root, nome_arquivo))

    for caminho in arquivos_dss:
        processar_alimentador(caminho)
