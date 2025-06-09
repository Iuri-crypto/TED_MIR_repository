import os
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

            # Reconfigura o ambiente DSS
            dss.text("Clear")
            dss.text("Set DefaultBaseFrequency=60")
            dss.text(f"Compile {caminho_arquivo}")
            dss.solution.max_iterations = 100
            dss.solution.max_control_iterations = 100
            dss.solution.solve()

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
0