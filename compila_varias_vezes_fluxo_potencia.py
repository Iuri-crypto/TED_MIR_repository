import os
import subprocess
import numpy as np
import matplotlib.pyplot as plt
import json


import py_dss_interface

dss = py_dss_interface.DSS()

class ModelaAlimentadores:
    def __init__(self, caminho_alimentadores):
        self.alimentadores_recebidos = caminho_alimentadores

    def Nomes_de_todos_alimentadores(self):
        """ Esta função guarda o nome de todos os alimentadores """
        pastas = []
        if isinstance(self.alimentadores_recebidos, str) and os.path.isdir(self.alimentadores_recebidos):
            for item in sorted(os.listdir(self.alimentadores_recebidos)):  # Ordenar para garantir sequência correta
                caminho_item = os.path.join(self.alimentadores_recebidos, item)
                if os.path.isdir(caminho_item):
                    pastas.append(item)
        else:
            print(f"O diretório base {self.alimentadores_recebidos} não existe ou não é válido.")

        return np.array(pastas)

def compilar_fluxo_opendss(script_path, alimentadores, alimentadores_recebidos, inicio="001043"):
    """ Compila e executa 'fluxo_opendss.py' para cada alimentador """
    if inicio in alimentadores:
        indice_inicio = np.where(alimentadores == inicio)[0][0]
        alimentadores = alimentadores[indice_inicio:]
    else:
        print(f"Alimentador '{inicio}' não encontrado. Executando todos os alimentadores.")

    for alimentador in alimentadores:
        print(f"Executando '{script_path}' para o alimentador: {alimentador}")

        # Executa o script e captura a saída
        subprocess.run(["python", script_path, alimentador, alimentadores_recebidos])

def plotar_resultados_grafico(caminho_arquivo_resultados, alimentadores):
    """ Esta função abre o arquivo de resultados e plota um gráfico de iterações vs potência das cargas """

    try:
        # Lê o arquivo JSON com os resultados
        with open(caminho_arquivo_resultados, 'r') as f:
            resultados = json.load(f)

        # Aqui, estamos assumindo que o arquivo JSON tem uma estrutura de dicionário,
        # onde cada alimentador tem dados sobre iterações e potência da carga.

        alimentadores_unicos = list(resultados.keys())
        iteracoes = []

        # Para cada alimentador, coletamos as iterações
        for alimentador in alimentadores_unicos:
            dados_alimentador = resultados[alimentador]
            iteracoes.append(dados_alimentador['iteracoes'])  # Coleta as iterações

        # Contar quantos alimentadores tem iterações >= 1000
        alimentadores_maiores_1000 = sum(1 for it in iteracoes if it >= 1000)

        # Vamos plotar os resultados
        plt.figure(figsize=(10, 6))

        # Cria um gráfico de dispersão para as iterações
        fig, ax = plt.subplots()

        # Cria um gráfico de pontos (dispersão)
        ax.scatter(np.arange(len(alimentadores_unicos)), iteracoes, color='tab:blue', marker='o')

        # Ajustando os rótulos dos eixos
        ax.set_xlabel('Alimentadores (Indexados)', fontsize=12)
        ax.set_ylabel('Número de Iterações', color='tab:blue', fontsize=12)
        ax.tick_params(axis='y', labelcolor='tab:blue')

        # Adicionando o rótulo com a quantidade de alimentadores com iterações >= 1000
        ax.text(0.95, 0.95, f'Alimentadores com iteracoes >= 1000: {alimentadores_maiores_1000}',
                transform=ax.transAxes, ha='right', fontsize=12, color='red', bbox=dict(facecolor='white', alpha=0.7))

        # Coloca o título
        plt.title('Iterações por Alimentador (indexados)', fontsize=14)
        plt.tight_layout()

        plt.show()

    except Exception as e:
        print(f"Erro ao abrir ou processar o arquivo de resultados: {e}")

if __name__ == "__main__":
    caminho_alimentadores = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISAMT\ALIMENTADORES"
    caminho_iteracoes = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISAMT\resultados de iteracoes\potencia de 100kw por carga\iteracoes.json"
    alimentadores = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISAMT\ALIMENTADORES"
    modela = ModelaAlimentadores(caminho_alimentadores)
    lista_alimentadores = modela.Nomes_de_todos_alimentadores()

    script_fluxo = r"C:\BDGD\ultimamodelagem\BDGD_COM_PYTHON\RODA FLUXO DE POTENCIA OPENDSS\FLUXO_2.py"

    compilar_fluxo_opendss(script_fluxo, lista_alimentadores, alimentadores, inicio="001043")

    # Chama a função para plotar os resultados
    plotar_resultados_grafico(caminho_iteracoes, lista_alimentadores)
