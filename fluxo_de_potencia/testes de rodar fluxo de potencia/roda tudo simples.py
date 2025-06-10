
import os
import py_dss_interface
import matplotlib.pyplot as plt
import sys
import time
import random



dss = py_dss_interface.DSS()


alimentador = str(sys.argv[1])


class Fluxo_Potencia:
    """ Classe para calcular o fluxo de potência nos alimentadores """

    def __init__(self, db_alimentadores_recebidos, modelagens):

        self.alimentadores_recebidos = db_alimentadores_recebidos
        self.caminhos_modelagens = modelagens



#***********************************************************************************************************************
    def Envia_Dados_alimentador_OpenDSS(self, alimentador):
        """ Este método envia todos os dados do alimentador para dentro do OpenDSS """

        for path in self.caminhos_modelagens:
            caminho = os.path.join(path, str(alimentador))
            if os.path.isdir(caminho):
                for arquivo in os.listdir(caminho):
                    caminho_arquivo = os.path.join(caminho, arquivo)
                    if os.path.isfile(caminho_arquivo):
                        caminho_arquivo = caminho_arquivo.replace('/', '\\')
                        try:
                            with open(caminho_arquivo, encoding='utf-8') as file:
                                for linha in file:
                                    dss.text(linha.strip())
                        except Exception as e:
                            print("Erro ao abrir o arquivo {}: {}".format(caminho_arquivo, e))
                    else:
                       continue

        return 0


#***********************************************************************************************************************
    def Convergencia(self):
        """ Este método ele força o modelo de carga mudar para z constante,
        melhora a convergencia do fluxo de potência """

        dss.solution.solve()
        nomes_cargas = dss.loads.names
        dss.loads.first()
        for nome in nomes_cargas:
            dss.text("load.{}.vminpu = 0.85".format(nome))
            dss.text("load.{}.vmaxpu = 1.15".format(nome))
            dss.loads.next()
        return []

    # ***********************************************************************************************************************



    def graficos(self):
        """ Método para gerar gráficos de tensões e potência total """

        dss.solution.solve()  # Resolver o circuito antes de pegar os dados
        #dss.text('show isolated')

        print("Iterações necessárias: {}".format(dss.solution.iterations))

        dados_tensoes = [
            100000, 110, 115, 120, 121, 125, 127, 208, 216, 216.5, 220, 230, 231, 240, 254, 380, 400,
            440, 480, 500, 600, 750, 1000, 2200, 3200, 3600, 3785, 3800, 3848, 3985, 4160, 4200, 4207,
            4368, 4560, 5000, 6000, 6600, 6930, 7960, 8670, 11400, 11900, 12000, 12600, 12700, 13200,
            13337, 13530, 13800, 13860, 14140, 14190, 14400, 14835, 15000, 15200, 19053, 19919, 21000,
            21500, 22000, 23000, 23100, 23827, 24000, 24200, 25000, 25800, 27000, 30000, 33000, 34500,
            36000, 38000, 40000, 44000, 45000, 45400, 48000, 60000, 66000, 69000, 72500, 88000, 88200,
            92000, 100000, 120000, 121000, 123000, 131600, 131630, 131635, 138000, 145000, 230000,
            245000, 345000, 500000, 550000, 750000, 1000000
        ]

        # Criar lista de barramentos e tensões, excluindo neutro (.4)
        barras_e_tensoes = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag))
        barras_e_tensoes = [tupla for tupla in barras_e_tensoes if not tupla[0].endswith('.4')]

        # Remover barramentos com tensões inválidas (None ou NaN)
        barras_e_tensoes = [(b, t) for b, t in barras_e_tensoes if t is not None and not isinstance(t, str)]

        # Função para encontrar a tensão base mais próxima
        def encontrar_tensao_base(valor):
            return min(dados_tensoes, key=lambda x: abs(x - valor))

        # Calcular tensões em pu
        barras_e_tensoes_pu = [(b, t / encontrar_tensao_base(t)) for b, t in barras_e_tensoes]

        # Atualizar a lista original com as tensões em pu
        barras_e_tensoes = barras_e_tensoes_pu

        if not barras_e_tensoes:
            print("Nenhum dado de tensão válido encontrado.")
            return

        # Filtrar 10% das tensões aleatoriamente (mínimo 1 ponto)
        num_amostras = max(1, int(0.5 * len(barras_e_tensoes)))
        if len(barras_e_tensoes) > num_amostras:
            barras_e_tensoes_filtradas = random.sample(barras_e_tensoes, num_amostras)
        else:
            barras_e_tensoes_filtradas = barras_e_tensoes  # Caso tenha poucos valores, usa todos

        # Separar barramentos e tensões filtradas
        barramentos = [tupla[0] for tupla in barras_e_tensoes_filtradas]
        tensoes = [tupla[1] for tupla in barras_e_tensoes_filtradas]

        # Criar gráfico de dispersão das tensões por barramento
        plt.figure(figsize=(10, 5))
        plt.scatter(range(len(tensoes)), tensoes, color='blue', alpha=0.7, label="Tensão (pu)")
        plt.axhline(y=1.05, color='r', linestyle='--', label='Limite Superior (1.05 pu)')
        plt.axhline(y=0.95, color='r', linestyle='--', label='Limite Inferior (0.95 pu)')
        plt.xlabel("Índice do Barramento")
        plt.ylabel("Tensão (pu)")
        plt.title(f"Tensões por Barramento (Amostra de {num_amostras} pontos)")

        # Ajuste para muitos barramentos
        if len(barramentos) > 20:
            plt.xticks([])  # Remove rótulos do eixo X para evitar sobreposição
        else:
            plt.xticks(range(len(barramentos)), barramentos, rotation=90)

        plt.legend()
        plt.grid()
        plt.show(block=False)  # Permite que o código continue rodando
        plt.draw()  # Garante a renderização correta
        plt.show()  # Exibe o gráfico na tela
        plt.pause(3)  # Espera 3 segundos
        plt.close()  # Fecha o gráfico
        # Potência total da subestação
        ativo = round(dss.circuit.total_power[0], 4)
        reativo = round(dss.circuit.total_power[1], 4)

        # Criar gráfico de barras para potência ativa e reativa
        plt.figure(figsize=(6, 5))
        plt.bar(["Ativo (MW)", "Reativo (MVAr)"], [ativo, reativo], color=['green', 'red'])
        plt.ylabel("Potência (MW / MVAr)")
        plt.title("Potência Total da Subestação")
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        plt.show(block=False)  # Permite que o código continue rodando
        plt.draw()  # Garante a renderização correta
        plt.show()  # Exibe o gráfico na tela
        plt.pause(3)  # Espera 3 segundos
        plt.close()  # Fecha o gráfico


if __name__ == "__main__":
########################################################################################################################
########################################      CAMINHOS DE DADOS DAS MODELAGENS     ######################################
########################################################################################################################
    alimentadores = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\BARRA_SLACK"

    caminhos_modelagens = [
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\BARRA_SLACK",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LINECODES_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LINECODES_RAMAIS_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LINECODES_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\CHAVES_SECCIONADORAS_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\CHAVES_SECCIONADORAS_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\COMPENSADORES_REATIVO_BAIXA",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\COMPENSADORES_REATIVO_MEDIA",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LINHAS_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LINHAS_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RAMAIS_LIGACAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\GERACAO_SHAPE_GERACAO_DISTRIBUIDA_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\REGULADORES_TENSAO_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\TRANSFORMADORES_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\GERADORES_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\CARGAS_BAIXA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\CARGAS_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\CARGAS_POSTE_ILUMINACAO_PUBLICA",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\TENSOES_BASE",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\GERACAO_SHAPE_GERACAO_DISTRIBUIDA_MEDIA_TENSAO",
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\TENSOES_BASE"]

    modelagem_curva_carga = [r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LOADSHAPE_CARGAS_BAIXA_TENSAO",
                            r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LOADSHAPE_CARGAS_MEDIA_TENSAO" ]

    modelagem_linhas_media_geometria = r"C:\MODELAGEM_LINHAS_MEDIA_TENSAO_BDGD_2023_ENERGISA_GEOMETRIA_POSTES"




#-----------------------------------------------------------------------------------------------------------------------
#-------------------------------- PARÂMETROS DE ENTRADA PARA RODAR O FLUXO DE POTÊNCIA ---------------------------------
#-----------------------------------------------------------------------------------------------------------------------

    max_iteracoes = 100

#***********************************************************************************************************************

    # Chama a Classe
    Classe = Fluxo_Potencia(alimentadores, caminhos_modelagens)

    inicio = time.time()

    # Chama os métodos
    dss.text('Clear')
    dss.text('Set DefaultBaseFrequency=60')


    # Este método envia todos os dados das modelagens do alimentador escolhido para dentro do OpenDSS usando comandos 'dss.text()'
    status_1 = Classe.Envia_Dados_alimentador_OpenDSS(alimentador)

    # Este método atribui a mudança de carga para o modelo z cte caso as tensões divergem muito do 1 pu
    status_2 = Classe.Convergencia()

    dss.solution.max_iterations = max_iteracoes
    dss.text('set maxcontroliter=100')

    # Método simula todos os alimentadores
    Classe.graficos()



    fim = time.time()
    print('Tempo de execução: ', fim - inicio)