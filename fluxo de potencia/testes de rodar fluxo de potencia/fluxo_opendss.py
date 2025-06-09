
import os
import py_dss_interface
import matplotlib.pyplot as plt
import sys
import time
import json



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

    def resolver_fluxo(self, rec_aminho_iteracoes):
        """ Método para resolver o fluxo de potência e retornar o número de iterações """
        dss.errorinterface.allow_forms = False

        nomes_cargas = dss.loads.names
        dss.loads.first()
        for nome in nomes_cargas:
            
            dss.loads._kw = 10
        
        dss.solution.solve()
        iteracoes = dss.solution.iterations
        print(f"ITERACOES: {iteracoes}")  # Para captura externa

        # Garante que a pasta existe
        os.makedirs(rec_aminho_iteracoes, exist_ok=True)

        # Define o caminho do arquivo JSON dentro da pasta
        caminho_arquivo = os.path.join(rec_aminho_iteracoes, "iteracoes.json")

        # Se o arquivo existir, carrega os dados; senão, cria um novo dicionário
        if os.path.exists(caminho_arquivo):
            try:
                with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
                    dados = json.load(arquivo)
            except (json.JSONDecodeError, IOError):
                dados = {}  # Se houver erro ao ler, inicia um novo dicionário
        else:
            dados = {}

        # Atualiza os dados do alimentador
        dados[alimentador] = {
            "iteracoes": iteracoes,
            "potencia_carga_kw": 1.0
        }

        # Salva no arquivo JSON
        with open(caminho_arquivo, 'w', encoding='utf-8') as arquivo:
            json.dump(dados, arquivo, indent=4, ensure_ascii=False)




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

    caminho_iteracoes = r"C:/BDGD_DISTRIBUIDORAS_BRASIL/BDGD_2023_ENERGISAMT/resultados de iteracoes/potencia de 10kw por carga"




#-----------------------------------------------------------------------------------------------------------------------
#-------------------------------- PARÂMETROS DE ENTRADA PARA RODAR O FLUXO DE POTÊNCIA ---------------------------------
#-----------------------------------------------------------------------------------------------------------------------

    max_iteracoes = 1000

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
    dss.text('set maxcontroliter=1000')

    # Método simula todos os alimentadores
    Classe.resolver_fluxo(caminho_iteracoes)

    fim = time.time()
    print(f'Tempo de execução: {fim - inicio} segundos')




