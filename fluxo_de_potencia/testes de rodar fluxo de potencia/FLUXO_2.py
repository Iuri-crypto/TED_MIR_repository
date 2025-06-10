import math
import os
import py_dss_interface
import numpy as np
import random
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import sys
import time
import json



dss = py_dss_interface.DSS()

#
alimentador = str(sys.argv[1])
alimentadores_recebidos = str(sys.argv[2])
#
#alimentador = '001043'
#alimentadores_recebidos = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISAMT\ALIMENTADORES"
#
# if len(sys.argv) > 0:
#     dia = str(sys.argv[1])
#     ponto_simulacao = int(sys.argv[2])
#     resposta = str(sys.argv[3])
# else:
#     print('Nenhum argumento foi passado.')

#

# ponto_simulacao = 54
# dia = 'SA'
#

#alimentador = str(sys.argv[1])

class Fluxo_Potencia:
    """ Classe para calcular o fluxo de potência nos alimentadores """

    def __init__(self, db_alimentadores_recebidos, db_caminhos_modelagens, db_irradiancia_96, db_alimentador,
                 db_medidores_pnt, db_mes, db_max_iteracoes, db_on_off,
                 db_percentual_tensoes, db_caminho_vmag_imag_kw_kvar, db_nome_arquivo_vmag_imag_kw_kvar,
                 db_caminho_coleta_dados_perdas_nao_tecnicas, db_nome_arquivo_coleta_dados_perdas_nao_tecnicas,
                 db_nome_informacoes_tensao, db_nome_numero_cargas_por_fase, db_nome_cargas_fraudulentas,
                 db_nome_potencia_total_subestacao, db_nome_carga_regiao, db_coletar_maximas_e_minimas_tensoes,
                 db_coletar_tensoes_outras_analises, db_coletar_tensoes_mais_recorrentes,
                 db_nome_tensoes_mais_recorrentes_por_fase, db_nome_tensoes_classificadas,
                 db_numero_de_fraudes, db_potencia_das_fraudes_kw, db_caminho_correntes_plot, db_correntes_plot):

        self.alimentadores_recebidos = db_alimentadores_recebidos
        self.caminhos_modelagens = db_caminhos_modelagens
        self.irradiancia_96 = db_irradiancia_96
        self.alimentador = db_alimentador
        self.medidores = db_medidores_pnt
        self.mes = db_mes
        self.max_iteracoes = db_max_iteracoes
        self._on_off = db_on_off
        self.percentual_tensoes = db_percentual_tensoes


        self.caminho_vmag_imag_kw_kvar = db_caminho_vmag_imag_kw_kvar
        self.nome_arquivo_vmag_imag_kw_kvar = db_nome_arquivo_vmag_imag_kw_kvar
        self.caminho_coleta_dados_perdas_nao_tecnicas = db_caminho_coleta_dados_perdas_nao_tecnicas
        self.nome_arquivo_coleta_dados_perdas_nao_tecnicas = db_nome_arquivo_coleta_dados_perdas_nao_tecnicas
        self.nome_tensoes_mais_recorrentes_por_fase = db_nome_tensoes_mais_recorrentes_por_fase


        self.nome_informacoes_tensao = db_nome_informacoes_tensao
        self.nome_numero_cargas_por_fase = db_nome_numero_cargas_por_fase
        self.nome_cargas_fraudulentas = db_nome_cargas_fraudulentas
        self.nome_potencia_total_subestacao = db_nome_potencia_total_subestacao
        self.nome_carga_regiao = db_nome_carga_regiao
        self.nome_tensoes_classificadas = db_nome_tensoes_classificadas
        
        
        # Plot no mapa
        self.caminho_correntes_plot = db_caminho_correntes_plot
        self.correntes_plot = db_correntes_plot
        self.dados_corrente = []


        self.coletar_maximas_e_minimas_tensoes = db_coletar_maximas_e_minimas_tensoes
        self.coletar_tensoes_outras_analises = db_coletar_tensoes_outras_analises
        self.coletar_tensoes_mais_recorrentes = db_coletar_tensoes_mais_recorrentes
        self.tensoes_classificadas = {}

        self.tensoes_por_fase = {}
        self.potencias_por_regiao = {}
        self.potencia_sub = {}
        self.associa_carga_na_regiao = {}
        self.barras_e_tensoes_somente_cargas = []
        self.barramentos = []
        self.dados_existentes = {}
        self.cargas_por_fase = {}
        self.informacoes_tensao_adicionais = {}
        self.tensoes_maximas_e_minimas = {}
        self.cargas_barramentos = {
            'medidor_00': [],
            'medidor_01': [],
            'medidor_02': [],
            'medidor_03': [],
            'medidor_04': []}

        self.numero_de_fraudes = db_numero_de_fraudes
        self.potencia_das_fraudes_kw = db_potencia_das_fraudes_kw




#***********************************************************************************************************************
    def Nomes_de_todos_alimentadores(self):
        """ Esta função guarda o nome de todos os alimentadores """

        pastas = list()
        if isinstance(self.alimentadores_recebidos, str):
            if os.path.isdir(self.alimentadores_recebidos):
                for item in os.listdir(self.alimentadores_recebidos):
                    caminho_item = os.path.join(self.alimentadores_recebidos, item)
                    if os.path.isdir(caminho_item):
                        pastas.append(item)
            else:
                print("O diretório base {} não existe ou não é válido.".format(self.alimentadores_recebidos))
        else:
            print("Erro: alimentadores_recebidos não é uma lista. Tipo recebido: {}".format(
                type(self.alimentadores_recebidos)))

        return np.array(pastas)
#***********************************************************************************************************************
    def Envia_Dados_alimentador_OpenDSS(self, rec_alimentador, rec_alimentadores_recebidos):
        """ Este método envia todos os dados do alimentador para dentro do OpenDSS """
        path = rec_alimentadores_recebidos
        caminho = os.path.join(path, str(rec_alimentador))
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
                    if arquivo == "geracao_shape_mes_{}".format(self.mes):
                        subdiretorio = os.path.join(caminho, arquivo)
                        if os.path.isdir(subdiretorio):
                            for subarquivo in os.listdir(subdiretorio):
                                caminho_subarquivo = os.path.join(subdiretorio, subarquivo)
                                if os.path.isfile(caminho_subarquivo):
                                    caminho_subarquivo = caminho_subarquivo.replace('/', '\\')
                                    try:
                                        with open(caminho_subarquivo) as subfile:
                                            for sublinha in subfile:
                                                dss.text(
                                                    sublinha.strip())
                                    except Exception as e:
                                        print("Erro ao abrir o arquivo {}: {}".format(caminho_arquivo, e))

        return 0
#***********************************************************************************************************************
    def Inserir_Energy_Meters(self, db_resposta):
        """ ESTE MÉTODO INSERE OS MEDIDORES DE ENERGIA
         NAS LINHAS PARA MONITORAR DETERMINADAS REGIÕES DE
         INTERESSE PARA A ANÁLISE DE CONSUMO """

        nomes_dos_medidores_das_zonas = []
        if db_resposta == 'não':
            print('Não será feito análise de perdas não técnicas no alimentador: {}'.format(self.alimentador))
        else:
            for medidor in self.medidores:
                nome_medidor, _, _, cod_id_linha = medidor
                dss.text("New Energymeter.{}  Line.{}  1".format(nome_medidor, cod_id_linha))
                nomes_dos_medidores_das_zonas.append((nome_medidor, cod_id_linha))
            return nomes_dos_medidores_das_zonas
#***********************************************************************************************************************
    def Coleta_Dados_das_Regioes_com_Energy_Meters(self):
        """ ESTE MÉTODO COLETA OS DADOS DE TODOS OS ELEMENTOS
        {CARGAS} QUE ESTÃO EM CADA REGIÃO MONITORADA POR ENERGYMETER
        DE MODO QUE EU TENHO ACESSO A TODAS AS CARGAS QUE ESTÃO NAQUELA
        DETERMINADA REGIÃO """

        regioes = {}
        dss.solution.solve()
        dss.meters.sample()
        todos_medidores = dss.meters.names
        dss.meters.first()
        for medidor in todos_medidores:
            regioes[medidor] = dss.meters.all_pce_in_zone
            dss.meters.next()
        return regioes
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
#***********************************************************************************************************************
    def Barramentos_Associados_as_cargas(self, recebe_02_regioes_monitoradas):
        """ Coleta dos nomes dos barramentos associados a cargas """

        db_medidor_00 = recebe_02_regioes_monitoradas['medidor_00']
        db_medidor_01 = recebe_02_regioes_monitoradas['medidor_01']
        db_medidor_02 = recebe_02_regioes_monitoradas['medidor_02']
        db_medidor_03 = recebe_02_regioes_monitoradas['medidor_03']
        db_medidor_04 = recebe_02_regioes_monitoradas['medidor_04']

        cargas_barramentos = {
            'medidor_00':[],
            'medidor_01':[],
            'medidor_02':[],
            'medidor_03':[],
            'medidor_04':[]
        }

        dss.loads.first()
        for _ in range(dss.loads.count):
            nome_carga = dss.loads.name
            nome_carga = 'Load.{}'.format(nome_carga)
            nome_barramento = dss.cktelement.bus_names

            cargas = (nome_barramento, nome_carga)

            if nome_carga in db_medidor_00:
                cargas_barramentos['medidor_00'].append(cargas)
            elif nome_carga in db_medidor_01:
                cargas_barramentos['medidor_01'].append(cargas)
            elif nome_carga in db_medidor_02:
                cargas_barramentos['medidor_02'].append(cargas)
            elif nome_carga in db_medidor_03:
                cargas_barramentos['medidor_03'].append(cargas)
            elif nome_carga in db_medidor_04:
                cargas_barramentos['medidor_04'].append(cargas)
            dss.loads.next()
        return cargas_barramentos
#***********************************************************************************************************************
    import os

    def Carregar_Dados_Curvas_Carga(self, alimentador, alimentadores_recebidos_caminho):
        """ Esta função carrega na memória RAM os dados das curvas de carga
        de média tensão e de baixa tensão para cada mês e os armazena em listas. """

        # Dicionários para armazenar as cargas
        from_cargas = {}

        path = alimentadores_recebidos_caminho


        caminho_alimentador = os.path.join(path, str(alimentador))
        caminho_mes_nome = os.path.join(caminho_alimentador, str(mes))

        # Verifica se o diretório existe antes de acessá-lo
        if os.path.isdir(caminho_mes_nome):

            # Percorrer os arquivos no diretório
            for arquivo in os.listdir(caminho_mes_nome):
                caminho_arquivo = os.path.join(caminho_mes_nome, arquivo)

                if os.path.isfile(caminho_arquivo):
                    nome_do_arquivo = os.path.splitext(arquivo)[0]  # Pega o nome do arquivo sem extensão

                    # Lê o conteúdo do arquivo
                    with open(caminho_arquivo) as file:
                        conteudo_curvas = file.read()


                        # Inicializa a estrutura de dados para este arquivo, se não existir
                        if nome_do_arquivo not in from_cargas:
                            from_cargas[nome_do_arquivo] = {}

                        # Processa as cargas para baixa tensão
                        for linha in conteudo_curvas.splitlines():
                            nome_carga, valores = linha.split(":")
                            valores_limpos = valores.strip().strip('[]')
                            lista_valores = []

                            # Preenche a lista de valores de carga
                            for v in valores_limpos.split(','):
                                try:
                                    lista_valores.append(float(v.strip()))  # Converte os valores para float
                                except ValueError as e:
                                    print(f"Erro ao converter para float: '{v}'. Detalhes: {e}")
                                    continue  # Ignora este valor

                            # Armazena os dados no dicionário apropriado
                            from_cargas[nome_do_arquivo][nome_carga] = lista_valores

        return from_cargas

    #*******************************************************************************************************************
    def Cria_Dicionarios(self, db_recebe_nomes_dos_medidores_das_zonas, resposta):
        """ Este método é resposável pela inicialização dos dicionários """

        if resposta == 'sim':
            from_energy_meter = {}
            tensao = {}
            corrente = {}
            loads_model = {}
            gd_kwh = {}


            for _, (medidor, linha_medida) in enumerate(db_recebe_nomes_dos_medidores_das_zonas):
                if medidor not in from_energy_meter:
                    from_energy_meter[medidor] = {}

                for indice, ponto_da_simulacao in enumerate(self.irradiancia_96):
                    # Inicializar as chaves principais para cada ponto de simulação
                    if indice not in from_energy_meter:
                        from_energy_meter[medidor][indice] = {}

                    # Inicializar as subchaves para cada ponto de simulação
                    if 'Zona_consumo_kwh' not in from_energy_meter[medidor][indice]:
                        from_energy_meter[medidor][indice]['Zona_consumo_kwh'] = {}
                    if 'Zona_consumo_kvarh' not in from_energy_meter[medidor][indice]:
                        from_energy_meter[medidor][indice]['Zona_consumo_kvarh'] = {}
                    if 'Zona_perdas_kwh' not in from_energy_meter[medidor][indice]:
                        from_energy_meter[medidor][indice]['Zona_perdas_kwh'] = {}
                    if 'Zona_perdas_kvarh' not in from_energy_meter[medidor][indice]:
                        from_energy_meter[medidor][indice]['Zona_perdas_kvarh'] = {}
            return from_energy_meter
        else:
            return 0
    #*******************************************************************************************************************
    def Aplica_Curvas_Geracao_Paineis_Fotovoltaicos(self, db_valor_irradiancia):
        """ Este método aplica as curvas de geração dos paineis fotovoltaicos """

        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            dss.pvsystems.irradiance = db_valor_irradiancia
            dss.pvsystems.next()
#***********************************************************************************************************************
    def coleta_dados_vmag_imag_kw_kvar(self, dia, ponto_simulacao):
        """ Este método coleta todos os dados necessários """


        # Cria tupla de tensão + barramento, excluindo a tensão do neutro(.4)
        barras_e_tensoes = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag))
        barras_e_tensoes = [tupla for tupla in barras_e_tensoes if not tupla[0].endswith('.4')]


        if dia not in self.tensoes_maximas_e_minimas:
            self.tensoes_maximas_e_minimas[dia] = {}
        if ponto_simulacao not in self.tensoes_maximas_e_minimas[dia]:
            self.tensoes_maximas_e_minimas[dia][ponto_simulacao] = {}
        if 'maior_tensao' not in self.tensoes_maximas_e_minimas[dia][ponto_simulacao]:
            self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['maior_tensao'] = {}
        if "menor_tensao" not in self.tensoes_maximas_e_minimas[dia][ponto_simulacao]:
            self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['menor_tensao'] = {}
        if "tensao_abaixo_0.1" not in self.tensoes_maximas_e_minimas[dia][ponto_simulacao]:
            self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['tensao_abaixo_0.1'] = {}

    #*******************************************************************************************************************
    #**********************************          coleta de dados         **********************************************
    #*******************************************************************************************************************



        ####################################### Coleta apenas a maior tensão e a menor tensão de cada ponto de simulação
        tensoes_plotadas = []
        tensoes_nao_plotadas = []
        valores = [13800 / math.sqrt(3), 380 / math.sqrt(3), 220 / math.sqrt(3), 230 / math.sqrt(3)]
        if self.coletar_maximas_e_minimas_tensoes:
            for tupla in barras_e_tensoes:
                barramento, tensao = tupla
                if tensao > 0.1:
                    valor_mais_proximo = min(valores, key=lambda v: abs(tensao - v))
                    tensoes_plotadas.append(tensao / valor_mais_proximo)
                elif tensao <= 0.1 and tensao > 0:
                    tensoes_nao_plotadas.append(tensao)
            if tensoes_plotadas:
                self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['maior_tensao'] = max(tensoes_plotadas)
                self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['menor_tensao'] = min(tensoes_plotadas)
            if tensoes_nao_plotadas:
                self.tensoes_maximas_e_minimas[dia][ponto_simulacao]['tensao_abaixo_0.1'] = tensoes_nao_plotadas



        ####################################### Coleta as tensões mais recorrentes
        # Coleta o nome do barramento de cada carga
        if self.coletar_tensoes_mais_recorrentes:
            # dss.loads.first()
            # for _ in range(dss.loads.count):
            #     barramentos = dss.cktelement.bus_names
            #     for barra in barramentos:
            #         barra = barra.split('.')[0]
            #         self.barramentos.append(barra)
            #     dss.loads.next()
            #
            # # Coleta apenas as tensões de barramentos que contém cargas
            # for tupla in barras_e_tensoes:
            #     barramento, tensao = tupla
            #     nome_carga = barramento.split('.')[0]
            #     if nome_carga in self.barramentos:
            #         self.barras_e_tensoes_somente_cargas.append(tupla)
            tensoes_por_fase_tem = {}



            if dia not in self.tensoes_por_fase:
                self.tensoes_por_fase[dia] = {}
            if ponto_simulacao not in self.tensoes_por_fase[dia]:
                self.tensoes_por_fase[dia][ponto_simulacao] = {}
            if 'fase A' not in self.tensoes_por_fase[dia][ponto_simulacao]:
                self.tensoes_por_fase[dia][ponto_simulacao]['fase A'] = []
            if 'fase B' not in self.tensoes_por_fase[dia][ponto_simulacao]:
                self.tensoes_por_fase[dia][ponto_simulacao]['fase B'] = []
            if 'fase C' not in self.tensoes_por_fase[dia][ponto_simulacao]:
                self.tensoes_por_fase[dia][ponto_simulacao]['fase C'] = []

            if dia not in self.tensoes_classificadas:
                self.tensoes_classificadas[dia] = {}
            if ponto_simulacao not in self.tensoes_classificadas[dia]:
                self.tensoes_classificadas[dia][ponto_simulacao] = {}
            if 'fase A' not in self.tensoes_classificadas[dia][ponto_simulacao]:
                self.tensoes_classificadas[dia][ponto_simulacao]['fase A'] = {}
            if 'fase B' not in self.tensoes_classificadas[dia][ponto_simulacao]:
                self.tensoes_classificadas[dia][ponto_simulacao]['fase B'] = {}
            if 'fase C' not in self.tensoes_classificadas[dia][ponto_simulacao]:
                self.tensoes_classificadas[dia][ponto_simulacao]['fase C'] = {}

            tn = 1

            def classificar_tensao(tl, tn):
                if 0.92 * tn <= tl <= 1.05 * tn:
                    return 'Adequada 0.92 a 1.05'
                elif (0.87 * tn <= tl < 0.92 * tn) or (1.05 * tn < tl <= 1.06 * tn):
                    return 'Precaria 0.87 a 0.92 ou 1.05 a 1.06'
                elif (0.7 * tn <= tl < 0.87 * tn) or (1.06 * tn < tl <= 1.23 * tn):
                    return 'Critica I - entre 0.7 e 0.87 ou entre 1.06 e 1.23'

                elif (0.53 * tn <= tl < 0.7 * tn) or (1.23 * tn < tl <= 1.40 * tn):
                    return 'Critica II - entre 0.53 e 0.7 ou entre 1.23 e 1.40'
                elif (0.36 * tn <= tl < 0.53 * tn) or (1.40 * tn < tl <= 1.67 * tn):
                    return 'Critica III - entre 0.36 e 0.53 ou entre 1.40 e 1.67'
                elif (0.19 * tn <= tl < 0.36 * tn) or (1.67 * tn < tl <= 1.84 * tn):
                    return 'Critica IV - entre 0.19 e 0.36 ou entre 1.67 e 1.84'
                elif tl < 0.19 or 1.84 < tl:
                    return 'Critica V - menor que 0.19 ou maior que 1.84'
                return None



            # Separa as tensões por fase
            for tupla in barras_e_tensoes:
                barramento, tensao = tupla
                valor_mais_proximo = min(valores, key=lambda v: abs(tensao - v))
                if round(tensao, 3) < 0.1:
                    continue
                if barramento.endswith('.1'):
                    self.tensoes_por_fase[dia][ponto_simulacao]['fase A'].append(round(tensao / valor_mais_proximo,3))
                    categoria = classificar_tensao(round(tensao / valor_mais_proximo, 3), tn)
                    if categoria:
                        if categoria not in self.tensoes_classificadas[dia][ponto_simulacao]['fase A']:
                            self.tensoes_classificadas[dia][ponto_simulacao]['fase A'][categoria] = 0
                        self.tensoes_classificadas[dia][ponto_simulacao]['fase A'][categoria] += 1
                elif barramento.endswith('.2'):
                    self.tensoes_por_fase[dia][ponto_simulacao]['fase B'].append(round(tensao / valor_mais_proximo, 3))
                    categoria = classificar_tensao(round(tensao / valor_mais_proximo, 3), tn)
                    if categoria:
                        if categoria not in self.tensoes_classificadas[dia][ponto_simulacao]['fase B']:
                            self.tensoes_classificadas[dia][ponto_simulacao]['fase B'][categoria] = 0
                        self.tensoes_classificadas[dia][ponto_simulacao]['fase B'][categoria] += 1
                elif barramento.endswith('.3'):
                    self.tensoes_por_fase[dia][ponto_simulacao]['fase C'].append(round(tensao / valor_mais_proximo, 3))
                    categoria = classificar_tensao(round(tensao / valor_mais_proximo, 3), tn)
                    if categoria:
                        if categoria not in self.tensoes_classificadas[dia][ponto_simulacao]['fase C']:
                            self.tensoes_classificadas[dia][ponto_simulacao]['fase C'][categoria] = 0
                        self.tensoes_classificadas[dia][ponto_simulacao]['fase C'][categoria] += 1






        ####################################### Coleta outras analises
        #if self.coletar_tensoes_outras_analises:



            # ***************************************************************************************************************





            # with open(caminho_cargas_por_fase, 'w') as arquivo:
            #     json.dump(self.cargas_por_fase, arquivo, indent=4)


    #*******************************************************************************************************************
    def Coleta_Dados_Perdas_nao_Tecnicas(self, db_energy_meter, dia, ponto_simulacao):
        """ Este método destina-se apenas a coleta dos dados para
        a análise de perdas não técnicas nas regiões previamente
        definidas do alimentador em questão """

        import random

        # Função para aplicar o multiplicador aleatório
        menor = 1
        maior = 1.02

        def aplicar_multiplicador(valor):
            return valor * random.uniform(menor, maior)

        # Processo de iteração e atualização de dados
        dss.meters.sample_all()
        dss.meters.first()

        for _ in range(dss.meters.count):
            medidor = dss.meters.name
            kwh = dss.meters.register_values[4]
            kvarh = dss.meters.register_values[5]
            losses_kwh = dss.meters.register_values[12]
            losses_kvarh = dss.meters.register_values[13]

            # Aplicando o multiplicador aleatório nos valores
            kwh = round(aplicar_multiplicador(kwh), 4)
            kvarh = round(aplicar_multiplicador(kvarh), 4)
            losses_kwh = round(aplicar_multiplicador(losses_kwh), 4)
            losses_kvarh = round(aplicar_multiplicador(losses_kvarh), 4)

            if medidor not in db_energy_meter:
                db_energy_meter[medidor] = {}

            db_energy_meter[medidor][ponto_simulacao] = {
                'Zona_consumo_kwh': kwh,
                'Zona_consumo_kvarh': kvarh,
                'Zona_perdas_kwh': losses_kwh,
                'Zona_perdas_kvarh': losses_kvarh
            }
            dss.meters.next()

        # Atualizar os dados existentes com os novos dados
        if dia not in self.potencias_por_regiao:
            self.potencias_por_regiao[dia] = {}

        for medidor, dados in db_energy_meter.items():
            if medidor not in self.potencias_por_regiao[dia]:
                self.potencias_por_regiao[dia][medidor] = {}

            dados = db_energy_meter[medidor][ponto_simulacao]
            self.potencias_por_regiao[dia][medidor][ponto_simulacao] = dados

        dss.text('reset')

    #*******************************************************************************************************************
    def Inserir_Cargas_Fraudes(self, resposta):
        """ Este método insere cargas aleatoriamente no alimentador
        para simular fraudes de energia """


        if resposta == 'sim':
            cargas_fraudes = []
            regioes = {}


            random.seed(42)
            total_loads = dss.loads.names
            random.shuffle(total_loads)
            armazena_cargas_fraudes = random.sample(total_loads, self.numero_de_fraudes)

            for carga in random.sample(total_loads, self.numero_de_fraudes):
                cargas_fraudes.append(carga)


            dss.solution.solve()
            dss.meters.sample()
            todos_medidores = dss.meters.names
            dss.meters.first()
            for medidor in todos_medidores:
                regioes[medidor] = dss.meters.all_pce_in_zone
                dss.meters.next()


            db_medidor_00 = regioes['medidor_00']
            db_medidor_01 = regioes['medidor_01']
            db_medidor_02 = regioes['medidor_02']
            db_medidor_03 = regioes['medidor_03']
            db_medidor_04 = regioes['medidor_04']


            dss.loads.first()
            for _ in range(dss.loads.count):
                nome_carga = dss.loads.name
                nome_carga = 'Load.{}'.format(nome_carga)
                nome_barramento = dss.cktelement.bus_names

                cargas = (nome_barramento, nome_carga)

                if nome_carga in db_medidor_00:
                    self.cargas_barramentos['medidor_00'].append(cargas)
                elif nome_carga in db_medidor_01:
                    self.cargas_barramentos['medidor_01'].append(cargas)
                elif nome_carga in db_medidor_02:
                    self.cargas_barramentos['medidor_02'].append(cargas)
                elif nome_carga in db_medidor_03:
                    self.cargas_barramentos['medidor_03'].append(cargas)
                elif nome_carga in db_medidor_04:
                    self.cargas_barramentos['medidor_04'].append(cargas)
                dss.loads.next()


            dss.solution.solve()
            for carga_fraude in armazena_cargas_fraudes:
                for medidor, valores in self.cargas_barramentos.items():
                    for _, (barramento, carga) in enumerate(valores):
                        if 'Load.{}'.format(carga_fraude) in carga:
                            if medidor not in self.associa_carga_na_regiao:
                                self.associa_carga_na_regiao[medidor] = []
                            self.associa_carga_na_regiao[medidor].append(carga)
            return 0

        else:
            return 0
    #*******************************************************************************************************************
    def coleta_potencia_subestacao(self, dia, ponto_simulacao):
        """Coleta potencias da subestação"""
        if dia not in self.potencia_sub:
            self.potencia_sub[dia] = {}
        if ponto_simulacao not in self.potencia_sub[dia]:
            self.potencia_sub[dia][ponto_simulacao] = {}
        if 'kw' not in self.potencia_sub[dia][ponto_simulacao]:
            self.potencia_sub[dia][ponto_simulacao]['kw'] = 0
        if 'kvar' not in self.potencia_sub[dia][ponto_simulacao]:
            self.potencia_sub[dia][ponto_simulacao]['kvar'] = 0

        self.potencia_sub[dia][ponto_simulacao]['kw'] = round(dss.circuit.total_power[0], 4)
        self.potencia_sub[dia][ponto_simulacao]['kvar'] = round(dss.circuit.total_power[1], 4)
    # *******************************************************************************************************************
  

    def coleta_corrente_linhas(self, dia, ponto_simulacao):
        """ Coleta as correntes dos condutores de média e de baixa tensão """
        
        # Carrega o arquivo JSON com as coordenadas
        caminho_arquivo = "C:/BDGD_DISTRIBUIDORAS_BRASIL/BDGD_2023_ENERGISA_MT/GEOMETRIA_SSDMT/764444/geometria_linha_media_tensao.json"
        with open(caminho_arquivo, 'r') as arquivo:
            dados_geometria = json.load(arquivo)

        # Cria um dicionário com o cod_id como chave e as coordenadas como valor
        coordenadas_dict = {}
        for item in dados_geometria:
            cod_id = item["cod_id"]
            coordenadas = {
                "longitude_inicio": item["longitude_inicio"],
                "latitude_inicio": item["latitude_inicio"],
                "longitude_fim": item["longitude_fim"],
                "latitude_fim": item["latitude_fim"]
            }
            coordenadas_dict[cod_id] = coordenadas

        # Dicionário que irá armazenar as informações de cada linha
        linhas_informacoes = {}

        if dia == 'DU' and ponto_simulacao == 0:
            dss.lines.first()
            
            for _ in range(dss.lines.count):
                nome = dss.lines.name
                corrente_nominal = dss.lines.norm_amps
                corrente_mag_ang = dss.cktelement.currents_mag_ang  

                # Obtém apenas os três primeiros valores de índices pares
                corrente_atual = [corrente_mag_ang[i] for i in range(0, len(corrente_mag_ang), 2)][:3]

                # Calcula a porcentagem da corrente em relação à nominal
                percentual_corrente = max(corrente_atual) / corrente_nominal if corrente_nominal > 0 else 0

                # Define cor e grossura com base na convenção
                if percentual_corrente < 0.5:
                    cor = "green"
                    grossura = 1  # Ajuste para espessura mínima visível no gráfico
                elif 0.5 <= percentual_corrente <= 1.0:
                    cor = "blue"
                    grossura = 2
                else:
                    cor = "red"
                    excesso = percentual_corrente - 1.0
                    grossura = 3 + excesso * 2  # Ajustando para maior visibilidade

                # Verifica se o nome da linha está no dicionário de coordenadas
                if nome in coordenadas_dict:
                    coordenadas = coordenadas_dict[nome]
                    
                    # Cria um novo dicionário para a linha com as informações de coordenadas, cor e espessura
                    informacoes_linha = {
                        "coordenadas": coordenadas,
                        "cor": cor,
                        "espessura": grossura
                    }
                    
                    # Adiciona as informações da linha no dicionário geral
                    linhas_informacoes[nome] = informacoes_linha

                # Avança para a próxima linha
                dss.lines.next()

            # ---- PLOTAGEM DO MAPA ----
        fig, ax = plt.subplots(figsize=(10, 10))

        for nome, info in linhas_informacoes.items():
            coords = info["coordenadas"]
            x_vals = [coords["longitude_inicio"], coords["longitude_fim"]]
            y_vals = [coords["latitude_inicio"], coords["latitude_fim"]]

            ax.plot(x_vals, y_vals, color=info["cor"], linewidth=info["espessura"])

        # Configuração do gráfico
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title("Mapa de Linhas Elétricas")

        plt.show()

        return linhas_informacoes


    


    #********************************************************************************************************************
    def graficos(self, db_cargas_fraudes):
        """ Plotagem de gráficos """
        """Plotagem de gráficos de vela para tensões máximas e mínimas"""



        # #
        # caminho_potencia_total_subestacao = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\nome_potencia_total_subestacao.json"
        # with open(caminho_potencia_total_subestacao) as cps:
        #     potencia_subestcao = json.load(cps)
        #
        # dias = ['DU', 'SA', 'DO']
        # ultimo_ponto = 0
        # pontos_eixo_x = []
        # pontos_totais = []
        # kwh_totais = []
        # pontos_horas = []
        #
        # for dia in dias:
        #     potencia_dia = potencia_subestcao.get(dia, {})
        #     pontos_dia = []
        #     pontos = []
        #     kwh_dia = []
        #
        #     for ponto, chaves in potencia_dia.items():
        #         Zona_consumo_kwh = -1 * chaves.get('kw', 0)
        #         #Zona_consumo_kvarh = chaves.get('kvarh', 0)
        #         pontos.append((int(ponto) * 15))
        #         pontos_dia.append((int(ponto) * 15))
        #         kwh_dia.append(Zona_consumo_kwh)
        #
        #     pontos_dia = [int(ponto) + int(ultimo_ponto) for ponto in pontos_dia]
        #
        #     pontos_totais.extend(pontos_dia)
        #     pontos_eixo_x.extend(pontos)
        #     kwh_totais.extend(kwh_dia)
        #
        #
        #     ultimo_ponto = pontos_dia[-1]
        #
        #
        # # Convertendo os pontos totais de minutos para horas (formato HH:MM)
        # pontos_horas = [(ponto // 60, ponto % 60) for ponto in pontos_eixo_x]  # (horas, minutos)
        #
        # # Formatando para exibição
        # pontos_horas_formatados = [f'{hora:02d}:{minuto:02d}' for hora, minuto in pontos_horas]
        #
        #
        # plt.figure(figsize=(12, 6))
        # # Alterando o fundo do gráfico para um cinza claro
        # plt.gca().set_facecolor('#f0f0f0')
        #
        # plt.plot(pontos_totais[:len(kwh_totais)//3], kwh_totais[:len(kwh_totais)//3], label='Dia Ùtil', color='green')
        # # Conectando o último ponto do "Dia Útil" com o primeiro ponto do "Dia DO"
        # plt.plot([pontos_totais[len(kwh_totais) // 3 - 1], pontos_totais[len(kwh_totais) // 3]],[kwh_totais[len(kwh_totais) // 3 - 1], kwh_totais[len(kwh_totais) // 3]], color='green')
        #
        #
        # plt.plot(pontos_totais[len(kwh_totais) // 3:2 * len(kwh_totais) // 3], kwh_totais[len(kwh_totais) // 3:2 * len(kwh_totais) // 3], label="Sábado", color='orange')
        # # Conectando o último ponto do "Dia DO" com o primeiro ponto do "Dia SA"
        # plt.plot([pontos_totais[2 * len(kwh_totais) // 3 - 1], pontos_totais[2 * len(kwh_totais) // 3]], [kwh_totais[2 * len(kwh_totais) // 3 - 1], kwh_totais[2 * len(kwh_totais) // 3]], color='orange')
        #
        # plt.plot(pontos_totais[2 * len(kwh_totais) // 3:], kwh_totais[2 * len(kwh_totais) // 3:], label="Domingo", color='brown')
        #
        # # Definindo os ticks no eixo X para mostrar as horas
        # plt.xticks(pontos_totais[::12], pontos_horas_formatados[::12], rotation=0)
        #
        # plt.title("Perfil de Carga por Dia", fontsize=16)
        # plt.xlabel('Horários', fontsize=14)
        # plt.ylabel('kwh', fontsize=14)
        #
        # plt.legend()
        #
        # plt.tight_layout()
        #
        # plt.show()
        #
        #
        #
        #
        #
        #
        #
        #
        # caminho_energia_ctmt_alimentador_mes = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\ENERGIA_CONSUMIDA_MES\010009\Energia.bin"
        #
        # with open(caminho_energia_ctmt_alimentador_mes, 'rb') as ctmt_energy:
        #     energy_ctmt = ctmt_energy.read()
        #
        # # Decodificar a string binária para uma string de texto
        # energy_ctmt_str = energy_ctmt.decode('utf-8')
        #
        # # Avaliar a string para converter em uma lista de valores numéricos
        # energia_meses = ast.literal_eval(energy_ctmt_str.split(":")[1].strip())
        #
        # # Agora você tem a lista de energias e pode acessar o valor desejado.
        # energia_mes_escolhido = energia_meses[8]  # por exemplo, o valor do 10º mês (índice 9)
        #
        #
        # caminho_potencia_total_subestacao = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\nome_potencia_total_subestacao.json"
        # with open(caminho_potencia_total_subestacao) as cps:
        #     potencia_subestcao = json.load(cps)
        #
        #
        # dias = ['DU', 'DO', 'SA']
        #
        # Zona_consumo_kwh = 0
        # cores_disponiveis = ['blue', 'green', 'red', 'yellow', 'orange', 'pink', 'purple', 'gray', 'black', 'white',
        #                      'lightblue', 'lightgreen', 'lightcoral', 'lightyellow', 'lightgray', 'darkblue',
        #                      'darkgreen',
        #                      'darkred', 'cyan', 'magenta', 'beige', 'gold', 'silver', 'brown', 'thistle']
        #
        # for dia in dias:
        #     potencia_dia = potencia_subestcao.get(dia, {})
        #
        #     for ponto, chaves in potencia_dia.items():
        #
        #         if dia == 'DU':
        #             Zona_consumo_kwh += -0.25 * 4.29 * 5 * chaves.get('kw', 0)
        #         else:
        #             Zona_consumo_kwh += -0.25 * 4.29 * chaves.get('kw', 0)
        #
        #     print('here')
        #
        # # Calculando o erro percentual
        # erro_percentual = abs(energia_mes_escolhido - Zona_consumo_kwh) / energia_mes_escolhido * 100
        #
        # plt.figure(figsize=(10, 6))
        #
        # # Barras para os dados de consumo de energia
        # labels = ['Ferramenta de Fluxo de Potência', 'Concessionária']
        # values = [Zona_consumo_kwh,
        #           energia_mes_escolhido]  # Os valores das barras (consumo da ferramenta e da concessionária)
        #
        # # Gerando o gráfico de barras com duas barras
        # plt.bar(labels[0], values[0], color='#1f77b4', label='Ferramenta de Fluxo de Potência')  # Azul
        # plt.bar(labels[1], values[1], color='#ff7f0e', label='Concessionária')  # Laranja
        #
        # # Adicionando texto do erro percentual, com posicionamento melhor
        # plt.text(0, Zona_consumo_kwh + 0.05 * max(Zona_consumo_kwh, energia_mes_escolhido),
        #          f'Erro: {erro_percentual:.2f}%', ha='center', fontsize=12, color='black', fontweight='bold')
        #
        # # Título e rótulos
        # plt.title("Comparação do Consumo de Energia", fontsize=18, weight='bold')
        # plt.xlabel('Tipo de Medição', fontsize=14)
        # plt.ylabel('Energia Consumida (kWh)', fontsize=14)
        #
        # # Ajustando o estilo do gráfico
        # plt.xticks(fontsize=12)
        # plt.yticks(fontsize=12)
        #
        # # # Adicionando a legenda (caso você deseje ativá-la)
        # # plt.legend(loc='upper left', fontsize=10, frameon=True, shadow=True, facecolor='white', borderpad=1,
        # #            fancybox=True, borderaxespad=1)
        #
        # # Ajuste de layout
        # plt.tight_layout()
        #
        # # Exibição
        # plt.show()

        # Novos caminhos para os dados adicionais
        caminho_perdas_sem_fraude_com_erro = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\coleta_dados_perdas_nao_tecnicas_sem_fraude_com_erro.json"
        caminho_perdas_com_fraude_com_erro = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\coleta_dados_perdas_nao_tecnicas_com_fraude_com_erro.json"

        with open(caminho_perdas_sem_fraude_com_erro) as c1:
            dados_sem_fraude = json.load(c1)
        with open(caminho_perdas_com_fraude_com_erro) as c2:
            dados_com_fraude = json.load(c2)

        dias = ['DU', 'DO', 'SA']
        soma_energia_medidores_sem_fraude = {}
        soma_energia_medidores_com_fraude = {}

        for dia in dias:

            dados_sem_fraude_dia = dados_sem_fraude.get(dia, {})
            dados_com_fraude_dia = dados_com_fraude.get(dia, {})

            for medidor, dados in dados_sem_fraude_dia.items():
                if medidor not in soma_energia_medidores_sem_fraude:
                    soma_energia_medidores_sem_fraude[medidor] = {}
                if medidor not in soma_energia_medidores_com_fraude:
                    soma_energia_medidores_com_fraude[medidor] = {}
                dados_medidor_com_fraude = dados_com_fraude_dia.get(medidor, {})
                dados_medidor_sem_fraude = dados_sem_fraude_dia.get(medidor, {})

                for ponto_simulacao, dados_ponto_com_fraude in dados_medidor_com_fraude.items():

                    Zona_consumo_kwh_com_fraude = dados_ponto_com_fraude.get('Zona_consumo_kwh', 0)
                    Zona_consumo_kvarh_com_fraude = dados_ponto_com_fraude.get('Zona_consumo_kvarh', 0)
                    Zona_perdas_kwh_com_fraude = dados_ponto_com_fraude.get('Zona_perdas_kwh', 0)
                    Zona_perdas_kvarh_com_fraude = dados_ponto_com_fraude.get('Zona_perdas_kvarh', 0)

                    Zona_consumo_kwh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get(
                        'Zona_consumo_kwh', 0)
                    Zona_consumo_kvarh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get(
                        'Zona_consumo_kvarh', 0)
                    Zona_perdas_kwh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get(
                        'Zona_perdas_kwh', 0)
                    Zona_perdas_kvarh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get(
                        'Zona_perdas_kvarh', 0)

                    if medidor not in soma_energia_medidores_com_fraude:
                        soma_energia_medidores_com_fraude[medidor] = {}
                    if 'Zona_consumo_kwh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] = 0
                    if 'Zona_consumo_kvarh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kvarh'] = 0
                    if 'Zona_perdas_kwh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] = 0
                    if 'Zona_perdas_kvarh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] = 0

                    if medidor not in soma_energia_medidores_sem_fraude:
                        soma_energia_medidores_sem_fraude[medidor] = {}
                    if 'Zona_consumo_kwh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] = 0
                    if 'Zona_consumo_kvarh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kvarh'] = 0
                    if 'Zona_perdas_kwh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] = 0
                    if 'Zona_perdas_kvarh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] = 0

                    if dia != 'DU':
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] += Zona_consumo_kwh_com_fraude
                        soma_energia_medidores_com_fraude[medidor][
                            'Zona_consumo_kvarh'] += Zona_consumo_kvarh_com_fraude
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] += Zona_perdas_kwh_com_fraude
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] += Zona_perdas_kvarh_com_fraude

                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] += Zona_consumo_kwh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor][
                            'Zona_consumo_kvarh'] += Zona_consumo_kvarh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] += Zona_perdas_kwh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] += Zona_perdas_kvarh_sem_fraude


                    else:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] += (
                                    5 * Zona_consumo_kwh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kvarh'] += (
                                    5 * Zona_consumo_kvarh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] += (
                                    5 * Zona_perdas_kwh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] += (
                                    5 * Zona_perdas_kvarh_com_fraude)

                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] += (
                                    5 * Zona_consumo_kwh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kvarh'] += (
                                    5 * Zona_consumo_kvarh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] += (
                                    5 * Zona_perdas_kwh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] += (
                                    5 * Zona_perdas_kvarh_sem_fraude)

        diferencas_perdas_kwh = {}
        com_fraude = soma_energia_medidores_com_fraude

        for medidor, dados in com_fraude.items():
            if medidor not in diferencas_perdas_kwh:
                diferencas_perdas_kwh[medidor] = {}
            dados_medidor_com_fraude = soma_energia_medidores_com_fraude.get(medidor, {})
            dados_medidor_sem_fraude = soma_energia_medidores_sem_fraude.get(medidor, {})

            diferencas_perdas_kwh[medidor] = {}

            perdas_com_fraude_kwh = dados_medidor_com_fraude.get('Zona_perdas_kwh', 0)
            perdas_sem_fraude_kwh = dados_medidor_sem_fraude.get('Zona_perdas_kwh', 0)

            diferenca_kwh = (perdas_com_fraude_kwh - perdas_sem_fraude_kwh)

            diferencas_perdas_kwh[medidor] = {
                'diferença_perda_kwh': round(diferenca_kwh, 4)
            }

        soma = 0
        new_percent = {}
        for medidor, valores in diferencas_perdas_kwh.items():
            soma += diferencas_perdas_kwh[medidor]['diferença_perda_kwh']
            new_percent[medidor] = {}

        for medidor, valores in diferencas_perdas_kwh.items():
            valor = diferencas_perdas_kwh[medidor]['diferença_perda_kwh']
            new_percent[medidor]['diferença_perda_kwh'] = (valor / soma) * 100

        # Prepare os dados para o gráfico
        medidores_com_fraude = list(soma_energia_medidores_com_fraude.keys())  # Lista de medidores
        zona_consumo_com_fraude = [soma_energia_medidores_com_fraude[medidor].get('Zona_perdas_kwh', 0) for medidor in
                                   medidores_com_fraude]
        zona_consumo_sem_fraude = [soma_energia_medidores_sem_fraude[medidor].get('Zona_perdas_kwh', 0) for medidor in
                                   medidores_com_fraude]

        # Prepare os dados para o gráfico de diferenças de perdas
        diferencas_com_fraude = [new_percent[medidor]['diferença_perda_kwh'] for medidor in medidores_com_fraude]








        # Continuar com o código do gráfico de linhas para as diferenças...
        # Sem erros de medição
        caminho_perdas_sem_fraude = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\coleta_dados_perdas_nao_tecnicas_sem_fraude.json"
        caminho_perdas_com_fraude = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\coleta_dados_perdas_nao_tecnicas_com_fraude.json"

        with open(caminho_perdas_sem_fraude) as c1:
            dados_sem_fraude = json.load(c1)
        with open(caminho_perdas_com_fraude) as c2:
            dados_com_fraude = json.load(c2)

        dias = ['DU', 'DO', 'SA']
        soma_energia_medidores_sem_fraude = {}
        soma_energia_medidores_com_fraude = {}

        for dia in dias:

            dados_sem_fraude_dia = dados_sem_fraude.get(dia, {})
            dados_com_fraude_dia = dados_com_fraude.get(dia, {})

            for medidor, dados in dados_sem_fraude_dia.items():
                if medidor not in soma_energia_medidores_sem_fraude:
                    soma_energia_medidores_sem_fraude[medidor] = {}
                if medidor not in soma_energia_medidores_com_fraude:
                    soma_energia_medidores_com_fraude[medidor] = {}
                dados_medidor_com_fraude = dados_com_fraude_dia.get(medidor, {})
                dados_medidor_sem_fraude = dados_sem_fraude_dia.get(medidor, {})

                for ponto_simulacao, dados_ponto_com_fraude in dados_medidor_com_fraude.items():


                    Zona_consumo_kwh_com_fraude = dados_ponto_com_fraude.get('Zona_consumo_kwh', 0)
                    Zona_consumo_kvarh_com_fraude = dados_ponto_com_fraude.get('Zona_consumo_kvarh', 0)
                    Zona_perdas_kwh_com_fraude = dados_ponto_com_fraude.get('Zona_perdas_kwh', 0)
                    Zona_perdas_kvarh_com_fraude = dados_ponto_com_fraude.get('Zona_perdas_kvarh', 0)

                    Zona_consumo_kwh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get('Zona_consumo_kwh', 0)
                    Zona_consumo_kvarh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get('Zona_consumo_kvarh', 0)
                    Zona_perdas_kwh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get('Zona_perdas_kwh', 0)
                    Zona_perdas_kvarh_sem_fraude = dados_medidor_sem_fraude.get(ponto_simulacao, {}).get('Zona_perdas_kvarh', 0)

                    if medidor not in soma_energia_medidores_com_fraude:
                        soma_energia_medidores_com_fraude[medidor] = {}
                    if 'Zona_consumo_kwh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] = 0
                    if 'Zona_consumo_kvarh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kvarh'] = 0
                    if 'Zona_perdas_kwh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] = 0
                    if 'Zona_perdas_kvarh' not in soma_energia_medidores_com_fraude[medidor]:
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] = 0

                    if medidor not in soma_energia_medidores_sem_fraude:
                        soma_energia_medidores_sem_fraude[medidor] = {}
                    if 'Zona_consumo_kwh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] = 0
                    if 'Zona_consumo_kvarh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kvarh'] = 0
                    if 'Zona_perdas_kwh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] = 0
                    if 'Zona_perdas_kvarh' not in soma_energia_medidores_sem_fraude[medidor]:
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] = 0

                    if dia != 'DU':
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] += Zona_consumo_kwh_com_fraude
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kvarh'] += Zona_consumo_kvarh_com_fraude
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] += Zona_perdas_kwh_com_fraude
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] += Zona_perdas_kvarh_com_fraude

                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] += Zona_consumo_kwh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kvarh'] += Zona_consumo_kvarh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] += Zona_perdas_kwh_sem_fraude
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] += Zona_perdas_kvarh_sem_fraude


                    else:
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kwh'] += (5 * Zona_consumo_kwh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_consumo_kvarh'] += (5* Zona_consumo_kvarh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kwh'] += (5* Zona_perdas_kwh_com_fraude)
                        soma_energia_medidores_com_fraude[medidor]['Zona_perdas_kvarh'] += (5 * Zona_perdas_kvarh_com_fraude)

                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kwh'] += (5 * Zona_consumo_kwh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_consumo_kvarh'] += (5 * Zona_consumo_kvarh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kwh'] += (5 * Zona_perdas_kwh_sem_fraude)
                        soma_energia_medidores_sem_fraude[medidor]['Zona_perdas_kvarh'] += (5 * Zona_perdas_kvarh_sem_fraude)

        diferencas_perdas_kwh = {}
        com_fraude = soma_energia_medidores_com_fraude


        for medidor, dados in com_fraude.items():
            if medidor not in diferencas_perdas_kwh:
                diferencas_perdas_kwh[medidor] = {}
            dados_medidor_com_fraude = soma_energia_medidores_com_fraude.get(medidor, {})
            dados_medidor_sem_fraude = soma_energia_medidores_sem_fraude.get(medidor, {})

            diferencas_perdas_kwh[medidor] = {}

            perdas_com_fraude_kwh = dados_medidor_com_fraude.get('Zona_perdas_kwh', 0)
            perdas_sem_fraude_kwh = dados_medidor_sem_fraude.get('Zona_perdas_kwh', 0)

            diferenca_kwh = (perdas_com_fraude_kwh - perdas_sem_fraude_kwh)

            diferencas_perdas_kwh[medidor] = {
                'diferença_perda_kwh': round(diferenca_kwh, 4)
            }

        soma = 0
        new_percent  = {}
        for medidor, valores in diferencas_perdas_kwh.items():
            soma += diferencas_perdas_kwh[medidor]['diferença_perda_kwh']
            new_percent[medidor] = {}

        for medidor, valores in diferencas_perdas_kwh.items():
            valor = diferencas_perdas_kwh[medidor]['diferença_perda_kwh']
            new_percent[medidor]['diferença_perda_kwh'] = (valor / soma) * 100

        # Prepare os dados para o gráfico
        medidores = list(soma_energia_medidores_com_fraude.keys())  # Lista de medidores
        zona_consumo_com_fraude = [soma_energia_medidores_com_fraude[medidor].get('Zona_perdas_kwh', 0) for medidor in
                                   medidores]
        zona_consumo_sem_fraude = [soma_energia_medidores_sem_fraude[medidor].get('Zona_perdas_kwh', 0) for medidor in
                                   medidores]

        # Prepare os dados para o gráfico de diferenças de perdas
        diferencas = [new_percent[medidor]['diferença_perda_kwh'] for medidor in medidores]

        # Tamanho do gráfico ajustado
        fig, ax1 = plt.subplots(figsize=(10, 6))

        # Estilo para as barras empilhadas
        largura_barras = 0.35

        # Ajustando as barras empilhadas (com e sem fraude)
        ax1.bar(range(len(medidores_com_fraude)), zona_consumo_sem_fraude, label='Sem Fraude', color='#6fa3ef',
                width=largura_barras, align='center', edgecolor='white', linewidth=1.2)
        ax1.bar(range(len(medidores_com_fraude)), zona_consumo_com_fraude, label='Com Fraude', color='#f4a261',
                width=largura_barras, align='center', edgecolor='white', linewidth=1.2,
                bottom=zona_consumo_sem_fraude)

        # Criando um segundo eixo Y à direita para as diferenças de perdas
        ax2 = ax1.twinx()

        # Estilo do gráfico de linha (com a cor vermelha mais suave para indicar que está associado ao eixo direito)
        ax2.plot(medidores_com_fraude, diferencas_com_fraude, label='Percentual de Aumento das Perdas (%)',
                 color='green', marker='o',
                 linestyle='-', linewidth=2, markersize=6)

        # Encontrar o ponto de maior valor
        max_value_index_com_fraude = diferencas_com_fraude.index(max(diferencas_com_fraude))
        max_value_com = diferencas_com_fraude[max_value_index_com_fraude]

        # Plotando o ponto de maior valor com uma bolinha maior e de cor vermelha escura
        ax2.plot(medidores_com_fraude[max_value_index_com_fraude], max_value_com, marker='o', color='green',
                 markersize=10,
                 label='Maior Aumento')

        # Títulos e rótulos
        ax1.set_ylabel('Perdas em kWh', fontsize=16, fontweight='regular', family='serif', color='#333333')
        ax1.set_title('Comparação das Perdas em kWh Com e Sem Fraude', fontsize=18,
                      fontweight='bold', family='serif', color='#333333')

        # Legenda (ajustada para abaixo do gráfico)
        ax1.legend(fontsize=12, loc='upper center', frameon=False, bbox_to_anchor=(0.5, -0.1), ncol=2)

        # Estilo do gráfico de linha (com a cor vermelha mais suave para indicar que está associado ao eixo direito)
        ax2.plot(medidores, diferencas, label='Percentual de Aumento das Perdas (%)', color='#e76f51', marker='o',
                 linestyle='-', linewidth=2, markersize=6)

        # Rótulo do eixo Y à direita com destaque para a cor
        ax2.set_ylabel('Aumento (%) de Perdas', color='#e76f51', fontsize=16, fontweight='regular', family='serif')

        # Encontrar o ponto de maior valor
        max_value_index = diferencas.index(max(diferencas))
        max_value = diferencas[max_value_index]

        # Plotando o ponto de maior valor com uma bolinha maior e de cor vermelha escura
        ax2.plot(medidores[max_value_index], max_value, marker='o', color='darkred', markersize=10,
                 label='Maior Aumento')

        # Ajustando a posição do eixo Y direito
        zero_eixo_esquerdo = ax1.get_ylim()[0]
        zero_eixo_direito = ax2.get_ylim()[0]
        ax2.spines['right'].set_position(('outward', zero_eixo_esquerdo))

        # Rótulos dos eixos X
        plt.xticks(range(len(medidores)), medidores, rotation=20, fontsize=14, fontweight='regular', color='#333333')

        # Ajustando o tamanho dos números dos eixos
        ax1.tick_params(axis='x', labelsize=14)  # Aumentando o tamanho dos números do eixo X
        ax1.tick_params(axis='y', labelsize=14)  # Aumentando o tamanho dos números do eixo Y
        ax2.tick_params(axis='y', labelsize=14)  # Aumentando o tamanho dos números do eixo Y direito

        # Formatação dos eixos
        formatter = FuncFormatter(lambda x, pos: f'{abs(x):.2f}')
        ax1.yaxis.set_major_formatter(formatter)

        # Ajuste nas bordas dos eixos para um visual mais refinado
        for spine in ax1.spines.values():
            spine.set_color('#999999')
            spine.set_linewidth(0.8)

        # Colorindo o eixo da esquerda de cinza escuro
        ax1.spines['left'].set_color('#999999')
        ax1.spines['left'].set_linewidth(1)  # Ajustando a espessura da linha

        # Colorindo o eixo da direita de cinza claro e com mais destaque para o eixo direito
        ax2.spines['right'].set_color('#e76f51')  # Alterando a cor do eixo à direita para vermelho
        ax2.spines['right'].set_linewidth(1.5)  # Ajustando a espessura da linha para maior visibilidade

        ax1.spines['top'].set_visible(False)

        # Estilo do grid, com linhas finas e discretas
        ax1.grid(axis='y', linestyle='-', color='grey', alpha=0.2)

        # Ajustando o layout para maior clareza
        plt.tight_layout()

        # Exibindo o gráfico
        plt.show()

        print('teste')







        caminho_tensoes_classificadas = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\nome_tensoes_classificadas.json"

        dias = ['SA']

        chaves = {
            "Adequada": "Adequada 0.92 a 1.05",
            "Precaria": "Precaria 0.87 a 0.92 ou 1.05 a 1.06",
            "Critica_1": "Critica I - entre 0.7 e 0.87 ou entre 1.06 e 1.23",
        }

        with open(caminho_tensoes_classificadas) as file:
            data = json.load(file)

        du_data = data.get("SA", {})
        pontos = []
        pontos_eixo_x = []
        pontos_horas = []

        pontos_resultados = {}

        for ponto, fases in du_data.items():
            pontos.append((int(ponto) * 15))
            total_adequada = 0
            total_precaria = 0
            total_critica_1 = 0
            total_chaves = 0

            for fase, classificacoes in fases.items():
                if chaves["Adequada"] in classificacoes:
                    total_adequada += classificacoes[chaves["Adequada"]]
                    total_chaves += classificacoes[chaves["Adequada"]]

                if chaves["Precaria"] in classificacoes:
                    total_precaria += classificacoes[chaves["Precaria"]]
                    total_chaves += classificacoes[chaves["Precaria"]]

                if chaves["Critica_1"] in classificacoes:
                    total_critica_1 += classificacoes[chaves["Critica_1"]]
                    total_chaves += classificacoes[chaves["Critica_1"]]

            percentual_adequada = (total_adequada / total_chaves) * 100 if total_chaves else 0
            percentual_precaria = (total_precaria / total_chaves) * 100 if total_chaves else 0
            percentual_critica_1 = (total_critica_1 / total_chaves) * 100 if total_chaves else 0

            pontos_resultados[ponto] = {
                "percentual_adequada": percentual_adequada,
                "percentual_precaria": percentual_precaria,
                "percentual_critica_1": percentual_critica_1,
            }

        pontos_eixo_x.extend(pontos)

        # Convertendo os pontos totais de minutos para horas (formato HH:MM)
        pontos_horas = [(ponto // 60, ponto % 60) for ponto in pontos_eixo_x]  # (horas, minutos)

        # Formatando para exibição
        pontos_horas_formatados = [f'{hora:02d}:{minuto:02d}' for hora, minuto in pontos_horas]

        bar_width = 0.3  # Largura das barras
        index = np.arange(len(pontos_resultados))
        # Definindo intervalo entre os rótulos a ser exibido
        intervalo = 5  # Exibir rótulos a cada 5 pontos (ou altere conforme necessário)

        fig, ax = plt.subplots(figsize=(15, 6))

        # Definindo as cores
        colors = ['red', 'blue', 'green']  # Crítica (vermelho), Precária (azul), Adequada (verde)

        # Inicializando a variável que vai acumular os percentuais para empilhar as barras
        bottoms = np.zeros(len(pontos_resultados))

        # Plotando as barras empilhadas, começando pelas barras de "Critica", depois "Precaria" e por último "Adequada"
        for i, chave in enumerate(['Critica_1', 'Precaria', 'Adequada']):
            percentuais = [pontos_resultados[ponto].get(f"percentual_{chave.lower()}", 0) for ponto in
                           pontos_resultados]
            ax.bar(index, percentuais, bar_width, bottom=bottoms, label=chaves[chave], color=colors[i],
                   edgecolor='white')

            # Atualizando a base para a próxima barra empilhada
            bottoms += percentuais

        ax.set_xlabel('Horas do Dia')
        ax.set_ylabel('Percentual (%)')
        ax.set_title('Percentual de Classificação de Tensão por Hora do Dia')

        # Selecionando os rótulos para o eixo X com base no intervalo
        selected_xticks = index[::intervalo]  # Seleciona os índices para os rótulos
        selected_labels = pontos_horas_formatados[::intervalo]  # Seleciona as horas/minutos para os rótulos

        # Ajustando os ticks do eixo X e colocando os rótulos de hora/minuto
        ax.set_xticks(selected_xticks)
        ax.set_xticklabels(selected_labels, rotation=45)  # Colocando os rótulos de hora/minuto

        ax.legend()

        plt.tight_layout()
        plt.show()



        # Definir os dias (SA, DU, DO) e as suas chaves (0 a 95)
        dias = ['SA']

        # Criação do gráfico
        fig, ax = plt.subplots(figsize=(15, 6))

        # Variável para o deslocamento no eixo X
        offset = 0
        caminho = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\010009\vmag_imag_kw_kvar.json"

        with open(caminho) as file:
            carrega_maximas = json.load(file)

        # Itera sobre os dias
        for dia in dias:
            # Obtém os pontos de simulação para o dia
            pontos_simulacao = carrega_maximas[dia].keys()

            # Dados do gráfico para o dia
            candles = []
            labels = []

            # Itera sobre os pontos de simulação para o dia
            for ponto_simulacao in pontos_simulacao:
                maior_tensao = carrega_maximas[dia][ponto_simulacao]['maior_tensao']
                menor_tensao = carrega_maximas[dia][ponto_simulacao]['menor_tensao']

                # Adiciona o ponto de simulação ao gráfico de vela
                candles.append([offset, menor_tensao, maior_tensao])
                labels.append(ponto_simulacao)

            # Converte os dados para o formato esperado pelo matplotlib
            candles = np.array(candles)

            # Desenha as velas para o dia
            ax.vlines(x=range(offset, offset + len(candles)), ymin=candles[:, 1], ymax=candles[:, 2], color='green',
                      lw=1)  # Vela
            ax.plot(range(offset, offset + len(candles)), candles[:, 1], color='blue',
                    lw=2)  # Linha representando o menor valor
            ax.plot(range(offset, offset + len(candles)), candles[:, 2], color='red',
                    lw=2)  # Linha representando o maior valor

            # Atualiza o deslocamento para o próximo dia
            offset += len(candles)  # Aumenta o deslocamento para o próximo conjunto de pontos

            # Ajusta os labels e título do gráfico
            ax.set_xticks(range(0, len(candles)))  # Posições no eixo X
            ax.set_xticklabels(labels, rotation=45)  # Rótulos usando os pontos de simulação do arquivo
            ax.set_xlabel('Ponto de Simulação')
            ax.set_ylabel('Tensão (PU)')
            ax.set_title('Tensões Máximas e Mínimas por Ponto de Simulação')

            # Exibe o gráfico
            plt.tight_layout()
            plt.show()


        # self.tensoes_por_fase
        # self.associa_carga_na_regiao
        # self.potencia_sub
        # self.potencias_por_regiao
    # ******************************************************************************************************************
    def aplica_curvas_carga_geracao(self, db_cargas_baixa, rec_caminho_iteracoes
                                    ):
        """ Esta função aplica curvas de carga e geração para cada ponto de simulação. """


        dss.solution.solve()
        print("Iterações necessárias: {}".format(dss.solution.iterations))
        print("Potência total subestação: {}".format(dss.circuit.total_power))

        cargas_fraudes = []
        random.seed(42)
        total_loads = dss.loads.names
        random.shuffle(total_loads)
        armazena_cargas_fraudes = random.sample(total_loads, self.numero_de_fraudes)

        for carga in random.sample(total_loads, self.numero_de_fraudes):
            cargas_fraudes.append(carga)



        for ponto_simulacao in range(96):


            for dia in ['DU','SA', 'DO']:
                #self.graficos(cargas_fraudes)

                dss.loads.first()
                for index, load in enumerate(total_loads):
                    if load in armazena_cargas_fraudes:
                        valor_de_interesse = self.potencia_das_fraudes_kw

                    elif load.endswith('_carga_baixa'):
                        carga_especifica = db_cargas_baixa.get('mes_{}_{}'.format(self.mes, dia), {})
                        valores_carga = carga_especifica.get(load, 0.01)
                        valor_de_interesse = valores_carga[ponto_simulacao]
                        valor_de_interesse = valor_de_interesse

                    elif load.endswith('_carga_media'):
                        carga_especifica = db_cargas_baixa.get('mes_{}_{}'.format(self.mes, dia), {})
                        valores_carga = carga_especifica.get(load, 0.01)
                        valor_de_interesse = valores_carga[ponto_simulacao]

                    elif load.endswith('_carga_poste_iluminacao_publica'):
                        carga_especifica = self._on_off
                        valor_de_interesse = carga_especifica[ponto_simulacao]

                    else:
                        valor_de_interesse = 0.0001

                    if valor_de_interesse == 0.0:
                        valor_de_interesse = 0.0001

                    dss.loads.kw = valor_de_interesse
                    dss.loads.next()

                #self.Aplica_Curvas_Geracao_Paineis_Fotovoltaicos(self.irradiancia_96[ponto_simulacao])
                #dss.text('New Line.13332847_linhwwa_{}ba{}ixa{} Phases = 3 Bus1 = 60516962827407584BT.1.2.3.4 Bus2 = 60520461827405210BT.1.2.3.4 Linecode = 107719289M-3NP_linecode_baixa Length = 0.1 units = m'.format( ponto_simulacao, dia, dia))

                dss.solution.solve()
                #dss.text('show isolated')
                # self.coleta_dados_vmag_imag_kw_kvar(dia, ponto_simulacao)
                #
                # if resposta == 'sim':
                #     self.Coleta_Dados_Perdas_nao_Tecnicas(db_recebe_from_energy_meter, dia, ponto_simulacao)
                #
                #
                # self.coleta_potencia_subestacao(dia, ponto_simulacao)
                # dados_corrente_plot = self.coleta_corrente_linhas(dia, ponto_simulacao)

                minutos_totais = ponto_simulacao * 15
                hora = minutos_totais // 60
                minutos = minutos_totais % 60


                print('-------------------------------------------------------------------------------------------------')
                print("Iterações necessárias: {}".format(dss.solution.iterations))
                print("Horário: {}:{}".format(hora, minutos))
                print("Potência total subestação: {}".format(dss.circuit.total_power))

                # Garante que a pasta existe
                os.makedirs(rec_caminho_iteracoes, exist_ok=True)

                # Define o caminho do arquivo JSON dentro da pasta
                caminho_arquivo = os.path.join(rec_caminho_iteracoes, "iteracoes.json")

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
                    "iteracoes": dss.solution.iterations,
                    "potencia_carga_kw": 1.0
                }

                # Salva no arquivo JSON
                with open(caminho_arquivo, 'w', encoding='utf-8') as arquivo:
                    json.dump(dados, arquivo, indent=4, ensure_ascii=False)




        #
        # # Dados salvos ao final da simulação
        # caminho_tensoes_maximas_e_minimas = os.path.join(self.caminho_vmag_imag_kw_kvar, self.nome_arquivo_vmag_imag_kw_kvar)
        # with open(caminho_tensoes_maximas_e_minimas, 'w') as arquivo:
        #     json.dump(self.tensoes_maximas_e_minimas, arquivo, indent=4)
        #
        #
        #
        # caminho_correntes = os.path.join(self.caminho_correntes_plot, self.correntes_plot)
        # with open(caminho_correntes, 'w') as arquivo:
        #     json.dump(self.dados_corrente, arquivo, indent=4)
        #
        #
        #
        # caminho_cargas_fraudulentas = os.path.join(self.caminho_coleta_dados_perdas_nao_tecnicas, self.nome_cargas_fraudulentas)
        # with open(caminho_cargas_fraudulentas, 'w') as arquivo:
        #     json.dump(cargas_fraudes, arquivo, indent=4)
        #
        #
        # # caminho_tensoes_mais_recorrentes_por_fase = os.path.join(self.caminho_vmag_imag_kw_kvar, self.nome_tensoes_mais_recorrentes_por_fase)
        # # for dia, dias in self.tensoes_por_fase.items():
        # #     for ponto, pontos in dias.items():
        # #         for fase, tensoes in pontos.items():
        # #             if tensoes:
        # #                 quantidade = Counter(tensoes)
        # #                 mais_comum = quantidade.most_common(int((self.percentual_tensoes / 100) * len(tensoes)))
        # #                 self.tensoes_por_fase[dia][ponto][fase] = [item[0] for item in mais_comum]
        # # with open(caminho_tensoes_mais_recorrentes_por_fase, 'w') as arquivo:
        # #     json.dump(self.tensoes_por_fase, arquivo, indent=4)
        #
        #
        # caminho_cargas_fraudulentas_por_regiao = os.path.join(self.caminho_coleta_dados_perdas_nao_tecnicas, self.nome_carga_regiao)
        # with open(caminho_cargas_fraudulentas_por_regiao, 'w') as arquivo:
        #     json.dump(self.associa_carga_na_regiao, arquivo, indent=4)
        #
        #
        # caminho_potencia_da_subestacao = os.path.join(self.caminho_vmag_imag_kw_kvar, self.nome_potencia_total_subestacao)
        # with open(caminho_potencia_da_subestacao, 'w') as arquivo:
        #     json.dump(self.potencia_sub, arquivo, indent=4)
        #
        #
        # caminho_potencias_por_regiao = os.path.join(self.caminho_coleta_dados_perdas_nao_tecnicas, self.nome_arquivo_coleta_dados_perdas_nao_tecnicas)
        # with open(caminho_potencias_por_regiao, 'w') as arquivo:
        #     json.dump(self.potencias_por_regiao, arquivo, indent=4)
        #
        #
        # caminho_tensoes_classificadas = os.path.join(self.caminho_coleta_dados_perdas_nao_tecnicas, self.nome_tensoes_classificadas)
        # with open(caminho_tensoes_classificadas, 'w') as arquivo:
        #     json.dump(self.tensoes_classificadas, arquivo, indent=4)
        #
        #
        #
        #
        #
        # return 0
        #






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
    r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\GERACAO_SHAPE_GERACAO_DISTRIBUIDA_MEDIA_TENSAO"]

    modelagem_curva_carga = [r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LOADSHAPE_CARGAS_BAIXA_TENSAO",
                            r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\LOADSHAPE_CARGAS_MEDIA_TENSAO" ]

    modelagem_linhas_media_geometria = r"C:\MODELAGEM_LINHAS_MEDIA_TENSAO_BDGD_2023_ENERGISA_GEOMETRIA_POSTES"

    caminho_iteracoes = r"C:/BDGD_DISTRIBUIDORAS_BRASIL/BDGD_2023_ENERGISAMT/resultados de iteracoes/alimentador_DONDA"
########################################################################################################################
############################################      ENTRADA DE DADOS        ##############################################
########################################################################################################################
# Curva de irradiancia dos painéis fotovoltaicos com 96 pontos de 15 em 15 minutos
    irradiance_96 = [
    0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.1678906012265873, 0.22728017851691293, 0.28791109647383534,
    0.348697510225378, 0.40887777758782434, 0.4678735480839848, 0.5252178619662079,
    0.580516535826924, 0.6334264811150465, 0.6836431225440153, 0.7308929413340143,
    0.7749289692720928, 0.815528070392363, 0.852489337281985, 0.8856331541734708,
    0.914800693562861, 0.9398536947638536, 0.9606743940716865, 0.9771655586384522,
    0.9892505930749959, 0.9968736923877655, 1.0, 0.9986154081300684, 0.9927260244461611,
    0.9823584760554767, 0.967560336057828, 0.9484000425869699, 0.9249667302675952,
    0.8973701037748086, 0.8657403774791488, 0.8302283019851124, 0.7910053139847273,
    0.7482639223207957, 0.702218457324121, 0.6531063758122171, 0.6011904981929989,
    0.5467627269971509, 0.49015018292462936, 0.431725497105491, 0.3719243673385379,
    0.31127638782044526, 0.2504614665506077, 0.19041815094248057, 0.13256340398061622,
    0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001, 0.00001,
    0.00001, 0.00001]

# Curva dos postes de iluminação pública com 96 pontos de 15 em 15 minutos
    on_off = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
   1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

#-----------------------------------------------------------------------------------------------------------------------
#------------------------------ ANALISE DE PERDAS NÃO TÉCNICAS ---------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

# Medidores usados para monitorar consumo de energia nas regiões
    medidores = [
    ("medidor_00", -15.6267609, -55.9963973, '13729182_linha_media'),
    ("medidor_01", -15.61616826, -56.02164433, '17180042_linha_media'),
    ("medidor_02", -15.61423265, -56.02440233, '13317777_linha_media'),
    ("medidor_03", -15.611004141, -56.024145513, '18288026_linha_media'),
    ("medidor_04", -15.606625420, -56.022536853, '13043454_linha_media'),
    ("medidor_05", -15.606625420, -56.022536853, '13035689_linha_media')  ]

    numero_de_fraudes = 100
    potencia_das_fraudes_kw = 3
    resposta='nao'

#-----------------------------------------------------------------------------------------------------------------------
#-------------------------------- PARÂMETROS DE ENTRADA PARA RODAR O FLUXO DE POTÊNCIA ---------------------------------
#-----------------------------------------------------------------------------------------------------------------------
    #alimentador = 'todos'
    mes = 1
    max_iteracoes = 1000
    simular = 'todos_alimentadores' # ou simular = alimentador'

#***********************************************************************************************************************
#***********************************************************************************************************************
#                                            COLETA DE DADOS
    percentual_tensoes = 0.3

    caminho_vmag_imag_kw_kvar = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\{}".format(alimentador)
    nome_vmag_imag_kw_kvar = 'vmag_imag_kw_kvar.json'
    nome_informacoes_tensao = 'informacoes_tensao.json'
    nome_numero_cargas_por_fase = 'numero_cargas_por_fase.json'
    nome_potencia_total_subestacao = 'nome_potencia_total_subestacao.json'

    caminho_coleta_dados_perdas_nao_tecnicas = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\{}".format(alimentador)
    nome_coleta_dados_perdas_nao_tecnicas = 'coleta_dados_perdas_nao_tecnicas.json'
    nome_cargas_fraudulentas = 'nome_cargas_fraudulentas.json'
    nome_carga_regiao = 'nome_carga_regiao.json'
    nome_tensoes_mais_recorrentes_por_fase = 'nome_tensoes_mais_recorrentes_por_fase.json'
    nome_tensoes_classificadas = 'nome_tensoes_classificadas.json'
    
    # Corrente PLot mapa
    db_caminho_correntes_plot = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISA_MT\RESULTADOS_SIMULACOES\{}".format(alimentador)
    db_correntes_plot = 'correntes_plot_mapa.json'

    coletar_maximas_e_minimas_tensoes = False
    coletar_tensoes_outras_analises = False
    coletar_tensoes_mais_recorrentes = False



#***********************************************************************************************************************

    # Chama a Classe
    Classe = Fluxo_Potencia(alimentadores, caminhos_modelagens, irradiance_96, alimentador, medidores,
                            mes, max_iteracoes, on_off, percentual_tensoes,
                            caminho_vmag_imag_kw_kvar, nome_vmag_imag_kw_kvar,
                            caminho_coleta_dados_perdas_nao_tecnicas,
                            nome_coleta_dados_perdas_nao_tecnicas, nome_informacoes_tensao,
                            nome_numero_cargas_por_fase, nome_cargas_fraudulentas,
                            nome_potencia_total_subestacao, nome_carga_regiao,
                            coletar_maximas_e_minimas_tensoes, coletar_tensoes_outras_analises,
                            coletar_tensoes_mais_recorrentes, nome_tensoes_mais_recorrentes_por_fase,
                            nome_tensoes_classificadas, numero_de_fraudes, potencia_das_fraudes_kw,
                            db_caminho_correntes_plot, db_correntes_plot)

    inicio = time.time()

    # Chama os métodos
    dss.text('Clear')
    dss.text('Set DefaultBaseFrequency=60')

    # Este método armazena o nome dos alimentadores
    recebe_pastas = Classe.Nomes_de_todos_alimentadores()



    # Este método envia todos os dados das modelagens do alimentador escolhido para dentro do OpenDSS usando comandos 'dss.text()'
    Classe.Envia_Dados_alimentador_OpenDSS(alimentador, alimentadores_recebidos)



    # Este método carrega as curvas de carga
    cargas_baixa= Classe.Carregar_Dados_Curvas_Carga(alimentador, alimentadores_recebidos)
    


    # Este método insere os medidores de energia nas regiões onde se pretende fazer a análise de perdas não técnicas
    #recebe_nomes_dos_medidores_das_zonas = Classe.Inserir_Energy_Meters(resposta)



    # Este método coleta os dados dos nomes das cargas que estão em cada região monitorada pelos medidores de energia já instalados
    #recebe_regioes_monitoradas = Classe.Coleta_Dados_das_Regioes_com_Energy_Meters()



    # Este método coleta os barramentos associados as cargas
   # recebe_barramentos_e_cargas = Classe.Barramentos_Associados_as_cargas(recebe_regioes_monitoradas)



    # Este método atribui a mudança de carga para o modelo z cte caso as tensões divergem muito do 1 pu
    status_2 = Classe.Convergencia()


    dss.solution.max_iterations = max_iteracoes



    # Este método cria as chaves dos dicionários
    #recebe_from_energy_meter = Classe.Cria_Dicionarios(recebe_nomes_dos_medidores_das_zonas, resposta)


    Classe.Inserir_Cargas_Fraudes(resposta)

    # Este método realiza a atualiazação dos valores das (cargas, geradores, painéis...) no tempo
    Classe.aplica_curvas_carga_geracao(cargas_baixa, caminho_iteracoes)


    fim = time.time()
    print('Tempo de execução: ', fim - inicio)