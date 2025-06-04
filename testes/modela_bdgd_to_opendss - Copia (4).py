import math
import time
import psycopg2
import os
import json
from shapely import wkb
from shapely.geometry import Point, LineString, MultiLineString
from pvlib import location
import pandas as pd
from shapely.wkb import loads
import re
from concurrent.futures import ThreadPoolExecutor
from shapely.wkt import loads
from shapely.geometry import MultiLineString, LineString
import csv





class Modela:
    """ Contém todos os métodos """
    def __init__(self, dbhost, dbport, dbdbname, dbuser, dbpassword,
                 dbcaminho_modelagens, dbirradiance_96, dbmapa_fases, dbdados_tabela_tensoes,
                 dbon_off, dbtrafos_pot_nom, dblimite_inferior_pu,
                 dblimite_superior_pu, dbten_ope_barra_slack_pu,
                 dbten_nom_barra_slack_codigo, dbten_nom_barra_slack_volts,
                 dbpot_nom_transformadores, dbten_nom_compensadores_media_codigo,
                 dbten_nom_compensadores_media_volts, dbpot_nom_compensadores_kva,
                 dbten_con_geradores_media_codigo, dbten_con_geradores_media_volts,
                 dbpot_inst_geradores_media_kva, dbr1_linecodes_baixa_ohms_km,
                 dbx1_linecodes_baixa_ohms_km, dbcnom_linecodes_baixa_amperes,
                 dbcmax_renamed_linecodes_baixa_amperes,dbtip_cnd_linhas,
                 dbten_forn_cargas_baixa_volts, dbten_forn_cargas_baixa_codigo,
                 dbtip_cc_cargas, dbten_con_gd_media_volts,dbten_con_gd_media_codigo,
                 dbtip_cc_pip, dbten_pri_transformadores_media_codigo,dbten_pri_transformadores_media_volts,
                 dbten_sec_transformadores_media_codigo, dbten_sec_transformadores_media_volts,
                 dbr1_transformadores_media, dbxhl_transformadores_media, dbpot_nom_reguladores_media,
                 dbten_nom_reguladores_media_codig, dbr_reguladores_media_ohms_km, dbbanda_volts_reguladores_tensao,
                 dbper_fer_transformadores_media):


        """ Inicializa os parâmetros de conexão """
        self.host = dbhost
        self.port = dbport
        self.dbname = dbdbname
        self.user = dbuser
        self.password = dbpassword
        self.conn = None
        self.cur = None

        """ declara todas as variaveis de instancia """
        self.caminho_modelagens = dbcaminho_modelagens
        self.irradiance_96 = dbirradiance_96
        self.mapa_fases = dbmapa_fases
        self.dados_tabela_tensoes = dbdados_tabela_tensoes
        self.on_off = dbon_off
        self.trafos_pot_nom = dbtrafos_pot_nom

        """ declara todas as variaveis de instancia externas que podem alterar a modelagem """
        self.limite_inferior_pu = dblimite_inferior_pu
        self.limite_superior_pu = dblimite_superior_pu
        self.ten_ope_barra_slack_pu = dbten_ope_barra_slack_pu
        self.ten_nom_barra_slack_codigo = dbten_nom_barra_slack_codigo
        self.ten_nom_barra_slack_volts = dbten_nom_barra_slack_volts
        self.pot_nom_transformadores = dbpot_nom_transformadores
        self.ten_nom_compensadores_media_codigo = dbten_nom_compensadores_media_codigo
        self.ten_nom_compensadores_media_volts = dbten_nom_compensadores_media_volts
        self.pot_nom_compensadores_kva = dbpot_nom_compensadores_kva
        self.ten_con_geradores_media_codigo = dbten_con_geradores_media_codigo
        self.ten_con_geradores_media_volts = dbten_con_geradores_media_volts
        self.pot_inst_geradores_media_kva = dbpot_inst_geradores_media_kva
        self.r1_linecodes_baixa_ohms_km = dbr1_linecodes_baixa_ohms_km
        self.x1_linecodes_baixa_ohms_km = dbx1_linecodes_baixa_ohms_km
        self.cnom_linecodes_baixa_amperes = dbcnom_linecodes_baixa_amperes
        self.cmax_renamed_linecodes_baixa_amperes = dbcmax_renamed_linecodes_baixa_amperes
        self.tip_cnd_linhas = dbtip_cnd_linhas
        self.ten_forn_cargas_baixa_volts = dbten_forn_cargas_baixa_volts
        self.ten_forn_cargas_baixa_codigo = dbten_forn_cargas_baixa_codigo
        self.tip_cc_cargas = dbtip_cc_cargas
        self.ten_con_gd_media_volts = dbten_con_gd_media_volts
        self.ten_con_gd_media_codigo = dbten_con_gd_media_codigo
        self.tip_cc_pip = dbtip_cc_pip
        self.ten_pri_transformadores_media_codigo = dbten_pri_transformadores_media_codigo
        self.ten_pri_transformadores_media_volts = dbten_pri_transformadores_media_volts
        self.ten_sec_transformadores_media_codigo = dbten_sec_transformadores_media_codigo
        self.ten_sec_transformadores_media_volts = dbten_sec_transformadores_media_volts
        self.r1_transformadores_media = dbr1_transformadores_media
        self.xhl_transformadores_media = dbxhl_transformadores_media
        self.pot_nom_reguladores_media = dbpot_nom_reguladores_media
        self.ten_nom_reguladores_media_codig = dbten_nom_reguladores_media_codig
        self.r_reguladores_media_ohms_km = dbr_reguladores_media_ohms_km
        self.banda_volts_reguladores_tensao = dbbanda_volts_reguladores_tensao
        self.per_fer_transformadores_media = dbper_fer_transformadores_media



    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def iniciar_conexao(self):
        """Estabelece a conexão com o banco de dados PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cur = self.conn.cursor()
            print("Conexão Estabelecida com sucesso.")
        except Exception as e:
            print("Erro ao conectar ao banco de dados: {}".format(e))




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def Barra_Slack(self):
        """ Cria o Comando Sourcebus do OpenDSS """

        results = []
        base_dir = caminho_modelagens
        ctmts_processados = {}


        # Coleta de dados
        try:
            query = """
            
                        SELECT DISTINCT
                                        nome, ten_ope, ten_nom, pac_ini
                        FROM 
                                        ctmt   
                                        
                       -- WHERE
                                       -- ctmt.cod_id = '764444'
                        
                        ORDER BY        nome  
                  
                   """
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao conectar com o banco de dados para a tabela ctmt: {}".format(e))

        if not results:
            print("Nenhum dado foi recuperado da tabela: ctmt")
            return

        # Itera pelas linhas da tabela
        for index, linha in enumerate(results):
            nome, ten_ope, ten_nom, pac_ini = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################

            ######################      TRATAMENTO PARA COLUNA NOME     ################################################


            if not nome:
                print("Linha {} contém valores nulos e será ELIMINADA DA MODELAGEM !!!: {}".format(index, linha))
                continue

            ######################      TRATAMENTO PARA COLUNA TEN_OPE     #############################################



            if not isinstance(ten_ope, (float or int)):
                print("Erro!- A coluna ten_ope da tabela ctmt não é int e nem float: {}, portanto, colocando valor padrão de ten_ope = {} pu para o alimentador {}".format(ten_ope, self.ten_ope_barra_slack_pu, nome))

                ten_ope = self.ten_ope_barra_slack_pu

            if ten_ope < self.limite_inferior_pu or ten_ope > self.limite_superior_pu:
                print("Erro! - A tensão de operação da coluna ten_ope da tabela ctmt está com valores "
                        "muito distantes do ideal, sendo o seu valor de: {}, colocando valor padrão de ten_ope = {} pu para o alimentador: {}".format(ten_ope, self.ten_ope_barra_slack_pu, nome))
                ten_ope = self.ten_ope_barra_slack_pu



            ######################      TRATAMENTO PARA COLUNA TEN_NOM     #############################################



            try:
                ten_nom = int(ten_nom)
            except Exception as e:
                print("Erro ao converter para int a ten_nom: {}".format(e))

            if not isinstance(ten_nom, (float, int)):
                print("Erro! - A coluna ten_nom da tabela ctmt não é nem int e nem float: {}, colocando ten_nom = 13800 v".format(ten_nom))
                ten_nom = self.ten_nom_barra_slack_codigo


            ten_nom_voltage = self.dados_tabela_tensoes.get(ten_nom, "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_nom da tabela ctmt: {}, colocando valor padrão ten_nom = 13800 v".format(ten_nom_voltage))
                ten_nom_voltage = self.ten_nom_barra_slack_volts



            ############################################################################################################

            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'Barra_Slack.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]


            command_linecode = (

                               'New Object = Circuit.{}_Barra_Slack\n'.format(nome) +
                               '~ basekv = {} pu = {} angle = 0\n'.format(ten_nom_voltage / 1000, ten_ope) +
                               'New line.{}{} phases = 3 bus1 = SourceBus bus2 = {}.1.2.3 switch = y\n\n'.format(nome, nome, pac_ini)


                                )

            if file:
                file.write(command_linecode.encode())

        for file in ctmts_processados.values():
            file.close()
        print("Modelagem: Barra_Slack gerada com sucesso !!!")




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Compensadores_Reativo_Media(self):
        """ Modela Compensadores de Reativos na media tensão """

        results = []
        base_dir = caminho_modelagens
        ctmts_processados = {}

        try:
            query = """
            
                SELECT DISTINCT
                                ctmt.nome,
                                uncrmt.fas_con,
                                uncrmt.tip_unid,
                                uncrmt.pot_nom,
                                uncrmt.pac_1,
                                ctmt.ten_nom,
                                uncrmt.cod_id
                FROM 
                                uncrmt
                JOIN 
                                ctmt ON uncrmt.ctmt = ctmt.cod_id
               -- AND             ctmt.cod_id = '764444'
                ORDER BY        nome
           
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a consulta na tabela uncrmt: {}".format(e))

        for index, linha in enumerate(results):
            nome, fas_con, tip_uni, pot_nom, pac_1, ten_nom, cod_id = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ############################        TRATAMENTO PARA A COLUNA NOME       ####################################



            if not nome:
                print("Linha {} contém valores nulos e será ELIMINADA DA MODELAGEM !!!: {}".format(index, linha))
                continue

            # if not nome.isdigit():
            #     print(
            #         "A coluna nome da tabela ctmt não têm digitos numéricos: {}, portanto será ELIMINADA DA MODELAGEM !!!".format(
            #             nome))




            ############################        TRATAMENTO PARA A COLUNA FAS_CON      ##################################



            if not fas_con:
                print("Coluna fas_con da tabela uncrmt vazia, inserindo fas_con = .1.2.3")
                fas_con = 'ABC'

            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -uncrmt-: {}, colocando valor padrão de fases: 1.2.3".format(
                        fas_con))
                fas_con = 'ABC'



            ############################        TRATAMENTO PARA A COLUNA PAC-1       ###################################



            if not pac_1:
                print("Coluna pac_1 da tabela uncrmt vazia, está linha será ELIMINADA DA MODELAGEM !!!")
                continue



            ############################        TRATAMENTO PARA A COLUNA TEN_NOM       #################################



            if not ten_nom:
                print("Coluna ten_nom da tabela uncrmt vazia, colocando valor padrão de: {} kv".format(self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo

            if ten_nom.isdigit():
                pass
            else:
                print("Erro! Há valores não numericos na coluna -ten_nom- da tabela -ctmt-: {}, colocando vlor padrão de ten_nom = {} ".format(ten_nom, self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo


            try:
                ten_nom = int(ten_nom)
            except Exception as e:
                print("Erro ao converter para int a ten_nom da tabela ctmt: {}, colocando valor padrão de ten_nom = {} v".format(e, self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo

            ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_nom), "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_nom da tabela uncrmt, colocando valor padrão de ten_nom = {} v".format(self.ten_nom_compensadores_media_volts))
                ten_nom_voltage = self.ten_nom_compensadores_media_volts



            ############################        TRATAMENTO PARA A COLUNA COD_ID       ##################################



            if not cod_id:
                print("Coluna cod_id da tabela uncrmt vazia, inserindo cod_id = {}".format(nome + index))
                cod_id = nome + index



            ############################        TRATAMENTO PARA A COLUNA POT_NOM       #################################

            if not pot_nom:
                print("Coluna pot_nom da tabela uncrmt está vazia, inserindo pot_nom = {} Mw ".format(self.pot_nom_compensadores_kva))
                pot_nom = self.pot_nom_compensadores_kva


            if pot_nom.isdigit():
                pot_nom = int(pot_nom)
                if pot_nom > self.pot_nom_compensadores_kva:
                    print(
                        "Erro! Há valores de potencia nominal dos compensadores de reativo de baixa tensão"
                        " acima dos {} kva na coluna -pot_nom- da tabela -uncrmt-: {}, colocando valor padrão de {} Mva".format(
                            self.pot_nom_compensadores_kva, pot_nom, self.pot_nom_compensadores_kva))
                    pot_nom = self.pot_nom_compensadores_kva
                else:
                    pass
            else:
                print("Erro! Há valores não numericos na coluna -pot_nom- da tabela -uncrmt-: {}, colocando valor padrão de {} kva".format(pot_nom, self.pot_nom_compensadores_kva))
                pot_nom = self.pot_nom_compensadores_kva



            ############################################################################################################

            # Segunda consulta para verificar tensões associadas
            try:
                second_query = """
                
                    SELECT DISTINCT
                                    ctmt.nome,
                                    untrmt.cod_id,
                                    untrmt.ctmt,
                                    eqtrmt.ten_pri,
                                    eqtrmt.ten_sec
                    FROM 
                                    untrmt
                    LEFT JOIN 
                                    eqtrmt ON eqtrmt.uni_tr_mt = untrmt.cod_id
                    LEFT JOIN 
                                    ctmt ON ctmt.cod_id = untrmt.ctmt
                    WHERE 
                                    ctmt.nome = '{}';
               
                """.format(nome)

                self.cur.execute(second_query)
                second_results = self.cur.fetchall()
            except Exception as e:
                print("Erro ao executar a segunda consulta: {}".format(e))
                continue

            # Verificar se a tensão da primeira consulta corresponde com a segunda consulta
            tension_match = False
            for sec_linha in second_results:
                ten_pri, ten_sec = sec_linha[3], sec_linha[4]

                ten_pri = int(ten_pri)
                ten_sec = int(ten_sec)

                # Se ten_nom da primeira consulta corresponder com ten_pri ou ten_sec da segunda consulta
                if ten_nom == ten_pri or ten_nom == ten_sec:
                    tension_match = True
                    break  # Se encontrar uma correspondência, parar a busca e continuar gerando a linha

            if not tension_match:
                print(
                    "Tensão {} de {} não corresponde com nenhuma das tensões de {} na segunda consulta. Pulando a linha.".format(ten_nom, nome, nome))
                continue  # Se não houver correspondência de tensões, pular esta linha

            # Se passar pela verificação, criar a pasta e gerar o arquivo
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'Compensadores_de_reativo_media_tensao.bin')

                # Abrindo o arquivo em modo binário ('wb')
                file = open(file_path, 'wb')  # 'wb' para escrever em binário
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABC')
            fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

            tensao = self.dados_tabela_tensoes.get(ten_nom, "não encontrado")

            # Verifica o tipo de unidade e gera a linha de comando correspondente
            if tip_uni == 56:
                command_linecode = (

                    "New Reactor.{}_Banco_de_Reator Bus1 = {}{} kv = {} kvar = {} phases = {} conn = wye\n\n".format(
                        cod_id, pac_1, rec_fases, tensao / 1000, pot_nom, len(fases)

                    )
                )
            else:
                command_linecode = (

                    'New Capacitor.{}_Banco_de_Capacitor Bus1 = {}{} kv = {} kVAR = {} phases = {} conn = wye\n\n'.format(
                        cod_id, pac_1, rec_fases, tensao / 1000, pot_nom, len(fases)

                    )
                )

            # Escreve a linha no arquivo
            if file:
                file.write(command_linecode.encode())

        # Fecha todos os arquivos
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Compensadores de Reativo de Média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Compensadores_Reativo_Baixa(self):
        """ Modela Compensadores de Reativos na baixa tensão """

        results = []
        base_dir = caminho_modelagens
        ctmts_processados = {}

        try:
            query = """

                       SELECT DISTINCT
                                       ctmt.nome,
                                       uncrbt.fas_con,
                                       uncrbt.tip_unid,
                                       uncrbt.pot_nom,
                                       uncrbt.pac_1,
                                       ctmt.ten_nom,
                                       uncrbt.cod_id
                       FROM 
                                       uncrbt
                       JOIN 
                                       ctmt ON uncrbt.ctmt = ctmt.cod_id
                       --AND              ctmt.cod_id = '764444'
                       
                       ORDER BY        nome

                   """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a consulta na tabela uncrbt: {}".format(e))

        for index, linha in enumerate(results):
            nome, fas_con, tip_uni, pot_nom, pac_1, ten_nom, cod_id = linha


            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ############################        TRATAMENTO PARA A COLUNA NOME       ####################################



            if not nome:
                print("Linha {} contém valores nulos e será ELIMINADA DA MODELAGEM !!!: {}".format(index, linha))
                continue

            # if not nome.isdigit():
            #     print(
            #         "A coluna nome da tabela ctmt não têm digitos numéricos: {}, portanto será ELIMINADA DA MODELAGEM !!!".format(
            #             nome))




            ############################        TRATAMENTO PARA A COLUNA FAS_CON      ##################################



            if not fas_con:
                print("Coluna fas_con da tabela uncrbt vazia, inserindo fas_con = .1.2.3")
                fas_con = 'ABC'

            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -uncrbt-: {}, colocando valor padrão de fases: 1.2.3".format(
                        fas_con))
                fas_con = 'ABC'



            ############################        TRATAMENTO PARA A COLUNA PAC-1       ###################################



            if not pac_1:
                print("Coluna pac_1 da tabela uncrbt vazia, está linha será ELIMINADA DA MODELAGEM !!!")
                continue



            ############################        TRATAMENTO PARA A COLUNA TEN_NOM       #################################



            if not ten_nom:
                print("Coluna ten_nom da tabela uncrbt vazia, colocando valor padrão de: {} kv".format(
                    self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo

            if ten_nom.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -ten_nom- da tabela -ctmt-: {}, colocando vlor padrão de ten_nom = {} ".format(
                        ten_nom, self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo

            try:
                ten_nom = int(ten_nom)
            except Exception as e:
                print(
                    "Erro ao converter para int a ten_nom da tabela ctmt: {}, colocando valor padrão de ten_nom = {} v".format(
                        e, self.ten_nom_compensadores_media_codigo))
                ten_nom = self.ten_nom_compensadores_media_codigo

            ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_nom), "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "ten_nom da tabela uncrbt, colocando valor padrão de ten_nom = {} v".format(
                    self.ten_nom_compensadores_media_volts))
                ten_nom_voltage = self.ten_nom_compensadores_media_volts



            ############################        TRATAMENTO PARA A COLUNA COD_ID       ##################################



            if not cod_id:
                print("Coluna cod_id da tabela uncrbt vazia, inserindo cod_id = {}".format(nome + index))
                cod_id = nome + index



            ############################        TRATAMENTO PARA A COLUNA POT_NOM       #################################



            if not pot_nom:
                print("Coluna pot_nom da tabela uncrbt está vazia, inserindo pot_nom = {} Mw ".format(
                    self.pot_nom_compensadores_kva))
                pot_nom = self.pot_nom_compensadores_kva

            if pot_nom.isdigit():
                pot_nom = int(pot_nom)
                if pot_nom > self.pot_nom_compensadores_kva:
                    print(
                        "Erro! Há valores de potencia nominal dos compensadores de reativo de baixa tensão"
                        " acima dos {} kva na coluna -pot_nom- da tabela -uncrbt-: {}, colocando valor padrão de {} Mva".format(
                            self.pot_nom_compensadores_kva, pot_nom, self.pot_nom_compensadores_kva))
                    pot_nom = self.pot_nom_compensadores_kva
                else:
                    pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -pot_nom- da tabela -uncrbt-: {}, colocando valor padrão de {} kva".format(
                        pot_nom, self.pot_nom_compensadores_kva))
                pot_nom = self.pot_nom_compensadores_kva



            ############################################################################################################



            # Segunda consulta para verificar tensões associadas
            try:
                second_query = """

                           SELECT DISTINCT
                                           ctmt.nome,
                                           untrmt.cod_id,
                                           untrmt.ctmt,
                                           eqtrmt.ten_pri,
                                           eqtrmt.ten_sec
                           FROM 
                                           untrmt
                           LEFT JOIN 
                                           eqtrmt ON eqtrmt.uni_tr_mt = untrmt.cod_id
                           LEFT JOIN 
                                           ctmt ON ctmt.cod_id = untrmt.ctmt
                           WHERE 
                                           ctmt.nome = '{}';

                       """.format(nome)

                self.cur.execute(second_query)
                second_results = self.cur.fetchall()
            except Exception as e:
                print("Erro ao executar a segunda consulta: {}".format(e))
                continue

            # Verificar se a tensão da primeira consulta corresponde com a segunda consulta
            tension_match = False
            for sec_linha in second_results:
                ten_pri, ten_sec = sec_linha[3], sec_linha[4]

                ten_pri = int(ten_pri)
                ten_sec = int(ten_sec)

                # Se ten_nom da primeira consulta corresponder com ten_pri ou ten_sec da segunda consulta
                if ten_nom == ten_pri or ten_nom == ten_sec:
                    tension_match = True
                    break  # Se encontrar uma correspondência, parar a busca e continuar gerando a linha

            if not tension_match:
                print(
                    "Tensão {} de {} não corresponde com nenhuma das tensões de {} na segunda consulta. Pulando a linha.".format(
                        ten_nom, nome, nome))
                continue  # Se não houver correspondência de tensões, pular esta linha

            # Se passar pela verificação, criar a pasta e gerar o arquivo
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'Compensadores_de_reativo_baixa_tensao.bin')

                # Abrindo o arquivo em modo binário ('wb')
                file = open(file_path, 'wb')  # 'wb' para escrever em binário
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABC')
            fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

            tensao = self.dados_tabela_tensoes.get(ten_nom, "não encontrado")

            # Verifica o tipo de unidade e gera a linha de comando correspondente
            if tip_uni == 56:
                command_linecode = (

                    "New Reactor.{}_Banco_de_Reator Bus1 = {}{} kv = {} kvar = {} phases = {} conn = wye\n\n".format(
                        cod_id, pac_1, rec_fases, tensao / 1000, pot_nom, len(fases)

                    )
                )
            else:
                command_linecode = (

                    'New Capacitor.{}_Banco_de_Capacitor Bus1 = {}{} kv = {} kVAR = {} phases = {} conn = wye\n\n'.format(
                        cod_id, pac_1, rec_fases, tensao / 1000, pot_nom, len(fases)

                    )
                )

            # Escreve a linha no arquivo
            if file:
                file.write(command_linecode.encode())

        # Fecha todos os arquivos
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Compensadores de Reativo de Baixa Tensão gerada com sucesso !!!")




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    """
        def Geometria_ssdmt(self):
         Modelagem das geometrias multilinhas das linhas de media tensão 

        base_dir = caminho_modelagens[3]
        results = []

        try:
            query = 
                        SELECT DISTINCT 
                            ctmt.nome, ssdmt.wkb_geometry, ssdmt.cod_id
                        FROM 
                            ctmt
                        JOIN
                            ssdmt on ctmt.cod_id = ssdmt.ctmt
                        AND ctmt.nome = '010009'
                        ORDER BY ctmt.nome
                    
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a consulta na tabela ctmt: {}".format(e))

        # Lista para armazenar as coordenadas que serão salvas no arquivo JSON
        coordenadas = list()

        # Processa as linhas retornadas da consulta
        for nome, wkb_geometry, cod_id in results:
            # Verifica se existe algum valor nulo no resultado
            if any(valor is None for valor in [nome, wkb_geometry, cod_id]):
                print(f"Linha com nome {nome} contém valores nulos e será ignorada.")
                continue  # Ignora linhas com valores nulos

            # Converte o WKB para geometria usando o Shapely
            if isinstance(wkb_geometry, memoryview):
                wkb_geometry = bytes(wkb_geometry)

            # Converte o WKB para geometria
            geometry = loads(wkb_geometry)

            # Verifica se a geometria é um MultiLineString ou LineString
            if isinstance(geometry, (MultiLineString, LineString)):
                # Acessa os bounds da geometria (para MultiLineString: minx, miny, maxx, maxy)
                bounds = geometry.bounds
                # Adiciona o cod_id correto e as coordenadas à lista
                coordenadas.append({
                    'cod_id': f"{cod_id}_linha_media",  # Adiciona _linha_baixa ao cod_id
                    'longitude_inicio': bounds[0],    # minx (long)
                    'latitude_inicio': bounds[1],     # miny (lat)
                    'longitude_fim': bounds[2],       # maxx (long)
                    'latitude_fim': bounds[3]         # maxy (lat)
                })
            elif isinstance(geometry, LineString):
                # Se for apenas uma LineString, obtemos as coordenadas dela
                coords = list(geometry.coords)
                # Para cada par de coordenadas, adicionamos ao resultado
                for coord in coords:
                    coordenadas.append({
                        'cod_id': f"{cod_id}_linha_media",  # Adiciona _linha_baixa ao cod_id
                        'longitude_inicio': coord[0],
                        'latitude_inicio': coord[1],
                        'longitude_fim': coord[0],
                        'latitude_fim': coord[1]
                    })
            else:
                print(f"Geometria de {nome} não é do tipo esperado: {type(geometry)}")
                continue

        # Cria o diretório específico para o CTMT
        ctmt_folder = os.path.join(base_dir, str(nome))
        os.makedirs(ctmt_folder, exist_ok=True)

        # Salva as coordenadas no formato JSON
        filepath = os.path.join(ctmt_folder, 'geometria_linha_media_tensao.json')

        # Adiciona todas as coordenadas de uma vez no arquivo JSON
        try:
            with open(filepath, 'w') as file:
                json.dump(coordenadas, file, indent=4)
        except Exception as e:
            print(f"Erro ao salvar as coordenadas no arquivo {filepath}: {e}")
        else:
            print("Modelagem: Coordenadas de latitude e longitude das linhas de Media Tensão geradas com sucesso!!!")
    """

    
    def Geometria_ssdmt(self):
        """ Modelagem das geometrias multilinhas das linhas de media tensão """
        
        base_dir = caminho_modelagens
        results = []

        # Caminho do arquivo CSV
        csv_file = r"C:\BDGD_DISTRIBUIDORAS_BRASIL\BDGD_2023_ENERGISAMT\coordenadas_linhas_media_tensao_2.csv"

        # Lê os dados do CSV
        try:
            with open(csv_file, mode='r') as file:
                csv_reader = csv.reader(file)
                # Pula o cabeçalho
                next(csv_reader)
                for row in csv_reader:
                    wkt_geometry = row[0]  # WKT
                    cod_id = row[1]        # COD_ID
                    ctmt = row[2]          # CTMT
                    # Extrai as coordenadas de latitude e longitude diretamente do CSV
                    coords = row[0].replace('MULTILINESTRING ((', '').replace(')', '').split(',')
                    
                    # Pega as coordenadas pares: lon1, lat1, lon2, lat2
                    for i in range(0, len(coords), 2):  # Itera de 2 em 2
                        lon1, lat1 = map(float, coords[i].strip().split())
                        lon2, lat2 = map(float, coords[i + 1].strip().split())
                        
                        # Adiciona as coordenadas no resultado
                        results.append((lon1, lat1, lon2, lat2, cod_id, ctmt))

        except Exception as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
            return

        # Lista para armazenar as coordenadas agrupadas por CTMT
        coordenadas_por_ctmt = {}

        # Agrupa as coordenadas por CTMT
        for lon1, lat1, lon2, lat2, cod_id, ctmt in results:
            if ctmt not in coordenadas_por_ctmt:
                coordenadas_por_ctmt[ctmt] = []

            coordenadas_por_ctmt[ctmt].append({
                'cod_id': f"{cod_id}_linha_media",  # Adiciona _linha_media ao cod_id
                'longitude_inicio': lon1,
                'latitude_inicio': lat1,
                'longitude_fim': lon2,
                'latitude_fim': lat2
            })

        # Salva as coordenadas no formato JSON para cada CTMT
        for ctmt, coordenadas in coordenadas_por_ctmt.items():
            # Cria o diretório específico para o CTMT
            ctmt_folder = os.path.join(base_dir, str(ctmt))
            os.makedirs(ctmt_folder, exist_ok=True)

            # Caminho do arquivo JSON para o CTMT
            filepath = os.path.join(ctmt_folder, 'geometria_linha_media_tensao.json')

            # Adiciona as coordenadas de uma vez no arquivo JSON
            try:
                with open(filepath, 'w') as file:
                    json.dump(coordenadas, file, indent=4)
            except Exception as e:
                print(f"Erro ao salvar as coordenadas no arquivo {filepath}: {e}")
            else:
                print(f"Modelagem: Coordenadas de latitude e longitude das linhas de Media Tensão para o CTMT {ctmt} geradas com sucesso!!!")
                
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Chaves_Seccionadoras_Baixa_Tensao(self):
        """ Modelagem de chaves seccionadoras de baixa tensão """

        base_dir = caminho_modelagens
        dados = []
        ctmts_processados = {}

        try:
            query = """
            
               SELECT DISTINCT
                                unsebt.pac_1, 
                                unsebt.pac_2, 
                                unsebt.cod_id, 
                                unsebt.ctmt, 
                                unsebt.cor_nom, 
                                unsebt.fas_con,
                                ctmt.nome,
                                unsebt.p_n_ope
                FROM 
                                unsebt
                JOIN 
                                ctmt ON unsebt.ctmt = ctmt.cod_id
                                
               -- AND             ctmt.cod_id = '764444'
                
                ORDER BY 
                                ctmt.nome;

            """
            self.cur.execute(query)
            dados = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para chaves seccionadoras de baixa tensão: {}".format(e))

            if not dados:
                print("Nenhum dado encontrado para chaves seccionadoras.")
                return


        for linha in dados:
            pac_1, pac_2, cod_id, ctmt, cor_nom, fas_con, nome, p_n_ope = linha


            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ############################        TRATAMENTO DA COLUNA NOME       ########################################



            if not nome:
                print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, esta linha será excluida da MODELAGEM !!!".format(nome))
            #



            ############################        TRATAMENTO DA COLUNA FAS_CON       #####################################



            if fas_con.isalpha():
                pass
            else:
                print("Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -unsebt-: {}, colocando valores padrão: ABC".format(fas_con))
                fas_con = 'ABCN'



            ############################################################################################################

            # Verifica e cria a pasta para o CTMT, se necessário
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'chaves_seccionadoras_baixa_tensao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')

            if p_n_ope == 'F':
                status = 'y'
            else:
                status = 'n'

            # Gera o comando OpenDSS
            command_switch = (

                'New line.{}_Chave_seccionadora_baixa_tensao phases = {} bus1 = {}{} bus2 = {}{} switch = {}\n\n'.format(cod_id,
                len(fas_con), pac_1, rec_fases, pac_2, rec_fases, status)

            )

            # Escreve o comando no arquivo correspondente
            if file:
                file.write(command_switch.encode())

        # Fecha todos os arquivos abertos
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Chaves seccionadoras de baixa Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Chaves_Seccionadoras_Media_Tensao(self):
        """ Modelagem de chaves seccionadoras de média tensão """

        base_dir = caminho_modelagens
        dados = []
        ctmts_processados = {}

        try:
            query = """

                     SELECT DISTINCT
                                      unsemt.pac_1, 
                                      unsemt.pac_2, 
                                      unsemt.cod_id, 
                                      unsemt.ctmt, 
                                      unsemt.cor_nom, 
                                      unsemt.fas_con,
                                      ctmt.nome,
                                      unsemt.p_n_ope
                      FROM 
                                      unsemt
                      JOIN 
                                      ctmt ON unsemt.ctmt = ctmt.cod_id
                     -- AND               
                         --             ctmt.cod_id = '764444'
                      ORDER BY 
                                      ctmt.nome;

                  """
            self.cur.execute(query)
            dados = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para chaves seccionadoras de média tensão: {}".format(e))

            if not dados:
                print("Nenhum dado encontrado para chaves seccionadoras.")
                return

        for linha in dados:
            pac_1, pac_2, cod_id, ctmt, cor_nom, fas_con, nome, p_n_ope = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ############################        TRATAMENTO DA COLUNA NOME       ########################################



            if not nome:
                print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print(
            #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, esta linha será excluida da MODELAGEM !!!".format(
            #             nome))




            ############################        TRATAMENTO DA COLUNA FAS_CON       #####################################



            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -unsemt-: {}, colocando valores padrão: ABC".format(
                        fas_con))
                fas_con = 'ABC'



            ############################################################################################################

            # Verifica e cria a pasta para o CTMT, se necessário
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'chaves_seccionadoras_media_tensao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')


            if p_n_ope == 'F':
                status = 'y'
            else:
                status = 'n'

            # Gera o comando OpenDSS
            command_switch = (

                'New line.{}_Chave_seccionadora_baixa_tensao phases = {} bus1 = {}{} bus2 = {}{} switch = {}\n\n'.format(
                    cod_id, len(fas_con), pac_1, rec_fases, pac_2, rec_fases, status)

            )

            # Escreve o comando no arquivo correspondente
            if file:
                file.write(command_switch.encode())

        # Fecha todos os arquivos abertos
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Chaves seccionadoras de Média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def Geradores_Media_tensao(self):
        """ Modelagem de geradores de média tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}
        dados = []

        try:
            query = """
            
               SELECT DISTINCT 
                                ugmt_tab.cod_id, 
                                ugmt_tab.pac, 
                                ugmt_tab.ctmt, 
                                ugmt_tab.fas_con, 
                                ugmt_tab.ten_con, 
                                ugmt_tab.pot_inst, 
                                ugmt_tab.cep, 
                                ugmt_tab.ceg_gd, 
                                ctmt.nome
                FROM 
                                ugmt_tab
                JOIN 
                                ctmt ON ugmt_tab.ctmt = ctmt.cod_id
                WHERE 
                                ceg_gd NOT LIKE 'GD%' AND ceg_gd NOT LIKE 'UFV%'
                ORDER BY 
                                ctmt.nome;

            
                """
            self.cur.execute(query)
            dados = self.cur.fetchall()

            if not dados:
                print("Nenhum dado encontrado na tabela ugmt_tab.")
                return

        except Exception as e:
            print("Erro ao gerar comandos de geradores: {}".format(e))


        for linha in dados:
            cod_id, pac, ctmt, fas_con, ten_con, pot_inst, cep, ceg_gd, nome = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ##########################      TRATAMENTO NA COLUNA NOME       ############################################



            if not nome:
                print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print(
            #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, esta linha será excluida da MODELAGEM !!!".format(
            #             nome))




            ##########################      TRATAMENTO NA COLUNA FAS_CON       ############################################



            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ugmt_tab-: {}, colocando valores padrão: ABC".format(
                        fas_con))
                fas_con = 'ABC'



            ##########################      TRATAMENTO NA COLUNA TEN_CON       ############################################



            if ten_con.isdigit():
                pass
            else:
                print("Erro! Há valores não numericos na coluna -ten_nom- da tabela -ugmt_tab-: {}, colocando ten_con = {} v".format(ten_con, self.ten_con_geradores_media_codigo))
                ten_con = self.ten_con_geradores_media_codigo

            if ten_con.isdigit():
                pass
            else:
                print("Erro! Há valores não numericos na coluna -ten_nom- da tabela -ugmt_tab-: {}, colocando ten_con = {}".format(ten_con, self.ten_con_geradores_media_codigo))
                ten_con = self.ten_con_geradores_media_codigo

            try:
                ten_con = int(ten_con)
            except Exception as e:
                print("Erro ao converter para int a ten_nom da tabela ugmt_tab: {}, colocando ten_con = {} v".format(e, self.ten_con_geradores_media_codigo))
                ten_con = self.ten_con_geradores_media_codigo

            ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_con), "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_nom da tabela ugmt_tab: {}, colocando ten_con = {} v".format(ten_nom_voltage, self.ten_con_geradores_media_volts))
            ten_nom_voltage = self.ten_con_geradores_media_volts

            ##########################      TRATAMENTO NA COLUNA POT_INST       ############################################

            if not isinstance(pot_inst, (int, float)):
                continue
            else:
                if pot_inst > self.pot_inst_geradores_media_kva:
                     print("Erro! Há valores de potencia instalada acima de {} Mw na coluna -pot_inst- da tabela -ugmt_tab-: {}, colocando"
                           "valor padrão: pot_inst = {} Mw".format(self.pot_inst_geradores_media_kva, pot_inst, self.pot_inst_geradores_media_kva))
                     pot_inst = self.pot_inst_geradores_media_kva



            ############################################################################################################


            # Verifica e cria a pasta para o CTMT, se necessário
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'Geradores_media_tensao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]


            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABC')

            # Gera o comando OpenDSS
            command_generator = (

                "New Generator.{}_Gerador_Media_tensao Bus1={}{} Model=1 PF=0.8\n".format(cod_id, pac, rec_fases) +
                "~ kva={} KV={} Xdp=0.27 xdpp=0.20 H=2\n".format(pot_inst, ten_nom_voltage / 1000) +
                "~ Conn=wye\n\n"

            )

            # Escreve o comando no arquivo correspondente
            if file:
                file.write(command_generator.encode())

        # Fecha todos os arquivos abertos
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Geradores de média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def Shape_Gd_Media_Tensao(self):
        """ Modelagem de Formato da curva de geração para gd de média tensão """

        base_dir = caminho_modelagens
        dados = []
        ctmts_processados = {}
        ctmt_folder = []


        try:
            # Consulta SQL para extrair os dados
            query = """
            
                 SELECT DISTINCT ugmt_tab.cod_id, ctmt.nome
                 FROM ugmt_tab
                 JOIN ctmt on ugmt_tab.ctmt = ctmt.cod_id
                 WHERE ugmt_tab.ceg_gd LIKE 'GD%' OR ugmt_tab.ceg_gd LIKE 'UFV%'
                 ORDER BY ctmt.nome
            
            """
            self.cur.execute(query)
            dados = self.cur.fetchall()

        except Exception as e:
            print("Erro ao executar consulta no banco de dados da tabela ugmt_tab: {}".format(e))
            return


        # Processa os dados
        for index_cima, linha in enumerate(dados):
            cod_id, nome = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################



            if not nome:
                print("Linha contém valores nulos e será ignorada: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}".format(nome))




            ############################################################################################################


            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                ctmts_processados[nome] = ctmt_folder

            # Gera os arquivos de irradiância para os 12 meses
            for indice in range(12):

                # Criação do diretório para o mês
                ene_folder = os.path.join(ctmt_folder, str(indice + 1))
                os.makedirs(ene_folder, exist_ok=True)

                for tip_dia in ['DO', 'DU', 'SA']:
                    # Caminho do arquivo .txt para o mês e tipo de dia
                    json_file_path = os.path.join(ene_folder, "mes_{}_{}.json".format(indice + 1, tip_dia))

                    # Chave no formato especificado
                    chave = "{}_{}_{}".format(cod_id, indice, tip_dia)

                    # Dados a serem salvos no arquivo JSON
                    data = {
                        chave: self.irradiance_96
                    }

                    # Escrita no arquivo JSON
                    with open(json_file_path, 'w') as file:
                        json.dump(data, file, indent=4)

        print("Modelagem: Formato de curva de geração da gd de média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Shape_Gd_Baixa_Tensao(self):
        """ Modelagem de Formato da curva de geração para gd de baixa tensão """

        base_dir = caminho_modelagens
        dados = []
        ctmts_processados = {}
        ctmt_folder = []

        try:
            # Consulta SQL para extrair os dados
            query = """

                       SELECT DISTINCT ugbt_tab.cod_id, ctmt.nome
                       FROM ugbt_tab
                       JOIN ctmt on ugbt_tab.ctmt = ctmt.cod_id
                       WHERE ugbt_tab.ceg_gd LIKE 'GD%' OR ugbt_tab.ceg_gd LIKE 'UFV%'
                       ORDER BY ctmt.nome

                  """
            self.cur.execute(query)
            dados = self.cur.fetchall()

        except Exception as e:
            print("Erro ao executar consulta no banco de dados da tabela ugbt_tab: {}".format(e))
            return

        # Processa os dados
        for index_cima, linha in enumerate(dados):
            cod_id, nome = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################



            if not nome:
                print("Linha contém valores nulos e será ignorada: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}".format(nome))




            ############################################################################################################

            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                ctmts_processados[nome] = ctmt_folder

            # Gera os arquivos de irradiância para os 12 meses
            for indice in range(12):

                # Criação do diretório para o mês
                ene_folder = os.path.join(ctmt_folder, str(indice + 1))
                os.makedirs(ene_folder, exist_ok=True)

                for tip_dia in ['DO', 'DU', 'SA']:
                    # Caminho do arquivo .txt para o mês e tipo de dia
                    json_file_path = os.path.join(ene_folder, "mes_{}_{}.json".format(indice + 1, tip_dia))

                    # Chave no formato especificado
                    chave = "{}_{}_{}".format(cod_id, indice, tip_dia)

                    # Dados a serem salvos no arquivo JSON
                    data = {
                        chave: self.irradiance_96
                    }

                    # Escrita no arquivo JSON
                    with open(json_file_path, 'w') as file:
                        json.dump(data, file, indent=4)

        print("Modelagem: Formato de curva de geração da gd de baixa Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Linecodes_Baixa_Tensao(self):
        """ Modelagem de código de linhas de baixa tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens

        def linecode_commands():
            """Consulta"""

            query = """
            
                   SELECT 
                                   ctmt.nome,
                                   ssdbt.tip_cnd,
                                   ssdbt.fas_con,
                                   --ssdbt.comp,
                                   (SELECT r1 FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS r1,
                                   (SELECT x1 FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS x1,
                                   (SELECT cnom FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS cnom,
                                   (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS cmax_renamed
                   FROM 
                                    ssdbt
                   JOIN             ctmt on ctmt.cod_id = ssdbt.ctmt  
                   --where ctmt.cod_id = '764444'
                   ORDER BY         ctmt.nome
              
               """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()


            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue

                # if nome.isdigit():
                #     pass
                # else:
                #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada ".format(nome))




                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print("Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdbt-: {}, colocando valor padrão: fas_con = ABC".format(
                        fas_con))
                    fas_con = 'ABC'



                ###############################     TRATAMENTO DA COLUNA COMP       ####################################



                # if not isinstance(comp, (float, int)):
                #     print("Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ssdbt-: {}, colocando valor padrão comp = 1".format(comp))
                #     comp = 1



                ###############################     TRATAMENTO DA COLUNA R1       ######################################



                if not isinstance(r1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -r1- da tabela -ssdbt-: {}, colocando valor padrão r1 = {}".format(
                            r1,self.r1_linecodes_baixa_ohms_km))
                    r1 = self.r1_linecodes_baixa_ohms_km

                else:
                    r1 = float(r1)
                    if r1 > self.r1_linecodes_baixa_ohms_km:
                        print(
                            "Erro! Há valores de r1 maiores do que {} ohms/km na coluna -r1- da tabela -ssdbt-: {}, substituindo r1 por r1 = {}".format(
                                r1,r1, self.r1_linecodes_baixa_ohms_km))
                        r1 = self.r1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA X1       ######################################



                if not isinstance(x1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -x1- da tabela -ssdbt-: {}, colocando valor padrão x1 = {}".format(
                            x1, self.x1_linecodes_baixa_ohms_km))
                    x1 = self.x1_linecodes_baixa_ohms_km


                else:
                    x1 = float(x1)
                    if x1 > self.x1_linecodes_baixa_ohms_km:
                        print(
                            "Erro! Há valores de x1 maiores do que 1.5 ohms/km na coluna -r1- da tabela -ssdbt-: {}, colocando valor padrão x1 = {}".format(
                                r1, self.x1_linecodes_baixa_ohms_km))
                        x1 = self.x1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA CNOM       ####################################



                if not isinstance(cnom, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cnom- da tabela -ssdbt-: {}, colocando valor padrão cnom = {} Amperes".format(
                            x1, self.cnom_linecodes_baixa_amperes))
                    cnom = self.cnom_linecodes_baixa_amperes

                else:
                    cnom = float(cnom)
                    if cnom > self.cnom_linecodes_baixa_amperes:
                        print(
                            "Erro! Há valores de cnom maiores do que 600.0 Amperes na coluna -r1- da tabela -ssdbt-: {}, colocando valor padrão cnom = {} Amperes".format(
                                cnom, self.cnom_linecodes_baixa_amperes))
                        cnom = self.cnom_linecodes_baixa_amperes



                ###############################     TRATAMENTO DA COLUNA CMAX_RENAMED       ############################



                if not isinstance(cmax_renamed, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cmax_renamed- da tabela -ssdbt-: {}, colocando valor padrão cmax_renamed = {} Amperes".format(
                            cmax_renamed, self.cmax_renamed_linecodes_baixa_amperes))
                    cmax_renamed = self.cmax_renamed_linecodes_baixa_amperes



                ########################################################################################################

                # Verificar se o ctmt já foi processado
                if nome not in ctmts_processados:
                    # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)

                    # Criar o novo arquivo .dss para este ctmt
                    file_path = os.path.join(ctmt_folder, 'linecodes_baixa_tensao.bin')
                    file = open(file_path, 'wb')

                    # Adicionar o ctmt ao dicionario de ctmts processados (armazena o arquivo aberto)
                    ctmts_processados[nome] = file
                else:
                    # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                    file = ctmts_processados[nome]

                fases_validades = [letra for letra in fas_con if letra in ['A', 'B', 'C']]

                # Gerar o comando para cada linha
                if len(fases_validades) in [1, 2, 3]:
                    nphases = len(fases_validades)  # Número de fases válido (1, 2 ou 3)
                    command_linecode = (

                        'New linecode.{}_linecode_baixa nphases={} BaseFreq=60\n'.format(tip_cnd, nphases) +
                        '~ r1={}\n'.format(r1 / 1000) +
                        '~ x1={}\n'.format(x1 / 1000) +
                        '~ c1=0\n'+
                        '~ Normamps = {}\n'.format(cnom) +
                        '~ Emergamps = {}\n\n'.format(cmax_renamed)

                    )
                else:
                    continue  # Caso não seja nenhuma das opções, ignora e continua

                # Escrever o comando no arquivo.dss
                if file:
                    file.write(command_linecode.encode())

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

        try:
            # Gerar comandos para o OpenDSS
            linecode_commands()
            print("Comandos para o OpenDSS gerados com sucesso.")
        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))

        print("Modelagem: Linecodes das linhas de baixa Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Linecodes_Media_Tensao(self):
        """ Modelagem de código de linhas de média tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens

        def linecode_commands():
            """Consulta"""

            query = """

                         SELECT 
                                         ctmt.nome,
                                         ssdmt.tip_cnd,
                                         ssdmt.fas_con,
                                         --ssdmt.comp,
                                         (SELECT r1 FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS r1,
                                         (SELECT x1 FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS x1,
                                         (SELECT cnom FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS cnom,
                                         (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS cmax_renamed
                         FROM 
                                        ssdmt
                         JOIN           ctmt on ctmt.cod_id = ssdmt.ctmt  
                       --   where ctmt.cod_id = '764444'
                         ORDER BY       ctmt.nome

                     """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue

                # if nome.isdigit():
                #     pass
                # else:
                #     print(
                #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada ".format(
                #             nome))




                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print(
                        "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdmt-: {}, colocando valor padrão: fas_con = ABC".format(
                            fas_con))
                    fas_con = 'ABC'



                ###############################     TRATAMENTO DA COLUNA COMP       ####################################


                #
                # if not isinstance(comp, (float, int)):
                #     print(
                #         "Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ssdmt-: {}, colocando valor padrão comp = 1".format(
                #             comp))
                #     comp = 1



                ###############################     TRATAMENTO DA COLUNA R1       ######################################



                if not isinstance(r1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -r1- da tabela -ssdmt-: {}, colocando valor padrão r1 = {}".format(
                            r1, self.r1_linecodes_baixa_ohms_km))
                    r1 = self.r1_linecodes_baixa_ohms_km

                else:
                    r1 = float(r1)
                    if r1 > 1.5:
                        print(
                            "Erro! Há valores de r1 maiores do que 1.5 ohms/km na coluna -r1- da tabela -ssdmt-: {}, substituindo r1 por r1 = {}".format(
                                r1, self.r1_linecodes_baixa_ohms_km))
                        r1 = self.r1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA X1       ######################################



                if not isinstance(x1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -x1- da tabela -ssdmt-: {}, colocando valor padrão x1 = {}".format(
                            x1, self.x1_linecodes_baixa_ohms_km))
                    x1 = self.x1_linecodes_baixa_ohms_km


                else:
                    x1 = float(x1)
                    if x1 > self.x1_linecodes_baixa_ohms_km:
                        print(
                            "Erro! Há valores de x1 maiores do que 1.5 ohms/km na coluna -r1- da tabela -ssdmt-: {}, colocando valor padrão x1 = {}".format(
                                r1, self.x1_linecodes_baixa_ohms_km))
                        x1 = self.x1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA CNOM       ####################################



                if not isinstance(cnom, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cnom- da tabela -ssdmt-: {}, colocando valor padrão cnom = {} Amperes".format(
                            x1, self.cnom_linecodes_baixa_amperes))
                    cnom = self.cnom_linecodes_baixa_amperes

                else:
                    cnom = float(cnom)
                    if cnom > self.cnom_linecodes_baixa_amperes:
                        print(
                            "Erro! Há valores de cnom maiores do que 600.0 Amperes na coluna -r1- da tabela -ssdmt-: {}, colocando valor padrão cnom = {} Amperes".format(
                                cnom, self.cnom_linecodes_baixa_amperes))
                        cnom = self.cnom_linecodes_baixa_amperes



                ###############################     TRATAMENTO DA COLUNA CMAX_RENAMED       ############################



                if not isinstance(cmax_renamed, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cmax_renamed- da tabela -ssdmt-: {}, colocando valor padrão cmax_renamed = {} Amperes".format(
                            cmax_renamed, self.cmax_renamed_linecodes_baixa_amperes))
                    cmax_renamed = self.cmax_renamed_linecodes_baixa_amperes



                ########################################################################################################

                # Verificar se o ctmt já foi processado
                if nome not in ctmts_processados:
                    # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)

                    # Criar o novo arquivo .dss para este ctmt
                    file_path = os.path.join(ctmt_folder, 'linecodes_media_tensao.bin')
                    file = open(file_path, 'wb')

                    # Adicionar o ctmt ao dicionario de ctmts processados (armazena o arquivo aberto)
                    ctmts_processados[nome] = file
                else:
                    # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                    file = ctmts_processados[nome]

                fases_validades = [letra for letra in fas_con if letra in ['A', 'B', 'C']]

                # Gerar o comando para cada linha
                if len(fases_validades) in [1, 2, 3]:
                    nphases = len(fases_validades)  # Número de fases válido (1, 2 ou 3)
                    command_linecode = (

                            'New linecode.{}_linecode_media nphases={} BaseFreq=60\n'.format(tip_cnd, nphases) +
                            '~ r1={}\n'.format(r1 / 1000) +
                            '~ x1={}\n'.format(x1 / 1000) +
                            '~ c1=0\n' +
                            '~ Normamps = {}\n'.format(cnom) +
                            '~ Emergamps = {}\n\n'.format(cmax_renamed)

                    )
                else:
                    continue  # Caso não seja nenhuma das opções, ignora e continua

                # Escrever o comando no arquivo.dss
                if file:
                    file.write(command_linecode.encode())

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

        try:
            # Gerar comandos para o OpenDSS
            linecode_commands()
            print("Comandos para o OpenDSS gerados com sucesso.")
        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))

        print("Modelagem: Linecodes das linhas de media Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Linecodes_Ramais_Baixa_Tensao(self):
        """ Modelagem de código de linhas de Ramais de ligação de média tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens

        def linecode_commands():
            """Consulta"""

            query = """

                                SELECT DISTINCT
                                                    ctmt.nome,
                                                    ramlig.tip_cnd,
                                                    ramlig.fas_con,
                                                    --ramlig.comp,
                                                    (SELECT r1 FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS r1,
                                                    (SELECT x1 FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS x1,
                                                    (SELECT cnom FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS cnom,
                                                    (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS cmax_renamed
                                FROM 
                                                    ramlig
                                JOIN                ctmt on ctmt.cod_id = ramlig.ctmt  
                                -- where ctmt.cod_id = '764444'
                                ORDER BY            ctmt.nome

                            """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue

                # if nome.isdigit():
                #     pass
                # else:
                #     print(
                #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada ".format(
                #             nome))




                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print(
                        "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ramlig-: {}, colocando valor padrão: fas_con = ABC".format(
                            fas_con))
                    fas_con = 'ABC'



                ###############################     TRATAMENTO DA COLUNA COMP       ####################################



                # if not isinstance(comp, (float, int)):
                #     print(
                #         "Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ramlig-: {}, colocando valor padrão comp = 1".format(
                #             comp))
                #     comp = 1



                ###############################     TRATAMENTO DA COLUNA R1       ######################################



                if not isinstance(r1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -r1- da tabela -ramlig-: {}, colocando valor padrão r1 = {}".format(
                            r1, self.r1_linecodes_baixa_ohms_km))
                    r1 = self.r1_linecodes_baixa_ohms_km

                else:
                    r1 = float(r1)
                    if r1 > self.r1_linecodes_baixa_ohms_km:
                        print(
                            "Erro! Há valores de r1 maiores do que 1.5 ohms/km na coluna -r1- da tabela -ramlig-: {}, substituindo r1 por r1 = {}".format(
                                r1, self.r1_linecodes_baixa_ohms_km))
                        r1 = self.r1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA X1       ######################################



                if not isinstance(x1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -x1- da tabela -ramlig-: {}, colocando valor padrão x1 = {}".format(
                            x1, self.x1_linecodes_baixa_ohms_km))
                    x1 = self.x1_linecodes_baixa_ohms_km


                else:
                    x1 = float(x1)
                    if x1 > self.x1_linecodes_baixa_ohms_km:
                        print(
                            "Erro! Há valores de x1 maiores do que 1.5 ohms/km na coluna -r1- da tabela -ramlig-: {}, colocando valor padrão x1 = {}".format(
                                r1, self.x1_linecodes_baixa_ohms_km))
                        x1 = self.x1_linecodes_baixa_ohms_km



                ###############################     TRATAMENTO DA COLUNA CNOM       ####################################



                if not isinstance(cnom, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cnom- da tabela -ramlig-: {}, colocando valor padrão cnom = {} Amperes".format(
                            x1, self.cnom_linecodes_baixa_amperes))
                    cnom = self.cnom_linecodes_baixa_amperes

                else:
                    cnom = float(cnom)
                    if cnom > self.cnom_linecodes_baixa_amperes:
                        print(
                            "Erro! Há valores de cnom maiores do que 600.0 Amperes na coluna -r1- da tabela -ramlig-: {}, colocando valor padrão cnom = {} Amperes".format(
                                cnom, self.cnom_linecodes_baixa_amperes))
                        cnom = self.cnom_linecodes_baixa_amperes



                ###############################     TRATAMENTO DA COLUNA CMAX_RENAMED       ############################



                if not isinstance(cmax_renamed, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -cmax_renamed- da tabela -ramlig-: {}, colocando valor padrão cmax_renamed = {} Amperes".format(
                            cmax_renamed, self.cmax_renamed_linecodes_baixa_amperes))
                    cmax_renamed = self.cmax_renamed_linecodes_baixa_amperes



                ########################################################################################################



                # Verificar se o ctmt já foi processado
                if nome not in ctmts_processados:
                    # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)

                    # Criar o novo arquivo .dss para este ctmt
                    file_path = os.path.join(ctmt_folder, 'linecodes_ramais_de_ligacao_baixa_tensao.bin')
                    file = open(file_path, 'wb')

                    # Adicionar o ctmt ao dicionario de ctmts processados (armazena o arquivo aberto)
                    ctmts_processados[nome] = file
                else:
                    # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                    file = ctmts_processados[nome]

                fases_validades = [letra for letra in fas_con if letra in ['A', 'B', 'C']]

                # Gerar o comando para cada linha
                if len(fases_validades) in [1, 2, 3]:
                    nphases = len(fases_validades)  # Número de fases válido (1, 2 ou 3)
                    command_linecode = (

                            'New linecode.{}_linecode_ramais nphases={} BaseFreq=60\n'.format(tip_cnd, nphases) +
                            '~ r1={}\n'.format(r1 / 1000) +
                            '~ x1={}\n'.format(x1 / 1000) +
                            '~ c1=0\n' +
                            '~ Normamps = {}\n'.format(cnom) +
                            '~ Emergamps = {}\n\n'.format(cmax_renamed)

                    )
                else:
                    continue  # Caso não seja nenhuma das opções, ignora e continua

                # Escrever o comando no arquivo.dss
                if file:
                    file.write(command_linecode.encode())

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

        try:
            # Gerar comandos para o OpenDSS
            linecode_commands()
            print("Comandos para o OpenDSS gerados com sucesso.")
        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))

        print("Modelagem: Linecodes dos ramais de ligação de baixa Tensão gerada com sucesso !!!")






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33






    def Linhas_Baixa_Tensao(self):
        """ Modelagem das linhas de distribuição de baixa tensão """

        results = []
        ctmts_processados = {}
        base_dir = caminho_modelagens


        try:

            query = """
            
                          SELECT DISTINCT
                                          ssdbt.cod_id,
                                          pac_1,
                                          pac_2,
                                          ctmt.nome,
                                          fas_con,
                                          comp,
                                          tip_cnd
                                        
                          FROM 
                                          ssdbt
                          JOIN 
                                          ctmt ON ctmt.cod_id = ssdbt.ctmt
                          ORDER BY            ctmt.nome
                     
                      """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))



        # Iterar sobre os dados e gerar uma subpasta para cada CTMT
        for index, linha in enumerate(results):
            cod_id, pac_1, pac_2, nome, fas_con, comp, tip_cnd = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ################################        TRATAMENTO DA COLUNA NOME       ####################################



            if not nome:
                print("A coluna nome da tabela ctmt está vazia, logo será excluída da MODELAGEM !!! : {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada !!!".format(nome))




            ################################        TRATAMENTO DA COLUNA FAS_CON       #################################



            if fas_con.isalpha():
                pass
            else:
                print("Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdbt-: {}, inserindo valor padrão: fas_con = ABC".format(
                    fas_con))
                fas_con = 'ABC'



            ################################        TRATAMENTO DA COLUNA COMP       ####################################



            if not isinstance(comp, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ssdbt-: {}, inserindo valor padrão: comp = 1 metro".format(
                        comp))
                comp = 1



            ################################        TRATAMENTO DA COLUNA COD_ID       ##################################



            if not cod_id:
                print("A coluna cod_id da tabela ssdbt está vazia, preenchendo com valor padrão: cod_id = {}_{}".format(nome, index))
                cod_id = nome + index



            ################################        TRATAMENTO DA COLUNA PAC_1       ###################################



            if not pac_1:
                print("Erro! Não há valores na coluna -pac_1- da tabela -ssdbt-: {}, esta linha será eliminada da modelagem !!!".format(
                    pac_1))
                continue



            ################################        TRATAMENTO DA COLUNA PAC_2       ###################################



            if not pac_2:
                print(
                    "Erro! Não há valores na coluna -pac_2- da tabela -ssdbt-: {}, esta linha será eliminada da modelagem !!!".format(
                        pac_2))
                continue



            ################################        TRATAMENTO DA COLUNA TIP_CND       #################################



            if not tip_cnd:
                print("Erro! A coluna tip_cnd está vazia, usando valores padrão na coluna -tip_cnd- da tabela -ssdbt-: {}".format(
                    self.tip_cnd_linhas_baixa))
                tip_cnd = self.tip_cnd_linhas



            ############################################################################################################

            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'linhas_baixa_tensao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
            fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

            if ".4" not in rec_fases:
                rec_fases = rec_fases + '.4'
            else:
                pass


            # Gerar o comando no formato desejado
            command_line = (

                'New Line.{}_linha_baixa Phases = {} Bus1 = {}{} Bus2 = {}{} Linecode = {}_linecode_baixa Length = {} units = m\n\n'.format(
                   cod_id, len(fases), pac_1, rec_fases, pac_2, rec_fases, tip_cnd, comp

                )
            )
            # Escrever o comando no arquivo .dss
            if file:
                file.write(command_line.encode())

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Linhas de distribuição de baixa Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Linhas_Media_Tensao(self):
        """ Modelagem das linhas de distribuição de media tensão """

        results = []
        ctmts_processados = {}
        base_dir = caminho_modelagens


        try:

            query = """
            
                          SELECT DISTINCT
                                          ssdmt.cod_id,
                                          pac_1,
                                          pac_2,
                                          ctmt.nome,
                                          fas_con,
                                          comp,
                                          tip_cnd
                                         
                          FROM 
                                          ssdmt
                          JOIN 
                                          ctmt ON ctmt.cod_id = ssdmt.ctmt
                          ORDER BY            ctmt.nome
                     
                     
                      """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))



        # Iterar sobre os dados e gerar uma subpasta para cada CTMT
        for index, linha in enumerate(results):
            cod_id, pac_1, pac_2, nome, fas_con, comp, tip_cnd = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ################################        TRATAMENTO DA COLUNA NOME       ####################################



            if not nome:
                print("A coluna nome da tabela ctmt está vazia, logo será excluída da MODELAGEM !!! : {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada !!!".format(nome))




            ################################        TRATAMENTO DA COLUNA FAS_CON       #################################



            if fas_con.isalpha():
                pass
            else:
                print("Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdmt-: {}, inserindo valor padrão: fas_con = ABC".format(
                    fas_con))
                fas_con = 'ABC'



            ################################        TRATAMENTO DA COLUNA COMP       ####################################



            if not isinstance(comp, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ssdmt-: {}, inserindo valor padrão: comp = 1 metro".format(
                        comp))
                comp = 1



            ################################        TRATAMENTO DA COLUNA COD_ID       ##################################



            if not cod_id:
                print("A coluna cod_id da tabela ssdbt está vazia, preenchendo com valor padrão: cod_id = {}_{}".format(nome, index))
                cod_id = nome + index



            ################################        TRATAMENTO DA COLUNA PAC_1       ###################################



            if not pac_1:
                print("Erro! Não há valores na coluna -pac_1- da tabela -ssdmt-: {}, esta linha será eliminada da modelagem !!!".format(
                    pac_1))
                continue



            ################################        TRATAMENTO DA COLUNA PAC_2       ###################################



            if not pac_2:
                print(
                    "Erro! Não há valores na coluna -pac_2- da tabela -ssdbt-: {}, esta linha será eliminada da modelagem !!!".format(
                        pac_2))
                continue



            ################################        TRATAMENTO DA COLUNA TIP_CND       #################################



            if not tip_cnd:
                print("Erro! A coluna tip_cnd está vazia, usando valores padrão na coluna -tip_cnd- da tabela -ssdmt-: {}".format(
                    self.tip_cnd_linhas_baixa))
                tip_cnd = self.tip_cnd_linhas



            ############################################################################################################

            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'linhas_media_tensao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
            fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

            # Remover a parte .4 e tudo o que vem depois
            if '.4' in rec_fases:
                rec_fases = rec_fases.split('.4')[0]

            # Gerar o comando no formato desejado
            command_line = (

                'New Line.{}_linha_media Phases = {} Bus1 = {}{} Bus2 = {}{} Linecode = {}_linecode_media Length = {} units = m\n\n'.format(
                   cod_id, len(fases), pac_1, rec_fases, pac_2, rec_fases, tip_cnd, comp

                )
            )
            # Escrever o comando no arquivo .dss
            if file:
                file.write(command_line.encode())

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Linhas de distribuição de media Tensão gerada com sucesso !!!")






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_Baixa_Tensao(self):
        """ Modelagem das Cargas de baixa tensão """

        offset = 0
        limit = 500000
        base_dir = caminho_modelagens
        ctmts_processados = {}
        processed_rows = 0  # Contador de linhas processadas


        try:

            total_rows_query = """
            
                            SELECT              
                                                COUNT(*)
                            FROM                
                                                ucbt_tab
                            WHERE 
                                                ucbt_tab.gru_ten = 'BT';
           
            """
            self.cur.execute(total_rows_query)
            total_rows = self.cur.fetchone()[0]

            # Processamento das linhas em lotes
            while processed_rows < total_rows:
                query = """
                
                   SELECT DISTINCT 
                                    ucbt_tab.cod_id,
                                    ucbt_tab.tip_cc, 
                                    ucbt_tab.pac, 
                                    ctmt.nome, 
                                    ucbt_tab.fas_con, 
                                    ucbt_tab.ten_forn
                                    
                    FROM 
                                    ucbt_tab
                    JOIN 
                                    ctmt ON ctmt.cod_id = ucbt_tab.ctmt
                    WHERE 
                                    ucbt_tab.gru_ten = 'BT'
                    --and ctmt.cod_id = '764444'
                    ORDER BY 
                                    ctmt.nome
                    LIMIT {} OFFSET {};
               
                """.format(limit, offset)

                self.cur.execute(query)
                rows = self.cur.fetchall()

                if not rows:
                    break  # Encerra se não houver mais dados

                for index, linha in enumerate(rows):

                    cod_id, tip_cc, pac, nome, fas_con, ten_forn = linha

                    # VERIFICAÇÃO ANTI-ERROS
                    ####################################################################################################
                    ###############################     TRATAMENTO COLUNA NOME      ####################################



                    if not nome:
                        print("A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(linha))
                        continue

                    # if nome.isdigit():
                    #     pass
                    # else:
                    #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(nome))
                    #



                    ###############################     TRATAMENTO COLUNA FAS_CON      #################################



                    if fas_con.isalpha():
                        pass
                    else:
                        print(
                            "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ucbt_tab-: {}, colocando valor padrão: fas_con = ABC".format(
                                fas_con))
                        fas_con = 'ABCN'



                    ###############################     TRATAMENTO COLUNA NOME      ####################################



                    if ten_forn.isdigit():
                        pass
                    else:
                        print("Erro! Há valores não numericos na coluna -ten_forn- da tabela -ucbt_tab-: {}, colocando valor padrão: ten_forn = {} ".format(ten_forn, self.ten_forn_cargas_baixa_codigo))
                        ten_forn = self.ten_forn_cargas_baixa_codigo

                    try:
                        ten_forn = int(ten_forn)
                    except Exception as e:
                        print("Erro ao converter para int a ten_forn da tabela ucbt_tab: {}, colocando valor padrão: ten_forn = {} ".format(e, self.ten_forn_cargas_baixa_codigo))
                        ten_forn = self.ten_forn_cargas_baixa_codigo


                    ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_forn), "não encontrado")

                    if ten_nom_voltage == "não encontrado":
                        print("Erro - Não existe correspondencia de tensão para a coluna "
                                "ten_forn da tabela ucbt_tab, colocando valor padrão: ten_forn = {} v")
                        ten_nom_voltage = self.ten_forn_cargas_baixa_volts



                    ###############################     TRATAMENTO COLUNA COD_ID      ####################################



                    if not cod_id:
                        print(
                            "Erro! Não há valores na coluna -cod_id- da tabela -ucbt_tab-: {}, inserindo valor padrão: {}_{}".format(
                                cod_id, nome, index))
                        cod_id = str(nome) + str(index)




                    ###############################     TRATAMENTO COLUNA TIP_CC      ####################################



                    if not tip_cc:
                        print(
                            "Erro! Não há valores na coluna -tip_cc- da tabela -ucbt_tab-, inserindo valores padrão: tip_cc = {}".format(
                                self.tip_cc_cargas))
                        tip_cc = self.tip_cc_cargas



                    ###############################     TRATAMENTO COLUNA PAC      ####################################



                    if not pac:
                        print(
                            "Erro! Não há valores na coluna -pac- da tabela -ucbt_tab-: {}, eliminando esta linha da modelagem !!!".format(
                                fas_con))
                        continue



                    ############################################################################################################

                    # Criação ou reutilização de arquivos para cada ctmt
                    if nome not in ctmts_processados:
                        ctmt_folder = os.path.join(base_dir, str(nome))
                        os.makedirs(ctmt_folder, exist_ok=True)
                        file_path = os.path.join(ctmt_folder, 'Cargas_Baixa_tensao.bin')
                        file = open(file_path, 'wb')
                        ctmts_processados[nome] = file
                    else:
                        file = ctmts_processados[nome]

                    # Determinar tensões e fases
                    rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
                    fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

                    if ".4" not in rec_fases:
                        rec_fases = rec_fases + '.4'
                    else:
                        pass

                    if len(fases) >= 2:
                        conn = 'wye'
                    else:
                        conn = 'wye'
                        ten_nom_voltage /= math.sqrt(3)



                    # Criar comandos para o OpenDSS
                    command_transformers = (

                        'New Load.{}_carga_baixa Bus1 = {}{} Phases = {}\n'.format(cod_id, pac, rec_fases, len(fases)) +
                        '~ Conn = {} Model = 1 Kv = {} Kw = 1 Kvar = 0\n\n'.format(conn, ten_nom_voltage / 1000)

                    )

                    # Escrever no arquivo correspondente
                    if file:
                        file.write(command_transformers.encode())

                    processed_rows += 1

                # Atualizar offset para próxima consulta
                offset += limit

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de baixa tensão processadas com sucesso !!!.".format(processed_rows))





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_Media_Tensao(self):
        """ Modelagem das Cargas de média tensão """

        offset = 0
        limit = 500000
        base_dir = caminho_modelagens
        ctmts_processados = {}
        processed_rows = 0  # Contador de linhas processadas

        try:

            total_rows_query = """

                                   SELECT              
                                                       COUNT(*)
                                   FROM                
                                                       ucmt_tab
                                   WHERE 
                                                       ucmt_tab.gru_ten = 'MT';

                   """
            self.cur.execute(total_rows_query)
            total_rows = self.cur.fetchone()[0]

            # Processamento das linhas em lotes
            while processed_rows < total_rows:
                query = """

                          SELECT DISTINCT 
                                           ucmt_tab.cod_id,
                                           ucmt_tab.tip_cc, 
                                           ucmt_tab.pac, 
                                           ctmt.nome, 
                                           ucmt_tab.fas_con, 
                                           ucmt_tab.ten_forn

                           FROM 
                                           ucmt_tab
                           JOIN 
                                           ctmt ON ctmt.cod_id = ucmt_tab.ctmt
                           WHERE 
                                           ucmt_tab.gru_ten = 'MT'
                           ORDER BY 
                                           ctmt.nome
                           LIMIT {} OFFSET {};

                       """.format(limit, offset)

                self.cur.execute(query)
                rows = self.cur.fetchall()

                if not rows:
                    break  # Encerra se não houver mais dados

                for index, linha in enumerate(rows):

                    cod_id, tip_cc, pac, nome, fas_con, ten_forn = linha

                    # VERIFICAÇÃO ANTI-ERROS
                    ####################################################################################################
                    ###############################     TRATAMENTO COLUNA NOME      ####################################



                    if not nome:
                        print(
                            "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(
                                linha))
                        continue

                    # if nome.isdigit():
                    #     pass
                    # else:
                    #     print(
                    #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(
                    #             nome))




                    ###############################     TRATAMENTO COLUNA FAS_CON      #################################



                    if fas_con.isalpha():
                        pass
                    else:
                        print(
                            "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ucmt_tab-: {}, colocando valor padrão: fas_con = ABC".format(
                                fas_con))
                        fas_con = 'ABCN'



                    ###############################     TRATAMENTO COLUNA NOME      ####################################



                    if ten_forn.isdigit():
                        pass
                    else:
                        print(
                            "Erro! Há valores não numericos na coluna -ten_forn- da tabela -ucmt_tab-: {}, colocando valor padrão: ten_forn = {}".format(
                                ten_forn, self.ten_forn_cargas_baixa_codigo))
                        ten_forn = self.ten_forn_cargas_baixa_codigo

                    try:
                        ten_forn = int(ten_forn)
                    except Exception as e:
                        print(
                            "Erro ao converter para int a ten_forn da tabela ucmt_tab: {}, colocando valor padrão: ten_forn = {}".format(
                                e, self.ten_forn_cargas_baixa_codigo))

                        ten_forn = self.ten_forn_cargas_baixa_codigo

                    ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_forn), "não encontrado")

                    if ten_nom_voltage == "não encontrado":
                        print("Erro - Não existe correspondencia de tensão para a coluna "
                              "ten_forn da tabela ucmt_tab, colocando valor padrão: ten_forn = {} v".format(self.ten_forn_cargas_baixa_volts))
                        ten_nom_voltage = self.ten_forn_cargas_baixa_volts



                    ###############################     TRATAMENTO COLUNA COD_ID      ####################################

                    if not cod_id:
                        print(
                            "Erro! Não há valores na coluna -cod_id- da tabela -ucmt_tab-: {}, inserindo valor padrão: {}_{}".format(
                                cod_id, nome, index))
                        cod_id = str(nome) + str(index)



                    ###############################     TRATAMENTO COLUNA TIP_CC      ####################################



                    if not tip_cc:
                        print(
                            "Erro! Não há valores na coluna -tip_cc- da tabela -ucmt_tab-:, inserindo valores padrão: {}".format(
                                self.tip_cc_cargas))
                        tip_cc = self.tip_cc_cargas



                    ###############################     TRATAMENTO COLUNA PAC      ####################################



                    if not pac:
                        print(
                            "Erro! Não há valores na coluna -pac- da tabela -ucmt_tab-: {}, eliminando esta linha da modelagem !!!".format(
                                pac))
                        continue



                    ############################################################################################################


                    # Criação ou reutilização de arquivos para cada ctmt
                    if nome not in ctmts_processados:
                        ctmt_folder = os.path.join(base_dir, str(nome))
                        os.makedirs(ctmt_folder, exist_ok=True)
                        file_path = os.path.join(ctmt_folder, 'Cargas_media_tensao.bin')
                        file = open(file_path, 'wb')
                        ctmts_processados[nome] = file
                    else:
                        file = ctmts_processados[nome]

                    # Determinar tensões e fases
                    rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
                    fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

                    if len(fases) >= 2:
                        conn = 'wye'
                    else:
                        conn = 'wye'
                        ten_nom_voltage /= math.sqrt(3)

                    if ".4" not in rec_fases:
                        rec_fases = rec_fases + '.4'
                    else:
                        pass



                    # Criar comandos para o OpenDSS
                    command_transformers = (

                            'New Load.{}_carga_media Bus1 = {}{} Phases = {}\n'.format(cod_id, pac, rec_fases,
                                                                                       len(fases)) +
                            '~ Conn = {} Model = 1 Kv = {} Kw = 1 Kvar = 0\n\n'.format(conn, ten_nom_voltage / 1000)

                    )

                    # Escrever no arquivo correspondente
                    if file:
                        file.write(command_transformers.encode())

                    processed_rows += 1

                # Atualizar offset para próxima consulta
                offset += limit

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de media tensão processadas com sucesso !!!.".format(processed_rows))





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def LoadShape_Poste_Iluminacao_Publica(self):
        """ Modelagem das Cargas de postes de iluminação pública """

        ctmt_folder = []
        base_dir = caminho_modelagens
        ctmts_processados = {}

        try:
            query = """
            
                SELECT DISTINCT
                                   pip.cod_id,
                                   ctmt.nome
                FROM       
                                   pip
                JOIN 
                                   ctmt on ctmt.cod_id = pip.ctmt
                ORDER BY    
                                   ctmt.nome
                    

            """
            self.cur.execute(query)
            dados = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return


        for index, linha in enumerate(dados):
            cod_id, nome = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ################################        TRATAMENTO NA COLUNA NOME       ####################################



            if not nome:
                print("A coluna nome da tabela ctmt possui valor nulo, eliminado da modelagem esta linha !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(nome))




            ################################        TRATAMENTO NA COLUNA COD_ID       ####################################



            if not cod_id:
                print(
                    "Erro! Não há valores na coluna -cod_id- da tabela -pip-: {}, inserindo valor padrão: {}_{}".format(
                        cod_id, nome, index))

                cod_id = str(nome) + str(index)



            ############################################################################################################

            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                ctmts_processados[nome] = ctmt_folder

            for index_2 in range(12):

                ene_folder = os.path.join(ctmt_folder, str(index + 1))
                os.makedirs(ene_folder, exist_ok=True)

                for tip_dia in ['DO', 'DU', 'SA']:
                    # Defina o caminho do arquivo JSON
                    json_file_path = os.path.join(ene_folder, "mes_{}_{}.json".format(index_2 + 1, tip_dia))

                    # Cria um dicionário com os dados a serem salvos
                    data = {"{}_{}".format(cod_id, tip_dia): self.on_off}

                    # Abre o arquivo JSON em modo de escrita e salva o dicionário
                    with open(json_file_path, 'a') as file:
                        json.dump(data, file)
                        file.write('\n')

        print("Modelagem: Formato de carga dos postes de iluminação gerada com sucesso !!!.")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33

    def write_to_file(self, file_path, cod_id, tip_dia, potencias_ajustadas, dados_baixa):



        with open(file_path, 'a') as file:
            file.write("{}_carga_baixa: {}\n".format(cod_id, potencias_ajustadas))

    def process_data_chunk(self, dados, output_dir, dados_baixa):
        for index, linha in enumerate(dados):
            ene_values = linha[:1]
            tip_dia = linha[14]
            nome = linha[16]
            pot_values = linha[19:115]
            cod_id = linha[115]

            # Verificações e tratamentos
            if not nome or not nome.isdigit():
                print(f"Erro ou ausência de valor na coluna nome: {linha}, ignorando.")
                continue

            if tip_dia not in ['DU', 'DO', 'SA']:
                print(f"Valor inesperado para tip_dia: {tip_dia}, alterando para 'DU'.")
                tip_dia = "DU"

            # Definindo o diretório e caminho do arquivo
            ctmt_folder = os.path.join(output_dir, nome)
            os.makedirs(ctmt_folder, exist_ok=True)

            for ene_index, ene in enumerate(ene_values):
                ene_folder = os.path.join(ctmt_folder, str(ene_index + 1))
                os.makedirs(ene_folder, exist_ok=True)
                txt_file_path = os.path.join(ene_folder, f"mes_{ene_index + 1}_{tip_dia}.txt")

                energia_curva_de_carga = sum(pot_values) * 0.25
                f = (int(ene_values[ene_index]) / 30) / energia_curva_de_carga
                potencias_ajustadas = [round(f * pot, 5) for pot in pot_values]

                # Chama a função de escrita de forma assíncrona para melhorar a performance
                self.write_to_file(txt_file_path, cod_id, tip_dia, potencias_ajustadas, dados_baixa)

    def LoadShape_Cargas_Baixa_Tensao(self):
        """ Modelagem do formato da curva de carga de baixa tensão """
        inicio = time.time()
        output_dir = caminho_modelagens
        offset = 0
        limit = 100000
        dados_baixa = {}

        try:
            with ThreadPoolExecutor(max_workers=4) as executor:  # Usando 4 threads para I/O paralelo
                while True:
                    query = """
                        SELECT 
                            ucbt_tab.ene_01, ucbt_tab.ene_02, ucbt_tab.ene_03, ucbt_tab.ene_04, ucbt_tab.ene_05,
                            ucbt_tab.ene_06, ucbt_tab.ene_07, ucbt_tab.ene_08, ucbt_tab.ene_09, ucbt_tab.ene_10,
                            ucbt_tab.ene_11, ucbt_tab.ene_12, ucbt_tab.tip_cc, ucbt_tab.gru_ten, crvcrg.tip_dia,
                            ucbt_tab.pac, ctmt.nome, ucbt_tab.fas_con, ucbt_tab.ten_forn,
                            crvcrg.pot_01, crvcrg.pot_02, crvcrg.pot_03, crvcrg.pot_04, crvcrg.pot_05, 
                            crvcrg.pot_06, crvcrg.pot_07, crvcrg.pot_08, crvcrg.pot_09, crvcrg.pot_10,
                            crvcrg.pot_11, crvcrg.pot_12, crvcrg.pot_13, crvcrg.pot_14, crvcrg.pot_15, 
                            crvcrg.pot_16, crvcrg.pot_17, crvcrg.pot_18, crvcrg.pot_19, crvcrg.pot_20,
                            crvcrg.pot_21, crvcrg.pot_22, crvcrg.pot_23, crvcrg.pot_24, crvcrg.pot_25, 
                            crvcrg.pot_26, crvcrg.pot_27, crvcrg.pot_28, crvcrg.pot_29, crvcrg.pot_30, 
                            crvcrg.pot_31, crvcrg.pot_32, crvcrg.pot_33, crvcrg.pot_34, crvcrg.pot_35, 
                            crvcrg.pot_36, crvcrg.pot_37, crvcrg.pot_38, crvcrg.pot_39, crvcrg.pot_40,
                            crvcrg.pot_41, crvcrg.pot_42, crvcrg.pot_43, crvcrg.pot_44, crvcrg.pot_45, 
                            crvcrg.pot_46, crvcrg.pot_47, crvcrg.pot_48, crvcrg.pot_49, crvcrg.pot_50, 
                            crvcrg.pot_51, crvcrg.pot_52, crvcrg.pot_53, crvcrg.pot_54, crvcrg.pot_55, 
                            crvcrg.pot_56, crvcrg.pot_57, crvcrg.pot_58, crvcrg.pot_59, crvcrg.pot_60,
                            crvcrg.pot_61, crvcrg.pot_62, crvcrg.pot_63, crvcrg.pot_64, crvcrg.pot_65,
                            crvcrg.pot_66, crvcrg.pot_67, crvcrg.pot_68, crvcrg.pot_69, crvcrg.pot_70,
                            crvcrg.pot_71, crvcrg.pot_72, crvcrg.pot_73, crvcrg.pot_74, crvcrg.pot_75,
                            crvcrg.pot_76, crvcrg.pot_77, crvcrg.pot_78, crvcrg.pot_79, crvcrg.pot_80,
                            crvcrg.pot_81, crvcrg.pot_82, crvcrg.pot_83, crvcrg.pot_84, crvcrg.pot_85,
                            crvcrg.pot_86, crvcrg.pot_87, crvcrg.pot_88, crvcrg.pot_89, crvcrg.pot_90,
                            crvcrg.pot_91, crvcrg.pot_92, crvcrg.pot_93, crvcrg.pot_94, crvcrg.pot_95,
                            crvcrg.pot_96, ucbt_tab.cod_id
                        FROM 
                            ucbt_tab
                        JOIN
                            crvcrg ON ucbt_tab.tip_cc = crvcrg.cod_id
                        JOIN 
                            ctmt ON ctmt.cod_id = ucbt_tab.ctmt
                        WHERE
                            ucbt_tab.gru_ten = 'BT'
                        AND
                            ucbt_tab.ctmt = '1429999'
                        ORDER BY            
                            ctmt.nome
                        LIMIT {} OFFSET {}
                    """.format(limit, offset)

                    # Executa a consulta SQL
                    self.cur.execute(query)
                    dados = self.cur.fetchall()

                    if not dados:
                        break  # Se não houver mais dados, sai do loop

                    # Processa os dados de forma assíncrona
                    executor.submit(self.process_data_chunk, dados, output_dir, dados_baixa)

                    # Atualiza o offset para o próximo bloco
                    offset += limit

        except Exception as e:
            print(f"Erro ao processar os dados para loadshape de cargas de baixa tensão: {e}")

        fim = time.time()
        print(
            f"Modelagem: Loadshape das cargas de baixa tensão processadas com sucesso !!!, em {fim - inicio} segundos.")






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33

    def LoadShape_Cargas_Media_Tensao(self):
        """ Modelagem do formato da curva de carga de média tensão """

        inicio = time.time()
        output_dir = caminho_modelagens
        offset = 0
        limit = 100000
        dados_media = {}

        try:
            while True:
                query = """
                    SELECT 
                        ucmt_tab.ene_01, ucmt_tab.ene_02, ucmt_tab.ene_03, ucmt_tab.ene_04, ucmt_tab.ene_05,
                        ucmt_tab.ene_06, ucmt_tab.ene_07, ucmt_tab.ene_08, ucmt_tab.ene_09, ucmt_tab.ene_10,
                        ucmt_tab.ene_11, ucmt_tab.ene_12, ucmt_tab.tip_cc, ucmt_tab.gru_ten, crvcrg.tip_dia,
                        ucmt_tab.pac, ctmt.nome, ucmt_tab.fas_con, ucmt_tab.ten_forn,
                        crvcrg.pot_01, crvcrg.pot_02, crvcrg.pot_03, crvcrg.pot_04, crvcrg.pot_05, 
                        crvcrg.pot_06, crvcrg.pot_07, crvcrg.pot_08, crvcrg.pot_09, crvcrg.pot_10,
                        crvcrg.pot_11, crvcrg.pot_12, crvcrg.pot_13, crvcrg.pot_14, crvcrg.pot_15, 
                        crvcrg.pot_16, crvcrg.pot_17, crvcrg.pot_18, crvcrg.pot_19, crvcrg.pot_20,
                        crvcrg.pot_21, crvcrg.pot_22, crvcrg.pot_23, crvcrg.pot_24, crvcrg.pot_25, 
                        crvcrg.pot_26, crvcrg.pot_27, crvcrg.pot_28, crvcrg.pot_29, crvcrg.pot_30, 
                        crvcrg.pot_31, crvcrg.pot_32, crvcrg.pot_33, crvcrg.pot_34, crvcrg.pot_35, 
                        crvcrg.pot_36, crvcrg.pot_37, crvcrg.pot_38, crvcrg.pot_39, crvcrg.pot_40,
                        crvcrg.pot_41, crvcrg.pot_42, crvcrg.pot_43, crvcrg.pot_44, crvcrg.pot_45, 
                        crvcrg.pot_46, crvcrg.pot_47, crvcrg.pot_48, crvcrg.pot_49, crvcrg.pot_50, 
                        crvcrg.pot_51, crvcrg.pot_52, crvcrg.pot_53, crvcrg.pot_54, crvcrg.pot_55, 
                        crvcrg.pot_56, crvcrg.pot_57, crvcrg.pot_58, crvcrg.pot_59, crvcrg.pot_60,
                        crvcrg.pot_61, crvcrg.pot_62, crvcrg.pot_63, crvcrg.pot_64, crvcrg.pot_65,
                        crvcrg.pot_66, crvcrg.pot_67, crvcrg.pot_68, crvcrg.pot_69, crvcrg.pot_70,
                        crvcrg.pot_71, crvcrg.pot_72, crvcrg.pot_73, crvcrg.pot_74, crvcrg.pot_75,
                        crvcrg.pot_76, crvcrg.pot_77, crvcrg.pot_78, crvcrg.pot_79, crvcrg.pot_80,
                        crvcrg.pot_81, crvcrg.pot_82, crvcrg.pot_83, crvcrg.pot_84, crvcrg.pot_85,
                        crvcrg.pot_86, crvcrg.pot_87, crvcrg.pot_88, crvcrg.pot_89, crvcrg.pot_90,
                        crvcrg.pot_91, crvcrg.pot_92, crvcrg.pot_93, crvcrg.pot_94, crvcrg.pot_95,
                        crvcrg.pot_96, ucmt_tab.cod_id
                    FROM 
                        ucmt_tab
                    JOIN
                        crvcrg ON ucmt_tab.tip_cc = crvcrg.cod_id
                    JOIN 
                        ctmt ON ctmt.cod_id = ucmt_tab.ctmt
                    WHERE
                        ucmt_tab.gru_ten = 'MT'
                    AND
                       ucmt_tab.ctmt = '1429999'
                    ORDER BY            
                        ctmt.nome
                    LIMIT {} OFFSET {}
                """.format(limit, offset)

                # Executa a consulta SQL
                self.cur.execute(query)
                dados = self.cur.fetchall()

                if not dados:
                    break  # Se não houver mais dados, sai do loop

                # Processa os dados e escreve imediatamente após a consulta
                for index, linha in enumerate(dados):
                    ene_values = linha[:1]
                    tip_dia = linha[14]
                    nome = linha[16]
                    pot_values = linha[19:115]
                    cod_id = linha[115]

                    # Verificações e tratamentos nos dados
                    ####################################################################################################
                    #######################     TRATAMENTO NA COLUNA NOME       ########################################



                    if not nome:
                        print(
                            "Não há valor na coluna nome da tabela ctmt, eliminando esta linha da modelagem !!!: {}".format(
                                linha))
                        continue
                    # if nome.isdigit():
                    #     pass
                    # else:
                    #     print(
                    #         "Erro! Há valores não numéricos na coluna -nome- da tabela -ctmt-: {}, eliminando esta coluna da modelagem !!!".format(
                    #             nome))




                    #######################     TRATAMENTO NA COLUNA ENE_VALUES       ##################################


                    # ene_values = [1 if e is None or e == 0 else e for e in ene_values]



                    #######################     TRATAMENTO NA COLUNA TIP_DIA       #####################################



                    if tip_dia not in ['DU', 'DO', 'SA']:
                        print("Valor inesperado para tip_dia: {}. Modificando para 'DU'.".format(tip_dia))
                        tip_dia = "DU"



                    #######################     TRATAMENTO NA COLUNA POT_VALUES       ##################################


                    # pot_values = [1 if p is None or p == 0 else p for p in pot_values]



                    ################################        TRATAMENTO NA COLUNA COD_ID       ####################################


                    if not cod_id:
                        cod_id = "{}{}".format(nome, index)



                    ####################################################################################################



                    ctmt_folder = os.path.join(output_dir, nome)
                    os.makedirs(ctmt_folder, exist_ok=True)

                    for ene_index, ene in enumerate(ene_values):
                        ene_folder = os.path.join(ctmt_folder, str(ene_index + 1))
                        os.makedirs(ene_folder, exist_ok=True)
                        txt_file_path = os.path.join(ene_folder, "mes_{}_{}.txt".format(ene_index + 1, tip_dia))
                        energia_curva_de_carga = sum(pot_values) * 0.25
                        f = (int(ene_values[ene_index]) / 30) / energia_curva_de_carga
                        potencias_ajustadas = [round(f * pot, 2) for pot in pot_values]

                        with open(txt_file_path, 'a') as file:
                            file.write("{}_carga_media: {}\n".format(cod_id, potencias_ajustadas))


                # Atualiza o offset para o próximo bloco
                offset += limit

        except Exception as e:
            print("Erro ao processar os dados para loadshape de cargas de média tensão: {}".format(e))

        fim = time.time()
        print("Modelagem: Loadshape das cargas de média tensão processadas com sucesso !!!, em {} segundos.".format(
            fim - inicio))





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def GeracaoShape_Geracao_Distribuida_Baixa_tensao(self):
        """ Modelagem do formato da curva de geração da gd de baixa tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}
        ctmt_folder = []


        try:
            query = """
            
            
             SELECT DISTINCT
                          ugbt_tab.cod_id, 
                          ugbt_tab.pac, 
                          ctmt.nome, 
                          ugbt_tab.fas_con,
                          ugbt_tab.ten_con, 
                          ugbt_tab.pot_inst, 
                          ugbt_tab.cep,
                          ugbt_tab.ceg_gd,
                          ugbt_tab.ene_01, 
                          ugbt_tab.ene_02, 
                          ugbt_tab.ene_03,
                          ugbt_tab.ene_04, 
                          ugbt_tab.ene_05, 
                          ugbt_tab.ene_06,
                          ugbt_tab.ene_07, 
                          ugbt_tab.ene_08, 
                          ugbt_tab.ene_09,
                          ugbt_tab.ene_10, 
                          ugbt_tab.ene_11, 
                          ugbt_tab.ene_12,
                          ugbt_tab.dem_cont
             FROM 
                          ugbt_tab
                          
             JOIN 
                          ctmt on ctmt.cod_id = ugbt_tab.ctmt
                          
             and ctmt.cod_id = '1429999'
             
             ORDER BY 
                          ctmt.nome

                          """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return


        for index, linha in enumerate(results):
            cod_id = linha[0]
            pac = linha[1]
            nome = linha[2]
            fas_con = linha[3]
            ten_con = linha[4]
            potencia_instalada_kwp = linha[5]
            potencia_max_inversor_kw = linha[5]
            energia_desejada = linha[8:9]
            ceg_gd = linha[7]

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ###############################     TRATAMENTO COLUNA NOME      ####################################



            if not nome:
                print(
                    "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print(
            #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(
            #             nome))
            #



            ###############################     TRATAMENTO COLUNA FAS_CON      #################################



            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ugbt_tab-: {}, colocando valor padrão: fas_con = ABC".format(
                        fas_con))
                fas_con = 'ABCN'



            ###############################     TRATAMENTO COLUNA TEN_FORN      ####################################



            if ten_con.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -ten_con- da tabela -ugbt_tab-: {}, colocando valor padrão: ten_con = {}".format(
                        ten_con, self.ten_con_gd_media_codigo))
                ten_con = self.ten_con_gd_media_codigo

            try:
                ten_con = int(ten_con)
            except Exception as e:
                print(
                    "Erro ao converter para int a ten_con da tabela ugbt_tab: {}, colocando valor padrão: ten_con = {}".format(
                        e, self.ten_con_gd_media_codigo))
                ten_con = self.ten_con_gd_media_codigo

            ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_con), "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "ten_con da tabela ugbt_tab, colocando valor padrão: ten_con = {} v".format(self.ten_con_gd_media_volts))
                ten_nom_voltage = self.ten_con_gd_media_volts



            ###############################     TRATAMENTO COLUNA COD_ID      ####################################

            if not cod_id:
                print(
                    "Erro! Não há valores na coluna -cod_id- da tabela -ugbt_tab-: {}, inserindo valor padrão: {}_{}".format(
                        cod_id, nome, index))
                cod_id = str(nome) + str(index)


            ###############################     TRATAMENTO COLUNA PAC      ####################################



            if not pac:
                print(
                    "Erro! Não há valores na coluna -pac- da tabela -ugbt_tab-: {}, eliminando esta linha da modelagem !!!".format(
                        fas_con))
                continue



            ###############################     TRATAMENTO COLUNA POTENCIA_INSTALADA_KWP      #################################



            # Verifica se o valor de potencia_instalada_kwp é int ou float
            if isinstance(potencia_instalada_kwp, (int, float)):
                # Verifica se é zero ou None
                if potencia_instalada_kwp == 0 or potencia_instalada_kwp is None:
                    print(
                        "Valor inválido ou zero para potencia_instalada_kwp: {}. Modificando para 1.".format(potencia_instalada_kwp))
                    potencia_instalada_kwp = 1
            else:
                # Caso não seja int ou float
                print(
                    "Valor não numérico encontrado para potencia_instalada_kwp: {}. Modificando para 1.".format(potencia_instalada_kwp))
                potencia_instalada_kwp = 1



            ###############################     TRATAMENTO COLUNA POTENCIA_MAX_INVERSOR_KW      #################################



            # Verifica se o valor de potencia_max_inversor_kw é int ou float
            if isinstance(potencia_max_inversor_kw, (int, float)):
                # Verifica se é zero ou None
                if potencia_max_inversor_kw == 0 or potencia_max_inversor_kw is None:
                    print(
                        "Valor inválido ou zero para potencia_max_inversor_kw: {}. Modificando para 1.".format(potencia_max_inversor_kw))
                    potencia_max_inversor_kw = 1
            else:
                # Caso não seja int ou float
                print(
                    "Valor não numérico encontrado para potencia_max_inversor_kw: {}. Modificando para 1.".format(potencia_max_inversor_kw))
                potencia_max_inversor_kw = 1



            ###############################     TRATAMENTO COLUNA ENERGIA_DESEJADA      #################################

            energia_desejada = list(energia_desejada)
            # Verifica se algum valor dentro da lista energia_desejada é int ou float
            for i in range(len(energia_desejada)):
                if isinstance(energia_desejada[i], (int, float)):
                    # Verifica se é zero ou None
                    if energia_desejada[i] == 0 or energia_desejada[i] is None:
                        print(
                            "Valor inválido ou zero encontrado para energia_desejada na posição {}: {}. Modificando para 1.".format(i, energia_desejada[i]))
                        energia_desejada[i] = 1
                else:
                    # Caso não seja int ou float
                    print(
                        "Valor não numérico encontrado para energia_desejada na posição {}: {}. Modificando para 1.".format(i, energia_desejada[i]))
                    energia_desejada[i] = 1



            ############################################################################################################



            if ceg_gd.startswith(('GD', 'UFV')):
                if nome not in ctmts_processados:
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)

                # Move a abertura do arquivo para fora do loop de 'index_dia' para evitar sobrescrever
                for index_mensal, mensal in enumerate(energia_desejada):
                    mensal_folder = os.path.join(ctmt_folder, str('geração_shape_mes_{}'.format(index_mensal + 1)))
                    os.makedirs(mensal_folder, exist_ok=True)

                    # Aqui, abre o arquivo uma vez para cada mês e grava todos os dados
                    diario_filename = 'dados_geracao_distribuida.txt'
                    file_path = os.path.join(mensal_folder, diario_filename)

                    # Usa 'w' para garantir que o arquivo será sobrescrito no início de cada mês
                    with open(file_path, 'a') as file:

                        def calcular_potencia_gerada(irradiancia, potencia_instalada_kwp_recebe, eficienci_a):
                            """Calcula a potência gerada pelo pv com base na irradiância solar"""
                            potencia = potencia_instalada_kwp_recebe * (irradiancia / 1000) * eficienci_a
                            return potencia

                        def calcular_irradianca_temperatura_desempenho(latitud_e, longitud_e,
                                                                       potencia_instalada_kwp_recebe, eficienci_a,
                                                                       potencia_max_inversor_kw_,
                                                                       energia_desejada_):
                            """Calcula a irradiância para cada tempo"""
                            site = location.Location(latitud_e, longitud_e, altitude=400)

                            rec_mensal = 0
                            rec_diario = 0
                            if int(index_mensal) < 1:
                                rec_mensal = 1

                            times = pd.date_range('2023-{}-{}'.format(str(index_mensal + 1).zfill(2), '01'),
                                                  '2023-{}-{} 23:59'.format(str(index_mensal + 1).zfill(2), '01'),
                                                  freq='15min',
                                                  tz='America/Cuiaba')

                            irradiance = site.get_clearsky(times)

                            # Calcular a potência gerada com base na irradiância (GHI)
                            potencia_gerada = calcular_potencia_gerada(irradiance['ghi'],
                                                                       potencia_instalada_kwp_recebe,
                                                                       eficienci_a)

                            energia_total_gerada = potencia_gerada.sum()
                            fator_ajuste = 1
                            if energia_total_gerada == 0:
                                energia_total_gerada = 1

                            if energia_desejada_[index_mensal] == 0:
                                fator_ajuste = (potencia_max_inversor_kw_ / 30) / energia_total_gerada
                            if energia_desejada_[index_mensal] != 0:
                                fator_ajuste = (energia_desejada_[index_mensal] / 30) / energia_total_gerada

                            # Ajustar a potência gerada para atingir a energia desejada
                            potencia_gerada_ajustad_a = potencia_gerada * fator_ajuste

                            return potencia_gerada_ajustad_a.tolist()

                        latitude = -15.59583
                        longitude = -56.09694
                        eficiencia = 0.18

                        potencia_gerada_ajustada = calcular_irradianca_temperatura_desempenho(
                            latitude,
                            longitude,
                            potencia_instalada_kwp, eficiencia,
                            potencia_max_inversor_kw, energia_desejada
                        )

                        # Usando o mapa de fases da variável de instância
                        rec_fases = self.mapa_fases.get(fas_con, 'ABCN')

                        if ".4" not in rec_fases:
                            rec_fases = rec_fases + '.4'
                        else:
                            pass

                        # Gravar o comando no arquivo
                        command_pvsystem = (
                                "New xycurve.mypvst_{} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n".format(cod_id) +
                                "New xycurve.myeff_{} npts = 4  xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n".format(cod_id) +
                                "New loadshape.myirrad_{} npts = 1 interval = 1 mult = [1]\n".format(cod_id) +
                                "New tshape.mytemp_{} npts = 1 interval = 1 temp = [25]\n".format(cod_id) +
                                "New pvsystem.pv_{} phases = {} conn = wye bus1 = {}\n".format(cod_id, len(fas_con),pac + rec_fases) +
                                "~ kv = {} kva = {} pmpp = {}\n".format(ten_nom_voltage / 1000, max(potencia_gerada_ajustada),max(potencia_gerada_ajustada)) +
                                "~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{}\n".format(cod_id) +
                                "~ p-tcurve = mypvst_{} daily = myirrad_{} tdaily = mytemp_{}\n\n".format(cod_id, cod_id, cod_id) +
                                "New load.{}_carga_no_pv bus1 = {} phases = {}\n".format(cod_id, pac + rec_fases, len(fas_con)) +
                                "~ conn = wye model = 1 kv = {} kw = 0.0001\n\n".format(ten_nom_voltage / 1000)
                        )

                        # Adiciona a linha de comando no arquivo
                        file.write(command_pvsystem)

        print("Modelagem: GD de baixa tensão processadas com sucesso !!!.")






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def GeracaoShape_Geracao_Distribuida_Media_tensao(self):
        """ Modelagem do formato da curva de geração da gd de media tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}
        ctmt_folder = []

        try:
            query = """


                    SELECT DISTINCT
                                 ugmt_tab.cod_id, 
                                 ugmt_tab.pac, 
                                 ctmt.nome, 
                                 ugmt_tab.fas_con,
                                 ugmt_tab.ten_con, 
                                 ugmt_tab.pot_inst, 
                                 ugmt_tab.cep,
                                 ugmt_tab.ceg_gd,
                                 ugmt_tab.ene_01, 
                                 ugmt_tab.ene_02, 
                                 ugmt_tab.ene_03,
                                 ugmt_tab.ene_04, 
                                 ugmt_tab.ene_05, 
                                 ugmt_tab.ene_06,
                                 ugmt_tab.ene_07, 
                                 ugmt_tab.ene_08, 
                                 ugmt_tab.ene_09,
                                 ugmt_tab.ene_10, 
                                 ugmt_tab.ene_11, 
                                 ugmt_tab.ene_12,
                                 ugmt_tab.dem_cont
                    FROM 
                                 ugmt_tab

                    JOIN 
                                 ctmt on ctmt.cod_id = ugmt_tab.ctmt
                                 
                     where ctmt.cod_id = '1429999'

                    ORDER BY 
                                 ctmt.nome

                                 """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return

        for index, linha in enumerate(results):
            cod_id = linha[0]
            pac = linha[1]
            nome = linha[2]
            fas_con = linha[3]
            ten_con = linha[4]
            potencia_instalada_kwp = linha[5]
            potencia_max_inversor_kw = linha[5]
            energia_desejada = linha[8:9]
            ceg_gd = linha[7]

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ###############################     TRATAMENTO COLUNA NOME      ####################################



            if not nome:
                print(
                    "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print(
            #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(
            #             nome))




            ###############################     TRATAMENTO COLUNA FAS_CON      #################################



            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -FAS_CON- da tabela -ugmt_tab-: {}, colocando valor padrão: FAS_CON = ABC".format(
                        fas_con))
                fas_con = 'ABC'



            ###############################     TRATAMENTO COLUNA TEN_FORN      ####################################



            if ten_con.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -TEN_FORN- da tabela -ugmt_tab-: {}, colocando valor padrão: TEN_FORN = {}".format(
                        ten_con, self.ten_con_gd_media_codigo))
                ten_con = self.ten_con_gd_media_codigo

            try:
                ten_con = int(ten_con)
            except Exception as e:
                print(
                    "Erro ao converter para int a coluna TEN_FORN da tabela ugmt_tab: {}, colocando valor padrão: TEN_FORN = {}".format(
                        e, self.ten_con_gd_media_codigo))
                ten_con = self.ten_con_gd_media_codigo

            ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_con), "não encontrado")

            if ten_nom_voltage == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "TEN_FORN da tabela ugmt_tab, colocando valor padrão: TEN_FORN = {} v".format(self.ten_con_gd_media_volts))
                ten_nom_voltage = self.ten_con_gd_media_volts



            ###############################     TRATAMENTO COLUNA COD_ID      ####################################



            if not cod_id:
                print(
                    "Erro! Não há valores na coluna -COD_ID- da tabela -ugmt_tab-: {}, inserindo valor padrão: {}_{}".format(
                        cod_id, nome, index))
                cod_id = str(nome) + str(index)



            ###############################     TRATAMENTO COLUNA PAC      ####################################



            if not pac:
                print(
                    "Erro! Não há valores na coluna -PAC- da tabela -ugmt_tab-: {}, eliminando esta linha da modelagem !!!".format(
                        fas_con))
                continue



            ###############################     TRATAMENTO COLUNA POTENCIA_INSTALADA_KWP      #################################



            # Verifica se o valor de potencia_instalada_kwp é int ou float
            if isinstance(potencia_instalada_kwp, (int, float)):
                # Verifica se é zero ou None
                if potencia_instalada_kwp == 0 or potencia_instalada_kwp is None:
                    print(
                        "Valor inválido ou zero para POTENCIA_INSTALADA_KWP: {}. Modificando para 1.".format(
                            potencia_instalada_kwp))
                    potencia_instalada_kwp = 1
            else:
                # Caso não seja int ou float
                print(
                    "Valor não numérico encontrado para POTENCIA_INSTALADA_KWP: {}. Modificando para 1.".format(
                        potencia_instalada_kwp))
                potencia_instalada_kwp = 1



            ###############################     TRATAMENTO COLUNA POTENCIA_MAX_INVERSOR_KW      #################################



            # Verifica se o valor de potencia_max_inversor_kw é int ou float
            if isinstance(potencia_max_inversor_kw, (int, float)):
                # Verifica se é zero ou None
                if potencia_max_inversor_kw == 0 or potencia_max_inversor_kw is None:
                    print(
                        "Valor inválido ou zero para POTENCIA_MAX_INVERSOR_KW: {}. Modificando para 1.".format(
                            potencia_max_inversor_kw))
                    potencia_max_inversor_kw = 1
            else:
                # Caso não seja int ou float
                print(
                    "Valor não numérico encontrado para POTENCIA_MAX_INVERSOR_KW: {}. Modificando para 1.".format(
                        potencia_max_inversor_kw))
                potencia_max_inversor_kw = 1



            ###############################     TRATAMENTO COLUNA ENERGIA_DESEJADA      #################################



            # Verifica se algum valor dentro da lista energia_desejada é int ou float
            energia_desejada = list(energia_desejada)
            for i in range(len(energia_desejada)):
                if isinstance(energia_desejada[i], (int, float)):
                    # Verifica se é zero ou None
                    if energia_desejada[i] == 0 or energia_desejada[i] is None:
                        print(
                            "Valor inválido ou zero encontrado para ENERGIA_DESEJADA na posição {}: {}. Modificando para 1.".format(
                                i, energia_desejada[i]))
                        energia_desejada[i] = 1
                else:
                    # Caso não seja int ou float
                    print(
                        "Valor não numérico encontrado para ENERGIA_DESEJADA na posição {}: {}. Modificando para 1.".format(
                            i, energia_desejada[i]))
                    energia_desejada[i] = 1



            ############################################################################################################



            if ceg_gd.startswith(('GD', 'UFV')):
                if nome not in ctmts_processados:
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)

                for index_mensal, mensal in enumerate(energia_desejada):
                    mensal_folder = os.path.join(ctmt_folder, str('geração_shape_mes_{}'.format(index_mensal + 1)))
                    os.makedirs(mensal_folder, exist_ok=True)

                    for index_dia in range(1):
                        diario_filename = 'dados_geracao_distribuida.txt'
                        file_path = os.path.join(mensal_folder, diario_filename)

                        with open(file_path, 'a') as file:

                            def calcular_potencia_gerada(irradiancia, potencia_instalada_kwp_recebe, eficienci_a):
                                """ Calcula a potência gerada pelo pv com base na irradiancia solar """

                                potencia = potencia_instalada_kwp_recebe * (irradiancia / 1000) * eficienci_a
                                return potencia

                            def calcular_irradianca_temperatura_desempenho(latitud_e, longitud_e,
                                                                           potencia_instalada_kwp_recebe, eficienci_a,
                                                                           potencia_max_inversor_kw_,
                                                                           energia_desejada_):
                                """ Calcula a irradiancia para cada tempo """
                                site = location.Location(latitud_e, longitud_e, altitude=400)

                                rec_mensal = 0
                                rec_diario = 0
                                if int(index_mensal) < 1:
                                    rec_mensal = 1

                                if int(index_dia) < 1:
                                    rec_diario = 1

                                # Ajuste para garantir que rec_diario nunca seja 0
                                index_3 = '{:02d}'.format(int(rec_mensal)) if rec_mensal > 0 else '01'
                                index_4 = '{:02d}'.format(int(rec_diario)) if rec_diario > 0 else '01'

                                times = pd.date_range('2023-{}-{}'.format(index_3, index_4),
                                                      '2023-{}-{} 23:59'.format(index_3, index_4), freq='15min',
                                                      tz='America/Cuiaba')

                                """Calcular a irradiança global horizontal (GHI), difusa (DHI) e direta (DNI)"""
                                irradiance = site.get_clearsky(times)

                                """Calcular a potência gerada com base na irradiância (GHI)"""
                                potencia_gerada = calcular_potencia_gerada(irradiance['ghi'],
                                                                           potencia_instalada_kwp_recebe,
                                                                           eficienci_a)

                                """Ajuste da potência gerada para atingir a energia desejada"""
                                energia_total_gerada = potencia_gerada.sum()
                                fator_ajuste = 1
                                if energia_total_gerada == 0:
                                    energia_total_gerada = 1

                                if energia_desejada_[index_mensal] == 0:
                                    fator_ajuste = (potencia_max_inversor_kw_ / 30) / energia_total_gerada
                                if energia_desejada_[index_mensal] != 0:
                                    fator_ajuste = (energia_desejada_[index_mensal] / 30) / energia_total_gerada

                                """Ajustar a potência gerada para atingir a energia desejada"""
                                potencia_gerada_ajustad_a = potencia_gerada * fator_ajuste

                                return potencia_gerada_ajustad_a.tolist()

                            latitude = -15.59583
                            longitude = -56.09694
                            altitude = 400

                            """ Eficiencia media de uma painel fotovoltaico """
                            eficiencia = 0.18

                            potencia_gerada_ajustada = calcular_irradianca_temperatura_desempenho(
                                latitude,
                                longitude,
                                potencia_instalada_kwp, eficiencia,
                                potencia_max_inversor_kw, energia_desejada
                            )

                            # Usando o mapa de fases da variável de instância
                            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')

                            if ".4" not in rec_fases:
                                rec_fases = rec_fases + '.4'
                            else:
                                pass

                            command_pvsystem = (
                                    "New xycurve.mypvst_{} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n".format(
                                        cod_id) +
                                    "New xycurve.myeff_{} npts = 4  xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n".format(
                                        cod_id) +
                                    "New loadshape.myirrad_{} npts = 1 interval = 1 mult = [1]\n".format(cod_id) +
                                    "New tshape.mytemp_{} npts = 1 interval = 1 temp = [25]\n".format(cod_id) +
                                    "New pvsystem.pv_{} phases = {} conn = wye bus1 = {}\n".format(cod_id, len(fas_con),
                                                                                                   pac + rec_fases) +
                                    "~ kv = {} kva = {} pmpp = {}\n".format(ten_nom_voltage / 1000,
                                                                            max(potencia_gerada_ajustada),
                                                                            max(potencia_gerada_ajustada)) +
                                    "~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{}\n".format(
                                        cod_id) +
                                    "~ p-tcurve = mypvst_{} daily = myirrad_{} tdaily = mytemp_{}\n\n".format(cod_id,
                                                                                                              cod_id,
                                                                                                              cod_id) +
                                    "New load.{}_carga_no_pv bus1 = {} phases = {}\n".format(cod_id, pac + rec_fases,
                                                                                             len(fas_con)) +
                                    "~ conn = wye model = 1 kv = {} kw = 0.0001\n\n".format(ten_nom_voltage / 1000)
                            )

                            file.write(command_pvsystem)

        print("Modelagem: GD de media tensão processadas com sucesso !!!.")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_Poste_Iluminacao_Publica_Baixa_Tensao(self):
        """ Modelagem das Cargas de postes de iluminação pública """

        offset = 0
        limit = 500000
        base_dir = caminho_modelagens
        ctmts_processados = {}
        processed_rows = 0

        try:

            total_rows_query = """

                      SELECT COUNT(*)
                      FROM pip
                    

                  """
            self.cur.execute(total_rows_query)
            total_rows = self.cur.fetchone()[0]

            # Processamento das linhas em lotes
            while processed_rows < total_rows:
                query = """

                         SELECT DISTINCT 
                                          pip.cod_id,
                                          pip.tip_cc, 
                                          pip.pac, 
                                          ctmt.nome, 
                                          pip.fas_con, 
                                          pip.ten_forn,
                                          pip.pot_lamp

                          FROM 
                                        pip
                          JOIN 
                                        ctmt ON ctmt.cod_id = pip.ctmt
                        --  where ctmt.cod_id = '764444'
                         
                          ORDER BY 
                                        ctmt.nome

                          LIMIT {} OFFSET {};

                      """.format(limit, offset)

                self.cur.execute(query)
                rows = self.cur.fetchall()

                if not rows:
                    break  # Encerra se não houver mais dados

                for index, linha in enumerate(rows):

                    cod_id, tip_cc, pac, nome, fas_con, ten_forn, pot_lamp = linha

                    # VERIFICAÇÃO ANTI-ERROS
                    ####################################################################################################
                    ###############################     TRATAMENTO COLUNA NOME      ####################################



                    if not nome:
                        print(
                            "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(
                                linha))
                        continue

                    # if nome.isdigit():
                    #     pass
                    # else:
                    #     print(
                    #         "Erro! Há valores não numericos na coluna -NOME- da tabela -ctmt-: {}, eliminando esta linha da modelagem !!!".format(
                    #             nome))




                    ###############################     TRATAMENTO COLUNA FAS_CON      #################################



                    if fas_con.isalpha():
                        pass
                    else:
                        print(
                            "Erro! Há caracteres diferentes de letras na coluna -FAS_CON- da tabela -pip-: {}, colocando valor padrão: FAS_CON = ABC".format(
                                fas_con))
                        fas_con = 'ABC'



                    ###############################     TRATAMENTO COLUNA TEN_FORN      ####################################



                    if ten_forn.isdigit():
                        pass
                    else:
                        print(
                            "Erro! Há valores não numericos na coluna -TEN_FORN- da tabela -pip-: {}, colocando valor padrão: TEN_FORN = {}".format(
                                ten_forn, self.ten_con_gd_media_codigo))
                        ten_forn = self.ten_con_gd_media_codigo

                    try:
                        ten_forn = int(ten_forn)
                    except Exception as e:
                        print(
                            "Erro ao converter para int a TEN_FORN da tabela pip: {}, colocando valor padrão: TEN_FORN = {}".format(
                                e, self.ten_con_gd_media_codigo))
                        ten_forn = self.ten_con_gd_media_codigo

                    ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_forn), "não encontrado")

                    if ten_nom_voltage == "não encontrado":
                        print("Erro - Não existe correspondencia de tensão para a coluna "
                              "TEN_FORN da tabela pip, colocando valor padrão: TEN_FORN = {} v".format(self.ten_con_gd_media_volts))
                        ten_nom_voltage = self.ten_con_gd_media_volts



                    ###############################     TRATAMENTO COLUNA COD_ID      ####################################



                    if not cod_id:
                        print(
                            "Erro! Não há valores na coluna -COD_ID- da tabela -pip-: {}, inserindo valor padrão: {}_{}".format(
                                cod_id, nome, index))
                        cod_id = str(nome) + str(index)



                    ###############################     TRATAMENTO COLUNA TIP_CC      ####################################



                    if not tip_cc:
                        print(
                            "Erro! Não há valores na coluna -TIP_CC- da tabela -pip-:, inserindo valores padrão: {}".format(
                                self.tip_cc_pip))
                        tip_cc = self.tip_cc_pip



                    ###############################     TRATAMENTO COLUNA PAC      ####################################



                    if not pac:
                        print(
                            "Erro! Não há valores na coluna -pac- da tabela -pip-: {}, eliminando esta linha da modelagem !!!".format(
                                fas_con))
                        continue



                    ############################################################################################################



                    # Criação ou reutilização de arquivos para cada ctmt
                    if nome not in ctmts_processados:
                        ctmt_folder = os.path.join(base_dir, str(nome))
                        os.makedirs(ctmt_folder, exist_ok=True)
                        file_path = os.path.join(ctmt_folder, 'Cargas_postes_iluminacao_publica.bin')
                        file = open(file_path, 'wb')
                        ctmts_processados[nome] = file
                    else:
                        file = ctmts_processados[nome]

                    # Determinar tensões e fases
                    rec_fases = self.mapa_fases.get(fas_con, 'ABCN')

                    if ".4" not in rec_fases:
                        rec_fases = rec_fases + '.4'
                    else:
                        pass

                    fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

                    if len(fases) >= 2:
                        conn = 'wye'
                    else:
                        conn = 'wye'
                        ten_nom_voltage /= math.sqrt(3)



                    # Criar comandos para o OpenDSS
                    command_transformers = (

                            'New Load.{}_carga_poste_iluminacao_publica Bus1 = {}{} Phases = {}\n'.format(cod_id, pac, rec_fases,
                                                                                       len(fases)) +
                            'Conn = {} Model = 1 Kv = {} Kw = {} Kvar = 0\n\n'.format(conn, ten_nom_voltage / 1000, pot_lamp / 1000)

                    )

                    # Escrever no arquivo correspondente
                    if file:
                        file.write(command_transformers.encode())

                    processed_rows += 1

                # Atualizar offset para próxima consulta
                offset += limit

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de postes de iluminação pública processadas com sucesso !!!.".format(processed_rows))






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Ramais_Ligacao(self):
        """ Modelagem dos ramais de ligação de baixa tensão tensão """

        results = []
        ctmts_processados = {}
        base_dir = caminho_modelagens

        try:

            query = """

                            SELECT DISTINCT
                                            ramlig.cod_id,
                                            pac_1,
                                            pac_2,
                                            ctmt.nome,
                                            fas_con,
                                            comp,
                                            tip_cnd
                                            
                            FROM 
                                            ramlig
                            JOIN 
                                            ctmt ON ctmt.cod_id = ramlig.ctmt
                           -- where ctmt.cod_id = '764444'
                            ORDER BY        ctmt.nome

                        """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        # Iterar sobre os dados e gerar uma subpasta para cada CTMT
        for index, linha in enumerate(results):
            cod_id, pac_1, pac_2, nome, fas_con, comp, tip_cnd = linha

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ################################        TRATAMENTO DA COLUNA NOME       ####################################



            if not nome:
                print("A coluna nome da tabela ctmt está vazia, logo será excluída da MODELAGEM !!! : {}".format(linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print(
            #         "Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}, logo esta linha não será modelada !!!".format(
            #             nome))




            ################################        TRATAMENTO DA COLUNA FAS_CON       #################################



            if fas_con.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ramlig-: {}, inserindo valor padrão: fas_con = ABC".format(
                        fas_con))
                fas_con = 'ABC'



            ################################        TRATAMENTO DA COLUNA COMP       ####################################



            if not isinstance(comp, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -comp- da tabela -ramlig-: {}, inserindo valor padrão: comp = 1 metro".format(
                        comp))
                comp = 1



            ################################        TRATAMENTO DA COLUNA COD_ID       ##################################



            if not cod_id:
                print("A coluna cod_id da tabela ramlig está vazia, preenchendo com valor padrão: cod_id = {}_{}".format(
                    nome, index))
                cod_id = str(nome) + str(index)



            ################################        TRATAMENTO DA COLUNA PAC_1       ###################################



            if not pac_1:
                print(
                    "Erro! Não há valores na coluna -pac_1- da tabela -ramlig-: {}, esta linha será eliminada da modelagem !!!".format(
                        pac_1))
                continue



            ################################        TRATAMENTO DA COLUNA PAC_2       ###################################



            if not pac_2:
                print(
                    "Erro! Não há valores na coluna -pac_2- da tabela -ramlig-: {}, esta linha será eliminada da modelagem !!!".format(
                        pac_2))
                continue



            ################################        TRATAMENTO DA COLUNA TIP_CND       #################################



            if not tip_cnd:
                print(
                    "Erro! A coluna tip_cnd está vazia, usando valores padrão na coluna -tip_cnd- da tabela -ramlig-: {}".format(
                        fas_con))
                # tip_cnd = default



            ############################################################################################################



            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'ramais_ligacao.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            # Usando o mapa de fases da variável de instância
            rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
            fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]

            if ".4" not in rec_fases:
                rec_fases = rec_fases + '.4'
            else:
                pass

            # Gerar o comando no formato desejado
            command_line = (

                    'New Line.{}_ramal Phases = {} Bus1 = {}{} Bus2 = {}{} Linecode = {}_linecode_ramais Length = {} units = m\n\n'.format(
                    cod_id, len(fases), pac_1, rec_fases, pac_2, rec_fases, tip_cnd, comp

                )
            )
            # Escrever o comando no arquivo .dss
            if file:
                file.write(command_line.encode())

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: Ramais de ligação de baixa Tensão gerada com sucesso !!!")






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def transformadores_Media_tensao(self):
        """ Modelagem dos transformadores de media tensão """


        try:
            query = """
            
               SELECT DISTINCT
                                untrmt.cod_id,
                                untrmt.pac_1,
                                untrmt.pac_2,
                                ctmt.nome,
                                eqtrmt.pot_nom,
                                eqtrmt.lig,
                                eqtrmt.ten_pri,
                                eqtrmt.ten_sec,
                                eqtrmt.lig_fas_p,
                                eqtrmt.lig_fas_s,
                                eqtrmt.r,
                                eqtrmt.xhl,
                                untrmt.per_fer,
                                eqtrmt.lig_fas_t,
                                untrmt.pac_3
                                
                FROM 
                                untrmt
                LEFT JOIN 
                                eqtrmt ON eqtrmt.uni_tr_mt = untrmt.cod_id
                LEFT JOIN
                                ctmt ON ctmt.cod_id = untrmt.ctmt
               -- and ctmt.cod_id = '764444'
                ORDER BY 
                                ctmt.nome;

           
            """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return

        base_dir = caminho_modelagens
        ctmts_processados = {}

        # Iterar sobre os dados e gerar uma subpasta para cada ctmt
        for index, linha in enumerate(results):
            cod_id = linha[0]
            pac_1 = linha[1]
            pac_2 = linha[2]
            nome = linha[3]
            pot_nom = linha[4]
            ten_pri = linha[6]
            ten_sec = linha[7]
            lig_fas_p = linha[8]
            lig_fas_s = linha[9]
            r = linha[10]
            xhl = linha[11]
            per_fer = linha[12]
            lig_fas_t = linha[13]
            pac_3 = linha[14]




            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ######################      TRATAMENTO NA COLUNA NOME       ################################################



            if not nome:
                print("Linha {} contém valores nulos e será ignorada: {}".format(index, linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}".format(nome))
            #



            ######################      TRATAMENTO NA COLUNA POT_NOM       #############################################



            if not pot_nom.isdigit():
                print("Erro! Há valores não numericos na coluna -pot_nom- da tabela -untrmt-: {}, inserindo valor padrão: pot_nom = {} kva".format(pot_nom, self.pot_nom_transformadores))
                pot_nom = self.pot_nom_transformadores


            try:
                pot_nom = int(pot_nom)
            except Exception as e:
                print("Erro ao converter para int a pot_nom da tabela untrmt: {}, inserindo valor padrão: pot_nom = {} kva".format(e, self.pot_nom_transformadores))


            pot_nom = self.trafos_pot_nom.get(str(pot_nom), "não encontrado")

            if isinstance(pot_nom, (float, int)):
                pot_nom = int(pot_nom)
                if pot_nom > self.pot_nom_transformadores:
                    print(
                        "Erro! Há valores de potencia nominal dos transformadores de media tensão acima dos 200 Mw na coluna -pot_nom- da tabela -untrmt-: {}, "
                        "inserindo valor padrão: pot_nom = {} kva".format(
                            pot_nom, self.pot_nom_transformadores))
                    pot_nom = self.pot_nom_transformadores


                else:
                    pass
            else:
                print("Erro! Há valores não numericos na coluna -pot_nom- da tabela -untrmt-: {}, inserindo valor padrão: pot_nom = {} kva".format(pot_nom, self.pot_nom_transformadores))
                pot_nom = self.pot_nom_transformadores


            if pot_nom == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "pot_nom da tabela untrmt, inserindo valor padrão: pot_nom = {} kva".format(self.pot_nom_transformadores))
                pot_nom = self.pot_nom_transformadores




            ######################      TRATAMENTO NA COLUNA TEN_PRI       #############################################



            if ten_pri.isdigit():
                pass
            else:
                print("Erro! Há valores não numericos na coluna -ten_pri- da tabela -eqtrmt-, inserindo valor padrão: ten_pri = {}".format(self.ten_pri_transformadores_media_codigo))
                ten_pri = self.ten_pri_transformadores_media_codigo

            try:
                ten_pri = int(ten_pri)
            except Exception as e:
                print("Erro ao converter para int a ten_pri da tabela eqtrmt: {}, inserindo valor padrão: ten_pri = {}".format(e, self.ten_pri_transformadores_media_codigo))
                ten_pri = self.ten_pri_transformadores_media_codigo


            ten_pri = self.dados_tabela_tensoes.get(int(ten_pri), "não encontrado")

            if ten_pri == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_pri da tabela eqtrmt, inserindo valor padrão: ten_pri = {} v".format(self.ten_pri_transformadores_media_volts))
                ten_pri = self.ten_pri_transformadores_media_volts



            ######################      TRATAMENTO NA COLUNA TEN_SEC       #############################################



            if ten_sec.isdigit():
                pass
            else:
                print("Erro! Há valores não numericos na coluna -ten_sec- da tabela -eqtrmt-, inserindo valor padrão: ten_pri = {}".format(self.ten_sec_transformadores_media_codigo))
                ten_pri = self.ten_sec_transformadores_media_codigo


            try:
                ten_sec = int(ten_sec)
            except Exception as e:
                print("Erro ao converter para int a ten_sec da tabela eqtrmt: {}, inserindo valor padrão: ten_pri = {}".format(e, self.ten_sec_transformadores_media_codigo))
                ten_pri = self.ten_sec_transformadores_media_codigo


            ten_sec = self.dados_tabela_tensoes.get(int(ten_sec), "não encontrado")

            if ten_sec == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_sec da tabela eqtrmt, inserindo valor padrão: ten_pri = {} v".format(self.ten_sec_transformadores_media_volts))
                ten_pri = self.ten_sec_transformadores_media_volts



            ######################      TRATAMENTO NA COLUNA PAC_1       ################################################



            if not pac_1:
                print(
                    "Erro! Não há valores na coluna -pac_1- da tabela -untrmt-: {}, eliminando esta linha da modelagem !!!".format(
                        pac_1))
                continue



            ######################      TRATAMENTO NA COLUNA PAC-2       ################################################



            if not pac_2:
                print(
                    "Erro! Não há valores na coluna -pac_2- da tabela -untrmt-: {}, eliminando esta linha da modelagem !!!".format(
                        pac_2))
                continue



            ######################      TRATAMENTO NA COLUNA LIG_FAS_P       ################################################



            if lig_fas_p.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_p- da tabela -eqtrmt-: {}, colocando valor padrão: fas_con = ABC".format(
                        lig_fas_p))
                lig_fas_p = 'ABC'



            ######################      TRATAMENTO NA COLUNA LIG_FAS_S       ################################################



            if lig_fas_s.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_s- da tabela -eqtrmt-: {}, colocando valor padrão: fas_con = ABC".format(
                        lig_fas_s))
                lig_fas_s = 'ABCN'



            ###############################     TRATAMENTO DA COLUNA R1       ##########################################



            if not isinstance(r, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -r1- da tabela -eqtrmt-: {}, colocando valor padrão r1 = {}".format(
                        r, self.r1_transformadores_media))
                r = self.r1_transformadores_media

            else:
                r = float(r)
                if r > self.r1_transformadores_media:
                    print(
                        "Erro! Há valores de r1 maiores do que {} ohms/km na coluna -r1- da tabela -eqtrmt-: {}, substituindo r1 por r1 = {}".format(
                            self.r1_transformadores_media, r, self.r1_transformadores_media))
                    r = self.r1_transformadores_media



            ###############################     TRATAMENTO DA COLUNA XHL      ##########################################



            if not isinstance(xhl, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -xhl- da tabela -eqtrmt-: {}, colocando valor padrão xhl = {}".format(
                        xhl, self.xhl_transformadores_media))
                xhl = self.xhl_transformadores_media


            else:
                xhl = float(xhl)
                if xhl > self.xhl_transformadores_media:
                    print(
                        "Erro! Há valores de xhl maiores do que {} ohms/km na coluna -xhl- da tabela -eqtrmt-: {}, colocando valor padrão xhl = {}".format(
                            self.xhl_transformadores_media ,xhl, self.xhl_transformadores_media))
                    xhl = self.xhl_transformadores_media


            ###############################     TRATAMENTO DA COLUNA XHL      ##########################################

            if not per_fer:
                print("A coluna per_fer da tabela untrmt está vazia, substiuindo por valor padrão: per_fer: {} ".format(self.per_fer_transformadores_media))
                per_fer = self.per_fer_transformadores_media

            if not isinstance(per_fer, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -xhl- da tabela -eqtrmt-: {}, colocando valor padrão xhl = {}".format(
                        per_fer, self.per_fer_transformadores_media))
                per_fer = self.per_fer_transformadores_media


            else:
                per_fer = float(per_fer)
                if per_fer > self.per_fer_transformadores_media:
                    print(
                        "Erro! Há valores de xhl maiores do que 0.8 ohms/km na coluna -xhl- da tabela -eqtrmt-: {}, colocando valor padrão xhl = {}".format(
                            per_fer, self.per_fer_transformadores_media))
                    per_fer = self.per_fer_transformadores_media

            ############################################################################################################


            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)

                # Criar o novo arquivo .dss para este ctmt
                file_path = os.path.join(ctmt_folder, 'Transformadores_Media_tensao.bin')
                file = open(file_path, 'wb')

                # Adicionar o ctmt ao dicionario de ctmts processados (armazena o arquivo aberto)
                ctmts_processados[nome] = file


            else:
                # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                file = ctmts_processados[nome]

            rec_fases_p = self.mapa_fases.get(lig_fas_p, 'ABC')

            # Remover a parte .4 e tudo o que vem depois
            if '.4' in rec_fases_p:
                rec_fases_p = rec_fases_p.split('.4')[0]

            rec_fases_s = self.mapa_fases.get(lig_fas_s, 'ABCN')

            if ".4" not in rec_fases_s:
                rec_fases_s = rec_fases_s + '.4'
            else:
                pass

            rec_fases_t = self.mapa_fases.get(lig_fas_t, 'ABCN')

            if ".4" not in rec_fases_t:
                rec_fases_t = rec_fases_t + '.4'
            else:
                pass

            if len(re.findall(r'\d+', rec_fases_p.split('.4')[0])) == 1:
                conn_p = 'delta'
                ten_primario = round(ten_pri, 5)
            else:
                conn_p = 'delta'
                ten_primario = round(ten_pri, 5)

            if len(re.findall(r'\d+', rec_fases_s.split('.4')[0])) == 1:
                conn_s = 'wye'
                ten_secundario = int(ten_sec) / round(math.sqrt(3), 5)
            else:
                conn_s = 'wye'
                ten_secundario = int(ten_sec)

            if len(re.findall(r'\d+', rec_fases_t.split('.4')[0])) == 1:
                conn_t = 'wye'
                ten_terciario = int(ten_sec) / round(math.sqrt(3), 5)
            else:
                conn_t = 'wye'
                ten_terciario = int(ten_sec)

            # Gerar o comando para cada linha
            """ %r são as perdas no cobre 
                %noloadloss são as perdas no ferro (histerese e correntes de facault)
                %loadloss são as perdas totais do trafo (perdas no ferro + perdas no cobre) não usado porque ja definido o %r
            """

            if len(re.findall(r'\d+', rec_fases_p)) == 1:
                command_transformers = (

                        'New Transformer.{} Phases={} Windings=3 xhl={} %noloadloss={}\n'.format(cod_id, len(lig_fas_p),
                                                                                                 xhl, per_fer) +
                        '~ wdg=1 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n'.format(pac_1, rec_fases_p, conn_p,
                                                                                     ten_primario / 1000, pot_nom, r) +
                        '~ wdg=2 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n'.format(pac_2, rec_fases_s, conn_s,
                                                                                     ten_secundario / 1000, pot_nom,
                                                                                     r) +
                        '~ wdg=3 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n\n'.format(pac_2, rec_fases_s, conn_t,
                                                                                       ten_secundario / 1000, pot_nom,
                                                                                       r)

                )

            else:
                command_transformers = (

                        'New Transformer.{} Phases={} Windings=2 xhl={} %noloadloss={}\n'.format(cod_id, len(lig_fas_p),
                                                                                                 xhl, per_fer) +
                        '~ wdg=1 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n'.format(pac_1, rec_fases_p, conn_p,
                                                                                     ten_primario / 1000, pot_nom, r) +
                        '~ wdg=2 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n'.format(pac_2, rec_fases_s, conn_s,
                                                                                     ten_secundario / 1000, pot_nom, r)

                )

            if file:
                    file.write(command_transformers.encode())

        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: Transformadores de Média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33







    def Reguladores_Media_Tensao(self):
        """ Modelagem Reguladores de Tensão na média tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}

        try:
            query = """
            
                SELECT 
                            unremt.cod_id, 
                            unremt.fas_con, 
                            unremt.pac_1, 
                            unremt.pac_2, 
                            ctmt.nome,
                            eqre.pot_nom, 
                            eqre.lig_fas_p, 
                            eqre.lig_fas_s, 
                            eqre.per_fer, 
                            eqre.per_tot, 
                            eqre.r, 
                            eqre.xhl,
                            ctmt.ten_nom,
                            eqre.cor_nom,
                            eqre.rel_tp,
                            eqre.rel_tc
                FROM 
                            unremt 
                JOIN 
                            eqre ON unremt.cod_id = eqre.un_re  
                 
                JOIN 
                            ctmt ON unremt.ctmt = ctmt.cod_id   
                            
                --and ctmt.cod_id = '764444'
            
                ORDER BY 
                            ctmt.nome; 
          
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return


        for index, linha in enumerate(results):
            cod_id = linha[0]
            pac_1 = linha[2]
            pac_2 = linha[3]
            nome = linha[4]
            pot_nom = linha[5]
            lig_fas_p = linha[6]
            lig_fas_s = linha[7]
            r = linha[10]
            ten_nom = linha[12]
            cor_nom = linha[13]
            rel_tp = linha[14]



            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ######################      TRATAMENTO NA COLUNA NOME       ################################################



            if not nome:
                print("Linha {} contém valores nulos e será ignorada: {}".format(index, linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}".format(nome))




            ######################      TRATAMENTO NA COLUNA POT_NOM       #############################################



            if not pot_nom.isdigit():
                print(
                    "Erro! Há valores não numericos na coluna -pot_nom- da tabela -eqre-: {}, inserindo valor padrão: pot_nom = 1 kva".format(
                        pot_nom))
                pot_nom = self.pot_nom_reguladores_media

            try:
                pot_nom = int(pot_nom)
            except Exception as e:
                print(
                    "Erro ao converter para int a pot_nom da tabela eqre: {}, inserindo valor padrão: pot_nom = 1 kva".format(
                        e))

            pot_nom = self.trafos_pot_nom.get(str(pot_nom), "não encontrado")

            if isinstance(pot_nom, (float, int)):
                pot_nom = int(pot_nom)
                if pot_nom > self.pot_nom_reguladores_media:
                    print(
                        "Erro! Há valores de potencia nominal dos transformadores de media tensão acima dos {} kva na coluna -pot_nom- da tabela -eqre-: {}, "
                        "inserindo valor padrão: pot_nom = {} kva".format(
                            self.pot_nom_reguladores_media, pot_nom, self.pot_nom_reguladores_media))
                    pot_nom = self.pot_nom_reguladores_media

                else:
                    pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -pot_nom- da tabela -eqre-: {}, inserindo valor padrão: pot_nom = {} kva".format(
                        pot_nom, self.pot_nom_reguladores_media))
                pot_nom = self.pot_nom_reguladores_media

            if pot_nom == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "pot_nom da tabela eqre, inserindo valor padrão: pot_nom = {} kva".format(self.pot_nom_reguladores_media))
                pot_nom = self.pot_nom_reguladores_media



            ######################      TRATAMENTO NA COLUNA TEN_NOM       #############################################



            if ten_nom.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -ten_nom- da tabela -eqre-, inserindo valor padrão: ten_nom = {}".format(self.ten_nom_reguladores_media_codigo))
                ten_nom = self.ten_nom_reguladores_media_codig

            try:
                ten_nom = int(ten_nom)
            except Exception as e:
                print(
                    "Erro ao converter para int a ten_nom da tabela eqre: {}, inserindo valor padrão: ten_nom = {}".format(
                        e, self.ten_nom_reguladores_media_codig))
                ten_nom = self.ten_nom_reguladores_media_codig

            ten_nom = self.dados_tabela_tensoes.get(int(ten_nom), "não encontrado")

            if ten_nom == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "ten_nom da tabela eqre, inserindo valor padrão: ten_nom = {}".format(self.ten_nom_reguladores_media_codig))
                ten_nom = self.ten_nom_reguladores_media_codig



            ######################      TRATAMENTO NA COLUNA PAC_1       ################################################


            if not pac_1:
                print(
                    "Erro! Não há valores na coluna -pac_1- da tabela -eqre-: {}, eliminando esta linha da modelagem !!!".format(
                        pac_1))
                continue



            ######################      TRATAMENTO NA COLUNA PAC-2       ################################################



            if not pac_2:
                print(
                    "Erro! Não há valores na coluna -pac_2- da tabela -eqre-: {}, eliminando esta linha da modelagem !!!".format(
                        pac_2))
                continue



            ######################      TRATAMENTO NA COLUNA LIG_FAS_P       ################################################



            if lig_fas_p.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_p- da tabela -eqre-: {}, colocando valor padrão: lig_fas_p = ABC".format(
                        lig_fas_p))
                lig_fas_p = 'ABC'



                ######################      TRATAMENTO NA COLUNA LIG_FAS_S       ################################################



            if lig_fas_s.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_s- da tabela -eqtrmt-: {}, colocando valor padrão: lig_fas_s = ABC".format(
                        lig_fas_s))
                lig_fas_s = 'ABC'



            ###############################     TRATAMENTO DA COLUNA R        ##########################################



            if not isinstance(r, (float, int)):
                print(
                    "Erro! Há valores que não são nem float e nem int na coluna -r- da tabela -eqre-: {}, colocando valor padrão r = {}".format(
                        r, self.r_reguladores_media_ohms_km))
                r = self.r_reguladores_media_ohms_km

            else:
                r = float(r)
                if r > self.r_reguladores_media_ohms_km:
                    print(
                        "Erro! Há valores de r maiores do que 1.5 ohms/km na coluna -r- da tabela -eqre-: {}, substituindo r por r = {}".format(
                            r, self.r_reguladores_media_ohms_km))
                    r = self.r_reguladores_media_ohms_km



            ############################################################################################################


            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)

                # Criar o novo arquivo .dss para este ctmt
                file_path = os.path.join(ctmt_folder, 'Reguladores_tensao_Media_Tensao.bin')
                file = open(file_path, 'wb')

                # Adicionar o ctmt ao dicionário de ctmts processados (armazena o arquivo aberto)
                ctmts_processados[nome] = file
            else:
                # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                file = ctmts_processados[nome]



            rec_fases_p = self.mapa_fases.get(lig_fas_p, 'ABC')

            rec_fases_s = self.mapa_fases.get(lig_fas_s, 'ABC')

            if len(re.findall(r'\d+', rec_fases_p)) == 1:
                conn_p = 'wye'
                ten_primario = int(ten_nom) / round(math.sqrt(3), 5)
            else:
                conn_p = 'delta'
                ten_primario = round(ten_nom, 5)

            if len(re.findall(r'\d+', rec_fases_s)) == 1:
                conn_s = 'wye'
                ten_secundario = int(ten_nom) / round(math.sqrt(3), 5)
            else:
                conn_s = 'delta'
                ten_secundario = round(ten_nom, 5)



            # Gerar o comando para cada linha
            command_transformers = (

            "new transformer.reg{}_{} phases={} windings=2 bank={} buses=({}{} {}{}) conns='{} {}' kvs='{} {}' kvas='{} {}' XHL = 1 %r={}\n".format(lig_fas_p, cod_id, len(lig_fas_p), cod_id,
           pac_1, rec_fases_p, pac_2, rec_fases_s, conn_p, conn_s, ten_primario / 1000,
           ten_secundario / 1000, pot_nom, pot_nom, r) +


            'new regcontrol.creg{}_{} transformer=reg{}_{} winding=2 vreg={} band={} ptratio={} ctprim={}\n\n'.
            format(lig_fas_p, cod_id, lig_fas_p, cod_id, int(ten_secundario) / int(rel_tp), self.banda_volts_reguladores_tensao, rel_tp, cor_nom)


                                    )



            if file:
                file.write(command_transformers.encode())

        # Fechar todos os arquivos antes de terminar o loop
        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: Reguladores de tensão de Média Tensão gerada com sucesso !!!")





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33

    def Energia_alimentador(self):
        """ Coleta as energias mensais em kwh consumidas por cada alimentador """

        results = []
        base_dir = caminho_modelagens
        ctmts_processados = {}

        # Coleta de dados
        try:
            query = """

                           SELECT DISTINCT
                                           nome, ene_01, ene_02, ene_03, ene_04, ene_05, ene_06, ene_07, ene_08, ene_09, 
                                           ene_10, ene_11, ene_12
                           FROM 
                                           ctmt   

                         --  WHERE
                            --               ctmt.cod_id = '764444'

                           ORDER BY        nome  

                      """
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao conectar com o banco de dados para a tabela ctmt: {}".format(e))

        if not results:
            print("Nenhum dado foi recuperado da tabela: ctmt")
            return

        # Itera pelas linhas da tabela
        for index, linha in enumerate(results):
            nome, ene_01, ene_02, ene_03, ene_04, ene_05, ene_06, ene_07, ene_08, ene_09, ene_10, ene_11, ene_12 = linha

            Energia = [ene_01, ene_02, ene_03, ene_04, ene_05, ene_06, ene_07, ene_08, ene_09, ene_10, ene_11, ene_12]


            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################

            ######################      TRATAMENTO PARA COLUNA NOME     ################################################

            if not nome:
                print("Linha {} contém valores nulos e será ELIMINADA DA MODELAGEM !!!: {}".format(index, linha))
                continue

            # if not nome.isdigit():
            #     print(
            #         "A coluna nome da tabela ctmt não têm digitos numéricos: {}, portanto será ELIMINADA DA MODELAGEM !!!".format(
            #             nome))


            if nome not in ctmts_processados:
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)
                file_path = os.path.join(ctmt_folder, 'Energia.bin')
                file = open(file_path, 'wb')
                ctmts_processados[nome] = file
            else:
                file = ctmts_processados[nome]

            command_linecode = (

                    'Energias_kwh_meses: {}'.format(Energia)


            )

            if file:
                file.write(command_linecode.encode())

        for file in ctmts_processados.values():
            file.close()
        print("Modelagem: Barra_Slack gerada com sucesso !!!")




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33







   
    def Geometria_ssdbt(self):
        """ Modelagem das geometrias multilinhas das linhas de baixa tensão """

        base_dir = caminho_modelagens
        results = []

        try:
            query = """
                        SELECT DISTINCT 
                            ctmt.nome, ssdbt.wkb_geometry, ssdbt.cod_id
                        FROM 
                            ctmt
                        JOIN
                            ssdbt on ctmt.cod_id = ssdbt.ctmt
                       -- AND ctmt.nome = '010009'
                        ORDER BY ctmt.nome
                    """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a consulta na tabela ctmt: {}".format(e))

        # Lista para armazenar as coordenadas que serão salvas no arquivo JSON
        coordenadas = list()

        # Processa as linhas retornadas da consulta
        for nome, wkb_geometry, cod_id in results:
            # Verifica se existe algum valor nulo no resultado
            if any(valor is None for valor in [nome, wkb_geometry, cod_id]):
                print(f"Linha com nome {nome} contém valores nulos e será ignorada.")
                continue  # Ignora linhas com valores nulos

            # Converte o WKB para geometria usando o Shapely
            if isinstance(wkb_geometry, memoryview):
                wkb_geometry = bytes(wkb_geometry)

            # Converte o WKB para geometria
            geometry = loads(wkb_geometry)

            # Verifica se a geometria é um MultiLineString ou LineString
            if isinstance(geometry, (MultiLineString, LineString)):
                # Acessa os bounds da geometria (para MultiLineString: minx, miny, maxx, maxy)
                bounds = geometry.bounds
                # Adiciona o cod_id correto e as coordenadas à lista
                coordenadas.append({
                    'cod_id': f"{cod_id}_linha_baixa",  # Adiciona _linha_baixa ao cod_id
                    'longitude_inicio': bounds[0],    # minx (long)
                    'latitude_inicio': bounds[1],     # miny (lat)
                    'longitude_fim': bounds[2],       # maxx (long)
                    'latitude_fim': bounds[3]         # maxy (lat)
                })
            elif isinstance(geometry, LineString):
                # Se for apenas uma LineString, obtemos as coordenadas dela
                coords = list(geometry.coords)
                # Para cada par de coordenadas, adicionamos ao resultado
                for coord in coords:
                    coordenadas.append({
                        'cod_id': f"{cod_id}_linha_baixa",  # Adiciona _linha_baixa ao cod_id
                        'longitude_inicio': coord[0],
                        'latitude_inicio': coord[1],
                        'longitude_fim': coord[0],
                        'latitude_fim': coord[1]
                    })
            else:
                print(f"Geometria de {nome} não é do tipo esperado: {type(geometry)}")
                continue

        # Cria o diretório específico para o CTMT
        ctmt_folder = os.path.join(base_dir, str(nome))
        os.makedirs(ctmt_folder, exist_ok=True)

        # Salva as coordenadas no formato JSON
        filepath = os.path.join(ctmt_folder, 'geometria_linha_baixa_tensao.json')

        # Adiciona todas as coordenadas de uma vez no arquivo JSON
        try:
            with open(filepath, 'w') as file:
                json.dump(coordenadas, file, indent=4)
        except Exception as e:
            print(f"Erro ao salvar as coordenadas no arquivo {filepath}: {e}")
        else:
            print("Modelagem: Coordenadas de latitude e longitude das linhas de Baixa Tensão geradas com sucesso!!!")



    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Pacs_Subestacoes(self):
        """ Modelagem extração das coordenadas de latitude e longitude dos barramentos das subestações """



        try:
            query = """
                          SELECT DISTINCT 
                                        ctmt.pac_ini, ctmt.nome
                           FROM 
                                        ctmt
                           JOIN 
                                        ssdmt ON ctmt.pac_ini = ssdmt.pac_1 OR ctmt.pac_ini = ssdmt.pac_2
                           ORDER BY ctmt.nome

               """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()
            return results
        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return

        base_dir = caminho_modelagens
        ctmt_data = {}
        # Caminho principal para salvar as subpastas
        os.makedirs(base_dir, exist_ok=True)


        # Iterar sobre os dados e agrupar por CTMT
        for linha in results:
            pac_ini = linha[0]
            ctmt = linha[1]

            # Adicionar os dados ao CTMT correspondente
            if ctmt not in ctmt_data:
                ctmt_data[ctmt] = []

            ctmt_data[ctmt].append({

                'pac_ini': pac_ini

            })

        # Criar arquivos JSON para cada CTMT
        for ctmt, linhas in ctmt_data.items():
            ctmt_folder = os.path.join(base_dir, str(ctmt))
            os.makedirs(ctmt_folder, exist_ok=True)
            file_path = os.path.join(ctmt_folder, 'Coordenadas_Subestacoes.json')

            with open(file_path, 'w') as file:
                json.dump(linhas, file, indent=4)

        print("Modelagem: Extração de coordenadas de latitude e longitude das subestações gerada com sucesso !!!")




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Tensoes_Base(self):
        """ Modelagem dAS Tensões base """

        tensoes = {}

        try:
            query = """

               SELECT DISTINCT
                                untrmt.cod_id,
                                ctmt.nome,
                                eqtrmt.ten_pri,
                                eqtrmt.ten_sec,
                                eqtrmt.lig_fas_p,
                                eqtrmt.lig_fas_s

                FROM 
                                untrmt
                LEFT JOIN 
                                eqtrmt ON eqtrmt.uni_tr_mt = untrmt.cod_id
                LEFT JOIN
                                ctmt ON ctmt.cod_id = untrmt.ctmt
                ORDER BY 
                                ctmt.nome;


            """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return

        base_dir = caminho_modelagens
        ctmts_processados = {}

        # Iterar sobre os dados e gerar uma subpasta para cada ctmt
        for index, linha in enumerate(results):
            cod_id = linha[0]
            nome = linha[1]
            ten_pri = linha[2]
            ten_sec = linha[3]
            lig_fas_p = linha[4]
            lig_fas_s = linha[5]

            # VERIFICAÇÃO ANTI-ERROS
            ############################################################################################################
            ######################      TRATAMENTO NA COLUNA NOME       ################################################

            if not nome:
                print("Linha {} contém valores nulos e será ignorada: {}".format(index, linha))
                continue

            # if nome.isdigit():
            #     pass
            # else:
            #     print("Erro! Há valores não numericos na coluna -nome- da tabela -ctmt-: {}".format(nome))


            ######################      TRATAMENTO NA COLUNA TEN_PRI       #############################################

            if ten_pri.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -ten_pri- da tabela -eqtrmt-, inserindo valor padrão: ten_pri = {}".format(
                        self.ten_pri_transformadores_media_codigo))
                ten_pri = self.ten_pri_transformadores_media_codigo

            try:
                ten_pri = int(ten_pri)
            except Exception as e:
                print(
                    "Erro ao converter para int a ten_pri da tabela eqtrmt: {}, inserindo valor padrão: ten_pri = {}".format(
                        e, self.ten_pri_transformadores_media_codigo))
                ten_pri = self.ten_pri_transformadores_media_codigo

            ten_pri = self.dados_tabela_tensoes.get(int(ten_pri), "não encontrado")

            if ten_pri == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "ten_pri da tabela eqtrmt, inserindo valor padrão: ten_pri = {} v".format(
                    self.ten_pri_transformadores_media_volts))
                ten_pri = self.ten_pri_transformadores_media_volts

            ######################      TRATAMENTO NA COLUNA TEN_SEC       #############################################

            if ten_sec.isdigit():
                pass
            else:
                print(
                    "Erro! Há valores não numericos na coluna -ten_sec- da tabela -eqtrmt-, inserindo valor padrão: ten_pri = {}".format(
                        self.ten_sec_transformadores_media_codigo))
                ten_pri = self.ten_sec_transformadores_media_codigo

            try:
                ten_sec = int(ten_sec)
            except Exception as e:
                print(
                    "Erro ao converter para int a ten_sec da tabela eqtrmt: {}, inserindo valor padrão: ten_pri = {}".format(
                        e, self.ten_sec_transformadores_media_codigo))
                ten_pri = self.ten_sec_transformadores_media_codigo

            ten_sec = self.dados_tabela_tensoes.get(int(ten_sec), "não encontrado")

            if ten_sec == "não encontrado":
                print("Erro - Não existe correspondencia de tensão para a coluna "
                      "ten_sec da tabela eqtrmt, inserindo valor padrão: ten_pri = {} v".format(
                    self.ten_sec_transformadores_media_volts))
                ten_pri = self.ten_sec_transformadores_media_volts

            ######################      TRATAMENTO NA COLUNA LIG_FAS_P       ################################################

            if lig_fas_p.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_p- da tabela -eqtrmt-: {}, colocando valor padrão: fas_con = ABC".format(
                        lig_fas_p))
                lig_fas_p = 'ABC'

            ######################      TRATAMENTO NA COLUNA LIG_FAS_S       ################################################

            if lig_fas_s.isalpha():
                pass
            else:
                print(
                    "Erro! Há caracteres diferentes de letras na coluna -lig_fas_s- da tabela -eqtrmt-: {}, colocando valor padrão: fas_con = ABC".format(
                        lig_fas_s))
                lig_fas_s = 'ABC'

            ############################################################################################################

            # Verificar se o ctmt já foi processado
            if nome not in ctmts_processados:
                # Se o ctmt não foi processado ainda, criar uma nova pasta para o ctmt
                ctmt_folder = os.path.join(base_dir, str(nome))
                os.makedirs(ctmt_folder, exist_ok=True)

                # Criar o novo arquivo .dss para este ctmt
                file_path = os.path.join(ctmt_folder, 'Tensoes_Base.bin')
                file = open(file_path, 'wb')

                # Adicionar o ctmt ao dicionario de ctmts processados (armazena o arquivo aberto)
                ctmts_processados[nome] = file


            else:
                # Se o ctmt já foi processado, usar o arquivo existente e abrir no modo append ('a')
                file = ctmts_processados[nome]

            rec_fases_p = self.mapa_fases.get(lig_fas_p, 'ABCN')

            rec_fases_s = self.mapa_fases.get(lig_fas_s, 'ABCN')

            if len(re.findall(r'\d+', rec_fases_p)) == 1:
                conn_p = 'wye'
                ten_primario = int(ten_pri) / round(math.sqrt(3), 5)
            else:
                conn_p = 'delta'
                ten_primario = round(ten_pri, 5)

            if len(re.findall(r'\d+', rec_fases_s)) == 1:
                conn_s = 'wye'
                ten_secundario = int(ten_sec) / round(math.sqrt(3), 5)
            else:
                conn_s = 'wye'
                ten_secundario = int(ten_sec)

            if nome not in tensoes:
                tensoes[nome] = set()
                tensoes[nome].add((ten_primario / 1000, ten_secundario / 1000))


        for nome, tensao in tensoes.items():
            ctmt_folder = os.path.join(base_dir, str(nome))
            os.makedirs(ctmt_folder, exist_ok=True)

            file_path = os.path.join(ctmt_folder, 'Tensoes_Base.bin')
            with open(file_path, 'wb') as file:
                for tension in  tensao:
                    command_transformers = ('Set VoltageBase = "{}, {}"\n CalcVoltageBase'.format(tension[0], tension[1]))

                    if file:
                        file.write(command_transformers.encode())

        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: Tensões Base gerada com sucesso !!!")

    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def fechar_conexao(self):
        """Fecha a conexão com o banco de dados"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Conexão com o banco de dados fechada.")




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



# Uso da classe
if __name__ == "__main__":


    # Parâmetros de conexão
    #nome_distribuidora = input("Qual o nome da Distribuidora de energia ? (digite: BDGD_ANO_NOME_DISTRIBUIDORA_ESTADO): ")
    nome_distribuidora = 'BDGD_2023_ENERGISAMT'
    host = 'localhost'
    port = '5432'
    dbname = 'BDGD_2023_ENERGISA_MT'
    user = 'iuri'
    password = 'aa11bb22'



##############################################################################################################################


    caminho_modelagens = "C://BDGD_DISTRIBUIDORAS_BRASIL//{}//ALIMENTADORES".format(nome_distribuidora)



    ##################################################################################################################################



    irradiance_96 = [
                                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                                    0.0001, 0.0001, 0.0020868143779637777, 0.021658633552047255, 0.06088779031692882,
                                    0.11144429816197712, 0.1678906012265873, 0.22728017851691293, 0.28791109647383534,
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
                                    0.07926139827877186, 0.03481418344816064, 0.006788902468278367, 3.7708368002252944E-5,
                                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                                    0.0001, 0.0001
    ]




    # Define o mapa de fases como uma variável de instância
    mapa_fases = {
                                'ABCN': '.1.2.3.4', 'ABNC': '.1.2.3.4', 'ACBN': '.1.2.3.4', 'ANBC': '.1.2.3.4', 'ANCB': '.1.2.3.4',
                                'BACN': '.1.2.3.4', 'BANC': '.1.2.3.4', 'BCAN': '.1.2.3.4', 'BCNA': '.1.2.3.4', 'BNAC': '.1.2.3.4',
                                'CABN': '.1.2.3.4', 'CBAN': '.1.2.3.4', 'CBNA': '.1.2.3.4', 'CNAB': '.1.2.3.4', 'NABC': '.1.2.3.4',
                                'NACB': '.1.2.3.4', 'NBAC': '.1.2.3.4', 'NBCA': '.1.2.3.4', 'NCAB': '.1.2.3.4', 'CABA': '.1.2.3',
                                'ABAC': '.1.2.3', 'ABC': '.1.2.3', 'ABN': '.1.2.4', 'ACB': '.1.2.3', 'ACN': '.1.3.4',
                                'ANB': '.1.2.4', 'ANC': '.1.3.4', 'BAC': '.1.2.3', 'BAN': '.1.2.4', 'BCA': '.1.2.3',
                                'BCN': '.2.3.4', 'BNA': '.1.2.4', 'BNC': '.2.3.4', 'CAB': '.1.2.3', 'CAN': '.1.3.4',
                                'CBA': '.1.2.3', 'CBN': '.2.3.4', 'CNA': '.1.3.4', 'NAB': '.1.2.4', 'NAC': '.1.3.4',
                                'NBC': '.2.3.4', 'NCA': '.1.3.4', 'AB': '.1.2', 'AC': '.1.3', 'AN': '.1.4',
                                'BA': '.1.2', 'BC': '.2.3', 'BN': '.2.4', 'CA': '.1.3', 'CB': '.2.3',
                                'CN': '.3.4', 'NA': '.1.4', 'NB': '.2.4', 'NC': '.3.4', 'A': '.1.4',
                                'B': '.2.4', 'C': '.3.4', 'N': '.4'
    }




    dados_tabela_tensoes = {
                            0: 100000, 1: 110, 2: 115, 3: 120, 4: 121, 5: 125, 6: 127, 7: 208,
                            8: 216, 9: 216.5, 10: 220, 11: 230, 12: 231, 13: 240, 14: 254, 15: 380, 16: 400,
                            17: 440, 18: 480, 19: 500, 20: 600, 21: 750, 22: 1000, 23: 2200, 24: 3200, 25: 3600,
                            26: 3785, 27: 3800, 28: 3848, 29: 3985, 30: 4160, 31: 4200, 32: 4207, 33: 4368, 34: 4560,
                            35: 5000, 36: 6000, 37: 6600, 38: 6930, 39: 7960, 40: 8670, 41: 11400, 42: 11900,
                            43: 12000, 44: 12600, 45: 12700, 46: 13200, 47: 13337, 48: 13530, 49: 13800, 50: 13860, 51: 14140,
                            52: 14190, 53: 14400, 54: 14835, 55: 15000, 56: 15200, 57: 19053, 58: 19919, 59: 21000,
                            60: 21500, 61: 22000, 62: 23000, 63: 23100, 64: 23827, 65: 24000, 66: 24200, 67: 25000,
                            68: 25800, 69: 27000, 70: 30000, 71: 33000, 72: 34500, 73: 36000, 74: 38000, 75: 40000,
                            76: 44000, 77: 45000, 78: 45400, 79: 48000, 80: 60000, 81: 66000, 82: 69000, 83: 72500,
                            84: 88000, 85: 88200, 86: 92000, 87: 100000, 88: 120000, 89: 121000, 90: 123000, 91: 131600,
                            92: 131630, 93: 131635, 94: 138000, 95: 145000, 96: 230000, 101: 245000, 97: 345000,
                            98: 500000, 102: 550000, 99: 750000, 100: 1000000
    }




    on_off = [
                                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                                               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
                   ]




    trafos_pot_nom = {
        '0': 0, '1': 3, '2': 5, '3': 10, '4': 15, '5': 20, '6': 22.5, '7': 25, '8': 30, '9': 35, '10': 37.5, '11': 38.1,
        '12': 40, '13': 45, '14': 50,
        '15': 60, '16': 75, '17': 76.2, '18': 88, '19': 100, '20': 112.5, '21': 114.3, '22': 120, '23': 138, '24': 150,
        '25': 167, '26': 175, '27': 180, '28': 200, '29': 207,
        '30': 225, '31': 250, '32': 276, '33': 288, '34': 300, '35': 332, '36': 333, '37': 400, '38': 414, '39': 432,
        '40': 500, '41': 509, '42': 667, '43': 750, '44': 833,
        '45': 1000, '46': 1250, '47': 1300, '48': 1500, '49': 1750, '50': 2000, '51': 2250, '52': 2300, '53': 2400,
        '54': 2500, '55': 2750, '56': 2900, '57': 3000, '58': 3125, '59': 3300,
        '60': 3750, '61': 4000, '62': 4200, '63': 4500, '64': 5000, '65': 6250, '66': 6500, '67': 7000, '68': 7500,
        '69': 7800, '70': 8000, '71': 9000, '72': 9375, '73': 9600, '74': 10000,
        '75': 12000, '76': 12500, '77': 13300, '78': 15000, '79': 16000, '80': 18000, '81': 18750, '82': 20000,
        '83': 25000, '84': 26000, '85': 26600, '86': 28000, '87': 30000, '88': 32000, '89': 33000,
        '90': 33300, '91': 40000, '92': 45000, '93': 50000, '94': 60000, '95': 67000, '96': 75000, '97': 80000,
        '98': 83000, '99': 85000, '100': 90000, '101': 100000, '102': 200000, '107': 300000, '108': 400000,
        '109': 500000, '103': 14550000, '104': 17320000, '105': 19100000, '106': 41550000
    }




    # Váriaveis de entrada para alteração de parâmetros da modelagem
    ####################################################################################################################
    #####################################   BARRA SLACK    #############################################################


    limite_inferior_pu_alimentador = 0.098
    limite_superior_pu_alimentador = 1.02
    ten_ope_barra_slack_pu = 1
    ten_nom_barra_slack_codigo = 49 # 13.8 kv
    ten_nom_barra_slack_volts = 13800


    #####################################   TRANSFORMADORES DE MÉDIA TENSÃO    #########################################


    pot_nom_transformadores = 1000000 # kva
    ten_pri_transformadores_media_codigo = 49
    ten_pri_transformadores_media_volts = 13800
    ten_sec_transformadores_media_codigo = 10
    ten_sec_transformadores_media_volts = 220
    r1_transformadores_media = 5
    xhl_transformadores_media = 5
    per_fer_transformadores_media = 0.20


    #####################################   COMPENSADORES DE REATIVO DE MEDIA TENSÃO    ################################


    ten_nom_compensadores_media_codigo = 49
    ten_nom_compensadores_media_volts = 13800
    pot_nom_compensadores_kva = 200000


    #####################################   GERADORES DE MÉDIA TENSÃO    ###############################################


    ten_con_geradores_media_codigo = 49
    ten_con_geradores_media_volts = 13800
    pot_inst_geradores_media_kva = 200000


    #####################################   LINECODES DE BAIXA, MÉDIA E RAMAIS TENSÃO    ###############################################


    r1_linecodes_baixa_ohms_km = 10
    x1_linecodes_baixa_ohms_km = 10
    cnom_linecodes_baixa_amperes = 600
    cmax_renamed_linecodes_baixa_amperes = 1000


    #####################################   LINHAS DE BAIXA E DE MEDIA TENSÃO     ######################################


    tip_cnd_linhas = 'tip_cnd_linhas'


    #####################################   CARGAS DE BAIXA TENSÃO    ##################################################


    ten_forn_cargas_baixa_volts = 220
    ten_forn_cargas_baixa_codigo = 10
    tip_cc_cargas = 'DEFAULT'


    #####################################   GERAÇÃO DISTRIBUIDA DE MÉDIA TENSÃO    #####################################


    ten_con_gd_media_volts = 220
    ten_con_gd_media_codigo = 10


    #####################################   POSTES DE ILUMINAÇÃO PÚBLICA    ############################################


    tip_cc_pip = 'DEFAULT'


    #####################################   REGULADORES DE MÉDIA TENSÃO     ############################################


    pot_nom_reguladores_media = 200000
    ten_nom_reguladores_media_codig = 49
    r_reguladores_media_ohms_km = 3
    banda_volts_reguladores_tensao = 20


    ####################################################################################################################

    # Criar uma instância da classe Modela
    modela = Modela(host, port, dbname, user, password, caminho_modelagens, irradiance_96,
                    mapa_fases, dados_tabela_tensoes, on_off, trafos_pot_nom,
                    limite_inferior_pu_alimentador, limite_superior_pu_alimentador,
                    ten_ope_barra_slack_pu, ten_nom_barra_slack_codigo,ten_nom_barra_slack_volts,
                    pot_nom_transformadores, ten_nom_compensadores_media_codigo,
                    ten_nom_compensadores_media_volts, pot_nom_compensadores_kva,
                    ten_con_geradores_media_codigo, ten_con_geradores_media_volts,
                    pot_inst_geradores_media_kva, r1_linecodes_baixa_ohms_km,
                    x1_linecodes_baixa_ohms_km, cnom_linecodes_baixa_amperes,
                    cmax_renamed_linecodes_baixa_amperes, tip_cnd_linhas,
                    ten_forn_cargas_baixa_volts, ten_forn_cargas_baixa_codigo,
                    tip_cc_cargas, ten_con_gd_media_volts, ten_con_gd_media_codigo,
                    tip_cc_pip, ten_pri_transformadores_media_codigo, ten_pri_transformadores_media_volts,
                    ten_sec_transformadores_media_codigo, ten_sec_transformadores_media_volts,
                    r1_transformadores_media, xhl_transformadores_media, pot_nom_reguladores_media,
                    ten_nom_reguladores_media_codig, r_reguladores_media_ohms_km,
                    banda_volts_reguladores_tensao, per_fer_transformadores_media
 )





    # Iniciar a conexão com o banco de dados
    modela.iniciar_conexao()

    # Gerar comandos para o OpenDSS
    modela.Barra_Slack()
    modela.Compensadores_Reativo_Media()
    modela.Compensadores_Reativo_Baixa()
    modela.Chaves_Seccionadoras_Baixa_Tensao()
    modela.Chaves_Seccionadoras_Media_Tensao()
    modela.Geradores_Media_tensao()
    modela.Linecodes_Baixa_Tensao()
    modela.Linecodes_Media_Tensao()
    modela.Linecodes_Ramais_Baixa_Tensao()
    modela.Linhas_Baixa_Tensao()
    modela.Linhas_Media_Tensao()
    modela.Cargas_Baixa_Tensao()
    modela.Cargas_Media_Tensao()
    modela.LoadShape_Cargas_Baixa_Tensao()
    modela.LoadShape_Cargas_Media_Tensao()
    modela.Cargas_Poste_Iluminacao_Publica_Baixa_Tensao()
    modela.Tensoes_Base()
    modela.Ramais_Ligacao()
    modela.transformadores_Media_tensao()
    modela.Reguladores_Media_Tensao()
    modela.GeracaoShape_Geracao_Distribuida_Baixa_tensao()
    modela.GeracaoShape_Geracao_Distribuida_Media_tensao()
    modela.Pacs_Subestacoes()
    modela.Geometria_ssdbt()
    modela.Geometria_ssdmt()
    modela.Shape_Gd_Media_Tensao()
    modela.Shape_Gd_Baixa_Tensao()
    modela.LoadShape_Poste_Iluminacao_Publica()
    modela.Energia_alimentador()



    # Fechar a conexão com o banco de dados
    modela.fechar_conexao()


