import math
import numpy as np
import psycopg2
import os
from pvlib import location
import pandas as pd
from shapely.wkb import loads
import re
from shapely.wkt import loads
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, Console
console = Console(force_terminal=True)

import sys
sys.stdout.reconfigure(encoding='utf-8')
from collections import defaultdict


# Classe para obtenção das curvas de carga
from curvas_de_carga import loashape_class







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



    def iniciar_conexao_base_dados_aneel(self):
        """Estabelece a conexão com o banco de dados PostgreSQL"""
        
        host = 'localhost'
        port = '5432'
        dbname = 'ANEEL'
        user = 'iuri'
        password = 'aa11bb22'
    
    
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
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
                        
                        ORDER BY        nome  
                  
                   """
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao conectar com o banco de dados para a tabela ctmt: {}".format(e))

        if not results:
            print("Nenhum dado foi recuperado da tabela: ctmt")
            return
        
        print("Modelando Barras Slacks...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))

         

            # Itera pelas linhas da tabela
            for index, linha in enumerate(results):
                nome, ten_ope, ten_nom, pac_ini = linha

                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################

                ######################      TRATAMENTO PARA COLUNA NOME     ################################################


                if not nome:
                    print("Linha {} da tabela ctmt contém valores nulos !!!".format(index))
                    nome = 'default'

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
                            "ten_nom da tabela ctmt: {}, colocando valor padrão ten_nom = {} v".format(ten_nom_voltage, self.ten_nom_barra_slack_volts))
                    ten_nom_voltage = self.ten_nom_barra_slack_volts



                ############################################################################################################

                if nome not in ctmts_processados:
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)
                    file_path = os.path.join(ctmt_folder, 'Barra_Slack.dss')
                    file = open(file_path, 'w', encoding='utf-8')             
                    ctmts_processados[nome] = file
                else:
                    file = ctmts_processados[nome]


                command_linecode = (

                                'New Object = Circuit.{}_Barra_Slack\n'.format(nome) +
                                '~ basekv = {} pu = {} angle = 0\n'.format(ten_nom_voltage / 1000, ten_ope) +
                                'New line.{}{} phases = 3 bus1 = SourceBus bus2 = {}.1.2.3 switch = y\n'.format(nome, nome, pac_ini)


                                    )

                if file:
                    file.write(command_linecode)
                
                progress.advance(task)

            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Barras Slacks !!!".format(len(results)))
            return




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def Compensadores_Reativo_Media(self):
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
                ORDER BY nome
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print(f"Erro ao executar a consulta na tabela uncrmt: {e}")
            return

        # Executar segunda consulta uma única vez
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
            """
            self.cur.execute(second_query)
            second_results = self.cur.fetchall()
        except Exception as e:
            print(f"Erro ao executar a segunda consulta: {e}")
            return

        # Criar um set com tensões válidas para busca rápida
        tensoes_validas = set()
        for _, _, _, ten_pri, ten_sec in second_results:
            try:
                tensoes_validas.add(int(ten_pri))
                tensoes_validas.add(int(ten_sec))
            except Exception:
                pass

        # Aqui seu setup de GUI e progresso se for usar barra gráfica ou rich progress (não mostrado)

        print("Modelando Compensadores Reativos de Média Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))            
            for index, linha in enumerate(results):
                nome, fas_con, tip_uni, pot_nom, pac_1, ten_nom, cod_id = linha
                
                
        
                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ############################        TRATAMENTO PARA A COLUNA NOME       ####################################



                if not nome:
                    print("Linha {} da tabela ctmt sem valor, eliminando da modelagem !!!: {}".format(index))
                    continue


                ############################        TRATAMENTO PARA A COLUNA FAS_CON      ##################################



                if not fas_con:
                    print("Coluna fas_con da tabela uncrmt vazia, inserindo fas_con = .1.2.3")
                    fas_con = 'ABC'

                elif fas_con.isalpha():
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

                elif ten_nom.isdigit():
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




                try:
                    ten_nom_int = int(ten_nom)
                except Exception:
                    print(f"ten_nom inválido na linha {index}, pulando")
                    continue

                if ten_nom_int not in tensoes_validas:
                    print(f"Tensão {ten_nom_int} de {nome} não corresponde com tensões da segunda consulta. Pulando a linha.")
                    continue

                # Criar pasta, abrir arquivo, gerar comando, etc.
                if nome not in ctmts_processados:
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)
                    file_path = os.path.join(ctmt_folder, 'Compensadores_de_reativo_media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
                    ctmts_processados[nome] = file
                else:
                    file = ctmts_processados[nome]

                rec_fases = self.mapa_fases.get(fas_con, 'ABC')
                fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]
                tensao = self.dados_tabela_tensoes.get(ten_nom_int, "não encontrado")

                if tip_uni == 56:
                    command_linecode = (
                        f"New Reactor.{cod_id}_Banco_de_Reator Bus1 = {pac_1}{rec_fases} kv = {tensao / 1000} kvar = {pot_nom} phases = {len(fases)} conn = wye\n\n"
                    )
                else:
                    command_linecode = (
                        f"New Capacitor.{cod_id}_Banco_de_Capacitor Bus1 = {pac_1}{rec_fases} kv = {tensao / 1000} kVAR = {pot_nom} phases = {len(fases)} conn = wye\n\n"
                    )

                file.write(command_linecode)

                # Atualizar progresso aqui se usar barra de progresso
                progress.advance(task)

            # Fechar arquivos
            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Compensadores de Reativo de Média Tensão gerada com sucesso !!!".format(len(results)))
            return





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def Compensadores_Reativo_Baixa(self):
        """Modela Compensadores de Reativos na baixa tensão"""

        results = []
        base_dir = caminho_modelagens
        ctmts_processados = {}

        # Consulta principal
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
                ORDER BY nome
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a consulta na tabela uncrbt: {}".format(e))
            return

        # Segunda consulta executada apenas 1 vez
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
            """
            self.cur.execute(second_query)
            second_results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao executar a segunda consulta: {}".format(e))
            return

        print("Modelando Compensadores Reativos de Baixa Tensão...")
        # Inicia barra de progresso
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Carregando", total=len(results))

            for index, linha in enumerate(results):
                nome, fas_con, tip_uni, pot_nom, pac_1, ten_nom, cod_id = linha
                


                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ############################        TRATAMENTO PARA A COLUNA NOME       ####################################



                if not nome:
                    print("Linha {} contém valores nulos e será ELIMINADA DA MODELAGEM !!!: {}".format(index, linha))
                    continue


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



                # Verifica correspondência de tensões
                tension_match = False
                for sec_linha in second_results:
                    ten_pri, ten_sec = int(sec_linha[3]), int(sec_linha[4])
                    if ten_nom == ten_pri or ten_nom == ten_sec:
                        tension_match = True
                        break

                if not tension_match:
                    print(f"Tensão {ten_nom} de {nome} não corresponde com tensões de {nome}. Pulando a linha.")
                    progress.advance(task)
                    continue

                # Criação de pasta/arquivo por ctmt
                if nome not in ctmts_processados:
                    ctmt_folder = os.path.join(base_dir, str(nome))
                    os.makedirs(ctmt_folder, exist_ok=True)
                    file_path = os.path.join(ctmt_folder, 'Compensadores_de_reativo_baixa_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
                    ctmts_processados[nome] = file
                else:
                    file = ctmts_processados[nome]

                # Geração de comando
                rec_fases = self.mapa_fases.get(fas_con, 'ABC')
                fases = [fas for fas in fas_con if fas in ['A', 'B', 'C']]
                tensao = self.dados_tabela_tensoes.get(ten_nom, "não encontrado")

                if tensao == "não encontrado":
                    print(f"Tensão nominal {ten_nom} não encontrada. Pulando {cod_id}.")
                    progress.advance(task)
                    continue

                if tip_uni == 56:
                    command_linecode = (
                        f"New Reactor.{cod_id}_Banco_de_Reator Bus1 = {pac_1}{rec_fases} "
                        f"kv = {tensao / 1000} kvar = {pot_nom} phases = {len(fases)} conn = wye\n\n"
                    )
                else:
                    command_linecode = (
                        f"New Capacitor.{cod_id}_Banco_de_Capacitor Bus1 = {pac_1}{rec_fases} "
                        f"kv = {tensao / 1000} kVAR = {pot_nom} phases = {len(fases)} conn = wye\n\n"
                    )

                if file:
                    file.write(command_linecode)

                progress.advance(task)

            # Fechando os arquivos
            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Compensadores de Reativo de Baixa Tensão gerada com sucesso !!!".format(len(results)))
            return




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
        
        print("Modelando Chaves Seccionadoras de Baixa Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(dados))


            for linha in dados:
                pac_1, pac_2, cod_id, ctmt, cor_nom, fas_con, nome, p_n_ope = linha


                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ############################        TRATAMENTO DA COLUNA NOME       ########################################



                if not nome:
                    print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                    continue

        
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
                    file_path = os.path.join(ctmt_folder, 'chaves_seccionadoras_baixa_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
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
                    file.write(command_switch)
                    
                progress.advance(task)

            # Fecha todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Chaves seccionadoras de baixa Tensão gerada com sucesso !!!".format(len(dados)))
            return





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

        print("Modelando Chaves Seccionadoras de Média Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(dados))
            
            for linha in dados:
                pac_1, pac_2, cod_id, ctmt, cor_nom, fas_con, nome, p_n_ope = linha

                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ############################        TRATAMENTO DA COLUNA NOME       ########################################



                if not nome:
                    print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                    continue

    

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
                    file_path = os.path.join(ctmt_folder, 'chaves_seccionadoras_media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
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
                    file.write(command_switch)
                progress.advance(task)

            # Fecha todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Chaves seccionadoras de Média Tensão gerada com sucesso !!!".format(len(dados)))
            return





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
            
        print("Modelando Geradores de Média Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(dados))

            for linha in dados:
                cod_id, pac, ctmt, fas_con, ten_con, pot_inst, cep, ceg_gd, nome = linha

                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ##########################      TRATAMENTO NA COLUNA NOME       ############################################



                if not nome:
                    print("A coluna nome não possui valor, esta linha será execluida da MODELAGEM !!!: {}".format(linha))
                    continue



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
                    file_path = os.path.join(ctmt_folder, 'Geradores_media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')  
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
                    file.write(command_generator)
                
                progress.advance(task)

            # Fecha todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

            print("Modelagem: {} Geradores de média Tensão gerada com sucesso !!!".format(len(dados)))
            return





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Linecodes_Baixa_Tensao(self):
        """ Modelagem de código de linhas de baixa tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens

            
        try:


            query = """
            
                   SELECT 
                                   ctmt.nome,
                                   ssdbt.tip_cnd,
                                   ssdbt.fas_con,
                                   (SELECT r1 FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS r1,
                                   (SELECT x1 FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS x1,
                                   (SELECT cnom FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS cnom,
                                   (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ssdbt.tip_cnd LIMIT 1) AS cmax_renamed
                   FROM 
                                    ssdbt
                   JOIN             ctmt on ctmt.cod_id = ssdbt.ctmt  
                   ORDER BY         ctmt.nome
              
               """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()


        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))
            
        
        print("Modelando Linecodes de Baixa Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            

            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue


                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print("Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdbt-: {}, colocando valor padrão: fas_con = ABC".format(
                        fas_con))
                    fas_con = 'ABC'



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
                    file_path = os.path.join(ctmt_folder, 'linecodes_baixa_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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
                    file.write(command_linecode)
                
                progress.advance(task)

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()
            

        print("Modelagem: {} Linecodes das linhas de baixa Tensão gerada com sucesso !!!".format(len(results)))
        return





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def Linecodes_Media_Tensao(self):
        """ Modelagem de código de linhas de média tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens

        try:
        

            query = """

                         SELECT 
                                         ctmt.nome,
                                         ssdmt.tip_cnd,
                                         ssdmt.fas_con,
                                         (SELECT r1 FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS r1,
                                         (SELECT x1 FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS x1,
                                         (SELECT cnom FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS cnom,
                                         (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ssdmt.tip_cnd LIMIT 1) AS cmax_renamed
                         FROM 
                                        ssdmt
                         JOIN           ctmt on ctmt.cod_id = ssdmt.ctmt  
                         ORDER BY       ctmt.nome

                     """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()


        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))
        
        
        print("Modelando Linecodes de Média Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            
            
            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue

                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print(
                        "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ssdmt-: {}, colocando valor padrão: fas_con = ABC".format(
                            fas_con))
                    fas_con = 'ABC'



                ###############################     TRATAMENTO DA COLUNA R1       ######################################



                if not isinstance(r1, (float, int)):
                    print(
                        "Erro! Há valores que não são nem float e nem int na coluna -r1- da tabela -ssdmt-: {}, colocando valor padrão r1 = {}".format(
                            r1, self.r1_linecodes_baixa_ohms_km))
                    r1 = self.r1_linecodes_baixa_ohms_km

                else:
                    r1 = float(r1)
                    if r1 > self.r1_linecodes_baixa_ohms_km:
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
                    file_path = os.path.join(ctmt_folder, 'linecodes_media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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
                    file.write(command_linecode)
                    
                progress.advance(task)

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

           

        print("Modelagem: {} Linecodes das linhas de media Tensão gerada com sucesso !!!".format(len(results)))
        return





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Linecodes_Ramais_Baixa_Tensao(self):
        """ Modelagem de código de linhas de Ramais de ligação de média tensão """

        ctmts_processados = {}
        base_dir = caminho_modelagens



        try:
            print("Comandos para o OpenDSS gerados com sucesso.")
            

            query = """

                                SELECT DISTINCT
                                                    ctmt.nome,
                                                    ramlig.tip_cnd,
                                                    ramlig.fas_con,
                                                    (SELECT r1 FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS r1,
                                                    (SELECT x1 FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS x1,
                                                    (SELECT cnom FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS cnom,
                                                    (SELECT cmax_renamed FROM segcon WHERE segcon.cod_id = ramlig.tip_cnd LIMIT 1) AS cmax_renamed
                                FROM 
                                                    ramlig
                                JOIN                ctmt on ctmt.cod_id = ramlig.ctmt  
                                ORDER BY            ctmt.nome

                            """

            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()



        except Exception as e:
            print("Erro ao gerar comandos: {}".format(e))
                
                
        print("Modelando Linecodes de Ramais de Baixa Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            


            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                nome, tip_cnd, fas_con, r1, x1, cnom, cmax_renamed = linha

                # VERIFICAÇÃO ANTI-ERROS
                ########################################################################################################
                ###############################     TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome: {} está fazia, logo está linha não será modelada !!!: ".format(linha))
                    continue




                ###############################     TRATAMENTO DA COLUNA FAS_CON       #################################



                if fas_con.isalpha():
                    pass
                else:
                    print(
                        "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ramlig-: {}, colocando valor padrão: fas_con = ABC".format(
                            fas_con))
                    fas_con = 'ABC'



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
                    file_path = os.path.join(ctmt_folder, 'linecodes_ramais_de_ligacao_baixa_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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
                    file.write(command_linecode)
                    
                progress.advance(task)

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

        


        print("Modelagem: {} Linecodes dos ramais de ligação de baixa Tensão gerada com sucesso !!!".format(len(results)))
        return






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

        print("Modelando Linhas de Baixa Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            



            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                cod_id, pac_1, pac_2, nome, fas_con, comp, tip_cnd = linha

                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ################################        TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome da tabela ctmt está vazia, logo será excluída da MODELAGEM !!! : {}".format(linha))
                    continue

        
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
                    file_path = os.path.join(ctmt_folder, 'linhas_baixa_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
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
                
                #rec_fases = '.1.2.3.4'


                # Gerar o comando no formato desejado
                command_line = (

                    'New Line.{}_linha_baixa Phases = {} Bus1 = {}{} Bus2 = {}{} Linecode = {}_linecode_baixa Length = {} units = m\n'.format(
                    cod_id, len(fases), pac_1, rec_fases, pac_2, rec_fases, tip_cnd, comp

                    )
                )
                # Escrever o comando no arquivo .dss
                if file:
                    file.write(command_line)
                    
                progress.advance(task)

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: {} Linhas de distribuição de baixa Tensão gerada com sucesso !!!".format(len(results)))
        return





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


        print("Modelando Linhas de Média Tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            


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
                    file_path = os.path.join(ctmt_folder, 'linhas_media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
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
                    file.write(command_line)
                
                progress.advance(task)

        # Fechar todos os arquivos antes de terminar o loop
        for file in ctmts_processados.values():
            file.close()

        print("Modelagem: {} Linhas de distribuição de media Tensão gerada com sucesso !!!".format(len(results)))
        return






    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_Baixa_Tensao(self):
        """ Modelagem das Cargas de baixa tensão """

        offset = 0
        limit = 100000
        base_dir = caminho_modelagens
        ctmts_processados = {}
        processed_rows = 0  
        ten_nom_voltage = 220  
        

        self.dic_mult_por_ctmt_load = defaultdict(lambda: defaultdict(float))


        # Extraindo o multiplicador de carga
        print("Extraindo o multiplicador de carga: Este processo demora uns 20 minutos")
        load = loashape_class()
        dic_mult_por_ctmt_load = load.curvas()
        print("Iniciando modelagem das cargas de baixa tensão: Este processo demora uns 20 minutos")



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
                        


                        -- Energias do alimentador
                        ctmt.ene_01 AS ene_01_alimentador, ctmt.ene_02 AS ene_02_alimentador,
                        ctmt.ene_03 AS ene_03_alimentador, ctmt.ene_04 AS ene_04_alimentador,
                        ctmt.ene_05 AS ene_05_alimentador, ctmt.ene_06 AS ene_06_alimentador,
                        ctmt.ene_07 AS ene_07_alimentador, ctmt.ene_08 AS ene_08_alimentador,
                        ctmt.ene_09 AS ene_09_alimentador, ctmt.ene_10 AS ene_10_alimentador,
                        ctmt.ene_11 AS ene_11_alimentador, ctmt.ene_12 AS ene_12_alimentador,

                        -- Pontos do alimentador
                        ctmt.pntbt_01 AS pntbt_01_alimentador, ctmt.pntbt_02 AS pntbt_02_alimentador,
                        ctmt.pntbt_03 AS pntbt_03_alimentador, ctmt.pntbt_04 AS pntbt_04_alimentador,
                        ctmt.pntbt_05 AS pntbt_05_alimentador, ctmt.pntbt_06 AS pntbt_06_alimentador,
                        ctmt.pntbt_07 AS pntbt_07_alimentador, ctmt.pntbt_08 AS pntbt_08_alimentador,
                        ctmt.pntbt_09 AS pntbt_09_alimentador, ctmt.pntbt_10 AS pntbt_10_alimentador,
                        ctmt.pntbt_11 AS pntbt_11_alimentador, ctmt.pntbt_12 AS pntbt_12_alimentador,

                        -- Informações da carga
                        ucbt_tab.fas_con, 
                        ucbt_tab.ten_forn, 

                        -- Energias da carga
                        ucbt_tab.ene_01 AS ene_01_carga, ucbt_tab.ene_02 AS ene_02_carga,
                        ucbt_tab.ene_03 AS ene_03_carga, ucbt_tab.ene_04 AS ene_04_carga,
                        ucbt_tab.ene_05 AS ene_05_carga, ucbt_tab.ene_06 AS ene_06_carga,
                        ucbt_tab.ene_07 AS ene_07_carga, ucbt_tab.ene_08 AS ene_08_carga,
                        ucbt_tab.ene_09 AS ene_09_carga, ucbt_tab.ene_10 AS ene_10_carga,
                        ucbt_tab.ene_11 AS ene_11_carga, ucbt_tab.ene_12 AS ene_12_carga, 
                        ucbt_tab.ctmt

                    FROM 
                        ucbt_tab
                    JOIN 
                        ctmt ON ctmt.cod_id = ucbt_tab.ctmt
                    WHERE 
                        ucbt_tab.gru_ten = 'BT'
                    ORDER BY 
                        ctmt.nome

                    LIMIT {} OFFSET {};
               
                """.format(limit, offset)

                self.cur.execute(query)
                rows = self.cur.fetchall()

                if not rows:
                    break  # Encerra se não houver mais dados

                print("Modelando Cargas de Baixa Tensão...")
                with Progress(
                    TextColumn("[bold green]Carregando..."),
                    BarColumn(bar_width=60, style="green"),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeRemainingColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Modelando...", total=len(rows))
                    

                    for index, linha in enumerate(rows):
                        (
                            cod_id,
                            tip_cc,
                            pac,
                            nome,

                            # Energias do alimentador
                            ene_01_alimentador, ene_02_alimentador, ene_03_alimentador, ene_04_alimentador,
                            ene_05_alimentador, ene_06_alimentador, ene_07_alimentador, ene_08_alimentador,
                            ene_09_alimentador, ene_10_alimentador, ene_11_alimentador, ene_12_alimentador,

                            # Pontos do alimentador
                            pntbt_01_alimentador, pntbt_02_alimentador, pntbt_03_alimentador, pntbt_04_alimentador,
                            pntbt_05_alimentador, pntbt_06_alimentador, pntbt_07_alimentador, pntbt_08_alimentador,
                            pntbt_09_alimentador, pntbt_10_alimentador, pntbt_11_alimentador, pntbt_12_alimentador,

                            # Informações da carga
                            fas_con,
                            ten_forn,

                            # Energias da carga
                            ene_01_carga, ene_02_carga, ene_03_carga, ene_04_carga,
                            ene_05_carga, ene_06_carga, ene_07_carga, ene_08_carga,
                            ene_09_carga, ene_10_carga, ene_11_carga, ene_12_carga, ucbt_ctmt
                        ) = linha

                        load_mult = dic_mult_por_ctmt_load.get(ucbt_ctmt, 1)
                        
                    
                        # Criando perfil da curva de carga anual
                        curva_anual_energia = [ene_01_alimentador, ene_02_alimentador, ene_03_alimentador, ene_04_alimentador,
                                 ene_05_alimentador, ene_06_alimentador, ene_07_alimentador, ene_08_alimentador,
                                 ene_09_alimentador, ene_10_alimentador, ene_11_alimentador, ene_12_alimentador]
                        
                        # Normalizando a curva anual
                        curva_anual_energia_normalizada = np.array(curva_anual_energia)
                        curva_anual_energia = np.array(curva_anual_energia)
             
             
                        # Se todos os valores forem zero
                        if np.all(curva_anual_energia_normalizada == 0):
                            curva_anual_energia_normalizada = np.ones_like(curva_anual_energia_normalizada)
                        else:
                            # Substituir todos os valores < 1 por 1
                            curva_anual_energia_normalizada[curva_anual_energia_normalizada < 1] = 1

                            # Normalizar pelo valor máximo
                            max_val = np.max(curva_anual_energia_normalizada)
                            curva_anual_energia_normalizada = curva_anual_energia_normalizada / max_val

                        
                        # Coloacando as perdas não técnicas anuais em uma lista
                        curva_anual_pnts = [pntbt_01_alimentador, pntbt_02_alimentador, pntbt_03_alimentador,
                                            pntbt_04_alimentador, pntbt_05_alimentador, pntbt_06_alimentador,
                                            pntbt_07_alimentador, pntbt_08_alimentador, pntbt_09_alimentador,
                                            pntbt_10_alimentador, pntbt_11_alimentador, pntbt_12_alimentador] 
                        
                        # Convertendo para array
                        curva_anual_pnts = np.array(curva_anual_pnts)
                        
                        # Calculando as porcentagens mensais de perdas não técnicas em relação ao total
                        curva_anual_energia = np.where(curva_anual_energia < 1, 1, curva_anual_energia)

                        curva_percentual_pnts_anual = curva_anual_pnts / curva_anual_energia
                        curva_percentual_pnts_anual = 1 + curva_percentual_pnts_anual
                        
                        # Calculando multiplicador da potência de cada carga mensalmente, considerando:
                        # -- fator multiplicativo da curva_anual_energia, para simulações anuais
                        # -- fator multiplicativo da curva_percentual_pnts_anual, para considerar o efeito das pnt's
                        
                        multiplicadores_carga = (curva_percentual_pnts_anual * curva_anual_energia_normalizada).round(3)
                        
                        # Potências da carga mensais que serão usadas para multiplicar pelos multiplicadores_carga
                        energias_carga_anuais = [ene_01_carga, ene_02_carga, ene_03_carga, ene_04_carga, ene_05_carga,
                                                  ene_06_carga, ene_07_carga, ene_08_carga, ene_09_carga, ene_10_carga,ene_11_carga, ene_12_carga]
                        
                        # Convertendo para array
                        energias_carga_anuais = np.array(energias_carga_anuais)
                        
                        # Convertendo para potencias anuais
                        potencias_carga_anuais = energias_carga_anuais / 720  # 720 horas em um mês (30 dias * 24 horas)
                        
                        # Multiplicando as potencias_carga_anuais * multiplicadores_carga
                        potencias_carga_anuais = ((potencias_carga_anuais * multiplicadores_carga)).tolist()
                        
                        # Multiplicando pelo multiplicador para conseguir se aproximar da curva do alimentador
                        potencias_carga_anuais = np.array(potencias_carga_anuais)
                        potencias_carga_anuais = (((potencias_carga_anuais * load_mult).round(2)).tolist())
                        
                        
                        
                        
                        # VERIFICAÇÃO ANTI-ERROS
                        ####################################################################################################
                        ###############################     TRATAMENTO COLUNA NOME      ####################################



                        if not nome:
                            print("A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(linha))
                            continue



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
                            file_path = os.path.join(ctmt_folder, 'Cargas_Baixa_tensao.dss')
                            file = open(file_path, 'w', encoding='utf-8')
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
                        potencias_str = '_'.join([str(round(p, 2)) for p in potencias_carga_anuais])

                        command_transformers = (
                            'New Load.nome_{}_curva_diaria_{}_curva_anual_{}_carga_baixa Bus1 = {}{} Phases = {}\n'.format(
                                cod_id, tip_cc, potencias_str, pac, rec_fases, len(fases)
                            ) +
                            '~ Conn = {} Model = 1 Kv = {} Kw = 1 Kvar = 0\n\n'.format(conn, ten_nom_voltage / 1000)
                        )


                        # Escrever no arquivo correspondente
                        if file:
                            file.write(command_transformers)

                        processed_rows += 1
                        progress.advance(task)

                # Atualizar offset para próxima consulta
                offset += limit

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de baixa tensão processadas com sucesso !!!.".format(processed_rows))
        return





    ############################################################################################################################################################################################################################
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_media_Tensao(self):
        """ Modelagem das Cargas de média tensão """

        offset = 0
        limit = 100000
        base_dir = caminho_modelagens
        ctmts_processados = {}
        processed_rows = 0  # Contador de linhas processadas
        ten_nom_voltage = 13800 
        

        self.dic_mult_por_ctmt_load = defaultdict(lambda: defaultdict(float))


        # Extraindo o multiplicador de carga
        print("Extraindo o multiplicador de carga: Este processo demora uns 20 minutos")
        load = loashape_class()
        dic_mult_por_ctmt_load = load.curvas()
        print("Iniciando modelagem das cargas de média tensão: Este processo demora uns 20 minutos")



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


                        -- Energias do alimentador
                        ctmt.ene_01 AS ene_01_alimentador, ctmt.ene_02 AS ene_02_alimentador,
                        ctmt.ene_03 AS ene_03_alimentador, ctmt.ene_04 AS ene_04_alimentador,
                        ctmt.ene_05 AS ene_05_alimentador, ctmt.ene_06 AS ene_06_alimentador,
                        ctmt.ene_07 AS ene_07_alimentador, ctmt.ene_08 AS ene_08_alimentador,
                        ctmt.ene_09 AS ene_09_alimentador, ctmt.ene_10 AS ene_10_alimentador,
                        ctmt.ene_11 AS ene_11_alimentador, ctmt.ene_12 AS ene_12_alimentador,

                        -- Pontos do alimentador
                        ctmt.pntmt_01 AS pntmt_01_alimentador, ctmt.pntmt_02 AS pntmt_02_alimentador,
                        ctmt.pntmt_03 AS pntmt_03_alimentador, ctmt.pntmt_04 AS pntmt_04_alimentador,
                        ctmt.pntmt_05 AS pntmt_05_alimentador, ctmt.pntmt_06 AS pntmt_06_alimentador,
                        ctmt.pntmt_07 AS pntmt_07_alimentador, ctmt.pntmt_08 AS pntmt_08_alimentador,
                        ctmt.pntmt_09 AS pntmt_09_alimentador, ctmt.pntmt_10 AS pntmt_10_alimentador,
                        ctmt.pntmt_11 AS pntmt_11_alimentador, ctmt.pntmt_12 AS pntmt_12_alimentador,

                        -- Informações da carga
                        ucmt_tab.fas_con, 
                        ucmt_tab.ten_forn, 

                        -- Energias da carga
                        ucmt_tab.ene_01 AS ene_01_carga, ucmt_tab.ene_02 AS ene_02_carga,
                        ucmt_tab.ene_03 AS ene_03_carga, ucmt_tab.ene_04 AS ene_04_carga,
                        ucmt_tab.ene_05 AS ene_05_carga, ucmt_tab.ene_06 AS ene_06_carga,
                        ucmt_tab.ene_07 AS ene_07_carga, ucmt_tab.ene_08 AS ene_08_carga,
                        ucmt_tab.ene_09 AS ene_09_carga, ucmt_tab.ene_10 AS ene_10_carga,
                        ucmt_tab.ene_11 AS ene_11_carga, ucmt_tab.ene_12 AS ene_12_carga, ucmt_tab.ctmt

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

                print("Modelando Cargas de Média Tensão...")
                with Progress(
                    TextColumn("[bold green]Carregando..."),
                    BarColumn(bar_width=60, style="green"),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeRemainingColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Modelando...", total=len(rows))
                    

                    for index, linha in enumerate(rows):
                        (
                            cod_id,
                            tip_cc,
                            pac,
                            nome,

                            # Energias do alimentador
                            ene_01_alimentador, ene_02_alimentador, ene_03_alimentador, ene_04_alimentador,
                            ene_05_alimentador, ene_06_alimentador, ene_07_alimentador, ene_08_alimentador,
                            ene_09_alimentador, ene_10_alimentador, ene_11_alimentador, ene_12_alimentador,

                            # Pontos do alimentador
                            pntmt_01_alimentador, pntmt_02_alimentador, pntmt_03_alimentador, pntmt_04_alimentador,
                            pntmt_05_alimentador, pntmt_06_alimentador, pntmt_07_alimentador, pntmt_08_alimentador,
                            pntmt_09_alimentador, pntmt_10_alimentador, pntmt_11_alimentador, pntmt_12_alimentador,

                            # Informações da carga
                            fas_con,
                            ten_forn,

                            # Energias da carga
                            ene_01_carga, ene_02_carga, ene_03_carga, ene_04_carga,
                            ene_05_carga, ene_06_carga, ene_07_carga, ene_08_carga,
                            ene_09_carga, ene_10_carga, ene_11_carga, ene_12_carga, ucmt_ctmt
                        ) = linha
                        

                        load_mult = dic_mult_por_ctmt_load.get(ucmt_ctmt, 1)

                        # Criando perfil da curva de carga anual
                        curva_anual_energia = [ene_01_alimentador, ene_02_alimentador, ene_03_alimentador, ene_04_alimentador,
                                 ene_05_alimentador, ene_06_alimentador, ene_07_alimentador, ene_08_alimentador,
                                 ene_09_alimentador, ene_10_alimentador, ene_11_alimentador, ene_12_alimentador]
                        
                        # Normalizando a curva anual
                        curva_anual_energia_normalizada = np.array(curva_anual_energia)
                        curva_anual_energia = np.array(curva_anual_energia)
                        
                        if curva_anual_energia_normalizada[0] < 1:
                            curva_anual_energia_normalizada[0] = 1
                        curva_anual_energia_normalizada = (curva_anual_energia_normalizada)/(curva_anual_energia_normalizada[0])
                        
                        
                        # Coloacando as perdas não técnicas anuais em uma lista
                        curva_anual_pnts = [pntmt_01_alimentador, pntmt_02_alimentador, pntmt_03_alimentador,
                                            pntmt_04_alimentador, pntmt_05_alimentador, pntmt_06_alimentador,
                                            pntmt_07_alimentador, pntmt_08_alimentador, pntmt_09_alimentador,
                                            pntmt_10_alimentador, pntmt_11_alimentador, pntmt_12_alimentador] 
                        
                        # Convertendo para array
                        curva_anual_pnts = np.array(curva_anual_pnts)
                        
                        # Calculando as porcentagens mensais de perdas não técnicas em relação ao total
                        curva_anual_energia = np.where(curva_anual_energia < 1, 1, curva_anual_energia)

                        curva_percentual_pnts_anual = curva_anual_pnts / curva_anual_energia
                        curva_percentual_pnts_anual = 1 + curva_percentual_pnts_anual
                        
                        # Calculando multiplicador da potência de cada carga mensalmente, considerando:
                        # -- fator multiplicativo da curva_anual_energia, para simulações anuais
                        # -- fator multiplicativo da curva_percentual_pnts_anual, para considerar o efeito das pnt's
                        
                        multiplicadores_carga = (curva_percentual_pnts_anual * curva_anual_energia_normalizada).round(3)
                        
                        # Potências da carga mensais que serão usadas para multiplicar pelos multiplicadores_carga
                        energias_carga_anuais = [ene_01_carga, ene_02_carga, ene_03_carga, ene_04_carga, ene_05_carga,
                                                  ene_06_carga, ene_07_carga, ene_08_carga, ene_09_carga, ene_10_carga,ene_11_carga, ene_12_carga]
                        
                        # Convertendo para array
                        energias_carga_anuais = np.array(energias_carga_anuais)
                        
                        # Convertendo para potencias anuais
                        potencias_carga_anuais = energias_carga_anuais / 720  # 720 horas em um mês (30 dias * 24 horas)
                        
                        # Multiplicando as potencias_carga_anuais * multiplicadores_carga
                        potencias_carga_anuais = ((potencias_carga_anuais * multiplicadores_carga).round(2)).tolist()
                        
                        
                        # Multiplicando pelo multiplicador para conseguir se aproximar da curva do alimentador
                        potencias_carga_anuais = np.array(potencias_carga_anuais)
                        potencias_carga_anuais = (((potencias_carga_anuais * load_mult).round(2)).tolist())
                        
                        
                        
                        
                        
                        # VERIFICAÇÃO ANTI-ERROS
                        ####################################################################################################
                        ###############################     TRATAMENTO COLUNA NOME      ####################################



                        if not nome:
                            print("A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(linha))
                            continue



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
                            print("Erro! Há valores não numericos na coluna -ten_forn- da tabela -ucmt_tab-: {}, colocando valor padrão: ten_forn = {} ".format(ten_forn, self.ten_forn_cargas_baixa_codigo))
                            ten_forn = self.ten_forn_cargas_baixa_codigo

                        try:
                            ten_forn = int(ten_forn)
                        except Exception as e:
                            print("Erro ao converter para int a ten_forn da tabela ucmt_tab: {}, colocando valor padrão: ten_forn = {} ".format(e, self.ten_forn_cargas_baixa_codigo))
                            ten_forn = self.ten_forn_cargas_baixa_codigo


                        ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_forn), "não encontrado")

                        if ten_nom_voltage == "não encontrado":
                            print("Erro - Não existe correspondencia de tensão para a coluna "
                                    "ten_forn da tabela ucmt_tab, colocando valor padrão: ten_forn = {} v")
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
                                "Erro! Não há valores na coluna -tip_cc- da tabela -ucmt_tab-, inserindo valores padrão: tip_cc = {}".format(
                                    self.tip_cc_cargas))
                            tip_cc = self.tip_cc_cargas



                        ###############################     TRATAMENTO COLUNA PAC      ####################################



                        if not pac:
                            print(
                                "Erro! Não há valores na coluna -pac- da tabela -ucmt_tab-: {}, eliminando esta linha da modelagem !!!".format(
                                    fas_con))
                            continue



                        ############################################################################################################

                        


                        # Criação ou reutilização de arquivos para cada ctmt
                        if nome not in ctmts_processados:
                            ctmt_folder = os.path.join(base_dir, str(nome))
                            os.makedirs(ctmt_folder, exist_ok=True)
                            file_path = os.path.join(ctmt_folder, 'Cargas_Media_tensao.dss')
                            file = open(file_path, 'w', encoding='utf-8')
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
                        potencias_str = '_'.join([str(round(p, 2)) for p in potencias_carga_anuais])

                        command_transformers = (
                            'New Load.nome_{}_curva_diaria_{}_curva_anual_{}_carga_Media Bus1 = {}{} Phases = {}\n'.format(
                                cod_id, tip_cc, potencias_str, pac, rec_fases, len(fases)
                            ) +
                            '~ Conn = {} Model = 1 Kv = {} Kw = 1 Kvar = 0\n\n'.format(conn, ten_nom_voltage / 1000)
                        )


                        # Escrever no arquivo correspondente
                        if file:
                            file.write(command_transformers)

                        processed_rows += 1
                        progress.advance(task)

                # Atualizar offset para próxima consulta
                offset += limit

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de Média tensão processadas com sucesso !!!.".format(processed_rows))
        return




    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def GeracaoShape_Geracao_Distribuida_Baixa_tensao(self):
        """ Modelagem do formato da curva de geração da gd de baixa tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}
        ctmt_folder = []
        
        
        
        # Conectando a base de dados da aneel para coletar as potências tanto dos 
        # inversores como do conjunto de módulos, considerando tais dados
        # contendo menor quantidade de erros de cadastro em relação ao BDGD
        self.iniciar_conexao_base_dados_aneel()


        try:
            query = """
            
            SELECT 
                codgeracaodistribuida,
                mdpotenciainstalada,
                mdpotenciaoutorgada
            FROM 
                gd_aneel_2025_base;

                """

            # Executa a consulta
            self.cur.execute(query)
            result1 = self.cur.fetchall()
            #(self.codgeracaodistribuida, self.mdpotenciainstalada, self.mdpotenciaoutorgada) = map(list, zip(*self.cur.fetchall()))

        except Exception as e:
            print("Erro ao Carregar a base de dados da Aneel - GD: {}".format(e))
            return
        

        # Fechando conexão com o banco de dados Anell
        self.fechar_conexao()
        
        # Inciando nova conexão com a BDGD
        self.iniciar_conexao()
        


        try:
            query = """
 
                    SELECT DISTINCT
                                ugbt_tab.cod_id, 
                                ugbt_tab.ceg_gd,
                                ugbt_tab.pac, 
                                ctmt.nome, 
                                ugbt_tab.fas_con,
                                ugbt_tab.ten_con, 
                                ugbt_tab.pot_inst
                        
                    FROM 
                                ugbt_tab
                                
                    JOIN 
                                ctmt on ctmt.cod_id = ugbt_tab.ctmt

                    ORDER BY 
                                ctmt.nome

                          """

            # Executa a consulta
            self.cur.execute(query)
            result2 = self.cur.fetchall()
            
            
            # Coleta dos dados e salvamento em listas
            #(self.cod_id, self.ceg_gd, self.pac, self.nome, self.fas_con,
             #self.ten_con, self.pot_inst) = map(list, zip(*self.cur.fetchall()))

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return
        
        
        # Para acelerar a busca, crie um dicionário para a consulta 1, chave: codgeracaodistribuida
        dict_gd_aneel = {row[0]: {'mdpotenciainstalada': row[1], 'mdpotenciaoutorgada': row[2]} for row in result1}

        # Agora itere a consulta 2, e para cada ceg_gd procure o valor no dict acima
        dados_cruzados = defaultdict(lambda: defaultdict(float))

        
        for row in result2:
            cod_id, ceg_gd, pac, nome, fas_con, ten_con, pot_inst = row

            try:
                dados_cruzados[ceg_gd] = {
                    'cod_id': cod_id,
                    'pac': pac,
                    'nome': nome,
                    'fas_con': fas_con,
                    'ten_con': ten_con,
                    'pot_inst': pot_inst,
                    'mdpotenciainstalada': dict_gd_aneel[ceg_gd]['mdpotenciainstalada'],
                    'mdpotenciaoutorgada': dict_gd_aneel[ceg_gd]['mdpotenciaoutorgada']
                }
            except KeyError:
                # Se a chave não existir em dict_gd_aneel, atribui pot_inst para os campos faltantes
                dados_cruzados[ceg_gd] = {
                    'cod_id': cod_id,
                    'pac': pac,
                    'nome': nome,
                    'fas_con': fas_con,
                    'ten_con': ten_con,
                    'pot_inst': pot_inst,
                    'mdpotenciainstalada': pot_inst,
                    'mdpotenciaoutorgada': pot_inst
                }



        print("Modelando a curva de geração distribuída de baixa tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            transient=True,  # limpa a barra depois que termina
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(dados_cruzados))
            


            arquivos_abertos = {}
            ctmts_processados = set()  # usar set para evitar duplicidade
            curvas_ja_escritas = set()  # fora do loop, antes da iteração principal

            for index, (ceg_gd, dados) in enumerate(dados_cruzados.items()):
                cod_id = dados['cod_id']
                pac = dados['pac']
                nome = dados['nome']
                fas_con = dados['fas_con']
                ten_con = dados['ten_con']
                potencia_instalada_kwp = float(dados['mdpotenciainstalada'])
                potencia_max_inversor_kw = float(dados['mdpotenciaoutorgada']) 
                
                 
                progress.advance(task)





                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ###############################     TRATAMENTO COLUNA NOME      ####################################



                if not nome:
                    print(
                        "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(index))
                    continue

    

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





                if ceg_gd.startswith(('GD', 'UFV')):
                    # Criar pasta e abrir arquivo se ainda não feito
                    if nome not in arquivos_abertos:
                        ctmt_folder = os.path.join(base_dir, str(nome))
                        os.makedirs(ctmt_folder, exist_ok=True)

                        file_path = os.path.join(ctmt_folder, 'GD_FV_baixa_tensao.dss')
                        try:
                            arquivos_abertos[nome] = open(file_path, 'a')
                        except Exception as e:
                            print(f"Erro ao abrir o arquivo para {nome}: {e}")
                            continue

                    file = arquivos_abertos[nome]

                    # Escreve as curvas apenas uma vez por nome
                    if nome not in curvas_ja_escritas:
                        file.write(
                            f"New xycurve.mypvst_{nome} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                            f"New xycurve.myeff_{nome} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                            f"New loadshape.myirrad_{nome} npts = 1 interval = 1 mult = [1]\n"
                            f"New tshape.mytemp_{nome} npts = 1 interval = 1 temp = [25]\n\n"
                        )
                        curvas_ja_escritas.add(nome)

                    rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
                    if ".4" not in rec_fases:
                        rec_fases += ".4"

                    try:
                        potencia_instalada_kwp = float(potencia_instalada_kwp)
                    except (ValueError, TypeError):
                        print(f"Valor não numérico para potencia_instalada_kwp: {potencia_instalada_kwp}. Usando 1.")
                        potencia_instalada_kwp = 1

                    try:
                        potencia_max_inversor_kw = float(potencia_max_inversor_kw)
                    except (ValueError, TypeError):
                        print(f"Valor não numérico para potencia_max_inversor_kw: {potencia_max_inversor_kw}. Usando 1.")
                        potencia_max_inversor_kw = 1

                    command_pvsystem = (
                        f"New pvsystem.pv_{cod_id} phases = {len(fas_con)} conn = wye bus1 = {pac + rec_fases}\n"
                        f"~ kv = {float(ten_nom_voltage) / 1000} kva = {potencia_max_inversor_kw} pmpp = {potencia_instalada_kwp}\n"
                        f"~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{nome}\n"
                        f"~ p-tcurve = mypvst_{nome} daily = myirrad_{nome} tdaily = mytemp_{nome}\n\n"
                        f"New load.{cod_id}_carga_no_pv bus1 = {pac + rec_fases} phases = {len(fas_con)}\n"
                        f"~ conn = wye model = 1 kv = {float(ten_nom_voltage) / 1000} kw = 0.0001\n\n"
                    )

                    file.write(command_pvsystem)


            # Fecha todos os arquivos abertos
            for f in arquivos_abertos.values():
                f.close()

        print("Modelagem: {} GD de baixa tensão processadas com sucesso !!!".format(len(dados_cruzados)))
        return


    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33






    def GeracaoShape_Geracao_Distribuida_Media_tensao(self):
        """ Modelagem do formato da curva de geração da gd da média tensão """

        base_dir = caminho_modelagens
        ctmts_processados = {}
        ctmt_folder = []
        
        
        
        # Conectando a base de dados da aneel para coletar as potências tanto dos 
        # inversores como do conjunto de módulos, considerando tais dados
        # contendo menor quantidade de erros de cadastro em relação ao BDGD
        self.iniciar_conexao_base_dados_aneel()


        try:
            query = """
            
            SELECT 
                codgeracaodistribuida,
                mdpotenciainstalada,
                mdpotenciaoutorgada
            FROM 
                gd_aneel_2025_base;

                """

            # Executa a consulta
            self.cur.execute(query)
            result1 = self.cur.fetchall()
            #(self.codgeracaodistribuida, self.mdpotenciainstalada, self.mdpotenciaoutorgada) = map(list, zip(*self.cur.fetchall()))

        except Exception as e:
            print("Erro ao Carregar a base de dados da Aneel - GD: {}".format(e))
            return
        

        # Fechando conexão com o banco de dados Anell
        self.fechar_conexao()
        
        # Inciando nova conexão com a BDGD
        self.iniciar_conexao()
        


        try:
            query = """
 
                    SELECT DISTINCT
                                ugmt_tab.cod_id, 
                                ugmt_tab.ceg_gd,
                                ugmt_tab.pac, 
                                ctmt.nome, 
                                ugmt_tab.fas_con,
                                ugmt_tab.ten_con, 
                                ugmt_tab.pot_inst
                        
                    FROM 
                                ugmt_tab
                                
                    JOIN 
                                ctmt on ctmt.cod_id = ugmt_tab.ctmt

                    ORDER BY 
                                ctmt.nome

                          """

            # Executa a consulta
            self.cur.execute(query)
            result2 = self.cur.fetchall()
            
            
            # Coleta dos dados e salvamento em listas
            #(self.cod_id, self.ceg_gd, self.pac, self.nome, self.fas_con,
             #self.ten_con, self.pot_inst) = map(list, zip(*self.cur.fetchall()))

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return
        
        
        # Para acelerar a busca, crie um dicionário para a consulta 1, chave: codgeracaodistribuida
        dict_gd_aneel = {row[0]: {'mdpotenciainstalada': row[1], 'mdpotenciaoutorgada': row[2]} for row in result1}

        # Agora itere a consulta 2, e para cada ceg_gd procure o valor no dict acima
        dados_cruzados = defaultdict(lambda: defaultdict(float))

        
        for row in result2:
            cod_id, ceg_gd, pac, nome, fas_con, ten_con, pot_inst = row

            try:
                dados_cruzados[ceg_gd] = {
                    'cod_id': cod_id,
                    'pac': pac,
                    'nome': nome,
                    'fas_con': fas_con,
                    'ten_con': ten_con,
                    'pot_inst': pot_inst,
                    'mdpotenciainstalada': dict_gd_aneel[ceg_gd]['mdpotenciainstalada'],
                    'mdpotenciaoutorgada': dict_gd_aneel[ceg_gd]['mdpotenciaoutorgada']
                }
            except KeyError:
                # Se a chave não existir em dict_gd_aneel, atribui pot_inst para os campos faltantes
                dados_cruzados[ceg_gd] = {
                    'cod_id': cod_id,
                    'pac': pac,
                    'nome': nome,
                    'fas_con': fas_con,
                    'ten_con': ten_con,
                    'pot_inst': pot_inst,
                    'mdpotenciainstalada': pot_inst,
                    'mdpotenciaoutorgada': pot_inst
                }



        print("Modelando a curva de geração distribuída de baixa tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            transient=True,  # limpa a barra depois que termina
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(dados_cruzados))
            


            arquivos_abertos = {}
            ctmts_processados = set()  # usar set para evitar duplicidade
            curvas_ja_escritas = set()
            for index, (ceg_gd, dados) in enumerate(dados_cruzados.items()):
                cod_id = dados['cod_id']
                pac = dados['pac']
                nome = dados['nome']
                fas_con = dados['fas_con']
                ten_con = dados['ten_con']
                potencia_instalada_kwp = float(dados['mdpotenciainstalada'])
                potencia_max_inversor_kw = float(dados['mdpotenciaoutorgada']) 
                
                 
                progress.advance(task)





                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ###############################     TRATAMENTO COLUNA NOME      ####################################



                if not nome:
                    print(
                        "A coluna nome da tabela ctmt está vazia, eliminado esta linha da modelagem !!!: {}".format(index))
                    continue

    

                ###############################     TRATAMENTO COLUNA FAS_CON      #################################



                if fas_con.isalpha():
                    pass
                else:
                    print(
                        "Erro! Há caracteres diferentes de letras na coluna -fas_con- da tabela -ugmt_tab-: {}, colocando valor padrão: fas_con = ABC".format(
                            fas_con))
                    fas_con = 'ABCN'



                ###############################     TRATAMENTO COLUNA TEN_FORN      ####################################



                if ten_con.isdigit():
                    pass
                else:
                    print(
                        "Erro! Há valores não numericos na coluna -ten_con- da tabela -ugmt_tab-: {}, colocando valor padrão: ten_con = {}".format(
                            ten_con, self.ten_con_gd_media_codigo))
                    ten_con = self.ten_con_gd_media_codigo

                try:
                    ten_con = int(ten_con)
                except Exception as e:
                    print(
                        "Erro ao converter para int a ten_con da tabela ugmt_tab: {}, colocando valor padrão: ten_con = {}".format(
                            e, self.ten_con_gd_media_codigo))
                    ten_con = self.ten_con_gd_media_codigo

                ten_nom_voltage = self.dados_tabela_tensoes.get(int(ten_con), "não encontrado")

                if ten_nom_voltage == "não encontrado":
                    print("Erro - Não existe correspondencia de tensão para a coluna "
                        "ten_con da tabela ugmt_tab, colocando valor padrão: ten_con = {} v".format(self.ten_con_gd_media_volts))
                    ten_nom_voltage = self.ten_con_gd_media_volts



                ###############################     TRATAMENTO COLUNA COD_ID      ####################################

                if not cod_id:
                    print(
                        "Erro! Não há valores na coluna -cod_id- da tabela -ugmt_tab-: {}, inserindo valor padrão: {}_{}".format(
                            cod_id, nome, index))
                    cod_id = str(nome) + str(index)


                ###############################     TRATAMENTO COLUNA PAC      ####################################



                if not pac:
                    print(
                        "Erro! Não há valores na coluna -pac- da tabela -ugmt_tab-: {}, eliminando esta linha da modelagem !!!".format(
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




                if ceg_gd.startswith(('GD', 'UFV')):
                    # Criar pasta e abrir arquivo se ainda não feito
                    if nome not in arquivos_abertos:
                        ctmt_folder = os.path.join(base_dir, str(nome))
                        os.makedirs(ctmt_folder, exist_ok=True)

                        file_path = os.path.join(ctmt_folder, 'GD_FV_media_tensao.dss')
                        try:
                            arquivos_abertos[nome] = open(file_path, 'a')
                        except Exception as e:
                            print(f"Erro ao abrir o arquivo para {nome}: {e}")
                            continue

                    file = arquivos_abertos[nome]

                    # Escreve as curvas apenas uma vez por nome
                    if nome not in curvas_ja_escritas:
                        file.write(
                            f"New xycurve.mypvst_{nome} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                            f"New xycurve.myeff_{nome} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                            f"New loadshape.myirrad_{nome} npts = 1 interval = 1 mult = [1]\n"
                            f"New tshape.mytemp_{nome} npts = 1 interval = 1 temp = [25]\n\n"
                        )
                        curvas_ja_escritas.add(nome)

                    rec_fases = self.mapa_fases.get(fas_con, 'ABCN')
                    if ".4" not in rec_fases:
                        rec_fases += ".4"

                    try:
                        potencia_instalada_kwp = float(potencia_instalada_kwp)
                    except (ValueError, TypeError):
                        print(f"Valor não numérico para potencia_instalada_kwp: {potencia_instalada_kwp}. Usando 1.")
                        potencia_instalada_kwp = 1

                    try:
                        potencia_max_inversor_kw = float(potencia_max_inversor_kw)
                    except (ValueError, TypeError):
                        print(f"Valor não numérico para potencia_max_inversor_kw: {potencia_max_inversor_kw}. Usando 1.")
                        potencia_max_inversor_kw = 1

                    command_pvsystem = (
                        f"New pvsystem.pv_{cod_id} phases = {len(fas_con)} conn = wye bus1 = {pac + rec_fases}\n"
                        f"~ kv = {float(ten_nom_voltage) / 1000} kva = {potencia_max_inversor_kw} pmpp = {potencia_instalada_kwp}\n"
                        f"~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{nome}\n"
                        f"~ p-tcurve = mypvst_{nome} daily = myirrad_{nome} tdaily = mytemp_{nome}\n\n"
                        f"New load.{cod_id}_carga_no_pv bus1 = {pac + rec_fases} phases = {len(fas_con)}\n"
                        f"~ conn = wye model = 1 kv = {float(ten_nom_voltage) / 1000} kw = 0.0001\n\n"
                    )

                    file.write(command_pvsystem)

            # Fecha todos os arquivos abertos
            for f in arquivos_abertos.values():
                f.close()

        print("Modelagem: {} GD de média tensão processadas com sucesso !!!".format(len(dados_cruzados)))
        return


    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    def Cargas_Poste_Iluminacao_Publica_Baixa_Tensao(self):
        """ Modelagem das Cargas de postes de iluminação pública """

        offset = 0
        limit = 100000
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
                         
                          ORDER BY 
                                        ctmt.nome

                          LIMIT {} OFFSET {};

                      """.format(limit, offset)

                self.cur.execute(query)
                rows = self.cur.fetchall()

                if not rows:
                    break  # Encerra se não houver mais dados


                print("Modelando as cargas de postes de iluminação pública...")
                with Progress(
                    TextColumn("[bold green]Carregando..."),
                    BarColumn(bar_width=60, style="green"),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeRemainingColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Modelando...", total=len(rows))
                    


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
                            file_path = os.path.join(ctmt_folder, 'Pip_load.dss')
                            file = open(file_path, 'w', encoding='utf-8')
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

                                'New Load.{}_pip_load_ Bus1 = {}{} Phases = {}\n'.format(cod_id, pac, rec_fases,
                                                                                        len(fases)) +
                                'Conn = {} Model = 1 Kv = {} Kw = {} Kvar = {}\n\n'.format(conn, ten_nom_voltage / 1000, pot_lamp / 1000, (pot_lamp / 1000) * 0.08)

                        )

                        # Escrever no arquivo correspondente
                        if file:
                            file.write(command_transformers)

                        processed_rows += 1
                        progress.advance(task)

                    # Atualizar offset para próxima consulta
                    offset += limit
                    

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        finally:
            # Fechar todos os arquivos abertos
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Cargas de postes de iluminação pública processadas com sucesso !!!.".format(processed_rows))
        return





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
                            ORDER BY        ctmt.nome

                        """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))

        print("Modelando os ramais de ligação de baixa tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            


            # Iterar sobre os dados e gerar uma subpasta para cada CTMT
            for index, linha in enumerate(results):
                cod_id, pac_1, pac_2, nome, fas_con, comp, tip_cnd = linha

                # VERIFICAÇÃO ANTI-ERROS
                ############################################################################################################
                ################################        TRATAMENTO DA COLUNA NOME       ####################################



                if not nome:
                    print("A coluna nome da tabela ctmt está vazia, logo será excluída da MODELAGEM !!! : {}".format(linha))
                    continue




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
                    file_path = os.path.join(ctmt_folder, 'ramais_ligacao.dss')
                    file = open(file_path, 'w', encoding='utf-8')
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
                    file.write(command_line)
                
                progress.advance(task)

            # Fechar todos os arquivos antes de terminar o loop
            for file in ctmts_processados.values():
                file.close()

        print("Modelagem: {} Ramais de ligação de baixa Tensão gerada com sucesso !!!".format(len(results)))
        return






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


        print("Modelando os transformadores de média tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
                

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
                    file_path = os.path.join(ctmt_folder, 'Transformadores_Media_tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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
                    rec_fases_s = '.1.2.3.4'
                    rec_fases_t = '1.2.3.4'
                    
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
                            '~ wdg=2 bus={}{} conn={} kv={} Kva={} %r={} tap=1\n\n'.format(pac_2, rec_fases_s, conn_s,
                                                                                        ten_secundario / 1000, pot_nom, r)

                    )

                if file:
                        file.write(command_transformers)
                progress.advance(task)

        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: {} Transformadores de Média Tensão gerada com sucesso !!!".format(len(results)))
        return




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
                            
            
                ORDER BY 
                            ctmt.nome; 
          
            """
            self.cur.execute(query)
            results = self.cur.fetchall()
        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return
        
        print("Modelando os reguladores de média tensão...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
                

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
                    file_path = os.path.join(ctmt_folder, 'Reguladores_tensao_Media_Tensao.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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
                    file.write(command_transformers)
                progress.advance(task)

        # Fechar todos os arquivos antes de terminar o loop
        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: {} Reguladores de tensão de Média Tensão gerada com sucesso !!!".format(len(results)))
        return




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


        print("Modelando as tensões base...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(results))
            
                
                
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
                    file_path = os.path.join(ctmt_folder, 'Tensoes_Base.dss')
                    file = open(file_path, 'w', encoding='utf-8')

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

                file_path = os.path.join(ctmt_folder, 'Tensoes_Base.dss')
                with open(file_path, 'w') as file:
                    for tension in  tensao:
                        command_transformers = ('Set VoltageBase = "{}, {}"\n CalcVoltageBase'.format(tension[0], tension[1]))

                        if file:
                            file.write(command_transformers)
                progress.advance(task)

        for nome, file in ctmts_processados.items():
            file.close()

        print("Modelagem: {} Tensões Base gerada com sucesso !!!".format(len(results)))
        return

    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33





    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33




    def curvas_de_carga(self):
        """ Extração das curvas de carga -- 300 bdgd 2023 energisa mt """

        tensoes = {}

        try:
            query = """
            
                    SELECT 
                        crvcrg.cod_id AS crvcrg_cod_id,
                        crvcrg.tip_dia,

                        crvcrg.pot_01, crvcrg.pot_02, crvcrg.pot_03, crvcrg.pot_04,
                        crvcrg.pot_05, crvcrg.pot_06, crvcrg.pot_07, crvcrg.pot_08,
                        crvcrg.pot_09, crvcrg.pot_10, crvcrg.pot_11, crvcrg.pot_12,
                        crvcrg.pot_13, crvcrg.pot_14, crvcrg.pot_15, crvcrg.pot_16,
                        crvcrg.pot_17, crvcrg.pot_18, crvcrg.pot_19, crvcrg.pot_20,
                        crvcrg.pot_21, crvcrg.pot_22, crvcrg.pot_23, crvcrg.pot_24,
                        crvcrg.pot_25, crvcrg.pot_26, crvcrg.pot_27, crvcrg.pot_28,
                        crvcrg.pot_29, crvcrg.pot_30, crvcrg.pot_31, crvcrg.pot_32,
                        crvcrg.pot_33, crvcrg.pot_34, crvcrg.pot_35, crvcrg.pot_36,
                        crvcrg.pot_37, crvcrg.pot_38, crvcrg.pot_39, crvcrg.pot_40,
                        crvcrg.pot_41, crvcrg.pot_42, crvcrg.pot_43, crvcrg.pot_44,
                        crvcrg.pot_45, crvcrg.pot_46, crvcrg.pot_47, crvcrg.pot_48,
                        crvcrg.pot_49, crvcrg.pot_50, crvcrg.pot_51, crvcrg.pot_52,
                        crvcrg.pot_53, crvcrg.pot_54, crvcrg.pot_55, crvcrg.pot_56,
                        crvcrg.pot_57, crvcrg.pot_58, crvcrg.pot_59, crvcrg.pot_60,
                        crvcrg.pot_61, crvcrg.pot_62, crvcrg.pot_63, crvcrg.pot_64,
                        crvcrg.pot_65, crvcrg.pot_66, crvcrg.pot_67, crvcrg.pot_68,
                        crvcrg.pot_69, crvcrg.pot_70, crvcrg.pot_71, crvcrg.pot_72,
                        crvcrg.pot_73, crvcrg.pot_74, crvcrg.pot_75, crvcrg.pot_76,
                        crvcrg.pot_77, crvcrg.pot_78, crvcrg.pot_79, crvcrg.pot_80,
                        crvcrg.pot_81, crvcrg.pot_82, crvcrg.pot_83, crvcrg.pot_84,
                        crvcrg.pot_85, crvcrg.pot_86, crvcrg.pot_87, crvcrg.pot_88,
                        crvcrg.pot_89, crvcrg.pot_90, crvcrg.pot_91, crvcrg.pot_92,
                        crvcrg.pot_93, crvcrg.pot_94, crvcrg.pot_95, crvcrg.pot_96

                    FROM 
                        crvcrg

            
            """
            # Executa a consulta
            self.cur.execute(query)
            results = self.cur.fetchall()

        except Exception as e:
            print("Erro ao gerar comandos para o OpenDSS: {}".format(e))
            return

        base_dir = caminho_modelagens
        ctmts_processados = {}


        print("Extração das curvas de carga...")
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Extraindo...", total=len(results))
            
                
            dic_curvas_ = defaultdict(lambda: defaultdict(float))
            
            # Iterar sobre os dados e gerar uma subpasta para cada ctmt
            for index, linha in enumerate(results):
                crvcrg_cod_id, tip_dia, *potencias = linha
                
                # Normalizando pelo máximo a curva
                pot_max = max(potencias) if max(potencias) != 0 else 1  # evita divisão por zero
                potencias_normalizadas = [p / pot_max for p in potencias]
                
                dic_curvas_[crvcrg_cod_id][tip_dia] = potencias_normalizadas
                
                
            file_path = os.path.join(base_dir, 'curvas_de_carga.txt')
            with open(file_path, 'w', encoding='utf-8') as file:
                for linha in results:
                    crvcrg_cod_id, tip_dia, *potencias = linha

                    pot_max = max(potencias) if max(potencias) != 0 else 1
                    potencias_normalizadas = [p / pot_max for p in potencias]

                    dic_curvas_[crvcrg_cod_id][tip_dia] = potencias_normalizadas

                    file.write(f"{crvcrg_cod_id} {tip_dia}: {' '.join(f'{p:.4f}' for p in potencias_normalizadas)}\n")

        print("Modelagem: {} Curvas de Carga extraídas com sucesso !!!".format(len(results)))
        return

    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33
    #############################################################################################################################################################################################################################33



    def criar_pasta_flag(self):
        base_dir = self.caminho_modelagens
        pasta_flag = os.path.join(base_dir, 'flag')
        if not os.path.exists(pasta_flag):
            os.makedirs(pasta_flag)
            print(f"Pasta criada: {pasta_flag}")
        else:
            print(f"A pasta já existe: {pasta_flag}")
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
    nome_distribuidora = 'BDGD_2023_ENERGISA_MT'
    host = 'localhost'
    port = '5432'
    dbname = 'BDGD_2023_ENERGISA_MT'
    user = 'iuri'
    password = 'aa11bb22'



##############################################################################################################################


    caminho_modelagens = r"C:\TED_MIR\roda_fluxo_potencia\bdgd_distribuidoras_modelados\{}\cenarios_modelados\SIMULACAO_POR_ALIMENTADOR_NORMAL".format(nome_distribuidora)


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


    limite_inferior_pu_alimentador = 0.980
    limite_superior_pu_alimentador = 1.03
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


    r1_linecodes_baixa_ohms_km = 3
    x1_linecodes_baixa_ohms_km = 3
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
    # modela.Barra_Slack()
    # modela.Compensadores_Reativo_Media()
    # modela.Compensadores_Reativo_Baixa()
    # modela.Chaves_Seccionadoras_Baixa_Tensao()
    # modela.Chaves_Seccionadoras_Media_Tensao()
    # modela.Geradores_Media_tensao()
    #modela.Linecodes_Baixa_Tensao()
    #modela.Linecodes_Media_Tensao()
    #modela.Linecodes_Ramais_Baixa_Tensao()
    #modela.Linhas_Baixa_Tensao()
    #modela.Linhas_Media_Tensao()
    modela.Cargas_Baixa_Tensao()
    #modela.Cargas_media_Tensao()
    #modela.Cargas_Poste_Iluminacao_Publica_Baixa_Tensao()
    #modela.Tensoes_Base()
    #modela.Ramais_Ligacao()
    #modela.transformadores_Media_tensao()
    # modela.Reguladores_Media_Tensao()
    # modela.GeracaoShape_Geracao_Distribuida_Baixa_tensao()
    # modela.GeracaoShape_Geracao_Distribuida_Media_tensao()
    #modela.curvas_de_carga()
    #modela.criar_pasta_flag()



    # Fechar a conexão com o banco de dados
    modela.fechar_conexao()


