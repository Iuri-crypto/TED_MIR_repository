from tabelas import create_table_class
from collections import defaultdict


class levantamento_tecnico:
    """ Analise de dados """
    def __init__(self, np, conn, cur, pd, os, local: str, codigos_tensoes: str, alimentador: str, mes: str, dia: str):  
        
        self.horas_no_mes = 720 # horas no mês
        
        # Local onde os arquivos xlsx serão salvos
        self.local = local
        
        # Local onde está a tabela de conversão de tensões
        self.codigos_tensoes = codigos_tensoes

        self.np = np
        self.conn = conn
        self.cur = cur
        self.pd = pd
        self.os = os
        
        # Atributos para armazenar dados de linhas
        self.resul_ssdbt = None
        self.resul_ssdmt = None
        
        # Atributos para armazenar dados de alimentadores das subestações
        self.descr = []
        self.cod_id = []
        self.nome = []
        self.ten_nom = []
        self.ten_ope = []
        self.potencia_mensal_alim_sub = []

     
        

    def dados_subestacao(self):
        """ Coleta o ct cod op, ctmt e KVA que flui da barra slack da subestação para as cargas """
        
        colunas = ["descr", "sub", "ten_nom", "ten_ope","cod_id"]
        potencia_mensal_subestacao = [f"ene_{str(i).zfill(2)}" for i in range(1, 13)]
        colunas_str = ", ".join(colunas + potencia_mensal_subestacao)
        query = f"SELECT {colunas_str} FROM ctmt"
        
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em listas
        self.descr, self.cod_id_sub, self.ten_nom, self.ten_ope, self.cod_id, *self.potencia_mensal_alim_sub = map(list, zip(*self.cur.fetchall()))

        self.potencia_mensal_alim_sub = ((self.np.array(self.potencia_mensal_alim_sub) / self.horas_no_mes)).round(3).T.tolist()
        
        
        # Covertendo o codigo das tensões para tensões de fato
        # Caminho relativo ao diretório onde está o script Python
        base_dir = self.os.path.dirname(__file__)  # pasta onde está o script .py
        arquivo_excel = self.os.path.join(base_dir, "tabelas_de_consulta", "codigos_tensoes.xlsx")
        
  
        # Lê a planilha
        df = self.pd.read_excel(arquivo_excel)

        # Extrai as colunas como listas
        lista_cod_id = df['COD_ID'].tolist()
        lista_tensao = df['TEN'].tolist()
        
        # Cria um dicionário de mapeamento de COD_ID -> TEN
        map_cod_tensao = dict(zip(lista_cod_id, lista_tensao))
        
        # Converte self.ten_nom em uma Series
        self.ten_nom = list(map(int, self.ten_nom))
        ten_nom_series = self.pd.Series(self.ten_nom)
        
        # Mapeia diretamente as tensões correspondentes
        tensoes_equivalentes = ten_nom_series.map(map_cod_tensao).tolist()
        self.ten_nom = tensoes_equivalentes

        # Criando a tabela com os dados coletados 
        tabela = create_table_class()
        tabela.tabela_subestacao(self.descr, self.cod_id_sub, self.ten_nom, self.ten_ope, self.cod_id, self.potencia_mensal_alim_sub, self.local)

        
        
    def dados_trafos_alta_tensao(self):
        """ Coleta os dados das colunas:  cod_id, ten_pri, ten_sec, conn """
        
        query = """
                SELECT 
                    untrat.cod_id, untrat.pot_nom, eqtrat.lig, eqtrat.ten_pri, eqtrat.ten_sec, ctmt.cod_id
                FROM 
                    ctmt
                LEFT JOIN 
                    untrat ON ctmt.uni_tr_at = untrat.cod_id
                LEFT JOIN 
                    eqtrat ON untrat.cod_id = eqtrat.uni_tr_at; 
                """
            
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em listas
        self.untrat_cod_id, self.untrat_pot_nom_Mva, self.eqtrat_lig, self.eqtrat_ten_pri_cod, self.eqtrat_ten_sec_cod, self.cod_id = map(list, zip(*self.cur.fetchall()))
        
        # convetertendo a lista para Series
        self.eqtrat_ten_pri_cod = self.pd.Series(self.eqtrat_ten_pri_cod)
        self.eqtrat_ten_sec_cod = self.pd.Series(self.eqtrat_ten_sec_cod)
        
        # Retirando temporariamentos os valores nulos
        mascara_nula_pri = self.eqtrat_ten_pri_cod.isnull() | (self.eqtrat_ten_pri_cod == ' ') | (self.eqtrat_ten_pri_cod == 'NaN')
        mascara_nula_sec = self.eqtrat_ten_sec_cod.isnull() | (self.eqtrat_ten_sec_cod == ' ') | (self.eqtrat_ten_sec_cod == 'NaN')
        
        # Gurda os índices e valores nulos
        indices_nulos_pri = self.eqtrat_ten_pri_cod[mascara_nula_pri].index.tolist()
        valores_nulos_pri = self.eqtrat_ten_pri_cod[mascara_nula_pri].tolist()
        indices_nulos_sec = self.eqtrat_ten_sec_cod[mascara_nula_sec].index.tolist()
        valores_nulos_sec = self.eqtrat_ten_sec_cod[mascara_nula_sec].tolist()
        
        # Caminho relativo ao diretório onde está o script python
        base_dir = self.os.path.dirname(__file__)
        arquivo_excel = self.os.path.join(base_dir, "tabelas_de_consulta", "codigos_tensoes.xlsx")
        arquivo_cod_trafos = self.os.path.join(base_dir, "tabelas_de_consulta", "codigos_conexoes_trafos.xlsx")
        
        # Lê a planilha
        df = self.pd.read_excel(arquivo_excel)
        df_cod_trafos = self.pd.read_excel(arquivo_cod_trafos)  
        
        # Extrai as colunas como listas
        lista_cod_id = df['COD_ID'].tolist()
        lista_tensao = df['TEN'].tolist()
        
        lista_cod_trafos = df_cod_trafos['COD_ID'].tolist()
        lista_cod_trafos = list(map(str, lista_cod_trafos))
        lista_descricao_conexoes = df_cod_trafos['DESCRICAO'].tolist()
        
        # Cria um dicionário de mapeamento de COD_ID -> TEN
        map_cod_tensao = dict(zip(lista_cod_id, lista_tensao))
        map_cod_trafos = dict(zip(lista_cod_trafos, lista_descricao_conexoes))
                
        # removendo os valores nulos
        self.eqtrat_ten_pri_cod = self.eqtrat_ten_pri_cod[~mascara_nula_pri]
        self.eqtrat_ten_sec_cod = self.eqtrat_ten_sec_cod[~mascara_nula_sec]
        
        # Converte os elementos de self.eqtrat_ten_pri_cod que são strings para inteiros  ex: '138' -> 138
        self.eqtrat_ten_pri_cod = list(map(int, self.eqtrat_ten_pri_cod))
        self.eqtrat_ten_sec_cod = list(map(int, self.eqtrat_ten_sec_cod))

        # Converte a lista para pandas series para facilitar o mapeamento
        self.eqtrat_ten_pri_cod_series = self.pd.Series(self.eqtrat_ten_pri_cod)
        self.eqtrat_ten_sec_cod_series = self.pd.Series(self.eqtrat_ten_sec_cod)
        self.eqtrat_lig_series = self.pd.Series(self.eqtrat_lig)

        # Mapeia diretamente as tensões correspondentes
        tensoes_equivalentes = self.eqtrat_ten_pri_cod_series.map(map_cod_tensao).tolist()
        self.eqtrat_ten_pri_cod = self.np.array(tensoes_equivalentes) / 1000
        self.eqtrat_ten_pri_cod = self.eqtrat_ten_pri_cod.tolist()
        
        tensoes_equivalentes = self.eqtrat_ten_sec_cod_series.map(map_cod_tensao).tolist()
        self.eqtrat_ten_sec_cod = self.np.array(tensoes_equivalentes) / 1000
        self.eqtrat_ten_sec_cod = self.eqtrat_ten_sec_cod.tolist()
        
        # Mapeiando para os trafos também
        self.eqtrat_lig = self.eqtrat_lig_series.map(map_cod_trafos).tolist()
        
        
        # Adicionando os valores nulos nas posições que estavam
        # converte para Series
        list(map(lambda iv: self.eqtrat_ten_pri_cod.insert(*iv), reversed(list(zip(indices_nulos_pri, valores_nulos_pri)))))
        list(map(lambda iv: self.eqtrat_ten_sec_cod.insert(*iv), reversed(list(zip(indices_nulos_sec, valores_nulos_sec)))))
     
        # Criando a tabela com os dados coletados 
        tabela = create_table_class()
        tabela.tabela_trafos_alta_tensao(self.untrat_cod_id, self.untrat_pot_nom_Mva, self.eqtrat_lig, self.eqtrat_ten_pri_cod, self.eqtrat_ten_sec_cod, self.cod_id)


    def dados_alimentadores(self):
        """ Coleta os dados das colunas: ct_cod_op, ctmt e kv """
        
        query = """
                SELECT 
                    cod_id, nome, ten_nom
                FROM 
                    ctmt
                """
            
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em listas
        self.ctmt, self.ct_cod_op, self.ten_nom = map(list, zip(*self.cur.fetchall()))
        
        # Carregando tabela de conversão de codigos para tensões
        base_dir = self.os.path.dirname(__file__)
        arquivo_excel = self.os.path.join(base_dir, "tabelas_de_consulta", "codigos_tensoes.xlsx")
        
        # Lê a planilha
        df = self.pd.read_excel(arquivo_excel)
        # Extrai as colunas como listas
        lista_cod_id = df['COD_ID'].tolist()
        lista_tensao = df['TEN'].tolist()
        # Cria um dicionário de mapeamento de COD_ID -> TEN
        map_cod_tensao = dict(zip(lista_cod_id, lista_tensao))
        # Converte self.ten_nom em uma series
        self.ten_nom = list(map(int, self.ten_nom))
        self.ten_nom = self.pd.Series(self.ten_nom)
        
        # Mapeia diretamente as tensões correspondentes
        self.ten_nom = self.ten_nom.map(map_cod_tensao).tolist()
        self.ten_nom = self.np.array(self.ten_nom) / 1000
        self.ten_nom = self.ten_nom.tolist()
        
        # Criando a tabela com os dados coletados 
        tabela = create_table_class()
        tabela.tabela_alimentadores(self.ct_cod_op, self.ctmt, self.ten_nom)
        
    def dados_linhas(self):
        """ Carrega os dados das linhas de média e baixa tensão 
        nas colunas: ctmt, tip_inst, comp"""
        
        query = """
                SELECT 
                    ssdmt.ctmt,
                    ssdmt.tip_inst,
                    ssdmt.comp
                FROM 
                    ctmt
                JOIN 
                    ssdmt ON ssdmt.ctmt = ctmt.cod_id
                ORDER BY 
                    ARRAY_POSITION(ARRAY(
                        SELECT cod_id FROM ctmt
                    ), ctmt.cod_id)
                """
        
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em listas
        self.ctmt_media, self.tip_inst_media, self.comp_media = map(list, zip(*self.cur.fetchall()))


        # Dicionário aninhado com soma acumulada
        nested_sum_dict = defaultdict(lambda: defaultdict(float))

        # Iteração direta pelas três listas ao mesmo tempo
        for ctmt, tip_inst, comp in zip(self.ctmt_media, self.tip_inst_media, self.comp_media):
            nested_sum_dict[ctmt][tip_inst] += comp


        query = """
                SELECT 
                    ssdbt.ctmt,
                    ssdbt.tip_inst,
                    ssdbt.comp
                FROM 
                    ctmt
                JOIN 
                    ssdbt ON ssdbt.ctmt = ctmt.cod_id
                ORDER BY 
                    ARRAY_POSITION(ARRAY(
                        SELECT cod_id FROM ctmt
                    ), ctmt.cod_id)
                """
        
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em listas
        self.ctmt_baixa, self.tip_inst_baixa, self.comp_baixa = map(list, zip(*self.cur.fetchall()))

        # Iteração direta pelas três listas ao mesmo tempo
        for ctmt, tip_inst, comp in zip(self.ctmt_baixa, self.tip_inst_baixa, self.comp_baixa):
            nested_sum_dict[ctmt][tip_inst] += comp

        # Mapeia os códigos para urbano/rural
        def classificar_area(tip_inst):
            if 'RUR' in tip_inst:
                return 'RURAL'
            elif 'URB' in tip_inst:
                return 'URBANO'
            else:
                return 'OUTRO'  # Para casos inesperados (opcional)

        # Iteração e agrupamento por URBANO/RURAL
        # 3. Novo dicionário com a reclassificação das subchaves
        # Novo dicionário com a reclassificação por área
        agrupado_dict = defaultdict(lambda: defaultdict(float))
        for ctmt, internas in nested_sum_dict.items():
            for tip_inst, comp in internas.items():
                area = classificar_area(tip_inst)
                agrupado_dict[ctmt][area] += comp

        # 3. Calcula total e substitui valores por percentuais
        for ctmt, internas in agrupado_dict.items():
            total_km = sum(internas.values())
            internas['TOTAL_KM'] = total_km
            for area in ['URBANO', 'RURAL', 'OUTRO']:
                if area in internas:
                    internas[area] = round((internas[area] / total_km) * 100, 2) if total_km > 0 else 0.0

        # Converte o dicionário aninhado para DataFrame
        df_resultado = self.pd.DataFrame.from_dict(agrupado_dict, orient='index')

        # Garante que todas as colunas desejadas existam
        for col in ['URBANO', 'RURAL', 'TOTAL_KM']:
            if col not in df_resultado.columns:
                df_resultado[col] = ''

        # Renomeia colunas
        df_resultado = df_resultado.rename(columns={
            'URBANO': 'URBANO (%)',
            'RURAL': 'RURAL (%)',
            'TOTAL_KM': 'TOTAL (KM)'
        })

        # Reseta o índice e define o nome da coluna de CTMT
        df_resultado = df_resultado.reset_index().rename(columns={'index': 'CTMT'})

        # Substitui NaN por string vazia
        df_resultado = df_resultado.fillna('0.0')

        # Extraindo as listas
        ctmts = df_resultado['CTMT'].tolist()
        urbanos = df_resultado['URBANO (%)'].tolist()
        rurais = df_resultado['RURAL (%)'].tolist()
        totais = df_resultado['TOTAL (KM)'].tolist()

        # Criando a tabela com os dados coletados
        tabela = create_table_class()
        tabela.tabela_linhas(ctmts, urbanos, rurais, totais)

    def dados_cargas(self):
        """ Coleta as quantidades de cargas residencias, comerciais, industriais, rurais e de média tensão """
        
        query = """ SELECT ctmt, tip_cc FROM ucbt_tab """
        self.cur.execute(query)
        # Alocando os dados em listas
        self.ctmt_cargas, self.tip_cc = map(list, zip(*self.cur.fetchall()))
        # Criando um dicionário para armazenar as cargas por CTMT
        self.cargas_classificacao = defaultdict(lambda: defaultdict(float))
        # Iterando sobre as listas para preencher o dicionário
        for ctmt, tip_cc in zip(self.ctmt_cargas, self.tip_cc):
            self.cargas_classificacao[ctmt][tip_cc] += 1
            
            
        query = """ SELECT ctmt, tip_cc FROM ucmt_tab """
        self.cur.execute(query)
        # Alocando os dados em listas
        self.ctmt_cargas_media, self.tip_cc_media = map(list, zip(*self.cur.fetchall()))
        
        for ctmt, tip_cc in zip(self.ctmt_cargas_media, self.tip_cc_media):
            self.cargas_classificacao[ctmt][tip_cc] += 1
            
            
       
        
        # 3. Calcula total e substitui valores por percentuais
        for ctmt, tip_cc in self.cargas_classificacao.items():
            total_ucs = sum(tip_cc.values())
            tip_cc['NUMERO_UCs'] = total_ucs

            keys_list = list(tip_cc.keys())

            for area in ['MT', 'COM', 'RUR', 'RES', 'IND', 'IP', 'SP']:
                for key in keys_list:
                    if key == 'NUMERO_UCs':
                        continue

                    # Quebra a chave por hífen e underline
                    partes = key.upper().replace('-', '_').split('_')

                    # Verifica se a área está em alguma das partes
                    if area in partes:
                        tip_cc[area] += round((tip_cc[key] / total_ucs) * 100, 2) if total_ucs > 0 else 0.0


         # Converte o dicionário aninhado para DataFrame
        df_resultado = self.pd.DataFrame.from_dict(self.cargas_classificacao, orient='index')

        # Garante que todas as colunas desejadas existam
        for col in ['MT', 'COM', 'RUR', 'RES', 'IND', 'IP', 'SP', 'NUMERO_UCs']:
            if col not in df_resultado.columns:
                df_resultado[col] = ''

        # Renomeia colunas
        df_resultado = df_resultado.rename(columns={
            'MT': 'MT (%)',
            'COM': 'COM (%)',
            'RUR': 'RUR (%)',
            'IND': 'IND (%)',
            'RES': 'RES (%)',
            'IP': 'IP (%)',
            'SP': 'SP (%)',
            'NUMERO_UCs': 'NUMERO_UCs (Nº)',
        })

        # Reseta o índice e define o nome da coluna de CTMT
        df_resultado = df_resultado.reset_index().rename(columns={'index': 'CTMT'})

        # Substitui NaN por string vazia
        df_resultado = df_resultado.fillna('0.0')

        # Extraindo as listas
        ctmts = df_resultado['CTMT'].tolist()
        MT = df_resultado['MT (%)'].tolist()
        COM = df_resultado['COM (%)'].tolist()
        RUR = df_resultado['RUR (%)'].tolist()
        IND = df_resultado['IND (%)'].tolist()
        RES = df_resultado['RES (%)'].tolist()
        IP = df_resultado['IP (%)'].tolist()
        SP = df_resultado['SP (%)'].tolist()
        NUMERO_UCs = df_resultado['NUMERO_UCs (Nº)'].tolist()


        # Criando a tabela com os dados coletados
        tabela = create_table_class()
        tabela.tabela_cargas(ctmts, MT, COM, RUR, IND, RES, IP, SP, NUMERO_UCs)
        
    def ajustar_tabelas(self):
        tabela = create_table_class()
        tabela.juntar_tabelas_por_ctmt()

        
    
