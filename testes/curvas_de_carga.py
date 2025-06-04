from collections import defaultdict
import numpy as np
import pandas as pd
import psycopg2





class loashape_class:
    """ Analise de dados """
    def __init__(self):
        
        self.conn = psycopg2.connect(dbname='BDGD_2023_ENERGISA_MT',user='iuri',password='aa11bb22',host='localhost',port='5432')
        self.cur = self.conn.cursor()
        
        self.todas_cargas = None
        self.ctmt = '764444'
        self.ct_cod_op = '010009'
        self.mes = 10
        self.resul_ssdbt = None
        self.resul_ssdmt = None
        self.resul_ucmt_tab = None
        self.resul_ucbt_tab = None
        self.resul_crvcrg = None
        self.resul_ctmt = None
        self.curvas_somadas = {}
        self.curva_alimentador_dic = {}
        self.np = np
        self.pd = pd
        
        self.curva_alim_temp = list()


        tempo = np.arange(0, 24, 0.25)

        # Cria uma curva com dois picos (manhã e noite)
        curva_substituta = (
            0.15 +  # Consumo mínimo
            0.4 * np.exp(-((tempo - 7)**2) / 4) +   # Pico da manhã (~7h)
            0.7 * np.exp(-((tempo - 19)**2) / 3)    # Pico da noite (~19h)
        )

        # Normaliza entre 0 e 1
        curva_substituta = (curva_substituta - curva_substituta.min()) / (curva_substituta.max() - curva_substituta.min())
        self.curva_temp_substituta = curva_substituta  





    def carrega_dados_alimentadores(self):
        """ Coleta os dados dos alimentadores """

        dados_1 = ["cod_id", "nome", "sub", "ten_nom"]
        dados_2 = [f"ene_{str(i).zfill(2)}" for i in range(1, 13)]
        colunas_str = ", ".join(dados_1 + dados_2)

        query = f"SELECT {colunas_str} FROM ctmt"
        self.cur.execute(query)
        self.resul_ctmt = self.cur.fetchall()
        return self.resul_ctmt

    


    def curva_alimentador(self):
        """ Curva de carga do alimentador """


        self.curva_alim_temp_extracao = defaultdict(lambda: defaultdict(float))


        for linha in self.resul_ctmt:
            ctmt = linha[0]
            nome = linha[1]
            sub = linha[2]
            ten_nom = linha[3]
            kwh_interesse = linha[4 + self.mes - 1]
     
            energia_diaria = round(kwh_interesse / 30, 3)

            if ctmt not in self.curva_alimentador_dic: self.curva_alimentador_dic[ctmt] = {}
            if nome not in self.curva_alimentador_dic[ctmt]: self.curva_alimentador_dic[ctmt][nome] = 0.0

            self.curva_alimentador_dic[ctmt][nome] = energia_diaria


        # Normalizando a curva resultantes das cargas
        curva_temp = {}
        for ctmt, curva in self.curvas_somadas.items():
            max_valor = max(curva)
            if max_valor != 0: curva_temp[ctmt] = self.np.round(self.np.array(curva) / max_valor, 3)
            else: curva_temp[ctmt] = self.np.zeros(len(curva))


        # Agora construindo a curva do alimentador baseado na curva normalizada das cargas
        #self.ctmt_substituto = '4311221'
        self.curva_alim_temp = defaultdict(lambda: defaultdict(float))
        for ctmt, sub_dict in self.curva_alimentador_dic.items():
            for nome, potencia in sub_dict.items():
                
                
                # Alimentador tem medição de energia 
                if round(potencia, 3) != 0.000:
                    
                    # E Alimentador tem cargas conectadas diretamente 
                    if not curva_temp.get(ctmt) is None: 
                        
                          # Somando os pontos da curva
                        soma_energia_dia = sum(curva_temp[self.ctmt])
                        
                        # calculando o parametro multilicador da curva
                        multiplicador = 4 * potencia / soma_energia_dia
                        self.curva_alim_temp[nome][ctmt] = multiplicador * self.np.array(curva_temp[ctmt])
      
                    else: 
                        # Somando os pontos da curva
                        soma_energia_dia = sum(self.curva_temp_substituta)
                        
                        # calculando o parametro multilicador da curva
                        multiplicador = 4 * potencia / soma_energia_dia
                        self.curva_alim_temp[nome][ctmt] = multiplicador * self.np.array(curva_temp[self.ctmt])
                
                # Alimentador não tem medição de energia
                else:
                    
                    # E Alimentador tem cargas conectadas diretamente 
                    if ctmt in self.curvas_somadas: self.curva_alim_temp[nome][ctmt] = self.curvas_somadas[ctmt]
                        
                    # E Alimentador não tem cargas conectadas diretamente
                    else: self.curva_alim_temp[nome][ctmt] = self.np.zeros(96) 
                    
                    
        # Extraindo max, min e media da curva do alimentador
        for nome, sub in self.curva_alim_temp.items():
            for ctmt, curva in sub.items():
                curva_array = self.np.array(curva)  

                media = self.np.mean(curva_array)
                minimo = self.np.min(curva_array)
                maximo = self.np.max(curva_array)

                self.curva_alim_temp_extracao[ctmt] = {
                    'KVA(MED)': round(media, 2),
                    'KVA(MIN)': round(minimo, 2),
                    'KVA(MAX) COINCIDENTE': round(maximo, 2)
                }
                        
        return self.curva_alim_temp_extracao

      


    def carrega_curvas_de_carga(self):
        """Consulta as curvas de carga da tabela crvcrg"""

        # Monta a lista de colunas
        colunas_potencia = [f"pot_{str(i).zfill(2)}" for i in range(1, 97)]
        colunas = ["cod_id", "tip_dia"] + colunas_potencia + ["descr"]
        colunas_str = ", ".join(colunas)

        # Cria e executa a query
        query = f"SELECT {colunas_str} FROM crvcrg"
        self.cur.execute(query)
        self.resul_crvcrg = self.cur.fetchall()
        return self.resul_crvcrg




    def carrega_dados_cargas(self):
        """ Coleta os dados das cargas da baixa e da média """

        colunas_1 = ["cod_id", "ctmt", "sub", "tip_cc"]
        colunas_2 = [f"ene_{str(i).zfill(2)}" for i in range(1, 13)]
        colunas_str = ", ".join(colunas_1 + colunas_2)

        # Consulta para ucbt_tab
        #query = f"SELECT {colunas_str} FROM ucbt_tab WHERE ctmt = %s"
        #self.cur.execute(query, (self.ctmt,))
        query = f"SELECT {colunas_str} FROM ucbt_tab"
        self.cur.execute(query)
        self.resul_ucbt_tab = self.cur.fetchall()

        # Consulta para ucmt_tab
        #query = f"SELECT {colunas_str} FROM ucmt_tab WHERE ctmt = %s"
        #self.cur.execute(query, (self.ctmt,))
        query = f"SELECT {colunas_str} FROM ucmt_tab"
        self.cur.execute(query)
        self.resul_ucmt_tab = self.cur.fetchall()

        return self.resul_ucmt_tab, self.resul_ucbt_tab



    def normalizar_curvas_de_carga(self):
        """ Normalizar as curvas de carga """
        curvas_normalizadas = []
        for linha in self.resul_crvcrg:
            valores = [float(v) for v in linha[2:98]]
            max_val = max(valores)
            curvas_normalizadas.append((
                linha[0], linha[1], linha[-1],
            [round(v / max_val, 3) for v in valores] if max_val != 0 else [0.0 * 96]))
        self.resul_crvcrg = curvas_normalizadas



    def achar_potencia_instalada_cargas(self):
        """ Acha a potência instaldas das cargas """

        carga_baixa = []
        carga_media = []
        for linha in self.resul_ucbt_tab:

            energias = [float(kwh) for kwh in linha[4:]]
            carga_baixa.append((
                linha[0], linha[1], linha[2],
                linha[3], [round(valor / 720, 3) for valor in energias] if any(valor != 0 for valor in energias)
                else [round(sum(energias) / len(energias) / 720, 3)]
           ))

        for linha in self.resul_ucmt_tab:
            energias = [float(kwh) for kwh in linha[4:]]
            carga_media.append((
                linha[0], linha[1], linha[2],
                linha[3], [round(valor / 720, 3) for valor in energias] if any(valor != 0 for valor in energias)
                else [round(sum(energias) / len(energias) / 720, 3)]
            ))

        self.resul_ucbt_tab = carga_baixa
        self.resul_ucmt_tab = carga_media
        self.todas_cargas =  carga_media + carga_baixa


    def somar_curvas_carga(self):
        """ Soma as curvas de carga da baixa e da media """

        self.cargas_classificacao = defaultdict(lambda: defaultdict(float))
        self.curvas_somadas_classficadas_nao_coincidente = defaultdict(lambda: defaultdict(float))
        self.curvas_somadas_extracao = defaultdict(lambda: defaultdict(float))


        global curva_valores
        for indice, carga in enumerate(self.todas_cargas):
            cod_id = carga[0]
            ctmt = carga[1]
            tip_cc = carga[3]
            if len(carga[4]) > 1: potencia = carga[4][self.mes - 1]
            else: potencia = 0.0

            if ctmt not in self.curvas_somadas: self.curvas_somadas[ctmt] = self.np.zeros(96)


            curva_acumulada = self.np.zeros(96)
            peso_total = 0

            # Encontrando a curva de carga correspondente para cada dia pela média das curvas de carga da semana inteira
            for curva in self.resul_crvcrg:
                if curva[0] == tip_cc:
                    tipo = curva[1]  # ex: 'DU' ou outro
                    peso = 5 if tipo.upper() == 'DU' else 1

                    curva_array = self.np.array(curva[3])
                    curva_acumulada += curva_array * peso
                    peso_total += peso

            if peso_total > 0:
                curva_media = (curva_acumulada / peso_total).tolist()
                self.cargas_classificacao[ctmt][cod_id] = curva_media
                curva_valores = curva_media

            # Multiplica curva normalizada pela potência por ponto
            curva_final = [round(potencia * float(p), 3) for p in curva_valores]
            self.curvas_somadas[ctmt] += self.np.array(curva_final)
            self.curvas_somadas_classficadas_nao_coincidente[ctmt]['KVA(MAX) NÃO COINCIDENTE'] += self.np.max(self.np.array(curva_final))


        # Extraindo max, min e media da curva das cargas
        for ctmt, curva in self.curvas_somadas.items():
            curva_array = self.np.array(curva)  

            media = self.np.mean(curva_array)
            minimo = self.np.min(curva_array)
            maximo = self.np.max(curva_array)

            self.curvas_somadas_extracao[ctmt] = {
                'KVA(MED)': round(media, 2),
                'KVA(MIN)': round(minimo, 2),
                'KVA(MAX) CONINCIDENTE': round(maximo, 2)
            }
            
        print("Curvas de carga somadas !!!")
        return self.curvas_somadas_extracao, self.curvas_somadas_classficadas_nao_coincidente
    
    
    def curva_final(self):  
        """ Faz o calculo da curva final por alimentador baseado na comparação 
        da curva das cargas somadas e a curva do alimentador """
        
        self.curva_final_por_alimentador_dic_extracao = defaultdict(lambda: defaultdict(float))
        self.curva_final_por_alimentador_dic = defaultdict(lambda: defaultdict(float))
        self.curva_final_por_alimentador_dic_temp = defaultdict(float)



        for nome, sub_dict in self.curva_alim_temp.items():
            for ctmt, curva in sub_dict.items():
                
                # Para cada alimentador, da tabela ctmt, verificar / comparar com a curva das cargas somadas
                if ctmt in self.curvas_somadas:
                    # Se a curva do alimentador for maior que a curva das cargas somadas
                    if self.np.max(curva) > self.np.max(self.curvas_somadas[ctmt]):
                        # Se a curva do alimentador for maior que a curva das cargas somadas
                        self.curva_final_por_alimentador_dic[ctmt][nome] = self.np.array(curva)
                        self.curva_final_por_alimentador_dic_temp[ctmt] = 0.0

                        
                    # Se a curva do alimentador for menor que a curva das cargas somadas
                    else:
                        # Se a curva do alimentador for menor que a curva das cargas somadas
                    
                        self.curva_final_por_alimentador_dic_temp[ctmt] = self.curvas_somadas_classficadas_nao_coincidente[ctmt]['KVA(MAX) NÃO COINCIDENTE'] 
                        self.curva_final_por_alimentador_dic[ctmt][nome] = self.np.array(self.curvas_somadas[ctmt])


                
                # Se não tiver nenhuma curv a de carga somadas, então pegar a do alimentador mesmo
                else:
                    self.curva_final_por_alimentador_dic[nome][ctmt] = curva
 
              
        # Extraindo max, min e media da curva final
        for ctmt, sub in self.curva_final_por_alimentador_dic.items():
            for nome, curva in sub.items():
                curva_final = self.np.array(curva)

                media = self.np.mean(curva_final)
                minimo = self.np.min(curva_final)
                maximo = self.np.max(curva_final)
                valor_nao_coincidente = self.curva_final_por_alimentador_dic_temp.get(ctmt, 0.0)
                

                self.curva_final_por_alimentador_dic_extracao[ctmt] = {
                    'KVA(MED)': round(media, 2),
                    'KVA(MIN)': round(minimo, 2),
                    'KVA(MAX) COINCIDENTE': round(maximo, 2),
                    'KVA(MAX) NÃO COINCIDENTE': round(valor_nao_coincidente, 2)
                }
        return self.curva_final_por_alimentador_dic_extracao
    
    
    def perdas_por_alimentador(self):
        """ Este método calcula as perdas percentuais por alimentador """
        
        query = """
                SELECT 
                         ctmt.ene_10 ,
            
                        ctmt.perd_med, ctmt.cod_id
                        
                FROM ctmt
                """
            
        # carregamento dos dados
        self.cur.execute(query)
        
        # Alocando os dados em váriaveis de instância
                  # Alocando os dados em variáveis de instância
        (self.ene_10, self.perd_med, self.cod_id) = map(list, zip(*self.cur.fetchall()))


        self.armazena_perdas_por_alimentador = defaultdict(lambda: defaultdict(float))

        # Função auxiliar para calcular perda percentual com tratamento
        def calcula_perda_percentual(energia, perda, default=0.1):
            if energia is None or energia == 0 or perda is None or perda == 0:
                perdas = default
            else:
                perdas = perda / energia
            return perdas

        # Calculando perdas percentuais para cada mês com tratamento
        # Iteração direta pelas três listas ao mesmo tempo
        for ene_10, perd_med, cod_id in zip(self.ene_10, self.perd_med, self.cod_id):

            # chama a função de calculo percentual de perdas
            self.ene_10_perda_percent = calcula_perda_percentual(ene_10, perd_med)
            self.armazena_perdas_por_alimentador[cod_id] = self.ene_10_perda_percent
        
        return self.armazena_perdas_por_alimentador
        
                        
                         
        
        
    def curvas(self):
        # Carrega as 300  curvas de carga da tabela crvcrg
        self.carrega_curvas_de_carga()
        
        # Normaliza cada uma das 300 curvas de carga pelo seu valor máximo
        self.normalizar_curvas_de_carga()
        
        # Coleta energias mensais dos alimentadores e seu nome
        self.carrega_dados_alimentadores()
        
        # Coleta os consum0 mensais das cargas e os nomes das suas curvas de carga
        self.carrega_dados_cargas()
        
        # Coleta energia consumida no referido mês e divide por 720 horas
        self.achar_potencia_instalada_cargas()
        
        # Soma curvas de carga (MAXC) -> máximo conincidente
        self.curvas_somadas_extracao, self.curvas_somadas_classficadas_nao_coincidente = self.somar_curvas_carga()
        self.curva_alim_temp_extracao = self.curva_alimentador()
        
        
        # Verificando quais alimentadores tem presença de curva e comparar as cruvas e selecionar a maior
        self.curva_final_por_alimentador_dic_extracao = self.curva_final()
        
        # Gerar tabelas de curvas de carga
         # Converte o dicionário aninhado para DataFrame
        curvas_somadas_extracao = self.pd.DataFrame.from_dict(self.curvas_somadas_extracao, orient='index')
        df_curvas_somadas_nao_coincidente = self.pd.DataFrame.from_dict(self.curvas_somadas_classficadas_nao_coincidente, orient='index')
        df_curva_alimentador = self.pd.DataFrame.from_dict(self.curva_alim_temp_extracao, orient='index')
        df_curva_final = self.pd.DataFrame.from_dict(self.curva_final_por_alimentador_dic_extracao, orient='index')

        # Reseta o índice e define o nome da coluna de CTMT
        df_curvas_somadas_extracao = curvas_somadas_extracao.reset_index().rename(columns={'index': 'CTMT'})
        df_curvas_somadas_nao_coincidente = df_curvas_somadas_nao_coincidente.reset_index().rename(columns={'index': 'CTMT'})
        df_curva_alimentador = df_curva_alimentador.reset_index().rename(columns={'index': 'CTMT'})
        df_curva_final = df_curva_final.reset_index().rename(columns={'index': 'CTMT'})


        # Substitui NaN por string vazia
        df_curvas_somadas_extracao = df_curvas_somadas_extracao.fillna('0.0')
        df_curvas_somadas_nao_coincidente = df_curvas_somadas_nao_coincidente.fillna('0.0')
        df_curva_alimentador = df_curva_alimentador.fillna('0.0')
        df_curva_final = df_curva_final.fillna('0.0')


        # Extraindo as listas da coluna cargas do excell
        ctmts_carga = df_curvas_somadas_extracao['CTMT'].tolist()
        kva_coninci_carga = df_curvas_somadas_extracao['KVA(MAX) CONINCIDENTE'].tolist()
 
 
        # Extraindo as listas da coluna alimentador do excell
        ctmts_alimentador = df_curva_alimentador['CTMT'].tolist()
        kva_coin_alimentador = df_curva_alimentador['KVA(MAX) COINCIDENTE'].tolist()
        
        
        # Mapeia os CTMTs para seus valores de carga
        dict_carga = dict(zip(ctmts_carga, kva_coninci_carga))

        # Mapeia os CTMTs para seus valores de alimentador
        dict_alimentador = dict(zip(ctmts_alimentador, kva_coin_alimentador))

        # Obtém a interseção dos CTMTs presentes nas duas listas
        ctmts_comuns = set(dict_carga.keys()) & set(dict_alimentador.keys())

        # Ordena os CTMTs em ordem alfabética ou conforme necessário
        ctmts_ordenados = sorted(ctmts_comuns)

        # Cria a lista final com os valores corretamente alinhados
        ctmt_kvas_zipado = [(ctmt, dict_carga[ctmt], dict_alimentador[ctmt]) for ctmt in ctmts_ordenados]
        
        # Pegando para cada ctmt o valor de dict_alimentador e subtraindo o percentual de perdas daquele alimentador
        # usando o dicionário: self.armazena_perdas_por_alimentador[ctmt]
        
        
        self.armazena_perdas_por_alimentador = self.perdas_por_alimentador() # de 7% a 10% na maioria dos alimentadores
        self.load_mult_por_alimentador = defaultdict(lambda: defaultdict(float))
        for ctmt, kva_carga, kva_alimentador in ctmt_kvas_zipado:
            
            if kva_carga == 0 or kva_carga is None or kva_alimentador == 0 or kva_alimentador is None:
                kva_carga = 100
                kva_alimentador = 100
            perda = self.armazena_perdas_por_alimentador.get(ctmt, 0.1)
            kva_alimentador = kva_alimentador - (kva_alimentador * perda)
            multiplicador = kva_alimentador / kva_carga
            self.load_mult_por_alimentador[ctmt] = multiplicador 

        return self.load_mult_por_alimentador

        
        
        

   




