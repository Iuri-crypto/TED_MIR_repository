from plot import plot_class
from collections import defaultdict
from tabelas import create_table_class







class loashape_class:
    """ Analise de dados """
    def __init__(self,pd, np, conn, cur, curva_temp_substituta, ctmt, ct_cod_op, mes, dia):
        self.todas_cargas = None
        self.ctmt = ctmt
        self.ct_cod_op = ct_cod_op
        self.mes = mes
        self.dia = dia
        self.resul_ssdbt = None
        self.resul_ssdmt = None
        self.resul_ucmt_tab = None
        self.resul_ucbt_tab = None
        self.resul_crvcrg = None
        self.resul_ctmt = None
        self.curvas_somadas = {}
        self.curva_alimentador_dic = {}
        self.np = np
        self.conn = conn
        self.cur = cur
        self.pd = pd
        
        self.curva_alim_temp = list()
        self.curva_temp_substituta = curva_temp_substituta  # CTMT que será usado como substituto para os alimentadores sem curva de carga





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
        query = f"SELECT {colunas_str} FROM ucmt_tab WHERE ctmt = %s"
        self.cur.execute(query, (self.ctmt,))
        #query = f"SELECT {colunas_str} FROM ucmt_tab"
        #self.cur.execute(query)
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
                        
                         
        
        
    def curvas(self, plotar: bool):
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
        kva_min_carga = df_curvas_somadas_extracao['KVA(MIN)'].tolist()
        kva_med_carga = df_curvas_somadas_extracao['KVA(MED)'].tolist()
        kva_coninci_carga = df_curvas_somadas_extracao['KVA(MAX) CONINCIDENTE'].tolist()
        kva_nao_coin_carga = df_curvas_somadas_nao_coincidente['KVA(MAX) NÃO COINCIDENTE'].tolist()
        
        # Extraindo as listas da coluna alimentador do excell
        ctmts_alimentador = df_curva_alimentador['CTMT'].tolist()
        kva_min_alimentador = df_curva_alimentador['KVA(MIN)'].tolist()
        kva_med_alimentador = df_curva_alimentador['KVA(MED)'].tolist()
        kva_coin_alimentador = df_curva_alimentador['KVA(MAX) COINCIDENTE'].tolist()
        
        # Extraindo as listas da coluna curva de carga final do excell
        ctmts_curva_final = df_curva_final['CTMT'].tolist()
        kva_min_curva_final = df_curva_final['KVA(MIN)'].tolist()
        kva_med_curva_final = df_curva_final['KVA(MED)'].tolist()
        kva_coin_curva_final = df_curva_final['KVA(MAX) COINCIDENTE'].tolist()
        kva_nao_coin_curva_final = df_curva_final['KVA(MAX) NÃO COINCIDENTE'].tolist()

        # Criando a tabela com os dados coletados
        tabela = create_table_class()
        tabela.tabela_curvas_carga(ctmts_carga, kva_min_carga, kva_med_carga, kva_coninci_carga,kva_nao_coin_carga,
                            ctmts_alimentador,kva_min_alimentador,kva_med_alimentador,kva_coin_alimentador,
                            ctmts_curva_final,kva_min_curva_final,kva_med_curva_final,kva_coin_curva_final,kva_nao_coin_curva_final)

        
        if plotar: 
            # instanciando classe
            plotar = plot_class(self.ctmt, self.ct_cod_op, self.curva_alim_temp, self.curvas_somadas)
            plotar.plotar_curvas_carga_comparadas()
  






