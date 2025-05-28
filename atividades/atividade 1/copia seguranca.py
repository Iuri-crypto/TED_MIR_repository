import os
import psycopg2
import numpy as np
import matplotlib.pyplot as plt





class Data_Analise:
    """ Analise de dados """
    def __init__(self, alimentador, mes, dia, ponto):
        self.todas_cargas = None
        self.alimentador = alimentador
        self.mes = mes
        self.dia = dia
        self.ponto = ponto
        self.resul_ssdbt = None
        self.resul_ssdmt = None
        self.resul_ucmt_tab = None
        self.resul_ucbt_tab = None
        self.resul_crvcrg = None
        self.resul_ctmt = None
        self.cur = None
        self.conn = None
        self.curvas_somadas = {}
        self.curva_alimentador_dic = {}
        self.cargas_classificacao = {}



    def conecta(self):
        """ Conecta a base de dados """
        self.conn = psycopg2.connect(
            dbname='BDGD_2023_ENERGISA_MT',
            user='iuri',
            password='aa11bb22',
            host='localhost',
            port='5432'
        )
        self.cur = self.conn.cursor()
        print("Conexão Estabelecida com sucesso.")


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

        for linha in self.resul_ctmt:
            ctmt = linha[0]
            nome = linha[1]
            sub = linha[2]
            ten_nom = linha[3]
            kwh_interesse = linha[4 + self.mes - 1]

            potencia_kw = round(kwh_interesse / 720, 3)

            if ctmt not in self.curva_alimentador_dic:
                self.curva_alimentador_dic[ctmt] = {}
            if nome not in self.curva_alimentador_dic[ctmt]:
                self.curva_alimentador_dic[ctmt][nome] = 0.0

            self.curva_alimentador_dic[ctmt][nome] = potencia_kw


        # Normalizando a curva resultantes das cargas
        curva_temp = {}
        for ctmt, curva in self.curvas_somadas.items():
            max_valor = max(curva)
            if max_valor != 0:
                 curva_temp[ctmt] = np.round(np.array(curva) / max_valor, 3)
            else:
                curva_temp[ctmt] = np.zeros(len(curva))


        # Agora construindo a curva do alimentador
        self.curva_alim_temp = {}
        for ctmt, sub_dict in self.curva_alimentador_dic.items():
            for nome, potencia in sub_dict.items():
                if round(potencia, 3) != 0.000:

                    if nome not in self.curva_alim_temp:
                        self.curva_alim_temp[nome] = {}
                    if ctmt not in self.curva_alim_temp[nome]:
                        self.curva_alim_temp[nome][ctmt] = 0.0


                    if not curva_temp.get(ctmt) is None:
                        self.curva_alim_temp[nome][ctmt] = potencia * np.array(curva_temp[ctmt])
                    else:
                        self.curva_alim_temp[nome][ctmt] = potencia * np.array(curva_temp['5647906'])
                else:
                    if nome not in self.curva_alim_temp:
                        self.curva_alim_temp[nome] = {}

                    if ctmt in self.curvas_somadas:
                        self.curva_alim_temp[nome][ctmt] = self.curvas_somadas[ctmt]
                    else:
                        self.curva_alim_temp[nome][ctmt] = np.zeros(96)  # ou outro valor padrão apropriado

        print("terminou")





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
        query = f"SELECT {colunas_str} FROM ucbt_tab WHERE ctmt = %s"
        self.cur.execute(query, (self.alimentador,))
        self.resul_ucbt_tab = self.cur.fetchall()

        # Consulta para ucmt_tab
        query = f"SELECT {colunas_str} FROM ucmt_tab WHERE ctmt = %s"
        self.cur.execute(query, (self.alimentador,))
        self.resul_ucmt_tab = self.cur.fetchall()

        return self.resul_ucmt_tab, self.resul_ucbt_tab




    def carrega_dados_linhas(self):
        """ Carrega linhas de média e linhas de baixa """

        colunas = ["cod_id", "ctmt", "ct_cod_op", "sub", "tip_inst", "comp"]
        colunas_str = ", ".join(colunas)
        query = f"SELECT {colunas_str} FROM ssdbt"
        self.cur.execute(query)
        self.resul_ssdbt = self.cur.fetchall()

        colunas = ["cod_id", "ctmt", "ct_cod_op", "sub", "tip_inst", "comp"]
        colunas_str = ", ".join(colunas)
        query = f"SELECT {colunas_str} FROM ssdmt"
        self.cur.execute(query)
        self.resul_ssdmt = self.cur.fetchall()

        return self.resul_ssdmt, self.resul_ssdbt



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

        global curva_valores
        for indice, carga in enumerate(self.todas_cargas):
            cod_id = carga[0]
            ctmt = carga[1]
            tip_cc = carga[3]
            if len(carga[4]) > 1:
                potencia = carga[4][self.mes - 1]
            else:
                potencia = 0.0

            if ctmt not in self.curvas_somadas:
                self.curvas_somadas[ctmt] = np.zeros(96)

            if ctmt not in self.cargas_classificacao:
                self.cargas_classificacao[ctmt] = {}
            if cod_id not in self.cargas_classificacao[ctmt]:
                self.cargas_classificacao[ctmt][cod_id] = 0

            # Encontrando a curva de carga correspondente
            for curva in self.resul_crvcrg:
                if curva[0] == tip_cc:
                    if curva[1] == self.dia:
                        self.cargas_classificacao[ctmt][cod_id] = curva[2]
                        curva_valores = curva[3]



            # Multiplica curva normalizada pela potência por ponto
            curva_final = [round(potencia * float(p), 3) for p in curva_valores]
            self.curvas_somadas[ctmt] += np.array(curva_final)
        print("Curvas de carga somadas !!!")

    def classificacao_uc(self):
        """ Classificação das Uc's quanto residencial, comercial e induatrial """



    def plotar_curva_carga(self):
        """
        Plota a curva de carga normalizada e ponderada para o alimentador e CTMT informados.
        """

        nome = '003013'
        ctmt = '5647906'

        try:
            curva_ponderada = self.curva_alim_temp.get(nome, {}).get(ctmt, None)

            if curva_ponderada is None:
                print(f"Não foi possível encontrar dados para nome '{nome}' e CTMT '{ctmt}'")
                return

            tempo = np.arange(0, 24, 0.25)  # 96 pontos (15 min)

            # Estilo aprimorado
            #plt.style.use('seaborn-whitegrid')  # estilo limpo com grid

            fig, ax = plt.subplots(figsize=(14, 6))
            ax.plot(tempo, curva_ponderada, label="Curva de carga ponderada", linewidth=2.5, color="#1f77b4")

            # Eixo X com marcação de hora cheia
            ax.set_xticks(np.arange(0, 25, 1))
            ax.set_xlim(0, 24)

            # Rótulos e título
            ax.set_xlabel("Hora do dia", fontsize=12)
            ax.set_ylabel("Potência (kW)", fontsize=12)
            ax.set_title(f"Curva de Carga - Nome: {nome}, CTMT: {ctmt}", fontsize=14, weight='bold')

            # Grade com mais destaque
            ax.grid(True, which='major', linestyle='--', linewidth=0.5, alpha=0.7)
            ax.legend(fontsize=11)
            plt.tight_layout()
            plt.show()

        except Exception as e:
            print(f"Ocorreu um erro ao plotar a curva: {e}")



    def desconecta(self):
        """Fecha a conexão com o banco de dados"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    alimentador = "5647906"
    mes = 10 # maior consumo ?
    dia = 'DO'
    ponto = 1

    analise = Data_Analise(alimentador, mes, dia, ponto)
    analise.conecta()

    analise.carrega_curvas_de_carga()
    analise.normalizar_curvas_de_carga()

    analise.carrega_dados_alimentadores()
    analise.carrega_dados_cargas()
    analise.achar_potencia_instalada_cargas()
    analise.somar_curvas_carga()
    analise.curva_alimentador()

    analise.plotar_curva_carga()

    #analise.carrega_dados_linhas()

    analise.desconecta()