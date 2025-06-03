import pandas as pd
import numpy as np
from collections import defaultdict
from bdgd_to_opendss.modelar.curvas_de_carga import loashape_class



class BTLoadCurveProcessor:
    @staticmethod
    def ajustando_curvas_de_carga_BT(conn, conn_aneel, cargas_baixa_tensao_df, ugbt_ugmt_bdgd_df, ugbt_ugmt_aneel_df):

        # Supondo que aqui você já fez o merge para criar df_combinado e pegou o dicionário de multiplicadores:
        df_combinado = pd.merge(ugbt_ugmt_bdgd_df, ugbt_ugmt_aneel_df,
                                left_on='ceg_gd', right_on='codgeracaodistribuida', how='left')

        # Exemplo: pegar os multiplicadores para cada ctmt
        dic_mult_por_ctmt_load = loashape_class(conn, conn_aneel, df_combinado).curvas()

        # Inicializa dicionário que vai armazenar resultados
        dict_curvas_cargas = defaultdict(lambda: defaultdict(float))


        # Define os nomes das colunas para iterar pelas linhas
        colunas = ["cod_id", "tip_cc", "pac", "nome",
                   "ene_01_alimentador", "ene_02_alimentador", "ene_03_alimentador", "ene_04_alimentador",
                   "ene_05_alimentador", "ene_06_alimentador", "ene_07_alimentador", "ene_08_alimentador",
                   "ene_09_alimentador", "ene_10_alimentador", "ene_11_alimentador", "ene_12_alimentador",
                   "pntbt_01_alimentador", "pntbt_02_alimentador", "pntbt_03_alimentador", "pntbt_04_alimentador",
                   "pntbt_05_alimentador", "pntbt_06_alimentador", "pntbt_07_alimentador", "pntbt_08_alimentador",
                   "pntbt_09_alimentador", "pntbt_10_alimentador", "pntbt_11_alimentador", "pntbt_12_alimentador",
                   "fas_con", "ten_forn",
                   "ene_01_carga", "ene_02_carga", "ene_03_carga", "ene_04_carga",
                   "ene_05_carga", "ene_06_carga", "ene_07_carga", "ene_08_carga",
                   "ene_09_carga", "ene_10_carga", "ene_11_carga", "ene_12_carga",
                   "ctmt"
                   ]


        # Percorre linha a linha
        # Assume que cargas_baixa_tensao_df já está carregado
        df = cargas_baixa_tensao_df.copy()

        # Obtem os vetores
        ene_alim = df[[f"ene_{i:02d}_alimentador" for i in range(1, 13)]].to_numpy()
        ene_carga = df[[f"ene_{i:02d}_carga" for i in range(1, 13)]].to_numpy()
        pnts = df[[f"pntbt_{i:02d}_alimentador" for i in range(1, 13)]].to_numpy()

        # Aplica load_mult
        df['load_mult'] = df['ctmt'].map(dic_mult_por_ctmt_load).fillna(1)

        # Normaliza energia do alimentador
        curva_anual_energia = np.where(ene_alim < 1, 1, ene_alim)
        max_energia = np.max(curva_anual_energia, axis=1).reshape(-1, 1)
        curva_anual_energia_normalizada = np.where(
            max_energia == 0, 1, curva_anual_energia / max_energia
        )

        # Percentual de perdas não técnicas
        curva_percentual_pnts_anual = 1 + (pnts / curva_anual_energia)

        # Multiplicador total
        multiplicadores_carga = curva_percentual_pnts_anual * curva_anual_energia_normalizada

        # Potência anual base (kWh / 720h)
        potencias_carga_anuais = ene_carga / 720

        # Aplica multiplicadores
        potencias_carga_anuais = potencias_carga_anuais * multiplicadores_carga

        # Aplica load_mult
        potencias_carga_anuais *= df['load_mult'].values.reshape(-1, 1)
        potencias_carga_anuais = np.round(potencias_carga_anuais, 2)

        # Converte potências para string
        df['potencias'] = [' '.join(map(str, linha)) for linha in potencias_carga_anuais]

        if 'conn' not in df.columns:
            df['conn'] = 'wye'


        # Monta DataFrame final
        df_curvas_cargas = df[[
            'cod_id', 'nome', 'tip_cc', 'pac', 'potencias',
            'fas_con', 'conn', 'ten_forn'
        ]].copy()

        df_curvas_cargas.rename(columns={
            'fas_con': 'fases',
            'ten_forn': 'ten_con'
        }, inplace=True)

        # Adiciona campos faltantes (rec_fases = None)
        df_curvas_cargas['rec_fases'] = None

     
        # Retorna o dicionário com os dados processados
        return df_curvas_cargas, dic_mult_por_ctmt_load


        
class MTLoadCurveProcessor:
    @staticmethod
    def ajustando_curvas_de_carga_MT(dic_mult_por_ctmt_load, conn, cargas_media_tensao_df,):
        dict_curvas_cargas = defaultdict(lambda: defaultdict(float))

        # Define os nomes das colunas para iterar pelas linhas
        colunas = ["cod_id", "tip_cc", "pac", "nome",
                   "ene_01_alimentador", "ene_02_alimentador", "ene_03_alimentador", "ene_04_alimentador",
                   "ene_05_alimentador", "ene_06_alimentador", "ene_07_alimentador", "ene_08_alimentador",
                   "ene_09_alimentador", "ene_10_alimentador", "ene_11_alimentador", "ene_12_alimentador",
                   "pntbt_01_alimentador", "pntbt_02_alimentador", "pntbt_03_alimentador", "pntbt_04_alimentador",
                   "pntbt_05_alimentador", "pntbt_06_alimentador", "pntbt_07_alimentador", "pntbt_08_alimentador",
                   "pntbt_09_alimentador", "pntbt_10_alimentador", "pntbt_11_alimentador", "pntbt_12_alimentador",
                   "fas_con", "ten_forn",
                   "ene_01_carga", "ene_02_carga", "ene_03_carga", "ene_04_carga",
                   "ene_05_carga", "ene_06_carga", "ene_07_carga", "ene_08_carga",
                   "ene_09_carga", "ene_10_carga", "ene_11_carga", "ene_12_carga",
                   "ctmt"
                   ]

        # Percorre linha a linha
        for idx, row in cargas_media_tensao_df.iterrows():
            # Extrai valores da linha
            cod_id = row["cod_id"]
            tip_cc = row["tip_cc"]
            pac = row["pac"]
            nome = row["nome"]

            # Energias alimentador mensal
            ene_alim = np.array([
                row["ene_01_alimentador"], row["ene_02_alimentador"], row["ene_03_alimentador"], row["ene_04_alimentador"],
                row["ene_05_alimentador"], row["ene_06_alimentador"], row["ene_07_alimentador"], row["ene_08_alimentador"],
                row["ene_09_alimentador"], row["ene_10_alimentador"], row["ene_11_alimentador"], row["ene_12_alimentador"]
            ])

            # Pontos de perdas não técnicas mensais
            pnts = np.array([
                row["pntmt_01_alimentador"], row["pntmt_02_alimentador"], row["pntmt_03_alimentador"], row["pntmt_04_alimentador"],
                row["pntmt_05_alimentador"], row["pntmt_06_alimentador"], row["pntmt_07_alimentador"], row["pntmt_08_alimentador"],
                row["pntmt_09_alimentador"], row["pntmt_10_alimentador"], row["pntmt_11_alimentador"], row["pntmt_12_alimentador"]
            ])

            fas_con = row["fas_con"]
            ten_forn = row["ten_forn"]

            # Energias da carga mensal
            ene_carga = np.array([
                row["ene_01_carga"], row["ene_02_carga"], row["ene_03_carga"], row["ene_04_carga"],
                row["ene_05_carga"], row["ene_06_carga"], row["ene_07_carga"], row["ene_08_carga"],
                row["ene_09_carga"], row["ene_10_carga"], row["ene_11_carga"], row["ene_12_carga"]
            ])

            ctmt = row["ctmt"]

            # Pega o multiplicador para o ctmt, padrão 1 se não existir
            load_mult = dic_mult_por_ctmt_load.get(ctmt, 1)

            # Ajusta energia alimentador, evita zeros, normaliza etc conforme sua lógica
            curva_anual_energia = ene_alim.copy()

            if np.all(curva_anual_energia == 0):
                curva_anual_energia_normalizada = np.ones_like(curva_anual_energia)
            else:
                curva_anual_energia_normalizada = curva_anual_energia.copy()
                curva_anual_energia_normalizada[curva_anual_energia_normalizada < 1] = 1
                max_val = np.max(curva_anual_energia_normalizada)
                curva_anual_energia_normalizada = curva_anual_energia_normalizada / max_val

            # Calcula percentual perdas não técnicas
            curva_anual_energia = np.where(curva_anual_energia < 1, 1, curva_anual_energia)
            curva_percentual_pnts_anual = pnts / curva_anual_energia
            curva_percentual_pnts_anual = 1 + curva_percentual_pnts_anual

            # Multiplicador total da potência mensal
            multiplicadores_carga = (curva_percentual_pnts_anual * curva_anual_energia_normalizada).round(3)

            # Potências da carga mensal (energia / 720h)
            potencias_carga_anuais = ene_carga / 720

            # Multiplica pela curva para ajustar
            potencias_carga_anuais = (potencias_carga_anuais * multiplicadores_carga).round(3)

            # Multiplica pelo multiplicador do ctmt
            potencias_carga_anuais = (potencias_carga_anuais * load_mult).round(2)

            # Converte para lista e depois string separada por espaço
            potencias_carga_anuais_list = potencias_carga_anuais.tolist()
            potencias_str = " ".join(map(str, potencias_carga_anuais_list))

            # Suposições para rec_fases e fases (precisa adaptar ao seu contexto)
            rec_fases = None  # ou defina baseado em outras colunas/ lógica
            fases = None


            # Guarda no dicionário
            dict_curvas_cargas[cod_id] = {
                "nome": nome,
                "tip_cc": tip_cc,
                "potencias": potencias_str,
                "pac": pac,
                "rec_fases": rec_fases,
                "fases": fas_con,
                "conn": 'wye',
                "ten_con": ten_forn
            }
            df_curvas_cargas = pd.DataFrame.from_dict(dict_curvas_cargas, orient='index').reset_index()
            df_curvas_cargas.rename(columns={'index': 'cod_id'}, inplace=True)

        # Retorna o dicionário com os dados processados
        return df_curvas_cargas
