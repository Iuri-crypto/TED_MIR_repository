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


        # Percorre linha a linha
        # Assume que cargas_baixa_tensao_df já está carregado
        df = cargas_baixa_tensao_df.copy()

        # Obtem os vetores
        ene_alim = df[[f"ene_{i:02d}_alimentador" for i in range(1, 13)]].to_numpy()
        ene_carga = df[[f"ene_{i:02d}_carga" for i in range(1, 13)]].to_numpy()
        pnts = df[[f"pntbt_{i:02d}_alimentador" for i in range(1, 13)]].to_numpy()

        # Aplica load_mult
        def extrai_mult(row):
            sub = row['sub']
            ctmt = row['ctmt']
            try:
                return dic_mult_por_ctmt_load.get(sub, {}).get(ctmt, 1)
            except:
                return 1  # fallback de segurança

        df['load_mult'] = df.apply(extrai_mult, axis=1)

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
            'fas_con', 'conn', 'ten_forn', 'sub'
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
    def ajustando_curvas_de_carga_MT(dic_mult_por_ctmt_load, conn, cargas_media_tensao_df):
 

        dict_curvas_cargas = defaultdict(dict)

        # Percorre linha a linha
        for idx, row in cargas_media_tensao_df.iterrows():
            # Extrai valores da linha
            cod_id = row["cod_id"]
            tip_cc = row["tip_cc"]
            pac = row["pac"]
            nome = row["nome"]
            fas_con = row["fas_con"]
            ten_forn = row["ten_forn"]
            sub = row["sub"]
            ctmt = row["ctmt"]

            # Energias alimentador mensal
            ene_alim = np.array([row[f"ene_{i:02d}_alimentador"] for i in range(1, 13)])
            pnts = np.array([row[f"pntmt_{i:02d}_alimentador"] for i in range(1, 13)])
            ene_carga = np.array([row[f"ene_{i:02d}_carga"] for i in range(1, 13)])

            # Pega o multiplicador por SUB + CTMT
            try:
                load_mult = dic_mult_por_ctmt_load.get(sub, {}).get(ctmt, 1)
            except:
                load_mult = 1  # Fallback

            # Ajusta energia alimentador e normaliza
            curva_anual_energia = ene_alim.copy()
            curva_anual_energia_normalizada = np.where(curva_anual_energia < 1, 1, curva_anual_energia)
            max_val = np.max(curva_anual_energia_normalizada)
            curva_anual_energia_normalizada = np.ones_like(curva_anual_energia_normalizada) if max_val == 0 else curva_anual_energia_normalizada / max_val

            # Percentual de perdas não técnicas
            curva_anual_energia = np.where(curva_anual_energia < 1, 1, curva_anual_energia)
            curva_percentual_pnts_anual = 1 + (pnts / curva_anual_energia)

            # Multiplicadores e potências
            multiplicadores_carga = (curva_percentual_pnts_anual * curva_anual_energia_normalizada).round(3)
            potencias_carga_anuais = (ene_carga / 720) * multiplicadores_carga
            potencias_carga_anuais *= load_mult
            potencias_carga_anuais = potencias_carga_anuais.round(2)

            # Formata string
            potencias_str = " ".join(map(str, potencias_carga_anuais.tolist()))

            # Adiciona ao dicionário
            dict_curvas_cargas[cod_id] = {
                "nome": nome,
                "tip_cc": tip_cc,
                "potencias": potencias_str,
                "pac": pac,
                "rec_fases": None,
                "fases": fas_con,
                "conn": 'wye',
                "ten_con": ten_forn,
                "sub": sub
            }

        # Converte para DataFrame final
        df_curvas_cargas = pd.DataFrame.from_dict(dict_curvas_cargas, orient='index').reset_index()
        df_curvas_cargas.rename(columns={'index': 'cod_id'}, inplace=True)

        return df_curvas_cargas
