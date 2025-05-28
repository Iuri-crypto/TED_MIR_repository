"""
    Autor: Iuri Lorenzo Quirino Moraes Silva
    Data: 2025-05-22
"""

import os
import pandas as pd
import numpy as np

class create_table_class:
    def __init__(self):
        self.coluna = None

        
    def tabela_subestacao(self, descr: str, nome: str, ten_nom: str, ten_ope: str, cod_id:str, potencia_mensal_alim_sub: list, local: str):
        """ Cria tabela automaticamente """
        
       # Nomes dos meses
        meses = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
                'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']


        # Adicionando os dados de potência mensal 
        # convertendo a lista de lista diretamente em umn dataframe
        df_potencias_mensais = pd.DataFrame(
            potencia_mensal_alim_sub,
            columns=meses
        )
        
        # Criando o Dataframe base com as colunas fixas
        # Montandos os dados como colunas
        dados_dict = {
            'NOME': descr,
            'CT COD OP': nome,
            'CTMT': cod_id,
            'kV': (np.array(ten_nom) / 1000).round(3),
            'TENSÃO EM PU': ten_ope
        }
        
        # Convertendo o dicionário em um Dataframe 
        df_dados_fixos = pd.DataFrame(dados_dict)  # Note o colchete, criando uma lista com o dicionário

        
        # Concatenando os dois DataFrames
        df = pd.concat([df_dados_fixos, df_potencias_mensais], axis=1)

        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df.to_excel(os.path.join("outputs", "tabela_subestacao.xlsx"), index=False)
        
        
    def tabela_trafos_alta_tensao(self, untrat_cod_id: str, untrat_pot_nom_Mva: str, eqtrat_lig: str, eqtrat_ten_pri_cod: str, eqtrat_ten_sec_cod: str, cod_id: str):
        """ Cria tabela automaticamente """

        # Criando o Dataframe
        df = pd.DataFrame({'CTMT': cod_id, 'COD_ID': untrat_cod_id, "KV PRI": eqtrat_ten_pri_cod, "KV SEC": eqtrat_ten_sec_cod, "MVA": untrat_pot_nom_Mva, "CONN": eqtrat_lig})
        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df.to_excel(os.path.join("outputs", "tabela_trafos_alta_tensao.xlsx"), index=False)
        

    def tabela_alimentadores(self, ct_cod_op: str, ctmt: str, ten_nom: str):
        """ Cria tabela automaticamente """

        # Criando o Dataframe
        df = pd.DataFrame({'CT_COD_OP': ct_cod_op, 'CTMT': ctmt, "KV": ten_nom})
        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df.to_excel(os.path.join("outputs", "tabela_alimentadores.xlsx"), index=False)



    def tabela_linhas(self, ctmts, urbanos, rurais, totais):
        """ Cria tabela automaticamente """

        # Criando o Dataframe
        df = pd.DataFrame({'CTMT': ctmts, 'URBANO(%)': urbanos, "RURAL(%)": rurais, 'TOTAL(KM)': totais})
        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df.to_excel(os.path.join("outputs", "tabela_dados_de_linha.xlsx"), index=False)




    def tabela_cargas(self, ctmts, MT, COM, RUR, IND, RES, IP, SP, NUMERO_UCs):
        """ Cria tabela automaticamente """

        # Criando o Dataframe
        df = pd.DataFrame({'CTMT': ctmts, 'MT(%)': MT, 'COM (%)': COM, "RUR(%)": RUR, 'IND(%)': IND, 'RES(%)': RES, 'IP(%)': IP, 'SP (%)': SP, 'NUMERO_UC': NUMERO_UCs})
        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df.to_excel(os.path.join("outputs", "tabela_dados_cargas.xlsx"), index=False)


    def tabela_curvas_carga(self, ctmts_carga, kva_min_carga, kva_med_carga, kva_coninci_carga,kva_nao_coin_carga,
                            ctmts_alimentador,kva_min_alimentador,kva_med_alimentador,kva_coin_alimentador,
                            ctmts_curva_final,kva_min_curva_final,kva_med_curva_final,kva_coin_curva_final,kva_nao_coin_curva_final):
        """ Cria tabela automaticamente """


        # Criando o Dataframe
        df1 = pd.DataFrame({'CTMT': ctmts_carga, 'KVA(MIN)': kva_min_carga, 'KVA(MED)': kva_med_carga, "KVA(MAX) COIN": kva_coninci_carga, 'KVA(MAX) NÃO COINC': kva_nao_coin_carga,
                           })
        
        df2 = pd.DataFrame({'CTMT': ctmts_alimentador, 'KVA(MIN)': kva_min_alimentador, 'KVA(MED)': kva_med_alimentador, "KVA(MAX) COIN": kva_coin_alimentador,
                          })
        df3 = pd.DataFrame({'CTMT': ctmts_curva_final, 'KVA(MIN)': kva_min_curva_final, 'KVA(MED)': kva_med_curva_final, "KVA(MAX) COIN": kva_coin_curva_final,
                           "KVA(MAX) NÃO COINC": kva_nao_coin_curva_final,})
        
        # Garante que a pasta 'outputs' existe
        os.makedirs("outputs", exist_ok=True)

        # Salva o arquivo Excel dentro da pasta 'outputs'
        df1.to_excel(os.path.join("outputs", "tabela_curvas_de_carga.xlsx"), index=False)
        df2.to_excel(os.path.join("outputs", "tabela_curvas_dos_alimentadores.xlsx"), index=False)
        df3.to_excel(os.path.join("outputs", "tabela_curvas_finais.xlsx"), index=False)
        


    def juntar_tabelas_por_ctmt(self):
        """ Junta as tabelas com base na coluna CTMT, organizando colunas tematicamente em blocos """

        caminho = "outputs"

        # Lê os arquivos
        tabela_subestacao = pd.read_excel(os.path.join(caminho, "tabela_subestacao.xlsx"))
        tabela_trafos = pd.read_excel(os.path.join(caminho, "tabela_trafos_alta_tensao.xlsx"))
        tabela_alimentadores = pd.read_excel(os.path.join(caminho, "tabela_alimentadores.xlsx"))
        tabela_linhas = pd.read_excel(os.path.join(caminho, "tabela_dados_de_linha.xlsx"))
        tabela_cargas = pd.read_excel(os.path.join(caminho, "tabela_dados_cargas.xlsx"))
        tabela_curvas_carga = pd.read_excel(os.path.join(caminho, "tabela_curvas_de_carga.xlsx"))
        tabela_curvas_alimentadores = pd.read_excel(os.path.join(caminho, "tabela_curvas_dos_alimentadores.xlsx"))
        tabela_curvas_finais = pd.read_excel(os.path.join(caminho, "tabela_curvas_finais.xlsx"))

        # Meses
        meses = ['JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
                'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']

        # Seleciona colunas específicas
        tabela_subestacao = tabela_subestacao[['NOME', 'CT COD OP', 'CTMT', 'kV', 'TENSÃO EM PU'] + meses]
        tabela_trafos = tabela_trafos[['CTMT','COD_ID', 'KV PRI', 'KV SEC', 'MVA', 'CONN']]
        tabela_alimentadores = tabela_alimentadores[['CT_COD_OP', 'CTMT', 'KV']]
        tabela_linhas = tabela_linhas[['CTMT', 'URBANO(%)', 'RURAL(%)', 'TOTAL(KM)']]
        tabela_cargas = tabela_cargas[['CTMT', 'MT(%)', 'COM (%)', 'RUR(%)', 'IND(%)', 'RES(%)', 'IP(%)', 'SP (%)', 'NUMERO_UC']]
        tabela_curvas_carga = tabela_curvas_carga[['CTMT', 'KVA(MIN)', 'KVA(MED)', 'KVA(MAX) COIN', 'KVA(MAX) NÃO COINC']]
        tabela_curvas_alimentadores = tabela_curvas_alimentadores.rename(columns={"CTM": "CTMT"})
        tabela_curvas_alimentadores = tabela_curvas_alimentadores[['CTMT', 'KVA(MIN)', 'KVA(MED)', 'KVA(MAX) COIN']]
        tabela_curvas_alimentadores.columns = ['CTMT', 'KVA(MIN)', 'KVA(MED)', 'KVA(MAX) COIN']
        tabela_curvas_finais = tabela_curvas_finais[['CTMT', 'KVA(MIN)', 'KVA(MED)', 'KVA(MAX) COIN', 'KVA(MAX) NÃO COINC']]

        # Realiza merge sucessivos com base em CTMT
        df_merged = tabela_subestacao.merge(tabela_trafos, how='left', left_on='CTMT', right_on='CTMT')
        df_merged = df_merged.merge(tabela_alimentadores, how='left', on='CTMT', suffixes=('', '_ALIM'))
        df_merged = df_merged.merge(tabela_linhas, how='left', on='CTMT')
        df_merged = df_merged.merge(tabela_cargas, how='left', on='CTMT')
        df_merged = df_merged.merge(tabela_curvas_carga, how='left', on='CTMT', suffixes=('', '_CARGAS'))
        df_merged = df_merged.merge(tabela_curvas_alimentadores, how='left', on='CTMT')
        df_merged = df_merged.merge(tabela_curvas_finais, how='left', on='CTMT')

        # Salva a tabela final
        df_merged.to_excel(os.path.join(caminho, "tabela_final_organizada_por_ctmt.xlsx"), index=False)
        print("Tabela final salva com sucesso em:", os.path.join(caminho, "tabela_final_organizada_por_ctmt.xlsx"))
