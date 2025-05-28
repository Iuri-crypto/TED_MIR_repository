"""
    Autor: Iuri Lorenzo Quirino Moraes Silva
    Data: 2025-05-22
"""

import psycopg2
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os



# Minhas Classes
from conexao import connect_class
from curvas_de_carga import loashape_class
from levantamento_tecnico_distribuicao import levantamento_tecnico



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


if __name__ == "__main__":
    
    # Diretórios onde os arquivos serão salvos
    local = r"\outputs"
    
    codigos_tensoes = r"\tabelas_de_consulta\codigos_tensoes.xlsx"
    
    # Conexão com o banco de dados
    conn = connect_class(psycopg2, dbname = 'BDGD_2023_ENERGISA_MT', user = 'iuri', password = 'aa11bb22', host = 'localhost', port = '5432',)
    conn, cur = conn.conecta()
    
    # Extração das curvas de carga, se alimentador igual a True, então carrega todas as curvas
    # Horas correspondentes aos 96 pontos
    tempo = np.arange(0, 24, 0.25)

    # Cria uma curva com dois picos (manhã e noite)
    curva_substituta = (
        0.15 +  # Consumo mínimo
        0.4 * np.exp(-((tempo - 7)**2) / 4) +   # Pico da manhã (~7h)
        0.7 * np.exp(-((tempo - 19)**2) / 3)    # Pico da noite (~19h)
    )

    # Normaliza entre 0 e 1
    curva_substituta = (curva_substituta - curva_substituta.min()) / (curva_substituta.max() - curva_substituta.min())

    #crvcrg = loashape_class(pd, np, conn, cur, curva_substituta, ctmt = "764444",  ct_cod_op = "010009", mes = 11, dia = 'DO')
    #crvcrg.curvas(plotar = True)
    
    
    # Extração de dados técnicos
    dados_tec = levantamento_tecnico(np, conn, cur, pd, os, local, codigos_tensoes, alimentador = "5647906", mes = 10, dia = 'DO')
    #dados_tec.dados_subestacao()
    #dados_tec.dados_trafos_alta_tensao()
    #dados_tec.dados_alimentadores()
    #dados_tec.dados_linhas()
    #dados_tec.dados_cargas()
    dados_tec.ajustar_tabelas()



   


    # Desconexão com o banco de dados
    conn.desconecta()