from fluxo_de_potencia import class_Fluxo_de_Potencia

# ============================
# Simulação de Fluxo de Potência
# ============================

# Configuração dos modos de simulação
modo_snapshot = False
modo_daily = True
modo_yearly = False

# Mês de simulação
mes_simulacao_nome = 'janeiro'
mes_index = {'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,'setembro': 9,
            'outubro': 10, 'novembro': 11, 'dezembro': 12}[mes_simulacao_nome.lower()]

# Modelo de carga (opções: 's_cte'= 1, 'i_cte'= 5, 'z_cte'= 2)
modelo_carga = 1

# Elementos da rede a serem considerados
usar_cargas_bt = True
usar_cargas_mt = True
usar_gd_bt = True
usar_gd_mt = True
usar_geracao_hidraulica = True

# Configurações de saída/relatórios
exibir_tensao = False
exibir_corrente = False
exibir_DEC = True
exibir_FEC = True
monitorar_subestacao = True
gerar_grafico_circuito = True

# ============================
# Execução da Simulação
# ============================

# caminho da modelagem
caminho = r"C:\TED_MIR\repositorios\TED_MIR_repository\fluxo_de_potencia\001001\run_cargas_agregadas.dss"

# Executa o fluxo de potência
class_Fluxo_de_Potencia.config_run(modo_snapshot, modo_daily, modo_yearly, mes_index, modelo_carga, usar_cargas_bt, 
                               usar_cargas_mt, usar_gd_bt, usar_geracao_hidraulica, exibir_tensao, exibir_corrente, 
                               exibir_DEC, exibir_FEC, monitorar_subestacao, gerar_grafico_circuito, caminho)
