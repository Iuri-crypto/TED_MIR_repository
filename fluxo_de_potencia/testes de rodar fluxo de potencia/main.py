#=====================================================================================================================
#                                   SIMULADOR DE FLUXO DE POTÊNCIA - OPENDSS
#=====================================================================================================================

# Bibliotecas
import os
import py_dss_interface
dss = py_dss_interface.DSS()

# Módulos internos
from class_modela import Modela_Cenarios
from envia_modelagem_opendss import send_models_to_opendss

#=====================================================================================================================
#                                   CONFIGURAÇÃO - MODO DE SIMULAÇÃO
#=====================================================================================================================
# Modos de simulação: Snapshot (instante), Daily (dia inteiro), Yearly (ano inteiro)
# Para Snapshot e Daily, defina mês e dia: 
# Mês: JANEIRO a DEZEMBRO | Dia: DU (útil), DO (domingo), SA (sábado)
# Padrão: Daily, Janeiro, DU
snapshtot = True
daily = False
mensal = False
yearly = False

#=====================================================================================================================
#                                   CONFIGURAÇÃO - MODELO DE CARGA E GERAÇÃO
#=====================================================================================================================
# Modelos de carga: S (potência), Z (impedância), I (corrente)
# Geração distribuída como carga (True) ou PVSystem (False)
model_load_S = True
model_load_Z = False
model_load_I = False
GD_model_like_Load = True

#=====================================================================================================================
#                                   ELEMENTOS A SEREM SIMULADOS
#=====================================================================================================================
# Selecione os elementos da rede a serem considerados na simulação
Load_low_voltage = True
Load_high_voltage = False
GD_low_voltage = False
GD_high_voltage = True
PCHS = False
#=====================================================================================================================

#                                   ESCOLHA DO CENÁRIO
#=====================================================================================================================
# Selecione o cenário a ser simulado
# Cenário 1: Carga média, cargas baixa e fluxo de potência por alimentador
# Cenário 2: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por alimentador
# Cenário 3: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por subestação
# Cenário 4: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por subestação considerando o sistema de subtransmissão

cenario_1 = True
cenario_2 = False
cenario_3 = False
cenario_4 = False


#                                  ESCOLHA COM BASE NO CENÁRIO ESCOLHIDO:
# SE CENÁRIO 1 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UM ALIMENTADOR OU TODOS OS ALIMENTADORES
# SE CENÁRIO 2 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UM ALIMENTADOR OU TODOS OS ALIMENTADORES
# SE CENÁRIO 3 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UMA OU TODAS AS SUBESTAÇÕES
# SE CENÁRIO 4 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UMA OU MAIS REGIÕES 


cenario_1_alimentador = False
cenario_2_alimentador = False
cenario_3_subestacao = False
cenario_4_regiao = False


mod = Modela_Cenarios(cenario_1, cenario_2, cenario_3, cenario_4, cenario_1_alimentador,
                      cenario_2_alimentador, cenario_3_subestacao, cenario_4_regiao)
mod.modelar
#=====================================================================================================================


#                                   Arquivos de sáida e gráficos - (escolha o que vai ser plotada/gerado)
#=====================================================================================================================
# com base no período de simulação será gerado os seguintes arquivos:

# gerar_1: gerar relatório das tensões das cargas com mapa das regiões
# gerar_2: gerar relatório das correntes dos transformadores de média tensão e alta tensão e com mapa das regiões
# gerar_3: gerar relatório das correntes dos condutores de média tensão e baixa tensão e com mapa das regiões
# gerar_4: gerar relatório da potência da subestação ao longo do periodo simulado e com mapa das regiões
# gerar_5: gerar relatório do consumo de energia pelas cargas de média e de baixa tensão e com mapa das regiões
# gerar_6: gerar relatório dos elementos que tiveram seus limites termicos violados e com mapa das regiões
# gerar_7: gerar relatório de geração de energia pelas fontes de geração distribuída e pchs e com mapa das regiões

# plot_1: gerar plot das tensões das cargas com mapa das regiões
# plot_2: gerar plot das correntes dos transformadores de média tensão e alta tensão e com mapa das regiões
# plot_3: gerar plot das correntes dos condutores de média tensão e baixa tensão e com mapa das regiões
# plot_4: gerar plot da potência da subestação ao longo do periodo simulado e com mapa das regiões
# plot_5: gerar plot do consumo de energia pelas cargas de média e de baixa tensão e com mapa das regiões
# plot_6: gerar plot dos elementos que tiveram seus limites termicos violados e com mapa das regiões
# plot_7: gerar plot de geração de energia pelas fontes de geração distribuída e pchs e com mapa das regiões


gerar_1 = True
gerar_2 = False
gerar_3 = False
gerar_4 = False
gerar_5 = False
gerar_6 = False
gerar_7 = False
plot_1 = True
plot_2 = False
plot_3 = False
plot_4 = False
plot_5 = False
plot_6 = False
plot_7 = False
#=====================================================================================================================



dss.text('Clear')
dss.text('Set DefaultBaseFrequency=60')


# Modela cenários


# Enviando ao OpenDSS as modelagens
caminho = r"C:\TED_MIR\roda_fluxo_potencia\bdgd_distribuidoras_modelados\bdgd_2023_energisa_mt\cenarios_modelados"
alimentador = "010010"
send_opendss = send_models_to_opendss(dss, os, caminho, alimentador)
send_opendss.compile()

dss.solution.max_iterations = 100
dss.solution.max_control_iterations = 100
dss.text("New Energymeter.M1  Line.010010010010  1")


dss.solution.solve()
print("Iterações necessárias: {}".format(dss.solution.iterations))
print("Potência total subestação: {}".format(dss.circuit.total_power))


v = sorted(dss.circuit.buses_vmag_pu)

#dss.text('show isolated')
#dss.text('show powers')

print("fim")

