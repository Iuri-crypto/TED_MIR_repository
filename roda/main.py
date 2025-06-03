"""
    Código Principal
"""
# Bibliotecas Externas
#===================================
import os
import py_dss_interface
dss = py_dss_interface.DSS()
#===================================

# Minhas Classes
#====================================
from envia_modelagem_opendss import send_models_to_opendss
#====================================



# Recolhendo o nome de todos os
# Alimentadores em um array

dss.text('Clear')
dss.text('Set DefaultBaseFrequency=60')


# Enviando ao OpenDSS as modelagens
caminho = r"C:\TED_MIR\editando\modela sistema eletrico_zip\modela sistema eletrico\bdgd_to_opendss\cenarios\cenario_1"

alimentador = "001001"
send_opendss = send_models_to_opendss(dss, os, caminho, alimentador)
send_opendss.compile()

dss.solution.max_iterations = 1000
dss.solution.max_control_iterations = 100
#dss.text("New Energymeter.M1  Line.010010010010  1")

#dss.text('show isolated')

dss.solution.solve()
print("Iterações necessárias: {}".format(dss.solution.iterations))
print("Potência total subestação: {}".format(dss.circuit.total_power))


#v = sorted(dss.circuit.buses_vmag_pu)

#dss.text('show isolated')
#dss.text('show powers')

print("fim")

