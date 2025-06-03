import os
import py_dss_interface
import matplotlib.pyplot as plt

from envia_modelagem_opendss import send_models_to_opendss

dss = py_dss_interface.DSS()

script_dir = os.path.dirname(os.path.abspath(__file__))

dss.text('Clear')
dss.text('Set DefaultBaseFrequency=60')

caminho = r"C:\run.dss"
dss.text(f"Compile {caminho}")
dss.solution.max_iterations = 100
dss.solution.max_control_iterations = 1000
dss.solution.solve()

print(f"Total de cargas: {dss.loads.count}")
print("Iterações necessárias: {}".format(dss.solution.iterations))
print("Potência total na subestação: {}".format(dss.circuit.total_power))


#dss.text("show isolated")
# Obtenção das tensões em PU
barras_e_tensoes = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag))




# Fases permitidas: apenas .1, .2 e .3
fases_validas = ('.1', '.2', '.3')

# Filtra: apenas fases válidas E tensão entre 0 e 300 V
barras_filtradas = [
    (nome, valor)
    for nome, valor in barras_e_tensoes
    if nome.endswith(fases_validas) and 0 < valor <= 300
]

# Ordena os resultados por nome (opcional)
barras_filtradas.sort(key=lambda x: x[0])

# Separa nomes e valores para plotagem
_, tensoes = zip(*barras_filtradas) if barras_filtradas else ([], [])

# Plot sem labels
plt.figure(figsize=(12, 6))
plt.plot(tensoes, marker='o', linestyle='-', color='green')
plt.title("Tensões entre 0 e 300 V (.1, .2, .3)")
plt.xlabel("Índice")
plt.ylabel("Tensão (V)")
plt.grid(True)
plt.tight_layout()
plt.show()