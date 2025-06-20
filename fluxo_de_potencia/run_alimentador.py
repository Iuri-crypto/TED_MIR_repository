import sys
import pickle
from fluxo_potencia import class_Fluxo_de_Potencia



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Erro: caminho do arquivo de argumentos não fornecido.")
        sys.exit(1)

    caminho_arquivo = sys.argv[1]

    with open(caminho_arquivo, 'rb') as f:
        args = pickle.load(f)

    class_Fluxo_de_Potencia.processa_alimentador(args)


