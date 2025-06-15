import os
import shutil

def elimina_subpastas_sem_linhas_dss(caminho_base):
    """Dentro de cada pasta do caminho base, remove subpastas que não têm o arquivo 'linhas.dss'."""
    for pasta_principal in os.listdir(caminho_base):
        caminho_pasta_principal = os.path.join(caminho_base, pasta_principal)
        if os.path.isdir(caminho_pasta_principal):
            # Agora entra nas subpastas desta pasta
            for subpasta in os.listdir(caminho_pasta_principal):
                caminho_subpasta = os.path.join(caminho_pasta_principal, subpasta)
                if os.path.isdir(caminho_subpasta):
                    # Verifica se existe o arquivo 'linhas.dss' nesta subpasta
                    arquivos = os.listdir(caminho_subpasta)
                    if "linhas.dss" not in arquivos:
                        try:
                            shutil.rmtree(caminho_subpasta)
                            print(f"Subpasta removida: {caminho_subpasta}")
                        except Exception as e:
                            print(f"Erro ao remover {caminho_subpasta}: {e}")
