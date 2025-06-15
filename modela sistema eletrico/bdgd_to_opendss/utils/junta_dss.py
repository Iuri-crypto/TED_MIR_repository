import os
import shutil

def processa_subpastas_gerando_run_dss(caminho_base):
    for pasta_principal in os.listdir(caminho_base):
        caminho_pasta_principal = os.path.join(caminho_base, pasta_principal)
        if os.path.isdir(caminho_pasta_principal):
            for subpasta in os.listdir(caminho_pasta_principal):
                caminho_subpasta = os.path.join(caminho_pasta_principal, subpasta)
                if os.path.isdir(caminho_subpasta):
                    arquivos_dss = [arq for arq in os.listdir(caminho_subpasta) if arq.endswith(".dss") and arq != "run.dss"]
                    conteudo_final = []

                    # Parte 1: Adiciona arquivos com prioridade (se existirem)
                    for nome_prioritario in ["slack.dss", "linecodes.dss"]:
                        if nome_prioritario in arquivos_dss:
                            caminho_arquivo = os.path.join(caminho_subpasta, nome_prioritario)
                            with open(caminho_arquivo, "r") as f:
                                conteudo_final.extend(f.readlines())
                            conteudo_final.extend(["\n"] * 3)
                            arquivos_dss.remove(nome_prioritario)

                    # Parte 2: Adiciona os demais arquivos .dss
                    for nome_arquivo in sorted(arquivos_dss):  # ordena para consistência
                        caminho_arquivo = os.path.join(caminho_subpasta, nome_arquivo)
                        with open(caminho_arquivo, "r") as f:
                            conteudo_final.extend(f.readlines())
                        conteudo_final.extend(["\n"] * 2)

                    # Parte 3: Cria run.dss
                    caminho_run = os.path.join(caminho_subpasta, "run.dss")
                    with open(caminho_run, "w") as f:
                        f.writelines(conteudo_final)

                    print(f"✔ run.dss criado em: {caminho_subpasta}")

                    # Parte 4: Agora remove todos os outros arquivos, mantendo apenas run.dss
                    for arquivo in os.listdir(caminho_subpasta):
                        caminho_arquivo = os.path.join(caminho_subpasta, arquivo)
                        if os.path.isfile(caminho_arquivo) and arquivo != "run.dss":
                            os.remove(caminho_arquivo)
                        elif os.path.isdir(caminho_arquivo):
                            shutil.rmtree(caminho_arquivo)
