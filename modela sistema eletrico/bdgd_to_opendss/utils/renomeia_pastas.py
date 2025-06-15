import os

def renomear_arquivos_sem_new_line(caminho_base):
    for pasta_principal in os.listdir(caminho_base):
        caminho_pasta_principal = os.path.join(caminho_base, pasta_principal)
        if not os.path.isdir(caminho_pasta_principal):
            continue

        for subpasta in os.listdir(caminho_pasta_principal):
            caminho_subpasta = os.path.join(caminho_pasta_principal, subpasta)
            if not os.path.isdir(caminho_subpasta):
                continue

            caminho_arquivo = os.path.join(caminho_subpasta, "run_cargas_agregadas.dss")
            if not os.path.isfile(caminho_arquivo):
                continue

            try:
                with open(caminho_arquivo, "r", encoding="utf-8") as f:
                    linhas = f.readlines()

                # Verifica se existe pelo menos uma linha que começa com "New Line"
                tem_new_line = any(linha.strip().startswith("New Line") for linha in linhas)

                if not tem_new_line:
                    novo_nome = os.path.join(caminho_subpasta, "dont_run.dss")
                    os.rename(caminho_arquivo, novo_nome)
                    print(f"Arquivo renomeado para dont_run.dss: {novo_nome}")

            except Exception as e:
                print(f"Erro ao processar {caminho_arquivo}: {e}")
