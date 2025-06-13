import os
import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console

console = Console()

class write_cenario_1:
    def __init__(self, caminho):
        self.caminho_saida = caminho

    def to_dss(self,
       modelated_slacks, modelated_compensadores_reativo_media, 
                                      modelated_chaves_seccionadoras_media_tensao,
                                     modelated_geradores_media_tensao, modelated_linecodes_media_tensao,
                                      
                                     modelated_linhas_media_tensao, 
                                     modelated_gd_media_tensao,
                                     modelated_transformadores_Media_tensao, 
                                     df_curvas_de_carga
    ):
        demais_dicionarios = [modelated_compensadores_reativo_media, 
                                      modelated_chaves_seccionadoras_media_tensao,
                                     modelated_geradores_media_tensao, modelated_linecodes_media_tensao,
                                      
                                     modelated_linhas_media_tensao, 
                                     modelated_gd_media_tensao,
                                     modelated_transformadores_Media_tensao, 
                                     df_curvas_de_carga
        ]

        with Progress(
            TextColumn("[bold cyan]Escrevendo modelagens: [green]{task.fields[barra_nome]}"),
            BarColumn(bar_width=50, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Escrevendo", total=len(modelated_slacks), barra_nome="")

            for nome in modelated_slacks.keys():
                progress.update(task, barra_nome=nome)

                pasta_saida = os.path.join(self.caminho_saida, nome)
                os.makedirs(pasta_saida, exist_ok=True)

                # Arquivo principal .dss
                nome_dss = os.path.join(pasta_saida, "run.dss")
                with open(nome_dss, "w", encoding="utf-8") as f_dss:
                    # 1. Slack
                    if nome in modelated_slacks:
                        f_dss.writelines(modelated_slacks[nome])


                    if nome in modelated_linecodes_media_tensao:
                        f_dss.writelines(modelated_linecodes_media_tensao[nome])

     
                    for d in demais_dicionarios:
                        if nome in d:
                            f_dss.writelines(d[nome])

                # Arquivo de curvas
                if not df_curvas_de_carga.empty:
                    curvas_path = os.path.join(pasta_saida, "curvas_de_carga.txt")
                    with open(curvas_path, "w", encoding="utf-8") as f_txt:
                        for _, linha in df_curvas_de_carga.iterrows():
                            cod = linha["crvcrg_cod_id"]
                            tip_dia = linha["tip_dia"]
                            potencias = [str(linha[f"pot_{i:02d}"]) for i in range(1, 97)]
                            linha_txt = f"{cod}:{tip_dia} = [{', '.join(potencias)}]\n"
                            f_txt.write(linha_txt)

                progress.advance(task)




class write_cenario_2:
    def __init__(self, caminho):
        self.caminho_saida = caminho

    def to_dss(self,
       modelated_slacks, modelated_compensadores_reativo_media, modelated_compensadores_reativo_baixa,
       modelated_chaves_seccionadoras_media_tensao, modelated_cargas_baixa_tensa, modelated_cargas_media_tensao,
       modelated_geradores_media_tensao, modelated_linecodes_media_tensao, modelated_gd_baixa_tensao,
       modelated_Cargas_PIP, modelated_linhas_media_tensao, modelated_gd_media_tensao,
       modelated_transformadores_Media_tensao, df_curvas_de_carga
    ):
        # Lista de dicionários a serem escritos nos arquivos
        demais_dicionarios = [
            modelated_compensadores_reativo_media, modelated_compensadores_reativo_baixa,
            modelated_chaves_seccionadoras_media_tensao, modelated_cargas_baixa_tensa, modelated_cargas_media_tensao,
            modelated_geradores_media_tensao, modelated_linecodes_media_tensao, modelated_gd_baixa_tensao,
            modelated_Cargas_PIP, modelated_linhas_media_tensao, modelated_gd_media_tensao,
            modelated_transformadores_Media_tensao
        ]

        # Definir curvas_path antes do bloco condicional
        curvas_path = os.path.join(self.caminho_saida, "curvas_de_carga.txt")

        # Criar curvas de carga uma única vez na pasta principal
        if not df_curvas_de_carga.empty:
            # caminho da pasta do arquivo
            pasta = os.path.dirname(curvas_path)
            if not os.path.exists(pasta):
                os.makedirs(pasta)
            # curvas_path já está definida aqui
            with open(curvas_path, "w", encoding="utf-8") as f_txt:
                for _, linha in df_curvas_de_carga.iterrows():
                    cod = linha["crvcrg_cod_id"]
                    tip_dia = linha["tip_dia"]
                    potencias = [str(linha[f"pot_{i:02d}"]) for i in range(1, 97)]
                    linha_txt = f"{cod}:{tip_dia} = [{', '.join(potencias)}]\n"
                    f_txt.write(linha_txt)

        # Progresso e escrita dos arquivos individuais
        with Progress(
            TextColumn("[bold cyan]Escrevendo modelagens: [green]{task.fields[barra_nome]}"),
            BarColumn(bar_width=50, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Escrevendo", total=len(modelated_slacks), barra_nome="")

            for nome in modelated_slacks.keys():
                progress.update(task, barra_nome=nome)

                pasta_saida = os.path.join(self.caminho_saida, nome)
                os.makedirs(pasta_saida, exist_ok=True)

                # Arquivo principal .dss
                nome_dss = os.path.join(pasta_saida, "run.dss")
                with open(nome_dss, "w", encoding="utf-8") as f_dss:
                    # 1. Slack
                    if nome in modelated_slacks:
                        f_dss.writelines(modelated_slacks[nome])

                    # 2. Linecodes
                    if nome in modelated_linecodes_media_tensao:
                        f_dss.writelines(modelated_linecodes_media_tensao[nome])

                    # 3. Demais elementos
                    for d in demais_dicionarios:
                        if nome in d:
                            f_dss.writelines(d[nome])

                progress.advance(task)