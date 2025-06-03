import os
import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console

console = Console()

class write_cenario_1:
    def __init__(self, caminho):
        self.caminho_saida = caminho

    def to_dss(self,
        modelated_slacks, modelated_compensadores_reativo_media, modelated_compensadores_reativo_baixa,
        modelated_chaves_seccionadoras_baixa_tensao, modelated_chaves_seccionadoras_media_tensao,
        modelated_geradores_media_tensao, modelated_inecodes_media_tensao, modelated_linhas_baixa_tensao,
        modelated_linhas_media_tensao, modelated_Ramais_Ligacao, modelated_cargas_baixa_tensa,
        modelated_cargas_media_tensao, modelated_gd_baixa_tensao, modelated_gd_media_tensao,
        modelated_Cargas_PIP, modelated_transformadores_Media_tensao, modelated_Reguladores_Media_Tensao,
        df_curvas_de_carga
    ):
        demais_dicionarios = [
            modelated_compensadores_reativo_media, modelated_compensadores_reativo_baixa,
            modelated_chaves_seccionadoras_baixa_tensao, modelated_chaves_seccionadoras_media_tensao,
            modelated_geradores_media_tensao, modelated_linhas_baixa_tensao, modelated_linhas_media_tensao,
            modelated_Ramais_Ligacao, modelated_cargas_baixa_tensa, modelated_cargas_media_tensao,
            modelated_gd_baixa_tensao, modelated_gd_media_tensao, modelated_Cargas_PIP,
            modelated_transformadores_Media_tensao, modelated_Reguladores_Media_Tensao,
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

                    # 2. Linecodes (inecodes_media_tensao)
                    if nome in modelated_inecodes_media_tensao:
                        f_dss.writelines(modelated_inecodes_media_tensao[nome])

                    # 3. Demais elementos
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
