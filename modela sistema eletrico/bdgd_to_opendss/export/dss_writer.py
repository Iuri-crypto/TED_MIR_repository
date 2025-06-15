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
               modelated_transformadores_Media_tensao, df_curvas_de_carga):

        todos_dics = [
            ("slack", modelated_slacks),
            ("comp_reativo_media", modelated_compensadores_reativo_media),
            ("comp_reativo_baixa", modelated_compensadores_reativo_baixa),
            ("chaves", modelated_chaves_seccionadoras_media_tensao),
            ("cargas_baixa", modelated_cargas_baixa_tensa),
            ("cargas_media", modelated_cargas_media_tensao),
            ("geradores", modelated_geradores_media_tensao),
            ("linecodes", modelated_linecodes_media_tensao),
            ("gd_baixa", modelated_gd_baixa_tensao),
            ("cargas_pip", modelated_Cargas_PIP),
            ("linhas", modelated_linhas_media_tensao),
            ("gd_media", modelated_gd_media_tensao),
            ("trafos", modelated_transformadores_Media_tensao)
        ]

        # Geração do arquivo de curvas de carga
        curvas_path = os.path.join(self.caminho_saida, "curvas_de_carga.txt")
        if not df_curvas_de_carga.empty:
            os.makedirs(os.path.dirname(curvas_path), exist_ok=True)
            with open(curvas_path, "w", encoding="utf-8") as f_txt:
                for _, linha in df_curvas_de_carga.iterrows():
                    cod = linha["crvcrg_cod_id"]
                    tip_dia = linha["tip_dia"]
                    potencias = [str(linha[f"pot_{i:02d}"]) for i in range(1, 97)]
                    linha_txt = f"{cod}:{tip_dia} = [{', '.join(potencias)}]\n"
                    f_txt.write(linha_txt)

        with Progress(
            TextColumn("[bold cyan]Escrevendo modelagens: [green]{task.fields[barra_nome]}"),
            BarColumn(bar_width=50, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            total_itens = sum(
                max(1, len(nomes_dict))
                for _, dic in todos_dics
                for nomes_dict in dic.values()
            )
            task = progress.add_task("Escrevendo", total=total_itens, barra_nome="")

            for nome_arquivo, dic in todos_dics:
                for sub, nomes_dict in dic.items():
                    if not nomes_dict:
                        nomes_dict = {"default": []}

                    for nome, linhas in nomes_dict.items():
                        sub_ajustado = sub.strip() if str(sub).strip() else "default"
                        nome_ajustado = nome.strip() if str(nome).strip() else "default"

                        pasta_saida = os.path.join(self.caminho_saida, sub_ajustado, nome_ajustado)
                        os.makedirs(pasta_saida, exist_ok=True)  # Garante que a estrutura existe

                        if isinstance(linhas, pd.Series):
                            linhas = linhas.astype(str).tolist()

                        caminho_arquivo = os.path.join(pasta_saida, f"{nome_arquivo}.dss")

                        with open(caminho_arquivo, "w", encoding="utf-8") as f_out:
                            f_out.writelines(l + '\n' for l in linhas)

                        progress.update(task, advance=1, barra_nome=f"{sub_ajustado}/{nome_ajustado}")





class write_cenario_2_subestacoes:
    def __init__(self, caminho):
        self.caminho_saida = caminho

    def to_dss(self, modelated_slacks):
        todos_dics = [("cod_id", modelated_slacks)]

        with Progress(
            TextColumn("[bold cyan]Escrevendo modelagens: [green]{task.fields[barra_nome]}"),
            BarColumn(bar_width=50, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            total_itens = sum(
                max(1, len(nomes_dict))
                for _, dic in todos_dics
                for nomes_dict in dic.values()
            )
            task = progress.add_task("Escrevendo", total=total_itens, barra_nome="")

            for nome_arquivo, dic in todos_dics:
                for sub, linhas in dic.items():  # linhas já é uma lista agora
                    sub_ajustado = sub.strip() if str(sub).strip() else "default"

                    if isinstance(linhas, pd.Series):
                        linhas = linhas.astype(str).tolist()

                    # Caminho base da subestação
                    pasta_base_sub = os.path.join(self.caminho_saida, sub_ajustado)
                    os.makedirs(pasta_base_sub, exist_ok=True)

                    # Pega todas as subpastas dentro da subestação
                    subpastas = [
                        os.path.join(pasta_base_sub, nome_subpasta)
                        for nome_subpasta in os.listdir(pasta_base_sub)
                        if os.path.isdir(os.path.join(pasta_base_sub, nome_subpasta))
                    ]

                    # Se não houver subpastas, salva diretamente em pasta_base_sub
                    if not subpastas:
                        subpastas = [pasta_base_sub]

                    for subpasta in subpastas:
                        caminho_arquivo = os.path.join(subpasta, "sub_coords.dss")
                        with open(caminho_arquivo, "w", encoding="utf-8") as f_out:
                            f_out.writelines(l + '\n' for l in linhas)

                        progress.update(task, advance=1, barra_nome=f"{sub_ajustado}/{os.path.basename(subpasta)}")
