import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console
import math
from collections import defaultdict
console = Console()


class SlackBus:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("===================================================================================================")
        print("Modelando Barras Slacks...")
        dss_dict = {}
        console = Console()

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_dict = {}
            for _, row in chunk.iterrows():
                nome = row['nome']
                linha = (
                    f"New Object = Circuit.{nome}_Barra_Slack\n"
                    f"~ basekv = {row['ten_nom_voltage'] / 1000} pu = {row['ten_ope']} angle = 0\n"
                    f"New line.{nome}{nome} phases = 3 bus1 = SourceBus bus2 = {row['pac_ini']}.1.2.3 switch = y\n\n"
                )
                chunk_dict.setdefault(nome, []).append(linha)
            return chunk_dict


        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_result = process_chunk(chunk)
                dss_dict.update(chunk_result)
                progress.update(task, advance=len(chunk))

        return dss_dict




class ReactiveCompensatorMT:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Compensadores Reativos MT...")
        dss_dict = {}
        console = Console()

        # Elimina duplicatas com base no 'cod_id', mantendo a primeira ocorrência
        df = df.drop_duplicates(subset="cod_id")

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            basekv = (chunk['ten_nom_voltage'] / 1000).round(3)
            rec_fases = chunk.get('rec_fases', pd.Series('', index=chunk.index)).astype(str)
            phases = chunk.get('phases', pd.Series(3, index=chunk.index))
            pot_nom = chunk['pot_nom']
            pac_1 = chunk['pac_1']
            tip_unid = chunk['tip_unid']
            cod_id = chunk['cod_id']
            nome = chunk['nome']

            for i in chunk.index:
                bus1 = f"{pac_1[i]}{rec_fases[i]}"
                kv = basekv[i]
                kvar = pot_nom[i]
                fases = phases[i]
                
                

                if fases < 2:
                    kv = kv / math.sqrt(3)
                    rec = '.4'

                if tip_unid[i] == 56:
                    linha = (
                        f"New Reactor.{cod_id[i]}_Banco_de_Reator Bus1 = {bus1}{rec} "
                        f"kv = {kv:.3f} kvar = {kvar} phases = {fases} conn = wye\n\n"
                    )
                else:
                    linha = (
                        f"New Capacitor.{cod_id[i]}_Banco_de_Capacitor Bus1 = {bus1} "
                        f"kv = {kv:.3f} kVAR = {kvar} phases = {fases} conn = wye\n\n"
                    )

                dss_key = str(nome[i])
                chunk_result.setdefault(dss_key, []).append(linha)

            return chunk_result

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = process_chunk(chunk)
                for nome, linhas in chunk_dict.items():
                    dss_dict.setdefault(nome, []).extend(linhas)
                progress.update(task, advance=len(chunk))

        return dss_dict



class ReactiveCompensatorBT:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Compensadores Reativos BT...")
        dss_dict = defaultdict(list)
        console = Console()

        # Elimina duplicatas com base no 'cod_id', mantendo a primeira ocorrência
        df = df.drop_duplicates(subset="cod_id")

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            basekv = (chunk['ten_nom_voltage'] / 1000).round(3)
            rec_fases = chunk.get('rec_fases', pd.Series('', index=chunk.index)).astype(str)
            phases = chunk.get('phases', pd.Series(3, index=chunk.index))
            pot_nom = chunk['pot_nom']
            pac_1 = chunk['pac_1']
            tip_unid = chunk['tip_unid']
            cod_id = chunk['cod_id']
            nome = chunk['nome']

            for i in chunk.index:
                bus1 = f"{pac_1[i]}{rec_fases[i]}"
                kv = basekv[i]
                kvar = pot_nom[i]
                fases = phases[i]

                if fases < 2:
                    kv = kv / math.sqrt(3)

                if tip_unid[i] == 56:
                    linha = (
                        f"New Reactor.{cod_id[i]}_Banco_de_Reator Bus1 = {bus1} "
                        f"kv = {kv:.3f} kvar = {kvar} phases = {fases} conn = wye\n\n"
                    )
                else:
                    linha = (
                        f"New Capacitor.{cod_id[i]}_Banco_de_Capacitor Bus1 = {bus1} "
                        f"kv = {kv:.3f} kVAR = {kvar} phases = {fases} conn = wye\n\n"
                    )

                dss_key = str(nome[i])
                chunk_result.setdefault(dss_key, []).append(linha)

            return chunk_result

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = process_chunk(chunk)

                # Acumula linhas para as mesmas chaves
                for chave, linhas in chunk_dict.items():
                    dss_dict[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return dict(dss_dict)

    
class SwitchLowVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Chaves Seccionadoras BT...")
        dss_dict = {}
        console = Console()

        df = df.drop_duplicates(subset="cod_id")

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            cod_id = chunk["cod_id"].astype(str)
            phases = chunk.get("phases", pd.Series(3, index=chunk.index))
            pac_1 = chunk["pac_1"].astype(str)
            pac_2 = chunk["pac_2"].astype(str)
            rec_fases = chunk["rec_fases"].astype(str)
            pn_ope = chunk["p_n_ope"].astype(str)
            nome = chunk["nome"].astype(str)

            for i in chunk.index:
                chave = str(cod_id[i])
                linha = (
                    f"New line.{chave}_Chave_seccionadora_baixa_tensao "
                    f"phases = {phases[i]} "
                    f"bus1 = {pac_1[i]}{rec_fases[i]} "
                    f"bus2 = {pac_2[i]}{rec_fases[i]} "
                    f"switch = {pn_ope[i]}\n\n"
                )
                chunk_result.setdefault(nome[i], []).append(linha)

            return chunk_result

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = process_chunk(chunk)
                for nome, linhas in chunk_dict.items():
                    dss_dict.setdefault(nome, []).extend(linhas)
                progress.update(task, advance=len(chunk))

        return dss_dict


class SwitchMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Chaves Seccionadoras MT...")
        dss_dict = {}
        console = Console()

        df = df.drop_duplicates(subset="cod_id")

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            cod_id = chunk["cod_id"].astype(str)
            phases = chunk.get("phases", pd.Series(3, index=chunk.index))
            pac_1 = chunk["pac_1"].astype(str)
            pac_2 = chunk["pac_2"].astype(str)
            rec_fases = chunk["rec_fases"].astype(str)
            pn_ope = chunk["p_n_ope"].astype(str)
            nome = chunk['nome'].astype(str)

            for i in chunk.index:
                chave = cod_id[i]
                linha = (
                    f"New line.{chave}_Chave_seccionadora_media_tensao "
                    f"phases = {phases[i]} "
                    f"bus1 = {pac_1[i]}{rec_fases[i]} "
                    f"bus2 = {pac_2[i]}{rec_fases[i]} "
                    f"switch = {pn_ope[i]}\n\n"
                )
                chunk_result.setdefault(nome[i], []).append(linha)  # ✅ use chunk_result aqui

            return chunk_result

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = process_chunk(chunk)
                for nome, linhas in chunk_dict.items():
                    dss_dict.setdefault(nome, []).extend(linhas)
                progress.update(task, advance=len(chunk))

        return dss_dict


class GeneratorMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Geradores MT...")
        dss_dict = {}
        console = Console()

        df = df.drop_duplicates(subset="cod_id")

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            rec_fases = chunk["rec_fases"].astype(str)
            rec_fases = rec_fases.apply(lambda x: x if '.4' in x else x + '.4')

            cod_id = chunk["cod_id"].astype(str)
            pot_inst = chunk["pot_inst"].astype(str)
            pac = chunk["pac"].astype(str)
            ten_nom_voltage = (chunk["ten_nom_voltage"] / 1000).astype(str)
            nome = chunk["nome"].astype(str)

            for i in chunk.index:
                chave = cod_id[i]
                linha = (
                    f"New Generator.{chave}_Gerador_Media_tensao Bus1={pac[i]}{rec_fases[i]} Model=1 PF=0.92\n"
                    f"~ kva={pot_inst[i]} KV={ten_nom_voltage[i]} Xdp=0.27 xdpp=0.20 H=2\n"
                    f"~ Conn=wye\n\n"
                )
                chunk_result.setdefault(nome[i], []).append(linha)  # ✅ Correção aqui

            return chunk_result

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = process_chunk(chunk)
                for nome, linhas in chunk_dict.items():
                    dss_dict.setdefault(nome, []).extend(linhas)
                progress.update(task, advance=len(chunk))

        return dss_dict

   
   
   
class LinecodeLowVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linecodes BT...")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Conversões vetoriais
            chunk["r1"] = (chunk["r1"] / 1000).astype(str)
            chunk["x1"] = (chunk["x1"] / 1000).astype(str)
            chunk["cnom"] = chunk["cnom"].astype(str)
            chunk["cmax_renamed"] = chunk["cmax_renamed"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)

            # Monta a string DSS vetorialmente
            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "_linecode_baixa nphases=" + chunk["phases"] + " BaseFreq=60\n" +
                "~ r1=" + chunk["r1"] + "\n" +
                "~ x1=" + chunk["x1"] + "\n" +
                "~ c1=0\n" +
                "~ Normamps = " + chunk["cnom"] + "\n" +
                "~ Emergamps = " + chunk["cmax_renamed"] + "\n\n"
            )

            return chunk.groupby("nome")["linha"].agg(list).to_dict()

        dss_dict = defaultdict(list)
        console = Console()

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for chave, linhas in chunk_dict.items():
                    dss_dict[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return dict(dss_dict)



class LinecodeMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linecodes MT...")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()
            # Conversões vetoriais
            chunk["r1"] = (chunk["r1"] / 1000).astype(str)
            chunk["x1"] = (chunk["x1"] / 1000).astype(str)
            chunk["cnom"] = chunk["cnom"].astype(str)
            chunk["cmax_renamed"] = chunk["cmax_renamed"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)

            # Construção vetorial da string DSS
            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "_linecode_media nphases=" + chunk["phases"] + " BaseFreq=60\n" +
                "~ r1=" + chunk["r1"] + "\n" +
                "~ x1=" + chunk["x1"] + "\n" +
                "~ c1=0\n" +
                "~ Normamps=" + chunk["cnom"] + "\n" +
                "~ Emergamps=" + chunk["cmax_renamed"] + "\n\n"
            )

            return chunk.groupby("nome")["linha"].agg(list).to_dict()

        linhas_dss = defaultdict(list)
        console = Console()

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                # ✅ Soma acumulada por chave
                for chave, linhas in chunk_dict.items():
                    linhas_dss[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return dict(linhas_dss)




class LinecodeRamais:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linecodes Ramais...")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()
            chunk["r1"] = (chunk["r1"] / 1000).astype(str)
            chunk["x1"] = (chunk["x1"] / 1000).astype(str)
            chunk["cnom"] = chunk["cnom"].astype(str)
            chunk["cmax_renamed"] = chunk["cmax_renamed"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)

            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "_linecode_ramais nphases=" + chunk["phases"] + " BaseFreq=60\n" +
                "~ r1=" + chunk["r1"] + "\n" +
                "~ x1=" + chunk["x1"] + "\n" +
                "~ c1=0\n" +
                "~ Normamps=" + chunk["cnom"] + "\n" +
                "~ Emergamps=" + chunk["cmax_renamed"] + "\n\n"
            )

            return chunk.groupby("nome")["linha"].agg(list).to_dict()

        linhas_dss = defaultdict(list)
        console = Console()

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                # ✅ soma corretamente as listas de linhas por chave
                for chave, linhas in chunk_dict.items():
                    linhas_dss[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return dict(linhas_dss)

    
class LineLowVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas BT...")

        df = df.drop_duplicates(subset="cod_id")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            chunk["cod_id"] = chunk["cod_id"].astype(str)
            chunk["pac_1"] = chunk["pac_1"].astype(str)
            chunk["pac_2"] = chunk["pac_2"].astype(str)
            chunk["comp"] = chunk["comp"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["rec_fases"] = chunk["rec_fases"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)

            chunk["linha"] = (
                "New Line." + chunk["cod_id"] + "_linha_baixa " +
                "Phases = " + chunk["phases"] + " " +
                "Bus1 = " + chunk["pac_1"] + chunk["rec_fases"] + " " +
                "Bus2 = " + chunk["pac_2"] + chunk["rec_fases"] + " " +
                "Linecode = " + chunk["tip_cnd"] + "_linecode_baixa " +
                "Length = " + chunk["comp"] + " units = m\n"
            )

            result = {}
            for nome, linhas in chunk.groupby("nome")["linha"]:
                result[nome] = linhas.tolist()

            return result

        linhas_dss = defaultdict(list)
        console = Console()

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df[start:start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for chave, linhas in chunk_dict.items():
                    linhas_dss[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss



class LineMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas MT...")

        # Elimina duplicatas com base no 'cod_id'
        df = df.drop_duplicates(subset="cod_id")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Converte tudo que for necessário para string
            chunk["cod_id"] = chunk["cod_id"].astype(str)
            chunk["pac_1"] = chunk["pac_1"].astype(str)
            chunk["pac_2"] = chunk["pac_2"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["comp"] = chunk["comp"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["rec_fases"] = chunk["rec_fases"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)

            # Cria a coluna 'linha' com a string DSS usando concatenação vetorizada
            chunk["linha"] = (
                "New Line." + chunk["cod_id"] + "_linha_media " +
                "Phases = " + chunk["phases"] + " " +
                "Bus1 = " + chunk["pac_1"] + chunk["rec_fases"] + " " +
                "Bus2 = " + chunk["pac_2"] + chunk["rec_fases"] + " " +
                "Linecode = " + chunk["tip_cnd"] + "_linecode_media " +
                "Length = " + chunk["comp"] + " units = m\n"
            )

            # Agrupa as linhas por 'nome'
            return chunk.groupby("nome")["linha"].agg(list).to_dict()

        linhas_dss = defaultdict(list)
        console = Console()

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for chave, linhas in chunk_dict.items():
                    linhas_dss[chave].extend(linhas)

                progress.update(task, advance=len(chunk))

        return dict(linhas_dss)



class RamalLine:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas Ramais...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            # Construir a coluna das linhas DSS usando concatenação vetorizada
            linhas = (
                'New Line.' + chunk['cod_id'].astype(str) + '_linha_ramal '
                + 'Phases=' + chunk['phases'].astype(str) + ' '
                + 'Bus1=' + chunk['pac_1'].astype(str) + chunk['rec_fases'].astype(str) + ' '
                + 'Bus2=' + chunk['pac_2'].astype(str) + chunk['rec_fases'].astype(str) + ' '
                + 'Linecode=' + chunk['tip_cnd'].astype(str) + '_linecode_ramais '
                + 'Length=' + chunk['comp'].astype(str) + ' units=m\n'
            )

            # Agrupar as linhas pelo nome e juntar em listas
            grouped = linhas.groupby(chunk['nome']).apply(list).to_dict()
            return grouped

        linhas_dss = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start : start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for nome, linhas in chunk_dict.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss


    
class LoadLowVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:

        print("Modelando cargas BT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            # Ajustar tensão nominal para monofásico
            ten_nom_voltage = chunk["ten_nom_voltage"] / 1000
            ten_nom_voltage = ten_nom_voltage.where(chunk["phases"] >= 2,
                                                    ten_nom_voltage / math.sqrt(3))
            
            rec_fases = chunk["rec_fases"].astype(str)
            rec_fases = rec_fases.apply(lambda x: x if '.4' in x else x + '.4')


            # Substituir espaços por tabs na string de potências
            potencias_tab = chunk["potencias"].astype(str).str.replace(" ", "_")
            
            # Montar as linhas DSS vetorized
            linhas = (
                'New Load.nome_' + chunk["cod_id"].astype(str)
                + '_curva_diaria_' + chunk["tip_cc"].astype(str)
                + '_curva_anual_' + potencias_tab
                + '_carga_baixa '
                + 'Bus1=' + chunk["pac"].astype(str) + rec_fases.astype(str) + ' '
                + 'Phases=' + chunk["phases"].astype(str) + '\n'
                + '~ Conn=' + chunk["conn"].astype(str)
                + ' Model=1 Kv=' + ten_nom_voltage.round(4).astype(str)
                + ' Kw=1 pf=0.92\n\n'
            )

            # Agrupar por nome, juntar as linhas em listas
            grouped = linhas.groupby(chunk["nome"]).apply(list).to_dict()
            return grouped

        linhas_dss = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start : start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for nome, linhas in chunk_dict.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss


class LoadMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:

        print("Modelando cargas MT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            # Ajustar tensão nominal para monofásico
            ten_nom_voltage = chunk["ten_nom_voltage"] / 1000
            ten_nom_voltage = ten_nom_voltage.where(chunk["phases"] >= 2,
                                                    ten_nom_voltage / math.sqrt(3))
            rec_fases = chunk["rec_fases"].astype(str)
            rec_fases = rec_fases.apply(lambda x: x if '.4' in x else x + '.4')


            # Substituir espaços por tabs na string de potências
            potencias_tab = chunk["potencias"].astype(str).str.replace(" ", "_")
            
            
            # Montar as linhas DSS vetorized
            linhas = (
                'New Load.nome_' + chunk["cod_id"].astype(str)
                + '_curva_diaria_' + chunk["tip_cc"].astype(str)
                + '_curva_anual_' + potencias_tab
                + '_carga_baixa '
                + 'Bus1=' + chunk["pac"].astype(str) +  rec_fases.astype(str) + ' '
                + 'Phases=' + chunk["phases"].astype(str) + '\n'
                + '~ Conn=' + chunk["conn"].astype(str)
                + ' Model=1 Kv=' + ten_nom_voltage.round(4).astype(str)
                + ' Kw=1 pf=0.92\n\n'
            )

            # Agrupar por nome, juntar as linhas em listas
            grouped = linhas.groupby(chunk["nome"]).apply(list).to_dict()
            return grouped

        linhas_dss = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start : start + chunk_size]
                chunk_dict = to_dss_vetorizado_chunk(chunk)

                for nome, linhas in chunk_dict.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss




class GD_FV_BT:
    @staticmethod
    def to_dss(df: pd.DataFrame) -> dict:
        print("Modelando GD_FV_BT...")
        chunk_dict = {}
        console = Console()

        # Remove duplicatas por 'cod_id'
        df = df.drop_duplicates(subset="cod_id")

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for _, row in df.iterrows():
                rec_fases = row.get('rec_fases', '')
                phases = row.get('phases', 3)
                nome = row.get('nome')
                cod_id = row.get('cod_id')
                ten_nom_voltage = row.get('ten_nom_voltage') / 1000
                mdpotenciaoutorgada = row.get('mdpotenciaoutorgada')
                mdpotenciainstalada = row.get('mdpotenciainstalada')
                pac = row.get('pac')
                nome = row['nome']

                if phases < 2:
                    ten_nom_voltage = ten_nom_voltage / math.sqrt(3)
                
                fases = rec_fases.strip('.').split('.')  # ['1', '2', '3']

                if '4' not in fases:
                    fases.append('4')

                # Recria a string no mesmo formato: '.1.2.3.4'
                rec = '.' + '.'.join(fases)


                linha = (
                    f"New xycurve.mypvst_{cod_id} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                    f"New xycurve.myeff_{cod_id} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                    f"New loadshape.myirrad_{cod_id} npts = 1 interval = 1 mult = [1]\n"
                    f"New tshape.mytemp_{cod_id} npts = 1 interval = 1 temp = [25]\n\n"
                    f"New pvsystem.pv_{cod_id} phases = {phases} conn = wye bus1 = {pac}{rec}\n"
                    f"~ kv = {ten_nom_voltage} kva = {mdpotenciaoutorgada} pmpp = {mdpotenciainstalada}\n"
                    f"~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{cod_id}\n"
                    f"~ p-tcurve = mypvst_{cod_id} daily = myirrad_{cod_id} tdaily = mytemp_{cod_id}\n\n"
                    f"New load.{cod_id}_carga_no_pv bus1 = {pac}{rec} phases = {phases}\n"
                    f"~ conn = wye model = 1 kv = {ten_nom_voltage} kw = 0.0001\n\n"
                )

                chunk_dict.setdefault(nome, []).append(linha)
                progress.advance(task)

        return chunk_dict




class GD_FV_MT:
    @staticmethod
    def to_dss(df: pd.DataFrame) -> dict:
        print("Modelando GD_FV_MT...")
        chunk_dict = {}
        console = Console()

        # Remove duplicatas por 'cod_id'
        df = df.drop_duplicates(subset="cod_id")
        
        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))
            
            for _, row in df.iterrows():
                rec_fases = row.get('rec_fases', '') 
                phases = row.get('phases', 3) 
                nome = row.get('nome')
                cod_id = row.get('cod_id')
                ten_nom_voltage = row.get('ten_nom_voltage') / 1000
                mdpotenciaoutorgada = row.get('mdpotenciaoutorgada')
                mdpotenciainstalada = row.get('mdpotenciainstalada')
                pac = row.get('pac')
                nome = row['nome']
                
                if phases < 2:
                    ten_nom_voltage = ten_nom_voltage / math.sqrt(3)

                fases = rec_fases.strip('.').split('.')  # ['1', '2', '3']

                if '4' not in fases:
                    fases.append('4')

                # Recria a string no mesmo formato: '.1.2.3.4'
                rec = '.' + '.'.join(fases)


                linha =  (
                    f"New xycurve.mypvst_{cod_id} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                    f"New xycurve.myeff_{cod_id} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                    f"New loadshape.myirrad_{cod_id} npts = 1 interval = 1 mult = [1]\n"
                    f"New tshape.mytemp_{cod_id} npts = 1 interval = 1 temp = [25]\n\n"
                    f"New pvsystem.pv_{cod_id} phases = {phases} conn = wye bus1 = {pac}{rec}\n"
                    f"~ kv = {ten_nom_voltage} kva = {mdpotenciaoutorgada} pmpp = {mdpotenciainstalada}\n"
                    f"~ pf = 0.92 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{cod_id}\n"
                    f"~ p-tcurve = mypvst_{cod_id} daily = myirrad_{cod_id} tdaily = mytemp_{cod_id}\n\n"
                    f"New load.{cod_id}_carga_no_pv bus1 = {pac}{rec} phases = {phases}\n"
                    f"~ conn = wye model = 1 kv = {ten_nom_voltage} kw = 0.0001\n\n"
                )
                
                chunk_dict.setdefault(nome, []).append(linha)
                progress.advance(task)

        return chunk_dict




class PublicLightingLoad:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando cargas PIP...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        chunk_dict = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size].copy()

                # Conversões vetoriais
                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac"] = chunk["pac"].astype(str)
                chunk["rec_fases"] = chunk["rec_fases"].astype(str)
                chunk["ten_nom_voltage"] = chunk["ten_nom_voltage"] / 1000
                chunk["pot_lamp"] = chunk["pot_lamp"] / 1000  # W → kW

                # Ajuste para fases monofásicas
                monofasicas = chunk["phases"] < 2
                chunk.loc[monofasicas, "ten_nom_voltage"] = chunk.loc[monofasicas, "ten_nom_voltage"] / math.sqrt(3)

                rec_fases = chunk["rec_fases"].astype(str)
                rec_fases = rec_fases.apply(lambda x: x if '.4' in x else x + '.4')


                # Geração das strings
                chunk["linha"] = (
                    "New Load.nome_" + chunk["cod_id"] + "_carga_pip Bus1 = " + chunk["pac"] +  rec_fases +
                    " Phases = " + chunk["phases"].astype(str) + "\n"
                    "~ Conn = wye Model = 1 Kv = " + chunk["ten_nom_voltage"].round(5).astype(str) +
                    " Kw = " + chunk["pot_lamp"].round(5).astype(str) + " pf = 0.92\n\n"
                )

                # Agrupamento por nome
                agrupado = chunk.groupby("nome")["linha"].apply(list).to_dict()

                for nome, linhas in agrupado.items():
                    chunk_dict.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return chunk_dict


class TransformerMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Transformadores MT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        linhas_dss = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size].copy()

                # Pré-processamento vetorial
                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac_1"] = chunk["pac_1"].astype(str)
                chunk["pac_2"] = chunk["pac_2"].astype(str)
                chunk["nome"] = chunk["nome"].astype(str)
                chunk["ten_pri"] = chunk["ten_pri"] / 1000
                chunk["ten_sec"] = chunk["ten_sec"] / 1000

                # Cria coluna rec_fases_t para fases 1 ou 2
                mask_monobifasico = chunk["phases_p"].isin([1, 2])
                def inverter_fases(fase_str):
                    if isinstance(fase_str, str):
                        fases = fase_str.replace('.', ' ').split()
                        if not fases:
                            return ""
                        return '.' + '.'.join(fases[::-1])
                    return ""

                chunk.loc[mask_monobifasico, "rec_fases_t"] = chunk.loc[mask_monobifasico, "rec_fases_s"].apply(inverter_fases)


                linhas = []

                def gerar_linha(row):
                    cod_id, pac_1, pac_2 = row["cod_id"], row["pac_1"], row["pac_2"]
                    nome, pot_nom = row["nome"], row["pot_nom"]
                    phases_p, ten_pri, ten_sec = row["phases_p"], row["ten_pri"], row["ten_sec"]
                    lig_fas_p, rec_fases_p = row["lig_fas_p"], row["rec_fases_p"]
                    lig_fas_s, rec_fases_s = row["lig_fas_s"], row["rec_fases_s"]
                    xhl, per_fer, r = row["xhl"], row["per_fer"], row["r"]
                    rec_fases_t = row.get("rec_fases_t", "")

                    if phases_p == 1:
                        ten_pri = round(ten_pri / math.sqrt(3), 3)
                        ten_sec = round(ten_sec / math.sqrt(3), 3)
                        return (
                            f"New Transformer.{cod_id} Phases={phases_p} Windings=3 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}{rec_fases_p} conn=wye kv={ten_pri} kva={pot_nom} %r={r/2} tap=1\n"
                            f"~ wdg=2 bus={pac_2}{rec_fases_s} conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=3 bus={pac_2}{rec_fases_t} conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n\n"
                        )

                    elif phases_p == 2:
                        ten_sec = round(ten_sec / math.sqrt(3), 3)
                        return (
                            f"New Transformer.{cod_id} Phases={phases_p} Windings=3 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}{rec_fases_p} conn=delta kv={ten_pri} kva={pot_nom} %r={r/2} tap=1\n"
                            f"~ wdg=2 bus={pac_2}{rec_fases_s} conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=3 bus={pac_2}{rec_fases_t} conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n\n"
                        )

                    elif phases_p == 3:
                        return (
                            f"New Transformer.{cod_id} Phases={phases_p} Windings=2 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}{rec_fases_p} conn=delta kv={ten_pri} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=2 bus={pac_2}{rec_fases_s} conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n\n"
                        )

                    return None  # Caso não esperado

                chunk["linha"] = chunk.apply(gerar_linha, axis=1)

                agrupado = chunk.groupby("nome")["linha"].apply(lambda x: [linha for linha in x if linha]).to_dict()

                for nome, linhas in agrupado.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss



class RegulatorMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Reguladores MT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        linhas_dss = {}

        with Progress(
            TextColumn("[bold green]Carregando..."),
            BarColumn(bar_width=60, style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Modelando...", total=len(df))

            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start:start + chunk_size].copy()

                # Conversões e ajustes vetoriais
                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac_1"] = chunk["pac_1"].astype(str)
                chunk["pac_2"] = chunk["pac_2"].astype(str)
                chunk["nome"] = chunk["nome"].astype(str)
                chunk["ten_nom"] = chunk["ten_nom"] / 1000
                banda_volts = 2  # constante

                def gerar_linha(row):
                    phases_p = row["phases_p"]
                    ten_nom = round(row["ten_nom"], 5)

                    if phases_p == 1:
                        ten_nom_adj = round(ten_nom / math.sqrt(3), 3)
                        vreg = ten_nom_adj / float(row["rel_tp"])
                    elif phases_p in [2, 3]:
                        ten_nom_adj = ten_nom
                        vreg = ten_nom_adj / float(row["rel_tp"])
                    else:
                        return None

                    cod_id = row["cod_id"]
                    lig_fas_p = row["lig_fas_p"]
                    pac_1 = row["pac_1"]
                    pac_2 = row["pac_2"]
                    rec_fases_p = row["rec_fases_p"]
                    rec_fases_s = row["rec_fases_s"]
                    pot_nom = row["pot_nom"]
                    r = row["r"]
                    rel_tp = row["rel_tp"]
                    cor_nom = row["cor_nom"]

                    return (
                        f"new transformer.reg{lig_fas_p}_{cod_id} phases={phases_p} windings=2 bank={cod_id} Maxtap=1.1 Mintap=0.9 ppm=0\n"
                        f"~ buses=({pac_1}{rec_fases_p} {pac_2}{rec_fases_s}) \n"
                        f"~ conns='wye wye' kvs='{ten_nom_adj} {ten_nom_adj}' \n"
                        f"~ kvas='{pot_nom} {pot_nom}' XHL = 0.1 %r={r}\n"
                        f"new regcontrol.creg{lig_fas_p}_{cod_id} transformer=reg{lig_fas_p}_{cod_id} winding=2 \n"
                        f"~ vreg={vreg:.5f} band={banda_volts} ptratio={rel_tp} ctprim={cor_nom}\n\n"
                    )

                # Aplica a função linha a linha
                chunk["linha"] = chunk.apply(gerar_linha, axis=1)

                # Agrupa por nome
                agrupado = chunk.groupby("nome")["linha"].apply(
                    lambda x: [linha for linha in x if linha is not None]
                ).to_dict()

                # Junta ao dicionário final
                for nome, linhas in agrupado.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss
