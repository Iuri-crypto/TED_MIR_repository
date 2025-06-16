import pandas as pd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console
import math
from collections import defaultdict
console = Console()
import ast



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
                
                sub = row['sub']
                nome = row['nome']
                kv = row['ten_nom_voltage'] / 1000
                v_pu = row['ten_ope']
                pac_ini = row['pac_ini']

                linha = (
                        f"New Object = Circuit.{nome}_Barra_Slack\n "
                        f"~ basekv = {kv} pu = {v_pu} angle = 0\n "
                        f"New line.{sub}_{nome}_ phases = 3 bus1 = SourceBus bus2 = {pac_ini}.1.2.3 switch = y\n\n "
                )
                chunk_dict.setdefault(sub, {}).setdefault(nome, []).append(linha)
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

        # Remove duplicatas mantendo a primeira ocorrência
        df = df.drop_duplicates(subset="cod_id")

        def format_coord(coord_raw):
            # Tenta converter string que representa tupla/lista para objeto Python
            if isinstance(coord_raw, str):
                try:
                    coord_raw = ast.literal_eval(coord_raw)
                except Exception:
                    pass
            if isinstance(coord_raw, (list, tuple)):
                try:
                    lat = round(float(coord_raw[0]), 6)
                    lon = round(float(coord_raw[1]), 6)
                    return f"{lat}_{lon}"
                except Exception:
                    return "sem_coord"
            else:
                return str(coord_raw).replace(" ", "_")[:30] or "sem_coord"

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            basekv = (chunk['ten_nom_voltage'] / 1000).round(3)
            rec_fases = chunk.get('rec_fases', pd.Series('', index=chunk.index)).astype(str)
            phases = chunk.get('phases', pd.Series(3, index=chunk.index))
            pot_nom = chunk['pot_nom'].astype(str)
            pac_1 = chunk['pac_1'].astype(str)
            tip_unid = chunk['tip_unid'].astype(str)
            cod_id = chunk['cod_id'].astype(str)
            nome = chunk['nome'].astype(str)
            sub = chunk['sub'].astype(str)
            coord_col = chunk['coord_latlon']

            for i in chunk.index:
                bus1 = f"{pac_1[i]}{rec_fases[i]}"
                kv = basekv[i]
                kvar = pot_nom[i]
                fases = phases[i]
                cod_id_rec = cod_id[i]

                coord_formatada = format_coord(coord_col[i])

                if fases < 2:
                    kv = round(kv / math.sqrt(3), 3)
                    rec = '.0'
                else:
                    rec = ''

                if tip_unid[i] == '56':
                    linha = (
                        f"New Reactor.coord_{coord_formatada}_{cod_id_rec}_Banco_de_Reator Bus1 = {bus1}{rec} "
                        f"kv = {kv} kvar = {kvar} phases = {fases} conn = wye\n\n"
                    )
                else:
                    linha = (
                        f"New Capacitor.coord_{coord_formatada}_{cod_id_rec}_Banco_de_Capacitor Bus1 = {bus1} "
                        f"kv = {kv} kVAR = {kvar} phases = {fases} conn = wye\n\n"
                    )

                chunk_result.setdefault(str(sub[i]), {}).setdefault(nome[i], []).append(linha)

            return chunk_result  # ← ESSENCIAL para retorno correto

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
                for sub, nomes_dict in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes_dict.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict



class ReactiveCompensatorBT:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Compensadores Reativos BT...")
        dss_dict = defaultdict(dict)
        console = Console()

        df = df.drop_duplicates(subset="cod_id")

        def format_coord(coord_raw):
            # Tenta converter string para lista/tupla (caso venha como texto)
            if isinstance(coord_raw, str):
                try:
                    import ast
                    coord_raw = ast.literal_eval(coord_raw)
                except Exception:
                    pass

            if isinstance(coord_raw, (list, tuple)):
                try:
                    lat = round(float(coord_raw[0]), 6)
                    lon = round(float(coord_raw[1]), 6)
                    return f"{lat}_{lon}"
                except Exception:
                    return "sem_coord"
            else:
                return str(coord_raw).replace(" ", "_")[:30] or "sem_coord"

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            basekv = (chunk['ten_nom_voltage'] / 1000).round(3)
            rec_fases = chunk.get('rec_fases', pd.Series('', index=chunk.index)).astype(str)
            phases = chunk.get('phases', pd.Series(3, index=chunk.index))
            pot_nom = chunk['pot_nom'].astype(str)
            pac_1 = chunk['pac_1'].astype(str)
            tip_unid = chunk['tip_unid'].astype(str)
            cod_id = chunk['cod_id'].astype(str)
            nome = chunk['nome'].astype(str)
            sub = chunk['sub'].astype(str)
            coord_col = chunk['coord_latlon']

            for i in chunk.index:
                bus1 = f"{pac_1[i]}{rec_fases[i]}"
                kv = basekv[i]
                kvar = pot_nom[i]
                fases = phases[i]
                cod_id_rec = cod_id[i]

                coord_formatada = format_coord(coord_col[i])

                if fases < 2:
                    kv = round(kv / math.sqrt(3), 3)
                    rec = '.0'
                else:
                    rec = ''

                if tip_unid[i] == '56':
                    linha = (
                        f"New Reactor.coord_{coord_formatada}_{cod_id_rec}_Banco_de_Reator Bus1 = {bus1}{rec} "
                        f"kv = {kv} kvar = {kvar} phases = {fases} conn = wye\n\n"
                    )
                else:
                    linha = (
                        f"New Capacitor.coord_{coord_formatada}_{cod_id_rec}_Banco_de_Capacitor Bus1 = {bus1}{rec} "
                        f"kv = {kv} kVAR = {kvar} phases = {fases} conn = wye\n\n"
                    )

                chunk_result.setdefault(str(sub[i]), {}).setdefault(nome[i], []).append(linha)

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

                for sub, nomes_dict in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes_dict.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

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
            rec_fases = chunk.get("rec_fases", pd.Series('', index=chunk.index)).astype(str)
            pn_ope = chunk.get("p_n_ope", pd.Series('', index=chunk.index)).astype(str)
            nome = chunk["nome"].astype(str)
            sub = chunk["sub"].astype(str)
            coord_col = chunk["coord_latlon"]  # ← mantém tipo original

            for i in chunk.index:
                if pn_ope[i] != 'F':
                    continue

                # ⬇️ Tratamento seguro para coordenada
                coord_raw = coord_col[i]
                if isinstance(coord_raw, (list, tuple)):
                    try:
                        coord_formatada = f"{round(coord_raw[0], 6)}_{round(coord_raw[1], 6)}"
                    except (IndexError, TypeError):
                        coord_formatada = "sem_coord"
                else:
                    coord_formatada = str(coord_raw).replace(" ", "_")[:30] or "sem_coord"

                linha = (
                    f"New line.coord_{coord_formatada}_{cod_id[i]}_chave_bt "
                    f"phases = {phases[i]} bus1 = {pac_1[i]}{rec_fases[i]} "
                    f"bus2 = {pac_2[i]}{rec_fases[i]} switch = y\n\n"
                )

                chunk_result.setdefault(sub[i], {}).setdefault(nome[i], []).append(linha)

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

                for sub, nomes_dict in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes_dict.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict


class SwitchMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Chaves Seccionadoras MT...")
        dss_dict = {}
        console = Console()

        df = df.drop_duplicates(subset="cod_id")

        def format_coords(coord_raw):
            if isinstance(coord_raw, (list, tuple)):
                if all(isinstance(p, (list, tuple)) and len(p) == 2 for p in coord_raw):
                    partes = []
                    for p in coord_raw:
                        try:
                            lat = round(float(p[0]), 6)
                            lon = round(float(p[1]), 6)
                            partes.append(f"{lat}_{lon}")
                        except Exception:
                            partes.append("sem_coord")
                    return "__".join(partes)
                else:
                    try:
                        lat = round(float(coord_raw[0]), 6)
                        lon = round(float(coord_raw[1]), 6)
                        return f"{lat}_{lon}"
                    except Exception:
                        return "sem_coord"
            # Remove parênteses, colchetes e espaços
            return str(coord_raw).replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace(" ", "_")[:50] or "sem_coord"

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_result = {}

            cod_id = chunk["cod_id"].astype(str)
            pac_1 = chunk["pac_1"].astype(str)
            pac_2 = chunk["pac_2"].astype(str)
            pn_ope = chunk["p_n_ope"].astype(str)
            nome = chunk["nome"].astype(str)
            sub = chunk["sub"].astype(str)
            coord_col = chunk["coord_latlon"]

            for i in chunk.index:
                if pn_ope[i] != 'F':
                    continue

                #coord_raw = coord_col[i]
                #coord_formatada = format_coords(coord_raw)

                chave = cod_id[i]

                linha = (
                    f"New line.{chave}_Chave_mt "
                    f"phases = 3 "
                    f"bus1 = {pac_1[i]}.1.2.3 "
                    f"bus2 = {pac_2[i]}.1.2.3 "
                    f"switch = y\n\n "
                )

                chunk_result.setdefault(sub[i], {}).setdefault(nome[i], []).append(linha)

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

                for sub, nomes_dict in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes_dict.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

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
            ten_nom_voltage = (chunk["ten_nom_voltage"] / 1000)
            nome = chunk["nome"].astype(str)
            sub = chunk["sub"].astype(str)

            for i in chunk.index:
                chave = cod_id[i]
                linha = (
                    f"New Generator.{chave}_Gerador_Media_tensao Bus1={pac[i]}.1.2.3.0 Model=1 PF=0.92\n "
                    f"~ kva={pot_inst[i]} KV={ten_nom_voltage[i]} Xdp=0.27 xdpp=0.20 H=2\n "
                    f"~ Conn=wye\n\n "
                )
                chunk_result.setdefault(sub[i], {}).setdefault(nome[i], []).append(linha) 

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
                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items(): 
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

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
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)

            # Geração das linhas DSS
            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "_linecode_baixa nphases=3 BaseFreq=60\n " +
                "~ r1=" + chunk["r1"] + "\n " +
                "~ x1=" + chunk["x1"] + "\n " +
                "~ c1=0\n " +
                "~ Normamps=" + chunk["cnom"] + "\n " +
                "~ Emergamps=" + chunk["cmax_renamed"] + "\n\n "
            )

            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}
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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict



class LinecodeMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linecodes MT...")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Conversões vetoriais
            chunk["r1"] = chunk["r1"] / 1000
            chunk["x1"] = chunk["x1"] / 1000

            # Converte os floats para string após divisão
            chunk["r1"] = chunk["r1"].astype(str)
            chunk["x1"] = chunk["x1"].astype(str)

            chunk["cnom"] = chunk["cnom"].astype(str)
            chunk["cmax_renamed"] = chunk["cmax_renamed"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)

            # Geração da string
            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "_linecode_media nphases=3 BaseFreq=60\n " +
                "~ r1=" + chunk["r1"] + "\n " +
                "~ x1=" + chunk["x1"] + "\n " +
                "~ c1=0\n " +
                "~ Normamps=" + chunk["cnom"] + "\n " +
                "~ Emergamps=" + chunk["cmax_renamed"] + "\n\n "
            )


            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}
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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict


class LinecodeRamais:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linecodes Ramais...")

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Conversões
            chunk["r1"] = (chunk["r1"] / 1000).astype(str)
            chunk["x1"] = (chunk["x1"] / 1000).astype(str)
            chunk["cnom"] = chunk["cnom"].astype(str)
            chunk["cmax_renamed"] = chunk["cmax_renamed"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)

            chunk["linha"] = (
                "New linecode." + chunk["tip_cnd"] + "linecode_rml nphases=3 BaseFreq=60\n " +
                "~ r1=" + chunk["r1"] + "\n " +
                "~ x1=" + chunk["x1"] + "\n " +
                "~ c1=0\n " +
                "~ Normamps=" + chunk["cnom"] + "\n " +
                "~ Emergamps=" + chunk["cmax_renamed"] + "\n\n "
            )

            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}
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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict
    
    
    
class LineLowVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas BT...")

        df = df.drop_duplicates(subset="cod_id")

        def format_coord(coord_val):
            if isinstance(coord_val, (list, tuple)):
                # Se for lista/tupla com pelo menos dois pontos, cada um sendo uma tupla (lat, lon)
                if len(coord_val) >= 2 and all(isinstance(p, (list, tuple)) and len(p) == 2 for p in coord_val[:2]):
                    partes = []
                    for p in coord_val[:2]:  # pega só os dois primeiros pontos da linha
                        try:
                            lat = round(float(p[0]), 6)
                            lon = round(float(p[1]), 6)
                            partes.append(f"{lat}_{lon}")
                        except Exception:
                            partes.append("sem_coord")
                    return "__".join(partes)
                else:
                    # Se for só um par lat-lon simples
                    try:
                        lat = round(float(coord_val[0]), 6)
                        lon = round(float(coord_val[1]), 6)
                        return f"{lat}_{lon}"
                    except Exception:
                        return "sem_coord"
            # Caso seja string ou outro tipo
            return str(coord_val).replace(" ", "_")[:30] or "sem_coord"

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Conversões para string
            chunk["cod_id"] = chunk["cod_id"].astype(str)
            chunk["pac_1"] = chunk["pac_1"].astype(str)
            chunk["pac_2"] = chunk["pac_2"].astype(str)
            chunk["comp"] = chunk["comp"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["rec_fases"] = chunk["rec_fases"].astype(str)
            chunk["phases"] = chunk.get("phases", pd.Series(3, index=chunk.index)).astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)

            # Aplica formatação segura nas coordenadas com múltiplos pontos
            chunk["coord_formatada"] = chunk["coord_latlon"].apply(format_coord)

            chunk["linha"] = (
                "New Line.coord_" + chunk["coord_formatada"] + "_" + chunk["cod_id"] + "_linha_baixa " +
                "Phases=" + chunk["phases"] + " " +
                "Bus1=" + chunk["pac_1"] + chunk["rec_fases"] + " " +
                "Bus2=" + chunk["pac_2"] + chunk["rec_fases"] + " " +
                "Linecode=" + chunk["tip_cnd"] + "_linecode_baixa " +
                "Length=" + chunk["comp"] + " units=m\n"
            )

            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}
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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict




class LineMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas MT...")

        df = df.drop_duplicates(subset="cod_id")

        def format_coord(coord_val):
            # Se for string que parece lista/tupla, tenta converter para objeto Python real
            if isinstance(coord_val, str):
                try:
                    coord_val = ast.literal_eval(coord_val)
                except Exception:
                    pass  # deixa coord_val como string mesmo se der erro

            if isinstance(coord_val, (list, tuple)):
                # Se for lista/tupla com pelo menos dois pontos, cada um sendo uma tupla (lat, lon)
                if len(coord_val) >= 2 and all(isinstance(p, (list, tuple)) and len(p) == 2 for p in coord_val[:2]):
                    partes = []
                    for p in coord_val[:2]:  # pega só os dois primeiros pontos da linha
                        try:
                            lat = round(float(p[0]), 6)
                            lon = round(float(p[1]), 6)
                            partes.append(f"{lat}_{lon}")
                        except Exception:
                            partes.append("sem_coord")
                    return "__".join(partes)
                else:
                    # Se for só um par lat-lon simples
                    try:
                        lat = round(float(coord_val[0]), 6)
                        lon = round(float(coord_val[1]), 6)
                        return f"{lat}_{lon}"
                    except Exception:
                        return "sem_coord"
            # Caso seja string ou outro tipo
            return str(coord_val).replace(" ", "_")[:30] or "sem_coord"
        
        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Conversões para string
            chunk["cod_id"] = chunk["cod_id"].astype(str)
            chunk["pac_1"] = chunk["pac_1"].astype(str)
            chunk["pac_2"] = chunk["pac_2"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)
            chunk["comp"] = chunk["comp"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["rec_fases"] = chunk["rec_fases"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)

            # Formata coordenadas com tratamento para múltiplos pontos
            chunk["coord_formatada"] = chunk["coord_latlon"].apply(format_coord)

            chunk["linha"] = (
                "New Line.coord_" + chunk["coord_formatada"] + "_" + chunk["cod_id"] + "_linha_media " +
                "Phases=3 " +
                "Bus1=" + chunk["pac_1"] + ".1.2.3 " +
                "Bus2=" + chunk["pac_2"] + ".1.2.3 " +
                "Linecode=" + chunk["tip_cnd"] + "_linecode_media " +
                "Length=" + chunk["comp"] + " units=m\n"
            )

            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}
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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict

    
    

class RamalLine:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Linhas Ramais...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()

        def to_dss_vetorizado_chunk(chunk: pd.DataFrame) -> dict:
            chunk = chunk.copy()

            # Garantir tipos corretos
            chunk["cod_id"] = chunk["cod_id"].astype(str)
            chunk["phases"] = chunk["phases"].astype(str)
            chunk["pac_1"] = chunk["pac_1"].astype(str)
            chunk["pac_2"] = chunk["pac_2"].astype(str)
            chunk["rec_fases"] = chunk["rec_fases"].astype(str)
            chunk["tip_cnd"] = chunk["tip_cnd"].astype(str)
            chunk["comp"] = chunk["comp"].astype(str)
            chunk["nome"] = chunk["nome"].astype(str)
            chunk["sub"] = chunk["sub"].astype(str)
            chunk['coord_latlon'] = chunk['coord_latlon'].astype(str)


            chunk["linha"] = (
                "New Line." + "coord_" + chunk['coord_latlon'] + "_" + chunk["cod_id"] + "_linha_ramal " +
                "Phases=" + chunk["phases"] + " " +
                "Bus1=" + chunk["pac_1"] + chunk["rec_fases"] + " " +
                "Bus2=" + chunk["pac_2"] + chunk["rec_fases"] + " " +
                "Linecode=" + chunk["tip_cnd"] + "_linecode_ramais " +
                "Length=" + chunk["comp"] + " units=m\n "
            )

            # Organiza por sub -> nome -> [linhas]
            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = chunk.at[i, "linha"]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

        dss_dict = {}

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

                for sub, nomes in chunk_dict.items():
                    if sub not in dss_dict:
                        dss_dict[sub] = {}
                    for nome, linhas in nomes.items():
                        dss_dict[sub].setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return dss_dict



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
            
            # Ajustar rec_fases: adicionar '.4' apenas onde phase == 1 e não termina com '.4'
            rec_fases = chunk["rec_fases"].astype(str).copy()
            mask = (chunk["phases"] == 1) & (~rec_fases.str.endswith('.4'))
            rec_fases.loc[mask] = rec_fases.loc[mask] + '.4'

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

            # Organizar por sub -> nome -> [linhas]
            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = linhas[i]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

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

                for sub, nomes in chunk_dict.items():
                    if sub not in linhas_dss:
                        linhas_dss[sub] = {}
                    for nome, linhas in nomes.items():
                        linhas_dss[sub].setdefault(nome, []).extend(linhas)

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

            # Ajustar rec_fases: adicionar '.4' apenas onde phase == 1 e não termina com '.4'
            rec_fases = chunk["rec_fases"].astype(str).copy()
            mask = (chunk["phases"] == 1) & (~rec_fases.str.endswith('.4'))
            rec_fases.loc[mask] = rec_fases.loc[mask] + '.4'

            # Substituir espaços por tabs na string de potências
            potencias_tab = chunk["potencias"].astype(str).str.replace(" ", "_")

            # Montar as linhas DSS vetorized
            linhas = (
                'New Load.nome_' + chunk["cod_id"].astype(str)
                + '_curva_diaria_' + chunk["tip_cc"].astype(str)
                + '_curva_anual_' + potencias_tab
                + '_carga_media '
                + 'Bus1=' + chunk["pac"].astype(str) + '.1.2.3.0' + ' '
                + 'Phases = 3\n '
                + '~ Conn=' + chunk["conn"].astype(str)
                + ' Model=1 Kv=' + ten_nom_voltage.round(4).astype(str)
                + ' Kw=1 pf=0.92\n\n'
            )

            # Organizar por sub -> nome -> [linhas]
            chunk_result = {}
            for i in chunk.index:
                sub = chunk.at[i, "sub"]
                nome = chunk.at[i, "nome"]
                linha = linhas[i]
                chunk_result.setdefault(sub, {}).setdefault(nome, []).append(linha)

            return chunk_result

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

                for sub, nomes in chunk_dict.items():
                    if sub not in linhas_dss:
                        linhas_dss[sub] = {}
                    for nome, linhas in nomes.items():
                        linhas_dss[sub].setdefault(nome, []).extend(linhas)

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

        # Verifica se 'sub' existe no dataframe, se não, cria coluna default
        if 'sub' not in df.columns:
            df['sub'] = 'sem_sub'  # Pode ajustar para valor padrão desejado

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
                sub = row.get('sub', 'sem_sub')  # pega sub ou default

                if phases < 2:
                    ten_nom_voltage = ten_nom_voltage / math.sqrt(3)

                fases = rec_fases.strip('.').split('.')  # Ex: ['1', '2', '3']

                # Adiciona '.4' somente se phases == 1 e ainda não contém '4'
                if phases == 1 and '4' not in fases:
                    fases.append('4')

                rec = '.' + '.'.join(fases)

                linha = (
                    f"New xycurve.mypvst_{cod_id}_gd_baixa_tensao npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                    f"New xycurve.myeff_{cod_id} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                    f"New loadshape.myirrad_{cod_id} npts = 1 interval = 1 mult = [1]\n"
                    f"New tshape.mytemp_{cod_id} npts = 1 interval = 1 temp = [25]\n\n"
                    f"New pvsystem.pv_{cod_id} phases = {phases} conn = wye bus1 = {pac}{rec}\n"
                    f"~ kv = {ten_nom_voltage} kva = {mdpotenciaoutorgada} pmpp = {mdpotenciainstalada}\n"
                    f"~ pf = 1 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{cod_id}\n"
                    f"~ p-tcurve = mypvst_{cod_id} daily = myirrad_{cod_id} tdaily = mytemp_{cod_id}\n\n"
                    f"New load.{cod_id}_carga_no_pv bus1 = {pac}{rec} phases = {phases}\n"
                    f"~ conn = wye model = 1 kv = {ten_nom_voltage} kw = 0.0001\n\n"
                )

                chunk_dict.setdefault(sub, {}).setdefault(nome, []).append(linha)
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

        # Verifica se 'sub' existe, senão cria coluna padrão
        if 'sub' not in df.columns:
            df['sub'] = 'sem_sub'

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
                sub = row.get('sub', 'sem_sub')

                if phases < 2:
                    ten_nom_voltage = ten_nom_voltage / math.sqrt(3)

                fases = rec_fases.strip('.').split('.')  # Ex: ['1', '2', '3']

                # Adiciona '.4' somente se phases == 1 e ainda não contém '4'
                if phases == 1 and '4' not in fases:
                    fases.append('4')

                rec = '.' + '.'.join(fases)

                linha =  (
                    f"New xycurve.mypvst_{cod_id}_gd_media_tensao npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                    f"New xycurve.myeff_{cod_id} npts = 4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                    f"New loadshape.myirrad_{cod_id} npts = 1 interval = 1 mult = [1]\n"
                    f"New tshape.mytemp_{cod_id} npts = 1 interval = 1 temp = [25]\n\n"
                    f"New pvsystem.pv_{cod_id} phases = {phases} conn = wye bus1 = {pac}{rec}\n"
                    f"~ kv = {ten_nom_voltage} kva = {mdpotenciaoutorgada} pmpp = {mdpotenciainstalada}\n"
                    f"~ pf = 1 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{cod_id}\n"
                    f"~ p-tcurve = mypvst_{cod_id} daily = myirrad_{cod_id} tdaily = mytemp_{cod_id}\n\n"
                    f"New load.{cod_id}_carga_no_pv bus1 = {pac}{rec} phases = {phases}\n"
                    f"~ conn = wye model = 1 kv = {ten_nom_voltage} kw = 0.0001\n\n"
                )

                chunk_dict.setdefault(sub, {}).setdefault(nome, []).append(linha)
                progress.advance(task)

        return chunk_dict



class PublicLightingLoad:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando cargas PIP...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        chunk_dict = {}

        # Garante que existe a coluna 'sub'
        if 'sub' not in df.columns:
            df['sub'] = 'sem_sub'

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

                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac"] = chunk["pac"].astype(str)
                chunk["rec_fases"] = chunk["rec_fases"].astype(str)
                chunk["ten_nom_voltage"] = chunk["ten_nom_voltage"] / 1000
                chunk["pot_lamp"] = chunk["pot_lamp"] / 1000  # W → kW

                # Ajuste para monofásicas
                monofasicas = chunk["phases"] < 2
                chunk.loc[monofasicas, "ten_nom_voltage"] /= math.sqrt(3)

                # Ajustar rec_fases: adicionar '.4' onde necessário
                rec_fases = chunk["rec_fases"].astype(str)
                mask = (chunk["phases"] == 1) & (~rec_fases.str.endswith('.4'))
                rec_fases[mask] = rec_fases[mask] + '.4'

                # Curva base: 96 valores, 0.0 entre 6h e 18h, 1.0 no restante
                curva_base_diaria = [0.0 if 6 <= (i * 0.25) < 18 else 1.0 for i in range(96)]

                linhas = []
                for i, row in chunk.iterrows():
                    cod_id = row["cod_id"]
                    tip_cc = str(row["tip_cc"])
                    pot_kw = round(row["pot_lamp"], 5)
                    curva_diaria = "_".join([str(round(v, 5)) for v in curva_base_diaria])
                    curva_anual = "_".join([str(pot_kw)] * 12)

                    nome_completo = (
                        "nome_" + cod_id +
                        "_curva_diaria_" + tip_cc + "_" + curva_diaria +
                        "_curva_anual_" + curva_anual +
                        "_carga_pip"
                    )

                    linha = (
                        "New Load." + nome_completo + " Bus1 = " + row["pac"] + rec_fases.loc[i] +
                        " Phases = " + str(row["phases"]) + "\n"
                        "~ Conn = wye Model = 1 Kv = " + str(round(row["ten_nom_voltage"], 5)) +
                        " Kw = " + str(pot_kw) + " pf = 0.92\n\n"
                    )

                    linhas.append((row["sub"], row["nome"], linha))

                # Agrupamento por sub e nome
                for sub, nome, linha in linhas:
                    chunk_dict.setdefault(sub, {}).setdefault(nome, []).append(linha)

                progress.update(task, advance=len(chunk))

        return chunk_dict



class TransformerMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Transformadores MT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        linhas_dss = {}

        if 'sub' not in df.columns:
            df['sub'] = 'sem_sub'

        def format_coord(coord_val):
            if isinstance(coord_val, str):
                try:
                    coord_val = ast.literal_eval(coord_val)
                except Exception:
                    pass
            if isinstance(coord_val, (list, tuple)):
                try:
                    return f"{round(coord_val[0], 6)}_{round(coord_val[1], 6)}"
                except (IndexError, TypeError, ValueError):
                    return "sem_coord"
            return str(coord_val).replace(" ", "_")[:30] or "sem_coord"

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

                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac_1"] = chunk["pac_1"].astype(str)
                chunk["pac_2"] = chunk["pac_2"].astype(str)
                chunk["nome"] = chunk["nome"].astype(str)
                chunk["coord_latlon"] = chunk["coord_latlon"].apply(format_coord)
                chunk["ten_pri"] = chunk["ten_pri"] / 1000
                chunk["ten_sec"] = chunk["ten_sec"] / 1000

                def inverter_fase_monofasica(fase: str) -> str:
                    if isinstance(fase, str) and fase.count('.') == 2:
                        partes = fase.strip('.').split('.')
                        if len(partes) == 2:
                            return f".{partes[1]}.{partes[0]}"
                    return ""

                mask_monofasico = chunk["phases_p"] == 1
                chunk.loc[mask_monofasico, "rec_fases_t"] = chunk.loc[mask_monofasico, "rec_fases_p"].apply(inverter_fase_monofasica)

                def substituir_quatro_por_zero(fase: str) -> str:
                    return fase.replace(".4", ".0") if isinstance(fase, str) else ""

                for col in ["rec_fases_p", "rec_fases_s", "rec_fases_t"]:
                    chunk[col] = chunk[col].apply(substituir_quatro_por_zero)

                def gerar_linha(row):
                    cod_id, pac_1, pac_2 = row["cod_id"], row["pac_1"], row["pac_2"]
                    nome, pot_nom = row["nome"], row["pot_nom"]
                    phases_p, ten_pri, ten_sec = row["phases_p"], row["ten_pri"], row["ten_sec"]
                    lig_fas_p, rec_fases_p = row["lig_fas_p"], row["rec_fases_p"]
                    lig_fas_s, rec_fases_s = row["lig_fas_s"], row["rec_fases_s"]
                    xhl, per_fer, r = row["xhl"], row["per_fer"], row["r"]
                    rec_fases_t = row.get("rec_fases_t", "")
                    coord = row["coord_latlon"]

                    if phases_p == 1:
                        return (
                            f"New Transformer.{cod_id} Phases=3 Windings=2 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}.1.2.3 conn=delta kv={ten_pri} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=2 bus={pac_2}.1.2.3.0 conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n\n"
                        )
                    elif phases_p == 2:
                        ten_sec_adj = round(ten_sec / math.sqrt(3), 3)
                        return (
                            f"New Transformer.{cod_id} Phases=2 Windings=3 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}{rec_fases_p} conn=delta kv={ten_pri} kva={pot_nom} %r={r/2} tap=1\n"
                            f"~ wdg=2 bus={pac_2}{rec_fases_s} conn=wye kv={ten_sec_adj} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=3 bus={pac_2}{rec_fases_t} conn=wye kv={ten_sec_adj} kva={pot_nom} %r={r} tap=1\n\n"
                        )
                    elif phases_p == 3:
                        return (
                            f"New Transformer.{cod_id} Phases=3 Windings=2 xhl={xhl} %noloadloss={per_fer}\n"
                            f"~ wdg=1 bus={pac_1}.1.2.3 conn=delta kv={ten_pri} kva={pot_nom} %r={r} tap=1\n"
                            f"~ wdg=2 bus={pac_2}.1.2.3.0 conn=wye kv={ten_sec} kva={pot_nom} %r={r} tap=1\n\n"
                        )
                    return None

                chunk["linha"] = chunk.apply(gerar_linha, axis=1)

                for (_, row) in chunk.iterrows():
                    sub = row.get("sub", "sem_sub")
                    nome = row["nome"]
                    linha = row["linha"]
                    if linha:
                        linhas_dss.setdefault(sub, {}).setdefault(nome, []).append(linha)

                progress.update(task, advance=len(chunk))

        return linhas_dss



class RegulatorMediumVoltage:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("Modelando Reguladores MT...")

        df = df.drop_duplicates(subset="cod_id")
        console = Console()
        linhas_dss = {}

        def format_coord(coord_val):
            if isinstance(coord_val, str):
                try:
                    coord_val = ast.literal_eval(coord_val)
                except Exception:
                    pass
            if isinstance(coord_val, (list, tuple)):
                try:
                    return f"{round(coord_val[0], 6)}_{round(coord_val[1], 6)}"
                except (IndexError, TypeError, ValueError):
                    return "sem_coord"
            return str(coord_val).replace(" ", "_")[:30] or "sem_coord"

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

                chunk["cod_id"] = chunk["cod_id"].astype(str)
                chunk["pac_1"] = chunk["pac_1"].astype(str)
                chunk["pac_2"] = chunk["pac_2"].astype(str)
                chunk["nome"] = chunk["nome"].astype(str)
                chunk["coord_latlon"] = chunk["coord_latlon"].apply(format_coord)
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
                    coord = row["coord_latlon"]

                    return (
                        f"new transformer.reg{lig_fas_p}_{cod_id} phases={phases_p} windings=2 bank={cod_id} Maxtap=1.1 Mintap=0.9 ppm=0\n"
                        f"~ buses=({pac_1}{rec_fases_p} {pac_2}{rec_fases_s}) \n"
                        f"~ conns='wye wye' kvs='{ten_nom_adj} {ten_nom_adj}' \n"
                        f"~ kvas='{pot_nom} {pot_nom}' XHL = 0.1 %r={r}\n"
                        f"new regcontrol.coord_{coord}_creg{lig_fas_p}_{cod_id} transformer=reg{lig_fas_p}_{cod_id} winding=2 \n"
                        f"~ vreg={vreg:.5f} band={banda_volts} ptratio={rel_tp} ctprim={cor_nom}\n\n"
                    )

                chunk["linha"] = chunk.apply(gerar_linha, axis=1)

                agrupado = chunk.groupby("nome")["linha"].apply(
                    lambda x: [linha for linha in x if linha is not None]
                ).to_dict()

                for nome, linhas in agrupado.items():
                    linhas_dss.setdefault(nome, []).extend(linhas)

                progress.update(task, advance=len(chunk))

        return linhas_dss



class Subestation_Coords_latlon:
    @staticmethod
    def to_dss(df: pd.DataFrame, chunk_size=1000) -> dict:
        print("===================================================================================================")
        print("Modelando Coordenadas das subestações...")
        dss_dict = {}
        console = Console()

        def process_chunk(chunk: pd.DataFrame) -> dict:
            chunk_dict = {}
            for _, row in chunk.iterrows():
                
                cod_id = row['cod_id']
                nome = row['nome']
                coords = row['coord_latlon']

                linha = (
                        f"!coord_{coords}_nome_{nome}_cod_{cod_id} "
                )
                chunk_dict.setdefault(cod_id, []).append(linha)
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
