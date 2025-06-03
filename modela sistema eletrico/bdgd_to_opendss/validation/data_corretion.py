import pandas as pd
from bdgd_to_opendss.validation.config_loader import load_validation_config
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class ValidatorBarraSlack:
    def __init__(self):
        config = load_validation_config("barra_slack")

        self.ten_ope_padrao = config.get("ten_ope_padrao")
        self.ten_nom_padrao = config.get("ten_nom_codigo_padrao")
        self.ten_nom_volts_padrao = config.get("ten_nom_volts_padrao")
        self.limite_inferior_pu = config.get("limite_inferior_pu")
        self.limite_superior_pu = config.get("limite_superior_pu")
        self.tabela_tensoes = config.get("tabela_tensoes", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # nome: preenche nulos com "default"
        df["nome"] = df["nome"].fillna("default")

        # ten_ope: converte, preenche nulos, aplica limite
        df["ten_ope"] = pd.to_numeric(df["ten_ope"], errors="coerce").fillna(self.ten_ope_padrao)
        df["ten_ope"] = df["ten_ope"].clip(self.limite_inferior_pu, self.limite_superior_pu)

        # ten_nom: força int, substitui inválidos
        df["ten_nom"] = pd.to_numeric(df["ten_nom"], errors="coerce").fillna(self.ten_nom_padrao).astype(int)

        # ten_nom_voltage: mapeia usando dicionário de tensões
        tensoes_map = pd.Series(self.tabela_tensoes)
        df["ten_nom_voltage"] = df["ten_nom"].map(tensoes_map).fillna(self.ten_nom_volts_padrao)

        # Colunas finais
        return df[["nome", "ten_ope", "ten_nom", "ten_nom_voltage", "pac_ini"]]




class ValidatorCompensadorMedia:
    def __init__(self):
        config = load_validation_config("compensadores_media_tensao")

        self.ten_nom_padrao_codigo = config.get("ten_nom_compensadores_media_codigo")
        self.ten_nom_padrao_volts = config.get("ten_nom_compensadores_media_volts")
        self.pot_nom_padrao_kva = config.get("pot_nom_compensadores_kva")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})


    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas com nome ou pac_1 nulos
        df = df[df["nome"].notna() & df["pac_1"].notna()]

        # fas_con: preenche com 'ABC' se inválido ou nulo
        df["fas_con"] = df["fas_con"].apply(lambda x: x if isinstance(x, str) and x.isalpha() else "ABC")

        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()
        
        # ten_nom: tenta converter, senão aplica padrão
        df["ten_nom"] = pd.to_numeric(df["ten_nom"], errors="coerce").fillna(self.ten_nom_padrao_codigo).astype(int)

        # Mapeia ten_nom para volts
        tensoes_map = pd.Series(self.dados_tabela_tensoes)
        df["ten_nom_voltage"] = df["ten_nom"].map(tensoes_map).fillna(self.ten_nom_padrao_volts)

        # cod_id: se nulo, gera como nome + index
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(lambda row: row["cod_id"] if row["cod_id"] else f"{row['nome']}{row.name}", axis=1)

        # pot_nom: converte e aplica padrão ou limite
        df["pot_nom"] = pd.to_numeric(df["pot_nom"], errors="coerce").fillna(self.pot_nom_padrao_kva)
        df["pot_nom"] = df["pot_nom"].clip(upper=self.pot_nom_padrao_kva)

        # Colunas finais
        return df[["nome", "fas_con", "pac_1", "ten_nom", "ten_nom_voltage", "cod_id", "pot_nom", "rec_fases", "phases", "tip_unid"]]



class ValidatorCompensadorBaixa:
    def __init__(self):
        config = load_validation_config("compensadores_baixa_tensao")

        self.ten_nom_padrao_codigo = config.get("ten_nom_compensadores_baixa_codigo")
        self.ten_nom_padrao_volts = config.get("ten_nom_compensadores_baixa_volts")
        self.pot_nom_padrao_kva = config.get("pot_nom_compensadores_kva")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Elimina linhas com nome ou pac_1 nulos
        df = df[df["nome"].notna() & df["pac_1"].notna()]

        # fas_con: padrão "ABC" se nulo ou inválido
        df["fas_con"] = df["fas_con"].apply(lambda x: x if isinstance(x, str) and x.isalpha() else "ABC")
        

        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()
        

        # ten_nom: tentativa de conversão para int, fallback no padrão
        df["ten_nom"] = pd.to_numeric(df["ten_nom"], errors="coerce").fillna(self.ten_nom_padrao_codigo).astype(int)

        # Mapear ten_nom para tensão em volts
        tensoes_map = pd.Series(self.dados_tabela_tensoes)
        df["ten_nom_voltage"] = df["ten_nom"].map(tensoes_map).fillna(self.ten_nom_padrao_volts)

        # cod_id: gerar cod_id se nulo
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(lambda row: row["cod_id"] if row["cod_id"] else f"{row['nome']}{row.name}", axis=1)

        # pot_nom: tentativa de conversão, fallback e limite superior
        df["pot_nom"] = pd.to_numeric(df["pot_nom"], errors="coerce").fillna(self.pot_nom_padrao_kva)
        df["pot_nom"] = df["pot_nom"].clip(upper=self.pot_nom_padrao_kva)

        # Colunas finais
        return df[["nome", "fas_con", "pac_1", "ten_nom", "ten_nom_voltage", "cod_id", "pot_nom", "rec_fases", "phases", "tip_unid"]]




class ValidatorChaveSeccionadoraBT:
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        config = load_validation_config("chave_seccionadoras_baixa_tensao")

        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        
        df = df.copy()

        # Remover linhas com 'nome', 'pac_1' ou 'pac_2' nulos ou vazios
        df = df[df["nome"].notna() & df["pac_1"].notna() & df["pac_2"].notna()]
        df = df[~df["pac_1"].astype(str).str.strip().eq("")]
        df = df[~df["pac_2"].astype(str).str.strip().eq("")]

        # fas_con: atribui "ABCN" se vazio ou inválido
        df["fas_con"] = df["fas_con"].apply(
            lambda x: x if isinstance(x, str) and x.isalpha() else "ABCN"
        )
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # cod_id: se nulo, atribui "{nome}_{index}"
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(lambda row: row["cod_id"] if row["cod_id"] else f"{row['nome']}_{row.name}", axis=1)

        # Colunas finais
        return df[["nome", "fas_con", "pac_1", "pac_2", "cod_id", "p_n_ope", "phases", "rec_fases"]]

 


class ValidatorChaveSeccionadoraMT:
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        config = load_validation_config("chave_seccionadoras_media_tensao")

        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        
        df = df.copy()

        # Remove linhas com nome, pac_1 ou pac_2 nulos ou vazios
        df = df[df["nome"].notna() & df["pac_1"].notna() & df["pac_2"].notna()]
        df = df[~df["pac_1"].astype(str).str.strip().eq("")]
        df = df[~df["pac_2"].astype(str).str.strip().eq("")]

        # Validação fas_con: se inválido, atribuir "ABC"
        df["fas_con"] = df["fas_con"].apply(
            lambda x: x if isinstance(x, str) and x.isalpha() else "ABC"
        )
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # cod_id: gerar se vazio
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(
            lambda row: row["cod_id"] if row["cod_id"] else f"{row['nome']}_{row.name}",
            axis=1
        )

        # Retornar apenas as colunas relevantes
        return df[["nome", "fas_con", "pac_1", "pac_2", "cod_id", "p_n_ope", "phases", "rec_fases"]]




class ValidatorGeradorMediaTensao:
    def __init__(self):
        config = load_validation_config("gerador_media_tensao")

        self.pot_inst_geradores_media_kva = config.get("pot_inst_geradores_media_kva")
        self.ten_con_geradores_media_codigo = config.get("ten_con_geradores_media_codigo")
        self.ten_con_geradores_media_volts = config.get("ten_con_geradores_media_volts")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})


    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Filtrar linhas onde 'nome' é nulo -> removidas
        df = df[df["nome"].notna()]

        # Validar 'fas_con': manter se string alfa, senão 'ABC'
        df["fas_con"] = df["fas_con"].apply(
            lambda x: x if isinstance(x, str) and x.isalpha() else "ABC"
        )
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # Validar 'ten_con': usar padrão se inválido ou não numérico
        def validar_ten_con(x):
            if x is None or not str(x).isdigit():
                return self.ten_con_geradores_media_codigo
            try:
                return int(x)
            except:
                return self.ten_con_geradores_media_codigo

        df["ten_con"] = df["ten_con"].apply(validar_ten_con)

        # Buscar tensão nominal na tabela, usar padrão se não encontrado
        df["ten_nom_voltage"] = df["ten_con"].apply(
            lambda x: self.dados_tabela_tensoes.get(x, self.ten_con_geradores_media_volts)
        )

        # Validar pot_inst: remover linhas onde não seja int ou float
        df = df[df["pot_inst"].apply(lambda x: isinstance(x, (int, float)))]

        # Limitar pot_inst ao máximo configurado
        df["pot_inst"] = df["pot_inst"].apply(
            lambda x: min(x, self.pot_inst_geradores_media_kva)
        )

        # Validar cod_id: preencher vazios com nome_index
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(
            lambda row: row["cod_id"] if row["cod_id"] else f"{row['nome']}_{row.name}",
            axis=1
        )

        # Selecionar colunas finais
        return df[["nome", "fas_con", "ten_con", "ten_nom_voltage", "pot_inst", "cod_id", "pac", "rec_fases", "phases"]]

  


class ValidatorLinecodesBaixaTensao:
    def __init__(self):
        config = load_validation_config("linecodes_baixa_tensao")
        self.r1_max = config.get("r1_linecodes_baixa_ohms_km")
        self.x1_max = config.get("x1_linecodes_baixa_ohms_km")
        self.cnom_max = config.get("cnom_linecodes_baixa_amperes")
        self.cmax_max = config.get("cmax_renamed_linecodes_baixa_amperes")

        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas com nome vazio
        df = df[df["nome"].notnull()]
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # fas_con: preenche com "ABC" onde não é string ou não é alpha
        df["fas_con"] = df["fas_con"].where(df["fas_con"].apply(lambda x: isinstance(x, str) and x.isalpha()), "ABC")

        # r1
        df["r1"] = pd.to_numeric(df["r1"], errors="coerce")
        df["r1"].fillna(self.r1_max, inplace=True)
        df["r1"] = np.minimum(df["r1"], self.r1_max)

        # x1
        df["x1"] = pd.to_numeric(df["x1"], errors="coerce")
        df["x1"].fillna(self.x1_max, inplace=True)
        df["x1"] = np.minimum(df["x1"], self.x1_max)

        # cnom
        df["cnom"] = pd.to_numeric(df["cnom"], errors="coerce")
        df["cnom"].fillna(self.cnom_max, inplace=True)
        df["cnom"] = np.minimum(df["cnom"], self.cnom_max)

        # cmax_renamed
        df["cmax_renamed"] = pd.to_numeric(df["cmax_renamed"], errors="coerce")
        df["cmax_renamed"].fillna(self.cmax_max, inplace=True)
        df["cmax_renamed"] = np.minimum(df["cmax_renamed"], self.cmax_max)

        # Colunas finais
        return df[["nome", "tip_cnd", "fas_con", "r1", "x1", "cnom", "cmax_renamed", "rec_fases", "phases"]]

    


class ValidatorLinecodesMediaTensao:
    def __init__(self):
        config = load_validation_config("linecodes_media_tensao")

        self.r1_max = config.get("r1_linecodes_media_ohms_km")
        self.x1_max = config.get("x1_linecodes_media_ohms_km")
        self.cnom_max = config.get("cnom_linecodes_media_amperes")
        self.cmax_max = config.get("cmax_renamed_linecodes_media_amperes")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas com 'nome' vazio
        df = df[df["nome"].notnull()]
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # fas_con: substitui valores inválidos por "ABC"
        df["fas_con"] = df["fas_con"].where(
            df["fas_con"].apply(lambda x: isinstance(x, str) and x.isalpha()), "ABC"
        )

        # r1
        df["r1"] = pd.to_numeric(df["r1"], errors="coerce").fillna(self.r1_max)
        df["r1"] = np.minimum(df["r1"], self.r1_max)

        # x1
        df["x1"] = pd.to_numeric(df["x1"], errors="coerce").fillna(self.x1_max)
        df["x1"] = np.minimum(df["x1"], self.x1_max)

        # cnom
        df["cnom"] = pd.to_numeric(df["cnom"], errors="coerce").fillna(self.cnom_max)
        df["cnom"] = np.minimum(df["cnom"], self.cnom_max)

        # cmax_renamed
        df["cmax_renamed"] = pd.to_numeric(df["cmax_renamed"], errors="coerce").fillna(self.cmax_max)
        df["cmax_renamed"] = np.minimum(df["cmax_renamed"], self.cmax_max)

        # Retorna somente colunas relevantes
        return df[["nome", "tip_cnd", "fas_con", "r1", "x1", "cnom", "cmax_renamed", "rec_fases", "phases"]]




class ValidatorLinecodesRamais:
    def __init__(self):
        config = load_validation_config("linecodes_ramais")
        self.r1_max = config.get("r1_linecodes_baixa_ohms_km")
        self.x1_max = config.get("x1_linecodes_baixa_ohms_km")
        self.cnom_max = config.get("cnom_linecodes_baixa_amperes")
        self.cmax_max = config.get("cmax_renamed_linecodes_baixa_amperes")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remover linhas sem 'nome'
        df = df[df["nome"].notnull()]
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # fas_con: padrão "ABC" para valores inválidos
        df["fas_con"] = df["fas_con"].where(
            df["fas_con"].apply(lambda x: isinstance(x, str) and x.isalpha()), "ABC"
        )

        # r1
        df["r1"] = pd.to_numeric(df["r1"], errors="coerce").fillna(self.r1_max)
        df["r1"] = np.minimum(df["r1"], self.r1_max)

        # x1
        df["x1"] = pd.to_numeric(df["x1"], errors="coerce").fillna(self.x1_max)
        df["x1"] = np.minimum(df["x1"], self.x1_max)

        # cnom
        df["cnom"] = pd.to_numeric(df["cnom"], errors="coerce").fillna(self.cnom_max)
        df["cnom"] = np.minimum(df["cnom"], self.cnom_max)

        # cmax_renamed
        df["cmax_renamed"] = pd.to_numeric(df["cmax_renamed"], errors="coerce").fillna(self.cmax_max)
        df["cmax_renamed"] = np.minimum(df["cmax_renamed"], self.cmax_max)

        # Seleciona somente colunas relevantes
        return df[["nome", "tip_cnd", "fas_con", "r1", "x1", "cnom", "cmax_renamed", "rec_fases", "phases"]]



class ValidatorLinhasBaixaTensao:
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        config = load_validation_config("linhas_baixa")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})

        df = df.copy()

        # Filtrar linhas onde 'nome' está vazio ou nulo
        df = df[df["nome"].notna()]
        df = df[df["nome"].astype(str).str.strip() != ""]

        # Validar 'fas_con': manter se string alfa, senão 'ABC'
        df["fas_con"] = df["fas_con"].apply(
            lambda x: x if isinstance(x, str) and x.isalpha() else "ABC"
        )
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # Validar 'comp': se não numérico, atribuir 1.0 e converter para float
        def validar_comp(x):
            if isinstance(x, (int, float)):
                return float(x)
            try:
                return float(x)
            except:
                print(f"Valor inválido em 'comp': {x}. Usando padrão 1 metro.")
                return 1.0

        df["comp"] = df["comp"].apply(validar_comp)

        # Filtrar linhas onde 'pac_1' ou 'pac_2' estejam nulos ou falsy
        df = df[df["pac_1"].notna() & df["pac_1"].astype(str).str.strip().ne("")]
        df = df[df["pac_2"].notna() & df["pac_2"].astype(str).str.strip().ne("")]

        # Selecionar colunas finais na ordem desejada
        return df[["cod_id", "pac_1", "pac_2", "nome", "fas_con", "comp", "tip_cnd", "rec_fases", "phases"]]


class ValidatorLinhasMediaTensao:
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        config = load_validation_config("linhas_media")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        df = df.copy()

        # Remove linhas onde 'nome' é vazio ou nulo
        df = df[df["nome"].notna()]
        df = df[df["nome"].astype(str).str.strip() != ""]

        # Valida fas_con: só aceita string alfa, senão 'ABC'
        df["fas_con"] = df["fas_con"].apply(
            lambda x: x if isinstance(x, str) and x.isalpha() else "ABC"
        )
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # Valida comp: se não numérico, padrão 1.0
        def validar_comp(x):
            if isinstance(x, (int, float)):
                return float(x)
            try:
                return float(x)
            except:
                print(f"Valor inválido em 'comp': {x}. Usando padrão 1 metro.")
                return 1.0
        df["comp"] = df["comp"].apply(validar_comp)

        # Valida cod_id: preenche vazio com {nome}_{index}
        df["cod_id"] = df["cod_id"].fillna("")
        df["cod_id"] = df.apply(
            lambda row: row["cod_id"] if str(row["cod_id"]).strip() != "" else f"{row['nome']}_{row.name}",
            axis=1
        )

        # Remove linhas onde pac_1 ou pac_2 são nulos ou vazios
        df = df[df["pac_1"].notna() & df["pac_1"].astype(str).str.strip().ne("")]
        df = df[df["pac_2"].notna() & df["pac_2"].astype(str).str.strip().ne("")]



        # Ordena e retorna as colunas
        return df[["cod_id", "pac_1", "pac_2", "nome", "fas_con", "comp", "tip_cnd", "rec_fases", "phases"]]



class ValidadorRamaisLigacao:
    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        config = load_validation_config("linhas_ramais")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        df = df.copy()

        # Remove linhas sem nome válido
        df = df[df['nome'].notna() & (df['nome'] != '')]

        # fas_con: se não for string alfa, substitui por 'ABC'
        mask_fas_con = ~df['fas_con'].apply(lambda x: isinstance(x, str) and x.isalpha())
        df.loc[mask_fas_con, 'fas_con'] = 'ABC'
        
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        

        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

        # comp: se não numérico, substitui por 1
        mask_comp = ~df['comp'].apply(lambda x: isinstance(x, (int, float)))
        df.loc[mask_comp, 'comp'] = 1

        # cod_id: se vazio, preenche com nome_index
        cod_id_empty = df['cod_id'].isna() | (df['cod_id'] == '')
        df.loc[cod_id_empty, 'cod_id'] = df.loc[cod_id_empty].apply(lambda row: f"{row['nome']}_{row.name}", axis=1)

        # Remove linhas onde pac_1 ou pac_2 estão vazios, nulos ou zero
        df = df[df['pac_1'].notna() & (df['pac_1'] != 0)]
        df = df[df['pac_2'].notna() & (df['pac_2'] != 0)]


        return df[["cod_id", "pac_1", "pac_2", "nome", "fas_con", "comp", "tip_cnd", "rec_fases", "phases"]]


class ValidadorCargasBaixaTensao:
    def __init__(self):
        config = load_validation_config("cargas_baixa_tensao")
        self.ten_forn_cargas_baixa_codigo = config.get("ten_forn_cargas_baixa_codigo")
        self.ten_forn_cargas_baixa_volts = config.get("ten_forn_cargas_baixa_volts")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        
        # Remove linhas onde 'nome' é vazio/nulo
        df = df[df["nome"].notna() & (df["nome"].astype(str).str.strip() != "")]

        # Validação fas_con (só letras), substitui por 'ABCN' se inválido
        def validar_fas_con(x):
            return x if isinstance(x, str) and x.isalpha() else "ABCN"
        df["fases"] = [validar_fas_con(val) for val in df["fases"]]

        # Validação ten_forn (numérico ou string numérica), substitui por padrão se inválido
        def validar_ten_forn(val):
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str) and val.isdigit():
                return int(val)
            return self.ten_forn_cargas_baixa_codigo
        df["ten_con"] = [validar_ten_forn(val) for val in df["ten_con"]]

        # Verifica tensão nominal no dicionário, substitui se não encontrado
        def validar_ten_nom_voltage(ten_forn):
            return self.dados_tabela_tensoes.get(ten_forn, self.ten_forn_cargas_baixa_volts)
        df["ten_nom_voltage"] = [validar_ten_nom_voltage(val) for val in df["ten_con"]]

        # cod_id: preenche vazio/nulo com padrão {nome}_{index}
        def validar_cod_id(cod_id, nome, idx):
            if cod_id and str(cod_id).strip() != "":
                return cod_id
            return f"{nome}_{idx}"
        df["cod_id"] = [validar_cod_id(cod, nome, idx) for idx, (cod, nome) in enumerate(zip(df["cod_id"], df["nome"]))]

        # Remove linhas onde 'pac' é vazio/nulo
        df = df[df["pac"].notna() & (df["pac"].astype(str).str.strip() != "")]

        df["rec_fases"] = df["fases"].map(self.dados_mapa_fases)
        df["phases"] = df["fases"].str.replace("N", "").str.len()


        return df[["cod_id", "pac", "nome", "potencias", "tip_cc", "conn", "phases", "rec_fases", "ten_nom_voltage"]]




class ValidadorCargasMediaTensao:
    def __init__(self):
        config = load_validation_config("cargas_media_tensao")
        self.ten_forn_cargas_media_codigo = config.get("ten_forn_cargas_media_codigo")
        self.ten_forn_cargas_media_volts = config.get("ten_forn_cargas_media_volts")
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas onde 'nome' é vazio/nulo
        df = df[df["nome"].notna() & (df["nome"].astype(str).str.strip() != "")]

   
        
        # Validação fas_con (só letras), substitui por 'ABCN' se inválido
        def validar_fas_con(x):
            return x if isinstance(x, str) and x.isalpha() else "ABCN"
        df["fases"] = [validar_fas_con(val) for val in df["fases"]]

        # Validação ten_forn (numérico ou string numérica), substitui por padrão se inválido
        def validar_ten_forn(val):
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str) and val.isdigit():
                return int(val)
            return self.ten_forn_cargas_media_codigo
        df["ten_con"] = [validar_ten_forn(val) for val in df["ten_con"]]

        # Verifica tensão nominal no dicionário, substitui se não encontrado
        def validar_ten_nom_voltage(ten_forn):
            return self.dados_tabela_tensoes.get(ten_forn, self.ten_forn_cargas_media_volts)
        df["ten_nom_voltage"] = [validar_ten_nom_voltage(val) for val in df["ten_con"]]

        # cod_id: preenche vazio/nulo com padrão {nome}_{index}
        def validar_cod_id(cod_id, nome, idx):
            if cod_id and str(cod_id).strip() != "":
                return cod_id
            return f"{nome}_{idx}"
        df["cod_id"] = [validar_cod_id(cod, nome, idx) for idx, (cod, nome) in enumerate(zip(df["cod_id"], df["nome"]))]

        # Remove linhas onde 'pac' é vazio/nulo
        df = df[df["pac"].notna() & (df["pac"].astype(str).str.strip() != "")]

        df["rec_fases"] = df["fases"].map(self.dados_mapa_fases)
        df["phases"] = df["fases"].str.replace("N", "").str.len()


        return df[["cod_id", "pac", "nome", "potencias", "tip_cc", "conn", "phases", "rec_fases", "ten_nom_voltage"]]



class ValidadorGDBaixaTensao:
    def __init__(self):
        config = load_validation_config("gd_baixa_tensao")
        self.ten_con_gd_baixa_codigo = config.get("ten_con_gd_baixa_codigo")
        self.ten_con_gd_baixa_volts = config.get("ten_con_gd_baixa_volts")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})


    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas com nome vazio
        df = df[df["nome"].notna() & (df["nome"].astype(str).str.strip() != "")]

        # fas_con: se inválido, seta padrão 'ABCN'
        def validar_fas_con(x):
            if isinstance(x, str) and x.isalpha():
                return x
            return "ABCN"
        df["fas_con"] = df["fas_con"].apply(validar_fas_con)

   
        # ten_nom_voltage baseado em ten_con, senão padrão
        df["ten_nom_voltage"] = df["ten_con"].map(self.dados_tabela_tensoes).fillna(self.ten_con_gd_baixa_volts)

        # cod_id: preenche vazio com nome_index
        def validar_cod_id(cod_id, nome, idx):
            if cod_id and str(cod_id).strip() != "":
                return cod_id
            return f"{nome}_{idx}"
        df["cod_id"] = [validar_cod_id(c, n, i) for i, (c, n) in enumerate(zip(df["cod_id"], df["nome"]))]

        # Remove linhas com pac vazio
        df = df[df["pac"].notna() & (df["pac"].astype(str).str.strip() != "")]

        # pot_inst: corrige valores não numéricos ou zero para 1
        def validar_pot_inst(val):
            if isinstance(val, (int, float)):
                if val == 0 or val is None:
                    return 1
                return val
            return 1
        df["pot_inst"] = df["pot_inst"].apply(validar_pot_inst)
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        df["phases"] = df["fas_con"].str.replace("N", "").str.len()

 
        return df.reset_index(drop=True)



class ValidadorGDMediaTensao:
    def __init__(self):
        config = load_validation_config("gd_media_tensao")
        self.ten_con_gd_media_codigo = config.get("ten_con_gd_media_codigo")
        self.ten_con_gd_media_volts = config.get("ten_con_gd_media_volts")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Remove linhas com nome vazio ou nulo
        df = df[df["nome"].notna() & (df["nome"].astype(str).str.strip() != "")]

        # fas_con: mantém se for string alfa, senão padrão 'ABCN'
        def validar_fas_con(x):
            if isinstance(x, str) and x.isalpha():
                return x
            return "ABCN"
        df["fas_con"] = df["fas_con"].apply(validar_fas_con)

        # ten_con: converte para int se possível, senão padrão
        def validar_ten_con(val):
            try:
                if isinstance(val, (int, float)):
                    return int(val)
                if isinstance(val, str) and val.isdigit():
                    return int(val)
            except:
                pass
            return self.ten_con_gd_media_codigo
        df["ten_con"] = df["ten_con"].apply(validar_ten_con)

        # ten_nom_voltage: busca na tabela, senão padrão
        df["ten_nom_voltage"] = df["ten_con"].map(self.dados_tabela_tensoes).fillna(self.ten_con_gd_media_volts)

        # cod_id: preenche vazio com nome_index
        def validar_cod_id(cod_id, nome, idx):
            if cod_id and str(cod_id).strip() != "":
                return cod_id
            return f"{nome}_{idx}"
        df["cod_id"] = [validar_cod_id(c, n, i) for i, (c, n) in enumerate(zip(df["cod_id"], df["nome"]))]

        # Remove linhas com pac vazio ou nulo
        df = df[df["pac"].notna() & (df["pac"].astype(str).str.strip() != "")]

        # pot_inst: corrige valores zero, nulos ou não numéricos para 1
        def validar_pot_inst(val):
            if isinstance(val, (int, float)):
                if val == 0 or val is None:
                    return 1
                return val
            return 1
        df["pot_inst"] = df["pot_inst"].apply(validar_pot_inst)
        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        df["phases"] = df["fas_con"].str.replace("N", "").str.len()


        return df.reset_index(drop=True)




class ValidadorPIP:
    def __init__(self):
        config = load_validation_config("pip")
        self.ten_con_pip_codigo = config.get("ten_con_codigo")
        self.ten_con_pip_volts = config.get("ten_con_volts")
        self.dados_tabela_tensoes = config.get("tabela_tensoes", {})
        self.dados_mapa_fases = config.get("tabela_mapa_fases", {})


    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Remover linhas sem 'nome'
        df = df[df['nome'].notna() & (df['nome'] != '')].copy()

        # fas_con: se não for string de letras, substituir por 'ABC'
        mask_fas_con = ~df['fas_con'].apply(lambda x: isinstance(x, str) and x.isalpha())
        df.loc[mask_fas_con, 'fas_con'] = 'ABC'

 
        # ten_nom_voltage: buscar tabela, se não encontrado usar padrão
        df['ten_nom_voltage'] = df['ten_forn'].map(self.dados_tabela_tensoes).fillna(self.ten_con_pip_volts)

        # cod_id: se vazio, criar com nome_index
        cod_id_empty = df['cod_id'].isna() | (df['cod_id'] == '')
        df.loc[cod_id_empty, 'cod_id'] = df.loc[cod_id_empty].apply(lambda row: f"{row['nome']}_{row.name}", axis=1)

        # pac: remover linhas com pac vazio ou NaN ou 0
        df = df[df['pac'].notna() & (df['pac'] != 0)]

        df["rec_fases"] = df["fas_con"].map(self.dados_mapa_fases)
        df["phases"] = df["fas_con"].str.replace("N", "").str.len()
        
        return df.reset_index(drop=True)








class ValidadorTransformadoresMediaTensao:
    def __init__(self):
        config = load_validation_config("transformadores_media_tensao")
        
        self.dados_mapa_fases = config.get("tabela_mapa_de_fases", {})
        self.pot_nom_default = config.get("pot_nom_transformadores")
        self.trafos_pot_nom = config.get("tabela_trafos_pot_nom", {})
        self.ten_pri_codigo = config.get("ten_pri_transformadores_media_codigo")
        self.ten_pri_volts = config.get("ten_pri_transformadores_media_volts")
        self.ten_sec_codigo = config.get("ten_sec_transformadores_media_codigo")
        self.ten_sec_volts = config.get("ten_sec_transformadores_media_volts")
        self.tensoes = config.get("tabela_tensoes", {})
        self.r1_default = config.get("r1_transformadores_media")
        self.xhl_default = config.get("xhl_transformadores_media")
        self.per_fer_default = config.get("per_fer_transformadores_media")

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Filtra linhas com nome válido
        df = df[df['nome'].notna() & (df['nome'] != '')]

        # Validar pot_nom
        def map_pot_nom(x):
            if not str(x).isdigit():
                return self.pot_nom_default
            try:
                val = int(x)
            except:
                return self.pot_nom_default
            mapped = self.trafos_pot_nom.get(val, "não encontrado")
            if mapped == "não encontrado" or (isinstance(mapped, (int, float)) and mapped > self.pot_nom_default):
                return self.pot_nom_default
            return mapped
        
        df['pot_nom'] = df['pot_nom'].apply(map_pot_nom)

 
        # Validar ten_pri
        def map_tensao(x, codigo_padrao, volts_padrao):
            if not str(x).isdigit():
                return volts_padrao
            try:
                val = int(x)
            except:
                return volts_padrao
            mapped = self.tensoes.get(val, "não encontrado")
            if mapped == "não encontrado":
                return volts_padrao
            return mapped

        df['ten_pri'] = df['ten_pri'].apply(lambda x: map_tensao(x, self.ten_pri_codigo, self.ten_pri_volts))
        df['ten_sec'] = df['ten_sec'].apply(lambda x: map_tensao(x, self.ten_sec_codigo, self.ten_sec_volts))

        # Remover linhas onde pac_1 ou pac_2 são nulos ou falsy (0, None, '')
        df = df[df['pac_1'].notna() & (df['pac_1'] != 0)]
        df = df[df['pac_2'].notna() & (df['pac_2'] != 0)]

        # Validar lig_fas_p e lig_fas_s
        df['lig_fas_p'] = np.where(
            df['lig_fas_p'].apply(lambda x: isinstance(x, str) and x.isalpha()),
            df['lig_fas_p'],
            'ABC'
        )
        
        df["rec_fases_p"] = df["lig_fas_p"].map(self.dados_mapa_fases)
        df["phases_p"] = df["lig_fas_p"].str.replace("N", "").str.len()

        df['lig_fas_s'] = np.where(
            df['lig_fas_s'].apply(lambda x: isinstance(x, str) and x.isalpha()),
            df['lig_fas_s'],
            'ABCN'
        )
        
        df["rec_fases_s"] = df["lig_fas_s"].map(self.dados_mapa_fases)
        df["phases_s"] = df["lig_fas_s"].str.replace("N", "").str.len()

        # Validar r, xhl, per_fer com limites máximos e defaults
        def validate_limit(value, default):
            if not isinstance(value, (int, float)):
                return default
            value = float(value)
            return min(value, default)

        df['r'] = df['r'].apply(lambda x: validate_limit(x, self.r1_default))
        df['xhl'] = df['xhl'].apply(lambda x: validate_limit(x, self.xhl_default))
        df['per_fer'] = df['per_fer'].apply(lambda x: validate_limit(x, self.per_fer_default))

        return df.reset_index(drop=True)




class ValidadorReguladoresMediaTensao:
    def __init__(self):
        config = load_validation_config("reguladores_media_tensao")
      
        self.tabela_mapa_de_fases = config.get("tabela_mapa_de_fases", {})
        self.pot_nom_reguladores = config.get("pot_nom_reguladores")
        self.tabela_trafos_pot_nom = config.get("tabela_trafos_pot_nom", {})
        self.ten_reguladores_media_codigo = config.get("ten_reguladores_media_codigo")
        self.ten_reguladores_media_volts = config.get("ten_reguladores_media_volts")
        self.tabela_tensoes = config.get("tabela_tensoes", {})
        self.r1_reguladores_media = config.get("r1_reguladores_media")
        self.xhl_reguladores_media = config.get("xhl_reguladores_media")
        self.per_fer_reguladores_media = config.get("per_fer_reguladores_media")

    def validate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Filtra linhas com nome válido
        df = df[df['nome'].notna() & (df['nome'] != '')]

        # Validar pot_nom
        def map_pot_nom(x):
            if not str(x).isdigit():
                return self.pot_nom_reguladores
            try:
                val = int(x)
            except:
                return self.pot_nom_reguladores
            mapped = self.tabela_trafos_pot_nom.get(val, "não encontrado")
            if mapped == "não encontrado" or (isinstance(mapped, (int, float)) and mapped > self.pot_nom_reguladores):
                return self.pot_nom_reguladores
            return mapped
        
        df['pot_nom'] = df['pot_nom'].apply(map_pot_nom)

 
        # Validar ten_pri
        def map_tensao(x, codigo_padrao, volts_padrao):
            if not str(x).isdigit():
                return volts_padrao
            try:
                val = int(x)
            except:
                return volts_padrao
            mapped = self.tabela_tensoes.get(val, "não encontrado")
            if mapped == "não encontrado":
                return volts_padrao
            return mapped

        df['ten_nom'] = df['ten_nom'].apply(lambda x: map_tensao(x, self.ten_reguladores_media_codigo, self.ten_reguladores_media_volts))

        # Remover linhas onde pac_1 ou pac_2 são nulos ou falsy (0, None, '')
        df = df[df['pac_1'].notna() & (df['pac_1'] != 0)]
        df = df[df['pac_2'].notna() & (df['pac_2'] != 0)]

        # Validar lig_fas_p e lig_fas_s
        df['lig_fas_p'] = np.where(
            df['lig_fas_p'].apply(lambda x: isinstance(x, str) and x.isalpha()),
            df['lig_fas_p'],
            'ABC'
        )
        
        df["rec_fases_p"] = df["lig_fas_p"].map(self.tabela_mapa_de_fases)
        df["phases_p"] = df["lig_fas_p"].str.replace("N", "").str.len()

        df['lig_fas_s'] = np.where(
            df['lig_fas_s'].apply(lambda x: isinstance(x, str) and x.isalpha()),
            df['lig_fas_s'],
            'ABCN'
        )
        
        df["rec_fases_s"] = df["lig_fas_s"].map(self.tabela_mapa_de_fases)
        df["phases_s"] = df["lig_fas_s"].str.replace("N", "").str.len()

        # Validar r, xhl, per_fer com limites máximos e defaults
        def validate_limit(value, default):
            if not isinstance(value, (int, float)):
                return default
            value = float(value)
            return min(value, default)

        df['r'] = df['r'].apply(lambda x: validate_limit(x, self.r1_reguladores_media))
        df['xhl'] = df['xhl'].apply(lambda x: validate_limit(x, self.xhl_reguladores_media))
        df['per_fer'] = df['per_fer'].apply(lambda x: validate_limit(x, self.per_fer_reguladores_media))

        return df.reset_index(drop=True)





