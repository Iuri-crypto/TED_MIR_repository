import pandas as pd
from bdgd_to_opendss.modelar.load_curve_processor import BTLoadCurveProcessor
from bdgd_to_opendss.modelar.load_curve_processor import MTLoadCurveProcessor

import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy.*")


class load_bdgd:
    
    @staticmethod
    def barra_slack(conn_mt) -> pd.DataFrame:
        query = "SELECT DISTINCT nome, ten_ope, ten_nom, pac_ini, sub FROM ctmt ORDER BY sub"
        try:
            return pd.read_sql(query, conn_mt)
        except Exception as e:
            print(f"Erro ao carregar barra slack: {e}")
            return pd.DataFrame()


    @staticmethod
    def compensadores_reativo_media(conn) -> pd.DataFrame:
        query1 = """
            SELECT DISTINCT ctmt.nome, uncrmt.fas_con, uncrmt.tip_unid, uncrmt.pot_nom,
                            uncrmt.pac_1, ctmt.ten_nom, uncrmt.cod_id, uncrmt.sub, uncrmt.ctmt
            FROM uncrmt
            JOIN ctmt ON uncrmt.ctmt = ctmt.cod_id
            ORDER BY uncrmt.sub
        """

        try:
            df1 = pd.read_sql(query1, conn)
            
            return df1
        except Exception as e:
            print(f"Erro ao carregar compensadores reativos média tensão: {e}")
        return pd.DataFrame()


    @staticmethod
    def compensadores_reativo_baixa(conn) -> pd.DataFrame:
        query1 = """
             SELECT DISTINCT ctmt.nome, uncrbt.fas_con, uncrbt.tip_unid, uncrbt.pot_nom,
                            uncrbt.pac_1, ctmt.ten_nom, uncrbt.cod_id, uncrbt.sub, uncrbt.ctmt
            FROM uncrbt
            JOIN ctmt ON uncrbt.ctmt = ctmt.cod_id
            ORDER BY uncrbt.sub
        """
      
        try:
            df1 = pd.read_sql(query1, conn)
            return df1
        except Exception as e:
            print(f"Erro ao carregar compensadores reativos baixa tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def chaves_seccionadoras_baixa_tensao(conn) -> pd.DataFrame:
        query = """
            
            SELECT DISTINCT 
                            unsebt.sub,
                            unsebt.pac_1, 
                            unsebt.pac_2, 
                            unsebt.cod_id, 
                            unsebt.ctmt, 
                            unsebt.cor_nom,
                            unsebt.fas_con, 
                            ctmt.nome, 
                            unsebt.p_n_ope
                        FROM unsebt 
                        JOIN ctmt ON unsebt.ctmt = ctmt.cod_id
                        ORDER BY unsebt.sub;
        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar chaves seccionadoras baixa tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def chaves_seccionadoras_media_tensao(conn) -> pd.DataFrame:
        query = """
           SELECT DISTINCT 
                            unsemt.sub,
                            unsemt.pac_1, 
                            unsemt.pac_2, 
                            unsemt.cod_id, 
                            unsemt.ctmt, 
                            unsemt.cor_nom,
                            unsemt.fas_con, 
                            ctmt.nome, 
                            unsemt.p_n_ope
            FROM unsemt 
            JOIN ctmt ON unsemt.ctmt = ctmt.cod_id
            ORDER BY unsemt.sub;
        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar chaves seccionadoras média tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def geradores_media_tensao(conn) -> pd.DataFrame:
        query = """
            SELECT 
                        DISTINCT ugmt_tab.cod_id, 
                        ugmt_tab.pac, 
                        ugmt_tab.ctmt, 
                        ugmt_tab.fas_con,
                        ugmt_tab.ten_con, 
                        ugmt_tab.pot_inst, 
                        ugmt_tab.cep, 
                        ugmt_tab.ceg_gd, 
                        ugmt_tab.sub,
                        ctmt.nome
            FROM ugmt_tab 
            JOIN ctmt ON ugmt_tab.ctmt = ctmt.cod_id
            WHERE ugmt_tab.ceg_gd NOT LIKE 'GD%' 
            AND ugmt_tab.ceg_gd NOT LIKE 'UFV%'
            ORDER BY ugmt_tab.sub;

        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar geradores média tensão: {e}")
            return pd.DataFrame()


    @staticmethod
    def linecodes_baixa_tensao(conn) -> pd.DataFrame:
        query = """
            select distinct 
                        ctmt.nome, 
                        ssdbt.tip_cnd, 
                        ssdbt.fas_con, 
						ssdbt.sub,
                        (select r1 from segcon where segcon.cod_id = ssdbt.tip_cnd limit 1) as r1,
                        (select x1 from segcon where segcon.cod_id = ssdbt.tip_cnd limit 1) as x1,
                        (select cnom from segcon where segcon.cod_id = ssdbt.tip_cnd limit 1) as cnom,
                        (select cmax_renamed from segcon where segcon.cod_id = ssdbt.tip_cnd limit 1) as cmax_renamed
            from ssdbt
            join ctmt on ctmt.cod_id = ssdbt.ctmt;


        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar linecodes baixa tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def linecodes_media_tensao(conn) -> pd.DataFrame:
        query = """
            select distinct 
            ctmt.nome, 
            ssdmt.tip_cnd, 
            ssdmt.fas_con, 
            ssdmt.sub,
            (select r1 from segcon where segcon.cod_id = ssdmt.tip_cnd limit 1) as r1,
            (select x1 from segcon where segcon.cod_id = ssdmt.tip_cnd limit 1) as x1,
            (select cnom from segcon where segcon.cod_id = ssdmt.tip_cnd limit 1) as cnom,
            (select cmax_renamed from segcon where segcon.cod_id = ssdmt.tip_cnd limit 1) as cmax_renamed
            from ssdmt
            join ctmt on ctmt.cod_id = ssdmt.ctmt;


        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar linecodes média tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def linecodes_ramais(conn) -> pd.DataFrame:
        query = """
 
        select distinct 
            ctmt.nome, 
            ramlig.tip_cnd, 
            ramlig.fas_con, 
            ramlig.sub,
            (select r1 from segcon where segcon.cod_id = ramlig.tip_cnd limit 1) as r1,
            (select x1 from segcon where segcon.cod_id = ramlig.tip_cnd limit 1) as x1,
            (select cnom from segcon where segcon.cod_id = ramlig.tip_cnd limit 1) as cnom,
            (select cmax_renamed from segcon where segcon.cod_id = ramlig.tip_cnd limit 1) as cmax_renamed
            from ramlig
            join ctmt on ctmt.cod_id = ramlig.ctmt;

        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar linecodes ramais baixa tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def linhas_baixa_tensao(conn) -> pd.DataFrame:
        query = """
            SELECT distinct ssdbt.cod_id, pac_1, pac_2, ctmt.nome, fas_con, comp, tip_cnd, ssdbt.sub
            FROM ssdbt
            JOIN ctmt ON ctmt.cod_id = ssdbt.ctmt

        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar linhas baixa tensão: {e}")
            return pd.DataFrame()


    @staticmethod
    def linhas_media_tensao(conn) -> pd.DataFrame:
        query = """
            SELECT DISTINCT ssdmt.cod_id, pac_1, pac_2, ctmt.nome, fas_con, comp, tip_cnd, ssdmt.sub
            FROM ssdmt
            JOIN ctmt ON ctmt.cod_id = ssdmt.ctmt
        """
        try:
            return pd.read_sql(query, conn)
        except Exception as e:
            print(f"Erro ao carregar linhas média tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def cargas_baixa_tensao_paginado(conn, conn_aneel, limit: int = 500000) -> pd.DataFrame:
        """Consulta paginada das cargas em baixa tensão"""
        
        ugbt_ugmt_aneel = """
                    SELECT 
                        s.codgeracaodistribuida, 
                        s.mdpotenciainstalada, 
                        s.mdpotenciaoutorgada 
                    FROM 
                        gd_aneel_2025_base AS s;

        """
        
        ugbt_ugmt_bdgd = """
                    SELECT 
                            ugbt_tab.ctmt, 
                            ugbt_tab.pot_inst, 
                            ugbt_tab.ceg_gd, 
                            ctmt.nome,
							ugbt_tab.sub
                    FROM 
                            ugbt_tab 
                    JOIN 
                            ctmt 
                    ON 
                            ugbt_tab.ctmt = ctmt.cod_id

                    UNION ALL

                    SELECT 
                            ugmt_tab.ctmt, 
                            ugmt_tab.pot_inst, 
                            ugmt_tab.ceg_gd, 
                            ctmt.nome,
							ugmt_tab.sub
                    FROM 
                            ugmt_tab 
                    JOIN 
                            ctmt 
                    ON 
                            ugmt_tab.ctmt = ctmt.cod_id;
        """

        query_total = """
            SELECT COUNT(*) AS total
            FROM ucbt_tab
            WHERE gru_ten = 'BT'
        """

        query_data_template = """
            SELECT DISTINCT
                        ucbt_tab.cod_id, ucbt_tab.tip_cc, ucbt_tab.pac, ctmt.nome,
                        ctmt.ene_01 AS ene_01_alimentador, ctmt.ene_02 AS ene_02_alimentador,
                        ctmt.ene_03 AS ene_03_alimentador, ctmt.ene_04 AS ene_04_alimentador,
                        ctmt.ene_05 AS ene_05_alimentador, ctmt.ene_06 AS ene_06_alimentador,
                        ctmt.ene_07 AS ene_07_alimentador, ctmt.ene_08 AS ene_08_alimentador,
                        ctmt.ene_09 AS ene_09_alimentador, ctmt.ene_10 AS ene_10_alimentador,
                        ctmt.ene_11 AS ene_11_alimentador, ctmt.ene_12 AS ene_12_alimentador,
                        ctmt.pntbt_01 AS pntbt_01_alimentador, ctmt.pntbt_02 AS pntbt_02_alimentador,
                        ctmt.pntbt_03 AS pntbt_03_alimentador, ctmt.pntbt_04 AS pntbt_04_alimentador,
                        ctmt.pntbt_05 AS pntbt_05_alimentador, ctmt.pntbt_06 AS pntbt_06_alimentador,
                        ctmt.pntbt_07 AS pntbt_07_alimentador, ctmt.pntbt_08 AS pntbt_08_alimentador,
                        ctmt.pntbt_09 AS pntbt_09_alimentador, ctmt.pntbt_10 AS pntbt_10_alimentador,
                        ctmt.pntbt_11 AS pntbt_11_alimentador, ctmt.pntbt_12 AS pntbt_12_alimentador,
                        ucbt_tab.fas_con, ucbt_tab.ten_forn,
                        ucbt_tab.ene_01 AS ene_01_carga, ucbt_tab.ene_02 AS ene_02_carga,
                        ucbt_tab.ene_03 AS ene_03_carga, ucbt_tab.ene_04 AS ene_04_carga,
                        ucbt_tab.ene_05 AS ene_05_carga, ucbt_tab.ene_06 AS ene_06_carga,
                        ucbt_tab.ene_07 AS ene_07_carga, ucbt_tab.ene_08 AS ene_08_carga,
                        ucbt_tab.ene_09 AS ene_09_carga, ucbt_tab.ene_10 AS ene_10_carga,
                        ucbt_tab.ene_11 AS ene_11_carga, ucbt_tab.ene_12 AS ene_12_carga, 
                        ucbt_tab.ctmt,
                        ucbt_tab.sub
            FROM ucbt_tab
            JOIN ctmt ON ctmt.cod_id = ucbt_tab.ctmt
            WHERE ucbt_tab.gru_ten = 'BT'
            ORDER BY ctmt.nome
            LIMIT {limit} OFFSET {offset}
        """

        try:
            # Executa as duas consultas auxiliares
            ugbt_ugmt_aneel_df = pd.read_sql(ugbt_ugmt_aneel, conn_aneel)
            ugbt_ugmt_bdgd_df = pd.read_sql(ugbt_ugmt_bdgd, conn)

            # Carrega total de registros
            total_df = pd.read_sql(query_total, conn)
            total_rows = int(total_df.iloc[0]['total'])

            # Paginador
            frames = []
            for offset in range(0, total_rows, limit):
                query = query_data_template.format(limit=limit, offset=offset)
                frames.append(pd.read_sql(query, conn))

            cargas_baixa_tensao_df = pd.concat(frames, ignore_index=True)
            
            
            # Ajustando as curvas de carga 
            dict_curvas_cargas, dic_mult_por_ctmt_load = BTLoadCurveProcessor().ajustando_curvas_de_carga_BT(conn, conn_aneel, cargas_baixa_tensao_df, 
                                                                          ugbt_ugmt_bdgd_df, ugbt_ugmt_aneel_df)

            # Retorna os três DataFrames
            return dict_curvas_cargas, dic_mult_por_ctmt_load

        except Exception as e:
            print(f"Erro ao carregar as cargas de baixa tensão: {e}")
            return 1




    @staticmethod
    def cargas_media_tensao_paginado(dic_mult_por_ctmt_load, conn, limit: int = 500000) -> pd.DataFrame:
        """Consulta paginada das cargas em média tensão"""

 
        query_total = """
            SELECT COUNT(*) AS total
            FROM ucmt_tab
            WHERE gru_ten = 'MT'
        """

        query_data_template = """
            SELECT DISTINCT 
                ucmt_tab.cod_id, ucmt_tab.tip_cc, ucmt_tab.pac, ctmt.nome,

                ctmt.ene_01 AS ene_01_alimentador, ctmt.ene_02 AS ene_02_alimentador,
                ctmt.ene_03 AS ene_03_alimentador, ctmt.ene_04 AS ene_04_alimentador,
                ctmt.ene_05 AS ene_05_alimentador, ctmt.ene_06 AS ene_06_alimentador,
                ctmt.ene_07 AS ene_07_alimentador, ctmt.ene_08 AS ene_08_alimentador,
                ctmt.ene_09 AS ene_09_alimentador, ctmt.ene_10 AS ene_10_alimentador,
                ctmt.ene_11 AS ene_11_alimentador, ctmt.ene_12 AS ene_12_alimentador,

                ctmt.pntmt_01 AS pntmt_01_alimentador, ctmt.pntmt_02 AS pntmt_02_alimentador,
                ctmt.pntmt_03 AS pntmt_03_alimentador, ctmt.pntmt_04 AS pntmt_04_alimentador,
                ctmt.pntmt_05 AS pntmt_05_alimentador, ctmt.pntmt_06 AS pntmt_06_alimentador,
                ctmt.pntmt_07 AS pntmt_07_alimentador, ctmt.pntmt_08 AS pntmt_08_alimentador,
                ctmt.pntmt_09 AS pntmt_09_alimentador, ctmt.pntmt_10 AS pntmt_10_alimentador,
                ctmt.pntmt_11 AS pntmt_11_alimentador, ctmt.pntmt_12 AS pntmt_12_alimentador,

                ucmt_tab.fas_con, ucmt_tab.ten_forn,

                ucmt_tab.ene_01 AS ene_01_carga, ucmt_tab.ene_02 AS ene_02_carga,
                ucmt_tab.ene_03 AS ene_03_carga, ucmt_tab.ene_04 AS ene_04_carga,
                ucmt_tab.ene_05 AS ene_05_carga, ucmt_tab.ene_06 AS ene_06_carga,
                ucmt_tab.ene_07 AS ene_07_carga, ucmt_tab.ene_08 AS ene_08_carga,
                ucmt_tab.ene_09 AS ene_09_carga, ucmt_tab.ene_10 AS ene_10_carga,
                ucmt_tab.ene_11 AS ene_11_carga, ucmt_tab.ene_12 AS ene_12_carga, 
                ucmt_tab.ctmt,
                ucmt_tab.sub
            FROM 
                ucmt_tab
            JOIN 
                ctmt ON ctmt.cod_id = ucmt_tab.ctmt
            WHERE 
                ucmt_tab.gru_ten = 'MT'
            ORDER BY 
                ctmt.nome
            LIMIT {limit} OFFSET {offset}
        """

        try:
 
            # Total de registros para paginação
            total_df = pd.read_sql(query_total, conn)
            total_rows = int(total_df.iloc[0]['total'])

            # Coleta paginada
            frames = []
            for offset in range(0, total_rows, limit):
                query = query_data_template.format(limit=limit, offset=offset)
                frames.append(pd.read_sql(query, conn))

            cargas_media_tensao_df = pd.concat(frames, ignore_index=True)

            # Ajustando as curvas de carga (você pode mudar o método se tiver um específico para MT)
            dic_mult_por_ctmt_load = MTLoadCurveProcessor().ajustando_curvas_de_carga_MT(dic_mult_por_ctmt_load, conn,
                cargas_media_tensao_df,
            )

            return dic_mult_por_ctmt_load

        except Exception as e:
            print(f"Erro ao carregar as cargas de média tensão: {e}")
            return 1



    @staticmethod
    def gd_baixa_tensao(conn_mt, conn_aneel, limit: int = 500000) -> pd.DataFrame:
        """Carrega dados de geração distribuída em baixa tensão com paginação e cruza com base da ANEEL.
        Para os que não tiverem correspondência, preenche mdpotenciainstalada e mdpotenciaoutorgada com pot_inst."""

        query_aneel = """
            SELECT codgeracaodistribuida, mdpotenciainstalada, mdpotenciaoutorgada
            FROM gd_aneel_2025_base
        """

        total_rows_query = "SELECT COUNT(*) FROM ugbt_tab"

        try:
            # Carrega dados da base da ANEEL
            df_aneel = pd.read_sql(query_aneel, conn_aneel)
            df_aneel.rename(columns={"codgeracaodistribuida": "ceg_gd"}, inplace=True)

            # Conta total de linhas em ugbt_tab
            total_rows_df = pd.read_sql(total_rows_query, conn_mt)
            total_rows = total_rows_df.iloc[0, 0] if not total_rows_df.empty else 0

            if total_rows == 0:
                print("Aviso: Nenhuma linha encontrada na tabela ugbt_tab.")
                return pd.DataFrame()

            # Paginação
            frames = []
            for offset in range(0, total_rows, limit):
                query_ugbt = f"""
                    SELECT DISTINCT 
                            ugbt_tab.cod_id, ugbt_tab.ceg_gd, ugbt_tab.pac, ctmt.nome, 
                            ugbt_tab.fas_con, ugbt_tab.ten_con, ugbt_tab.pot_inst, ugbt_tab.sub
                    FROM 
                            ugbt_tab
                    JOIN 
                        ctmt ON ctmt.cod_id = ugbt_tab.ctmt
                    ORDER BY 
                        ctmt.nome
                    LIMIT {limit} OFFSET {offset}
                """
                df_chunk = pd.read_sql(query_ugbt, conn_mt)
                if df_chunk.empty:
                    print(f"Aviso: Nenhum dado retornado no offset {offset}.")
                    break
                frames.append(df_chunk)

            df_ugbt = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

            # LEFT JOIN entre ugbt e aneel
            df_merged = pd.merge(df_ugbt, df_aneel, on="ceg_gd", how="left")

            # Preencher os valores ausentes com pot_inst
            df_merged["mdpotenciainstalada"] = df_merged["mdpotenciainstalada"].fillna(df_merged["pot_inst"])
            df_merged["mdpotenciaoutorgada"] = df_merged["mdpotenciaoutorgada"].fillna(df_merged["pot_inst"])

            return df_merged

        except Exception as e:
            print(f"Erro ao carregar dados GD baixa tensão: {e}")
            return pd.DataFrame()



    @staticmethod
    def gd_media_tensao(conn_mt, conn_aneel, limit: int = 500000) -> pd.DataFrame:
        """Carrega dados de geração distribuída em média tensão com paginação e cruza com base da ANEEL.
        Para os que não tiverem correspondência, preenche mdpotenciainstalada e mdpotenciaoutorgada com pot_inst."""

        query_aneel = """
            SELECT codgeracaodistribuida, mdpotenciainstalada, mdpotenciaoutorgada
            FROM gd_aneel_2025_base
        """

        total_rows_query = "SELECT COUNT(*) FROM ugmt_tab"

        try:
            # Carrega dados da base da ANEEL
            df_aneel = pd.read_sql(query_aneel, conn_aneel)
            df_aneel.rename(columns={"codgeracaodistribuida": "ceg_gd"}, inplace=True)

            # Conta total de linhas em ugmt_tab
            total_rows_df = pd.read_sql(total_rows_query, conn_mt)
            total_rows = total_rows_df.iloc[0, 0] if not total_rows_df.empty else 0

            if total_rows == 0:
                print("Aviso: Nenhuma linha encontrada na tabela ugmt_tab.")
                return pd.DataFrame()

            # Paginação
            frames = []
            for offset in range(0, total_rows, limit):
                query_ugmt = f"""
                    SELECT DISTINCT 
                        ugmt_tab.cod_id, ugmt_tab.ceg_gd, ugmt_tab.pac, ctmt.nome, 
                        ugmt_tab.fas_con, ugmt_tab.ten_con, ugmt_tab.pot_inst, ugmt_tab.sub
                    FROM 
                        ugmt_tab
                    JOIN 
                        ctmt ON ctmt.cod_id = ugmt_tab.ctmt
                    ORDER BY 
                        ctmt.nome
                    LIMIT {limit} OFFSET {offset}
                """
                df_chunk = pd.read_sql(query_ugmt, conn_mt)
                if df_chunk.empty:
                    print(f"Aviso: Nenhum dado retornado no offset {offset}.")
                    break
                frames.append(df_chunk)

            df_ugmt = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

            # LEFT JOIN entre ugmt e aneel
            df_merged = pd.merge(df_ugmt, df_aneel, on="ceg_gd", how="left")

            # Preencher os valores ausentes com pot_inst
            df_merged["mdpotenciainstalada"] = df_merged["mdpotenciainstalada"].fillna(df_merged["pot_inst"])
            df_merged["mdpotenciaoutorgada"] = df_merged["mdpotenciaoutorgada"].fillna(df_merged["pot_inst"])

            return df_merged

        except Exception as e:
            print(f"Erro ao carregar dados GD média tensão: {e}")
            return pd.DataFrame()



    def Cargas_Poste_Iluminacao_Publica(conn_mt) -> pd.DataFrame:
        """Modelagem das Cargas de postes de iluminação pública"""
        
        offset, limit = 0, 100000
        dfs = []

        try:
            total_rows_query = "SELECT COUNT(*) AS total FROM pip"
            total_rows_df = pd.read_sql(total_rows_query, conn_mt)
            total_rows = total_rows_df.iloc[0]['total'] if not total_rows_df.empty else 0


            while offset < total_rows:
                query = f"""
                    SELECT DISTINCT 
                        pip.cod_id, pip.tip_cc, pip.pac, ctmt.nome, 
                        pip.fas_con, pip.ten_forn, pip.pot_lamp, pip.sub 
                    FROM pip 
                    JOIN ctmt ON ctmt.cod_id = pip.ctmt 
                    ORDER BY ctmt.nome 
                    LIMIT {limit} OFFSET {offset}
                """
                df_chunk = pd.read_sql(query, conn_mt)
                dfs.append(df_chunk)
                offset += limit

        except Exception as e:
            print(f"Erro ao processar cargas postes iluminação: {e}")
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()



    @staticmethod
    def Ramais_Ligacao(conn) -> pd.DataFrame:
        """Modelagem dos ramais de ligação de baixa tensão"""

        query = (
            "SELECT DISTINCT ramlig.cod_id, pac_1, pac_2, ctmt.nome, fas_con, comp, tip_cnd, ramlig.sub "
            "FROM ramlig JOIN ctmt ON ctmt.cod_id = ramlig.ctmt ORDER BY ctmt.nome"
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao processar ramais de ligação: {e}")
            return pd.DataFrame()



    @staticmethod
    def transformadores_Media_tensao(conn) -> pd.DataFrame:
        """Modelagem dos transformadores de média tensão"""

        query = (
            "SELECT DISTINCT untrmt.cod_id, untrmt.pac_1, untrmt.pac_2, ctmt.nome, eqtrmt.pot_nom, "
            "eqtrmt.lig, eqtrmt.ten_pri, eqtrmt.ten_sec, eqtrmt.lig_fas_p, eqtrmt.lig_fas_s, eqtrmt.r, "
            "eqtrmt.xhl, untrmt.per_fer, eqtrmt.lig_fas_t, untrmt.pac_3, untrmt.sub "
            "FROM untrmt "
            "LEFT JOIN eqtrmt ON eqtrmt.uni_tr_mt = untrmt.cod_id "
            "LEFT JOIN ctmt ON ctmt.cod_id = untrmt.ctmt "
            "ORDER BY ctmt.nome"
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao carregar transformadores média tensão: {e}")
            return pd.DataFrame()

        

    @staticmethod
    def Reguladores_Media_Tensao(conn) -> pd.DataFrame:
        """Modelagem dos Reguladores de Tensão na média tensão"""

        query = (
            "SELECT DISTINCT unremt.cod_id, unremt.fas_con, unremt.pac_1, unremt.pac_2, ctmt.nome, "
            "eqre.pot_nom, eqre.lig_fas_p, eqre.lig_fas_s, eqre.per_fer, eqre.per_tot, eqre.r, eqre.xhl, "
            "ctmt.ten_nom, eqre.cor_nom, eqre.rel_tp, eqre.rel_tc "
            "FROM unremt "
            "JOIN eqre ON unremt.cod_id = eqre.un_re "
            "JOIN ctmt ON unremt.ctmt = ctmt.cod_id "
            "ORDER BY ctmt.nome"
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao carregar reguladores de média tensão: {e}")
            return pd.DataFrame()


    @staticmethod
    def Curvas_de_Carga(conn) -> pd.DataFrame:
        """Modelagem dos Reguladores de Tensão na média tensão"""

        query = ( """
            SELECT 
                        crvcrg.cod_id AS crvcrg_cod_id,
                        crvcrg.tip_dia,

                        crvcrg.pot_01, crvcrg.pot_02, crvcrg.pot_03, crvcrg.pot_04,
                        crvcrg.pot_05, crvcrg.pot_06, crvcrg.pot_07, crvcrg.pot_08,
                        crvcrg.pot_09, crvcrg.pot_10, crvcrg.pot_11, crvcrg.pot_12,
                        crvcrg.pot_13, crvcrg.pot_14, crvcrg.pot_15, crvcrg.pot_16,
                        crvcrg.pot_17, crvcrg.pot_18, crvcrg.pot_19, crvcrg.pot_20,
                        crvcrg.pot_21, crvcrg.pot_22, crvcrg.pot_23, crvcrg.pot_24,
                        crvcrg.pot_25, crvcrg.pot_26, crvcrg.pot_27, crvcrg.pot_28,
                        crvcrg.pot_29, crvcrg.pot_30, crvcrg.pot_31, crvcrg.pot_32,
                        crvcrg.pot_33, crvcrg.pot_34, crvcrg.pot_35, crvcrg.pot_36,
                        crvcrg.pot_37, crvcrg.pot_38, crvcrg.pot_39, crvcrg.pot_40,
                        crvcrg.pot_41, crvcrg.pot_42, crvcrg.pot_43, crvcrg.pot_44,
                        crvcrg.pot_45, crvcrg.pot_46, crvcrg.pot_47, crvcrg.pot_48,
                        crvcrg.pot_49, crvcrg.pot_50, crvcrg.pot_51, crvcrg.pot_52,
                        crvcrg.pot_53, crvcrg.pot_54, crvcrg.pot_55, crvcrg.pot_56,
                        crvcrg.pot_57, crvcrg.pot_58, crvcrg.pot_59, crvcrg.pot_60,
                        crvcrg.pot_61, crvcrg.pot_62, crvcrg.pot_63, crvcrg.pot_64,
                        crvcrg.pot_65, crvcrg.pot_66, crvcrg.pot_67, crvcrg.pot_68,
                        crvcrg.pot_69, crvcrg.pot_70, crvcrg.pot_71, crvcrg.pot_72,
                        crvcrg.pot_73, crvcrg.pot_74, crvcrg.pot_75, crvcrg.pot_76,
                        crvcrg.pot_77, crvcrg.pot_78, crvcrg.pot_79, crvcrg.pot_80,
                        crvcrg.pot_81, crvcrg.pot_82, crvcrg.pot_83, crvcrg.pot_84,
                        crvcrg.pot_85, crvcrg.pot_86, crvcrg.pot_87, crvcrg.pot_88,
                        crvcrg.pot_89, crvcrg.pot_90, crvcrg.pot_91, crvcrg.pot_92,
                        crvcrg.pot_93, crvcrg.pot_94, crvcrg.pot_95, crvcrg.pot_96

                    FROM 
                        crvcrg  
                        
                """
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao carregar reguladores de média tensão: {e}")
            return pd.DataFrame()




    @staticmethod
    def Consulta_Cargas_Agregadas(conn) -> pd.DataFrame:
        """Extração dos dados das cargas e os trafos que elas estão vinculadas"""

        query = ( """
            SELECT 
                ucbt_tab.uni_tr_mt, 
                ucbt_tab.cod_id, 
                ucbt_tab.ctmt, 
                ctmt.nome 
            FROM 
                ucbt_tab
            JOIN 
                ctmt 
                ON ctmt.cod_id = ucbt_tab.ctmt;
                """
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao carregar a Consulta de Cargas agregadas: {e}")
            return pd.DataFrame()



    @staticmethod
    def Consulta_GD_FV_BT_Agregados(conn) -> pd.DataFrame:
        """Extração dos dados das GD's de BT aos seus trafos vinculados """

        query = ( """
            SELECT 
                ugbt_tab.uni_tr_mt, 
                ugbt_tab.cod_id, 
                ugbt_tab.ctmt, 
                ctmt.nome 
            FROM 
                ugbt_tab
            JOIN 
                ctmt 
                ON ctmt.cod_id = ugbt_tab.ctmt;
                """
        )

        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Erro ao carregar a Consulta de GD's Agregadas: {e}")
            return pd.DataFrame()





                    
