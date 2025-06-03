from bdgd_to_opendss.db.data_loader import load_bdgd
from bdgd_to_opendss.db.db_extract_data_bank import load_db_config
from bdgd_to_opendss.db.db_conector import get_connection

from bdgd_to_opendss.validation.data_corretion import (
    
    ValidatorBarraSlack,ValidatorCompensadorMedia,ValidatorCompensadorBaixa,
    ValidatorChaveSeccionadoraBT,ValidatorChaveSeccionadoraMT,
    ValidatorGeradorMediaTensao,ValidadorCargasBaixaTensao,ValidadorCargasMediaTensao,
    ValidadorGDBaixaTensao,ValidadorGDMediaTensao,ValidatorLinecodesBaixaTensao,
    ValidatorLinecodesMediaTensao,ValidatorLinecodesRamais,ValidatorLinhasBaixaTensao,
    ValidatorLinhasMediaTensao,ValidadorRamaisLigacao,ValidadorPIP,
    ValidadorTransformadoresMediaTensao,ValidadorReguladoresMediaTensao
)

from bdgd_to_opendss.modelar.components import (
    
    SlackBus, ReactiveCompensatorMT, ReactiveCompensatorBT, SwitchLowVoltage, SwitchMediumVoltage,
    GeneratorMediumVoltage, LinecodeLowVoltage, LinecodeMediumVoltage, LinecodeRamais, LineLowVoltage,
    LineMediumVoltage, RamalLine, LoadLowVoltage, LoadMediumVoltage, GD_FV_BT, GD_FV_MT, PublicLightingLoad,
    TransformerMediumVoltage, RegulatorMediumVoltage
    )

from bdgd_to_opendss.export.dss_writer import (write_cenario_1)





class ModelRunner:
    def __init__(self, scenario):
        self.scenario = scenario

    def run(self, caminho):
        print(f">>> Iniciando modelagem com o cenário: {self.scenario}")
        
        # ---------------- CENÁRIO 1 ------------------------
        if self.scenario == 'cenario_1':
            
            # 1. Conectar aos bancos de dados 
            db_configs = load_db_config("energisa_mt", "aneel")
            conn_mt = get_connection(**db_configs["energisa_mt"])
            conn_aneel = get_connection(**db_configs["aneel"])

            df_slack = load_bdgd.barra_slack(conn_mt)
            df_slack = ValidatorBarraSlack().validate_dataframe(df_slack)
            modelated_slacks = SlackBus().to_dss(df_slack)

            df_compensadores_reativo_media = load_bdgd.compensadores_reativo_media(conn_mt)
            df_compensadores_reativo_media = ValidatorCompensadorMedia().validate_dataframe(df_compensadores_reativo_media)
            modelated_compensadores_reativo_media = ReactiveCompensatorMT().to_dss(df_compensadores_reativo_media)

            df_compensadores_reativo_baixa = load_bdgd.compensadores_reativo_baixa(conn_mt)
            df_compensadores_reativo_baixa = ValidatorCompensadorBaixa().validate_dataframe(df_compensadores_reativo_baixa)
            modelated_compensadores_reativo_baixa = ReactiveCompensatorBT().to_dss(df_compensadores_reativo_baixa)
 
            df_chaves_seccionadoras_baixa_tensao = load_bdgd.chaves_seccionadoras_baixa_tensao(conn_mt)
            df_chaves_seccionadoras_baixa_tensao = ValidatorChaveSeccionadoraBT().validate_dataframe(df_chaves_seccionadoras_baixa_tensao)
            modelated_chaves_seccionadoras_baixa_tensao = SwitchLowVoltage().to_dss(df_chaves_seccionadoras_baixa_tensao)

            df_chaves_seccionadoras_media_tensao = load_bdgd.chaves_seccionadoras_media_tensao(conn_mt)
            df_chaves_seccionadoras_media_tensao = ValidatorChaveSeccionadoraMT().validate_dataframe(df_chaves_seccionadoras_media_tensao)
            modelated_chaves_seccionadoras_media_tensao = SwitchMediumVoltage().to_dss(df_chaves_seccionadoras_media_tensao)

            df_geradores_media_tensao = load_bdgd.geradores_media_tensao(conn_mt)
            df_geradores_media_tensao = ValidatorGeradorMediaTensao().validate_dataframe(df_geradores_media_tensao)
            modelated_geradores_media_tensao = GeneratorMediumVoltage().to_dss(df_geradores_media_tensao)

            df_linecodes_baixa_tensao = load_bdgd.linecodes_baixa_tensao(conn_mt)
            df_linecodes_baixa_tensao = ValidatorLinecodesBaixaTensao().validate_dataframe(df_linecodes_baixa_tensao)
            modelated_linecodes_baixa_tensao = LinecodeLowVoltage().to_dss(df_linecodes_baixa_tensao)
            
            df_linecodes_media_tensao = load_bdgd.linecodes_media_tensao(conn_mt)
            df_linecodes_media_tensao = ValidatorLinecodesMediaTensao().validate_dataframe(df_linecodes_media_tensao)
            modelated_linecodes_media_tensao = LinecodeMediumVoltage().to_dss(df_linecodes_media_tensao)

            df_linecodes_ramais = load_bdgd.linecodes_ramais(conn_mt)
            df_linecodes_ramais = ValidatorLinecodesRamais().validate_dataframe(df_linecodes_ramais)
            modelated_linecodes_ramais = LinecodeRamais().to_dss(df_linecodes_ramais)

            df_linhas_baixa_tensao = load_bdgd.linhas_baixa_tensao(conn_mt)
            df_linhas_baixa_tensao = ValidatorLinhasBaixaTensao().validate_dataframe(df_linhas_baixa_tensao)
            modelated_linhas_baixa_tensao = LineLowVoltage().to_dss(df_linhas_baixa_tensao)

            df_linhas_media_tensao = load_bdgd.linhas_media_tensao(conn_mt)
            df_linhas_media_tensao = ValidatorLinhasMediaTensao().validate_dataframe(df_linhas_media_tensao)
            modelated_linhas_media_tensao = LineMediumVoltage().to_dss(df_linhas_media_tensao)
            
            df_Ramais_Ligacao = load_bdgd.Ramais_Ligacao(conn_mt)
            df_Ramais_Ligacao = ValidadorRamaisLigacao().validate_dataframe(df_Ramais_Ligacao)
            modelated_Ramais_Ligacao = RamalLine().to_dss(df_Ramais_Ligacao)

            df_curvas_cargas, df_multiplicadores = load_bdgd.cargas_baixa_tensao_paginado(conn_mt, conn_aneel)
            df_cargas_baixa_tensao = ValidadorCargasBaixaTensao().validate_dataframe(df_curvas_cargas)
            modelated_cargas_baixa_tensa = LoadLowVoltage().to_dss(df_cargas_baixa_tensao)

            df_curvas_cargas = load_bdgd.cargas_media_tensao_paginado(df_multiplicadores, conn_mt)
            df_cargas_media_tensao = ValidadorCargasMediaTensao().validate_dataframe(df_curvas_cargas)
            modelated_cargas_media_tensao = LoadMediumVoltage().to_dss(df_cargas_media_tensao)

            df_gd_baixa_tensao = load_bdgd.gd_baixa_tensao(conn_mt, conn_aneel)
            df_gd_baixa_tensao = ValidadorGDBaixaTensao().validate_dataframe(df_gd_baixa_tensao)
            modelated_gd_baixa_tensao = GD_FV_BT().to_dss(df_gd_baixa_tensao)

            df_gd_media_tensao = load_bdgd.gd_media_tensao(conn_mt, conn_aneel)
            df_gd_media_tensao = ValidadorGDMediaTensao().validate_dataframe(df_gd_media_tensao)
            modelated_gd_media_tensao = GD_FV_MT().to_dss(df_gd_media_tensao)

            df_Cargas_PIP = load_bdgd.Cargas_Poste_Iluminacao_Publica(conn_mt)
            df_Cargas_PIP = ValidadorPIP().validate_dataframe(df_Cargas_PIP)
            modelated_Cargas_PIP = PublicLightingLoad().to_dss(df_Cargas_PIP)

            df_transformadores_Media_tensao = load_bdgd.transformadores_Media_tensao(conn_mt)
            df_transformadores_Media_tensao = ValidadorTransformadoresMediaTensao().validate_dataframe(df_transformadores_Media_tensao)
            modelated_transformadores_Media_tensao = TransformerMediumVoltage().to_dss(df_transformadores_Media_tensao)

            df_Reguladores_Media_Tensao = load_bdgd.Reguladores_Media_Tensao(conn_mt)
            df_Reguladores_Media_Tensao = ValidadorReguladoresMediaTensao().validate_dataframe(df_Reguladores_Media_Tensao)
            modelated_Reguladores_Media_Tensao = RegulatorMediumVoltage().to_dss(df_Reguladores_Media_Tensao)

            df_curvas_de_carga = load_bdgd.Curvas_de_Carga(conn_mt)
            
            
            # Escrita da modelagem
            write_cenario_1(caminho).to_dss(modelated_slacks, modelated_compensadores_reativo_media, modelated_compensadores_reativo_baixa,
                                     modelated_chaves_seccionadoras_baixa_tensao, modelated_chaves_seccionadoras_media_tensao,
                                     modelated_geradores_media_tensao, modelated_linecodes_baixa_tensao, modelated_linecodes_media_tensao,
                                     modelated_linhas_baixa_tensao, modelated_linecodes_ramais,
                                     modelated_linhas_media_tensao, modelated_Ramais_Ligacao, modelated_cargas_baixa_tensa,
                                     modelated_cargas_media_tensao, modelated_gd_baixa_tensao, modelated_gd_media_tensao,
                                     modelated_Cargas_PIP, modelated_transformadores_Media_tensao, modelated_Reguladores_Media_Tensao,
                                     df_curvas_de_carga)
            
            
            print("teste")
            
            # Correção de erros comuns
        

            #lines = model_lines(df_line)
            #transformers = model_transformers(df_tr)
            #loads = model_loads(df_load)

            #export_lines(lines)
            #export_transformers(transformers)
            #export_loads(loads)

        # ---------------- CENÁRIO 2 ------------------------
        #elif self.scenario == 'cenario_2':
        #    df_line = load_data("lnmt", self.conn)
        #     df_tr = load_data("eqtrmt", self.conn)
        #     df_load = load_data("carga", self.conn)

        #     lines = model_lines(df_line)
        #     transformers = model_transformers(df_tr, aggregate_bt_loads=True)
        #     loads = model_loads(df_load, ignore_if_aggregated=True)

        #     export_lines(lines)
        #     export_transformers(transformers)
        #     export_loads(loads)

        # else:
        #     print("Cenário inválido. Nenhum modelo foi gerado.")