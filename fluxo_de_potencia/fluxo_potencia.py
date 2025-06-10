import py_dss_interface
dss = py_dss_interface.DSS()
import re

""" Esta classe possui os métodos que configuram cenarios de simulação """

class class_Fluxo_de_Potencia:

    
    @staticmethod
    def config_run(
        modo_snapshot: bool, modo_daily: bool, modo_yearly: bool, mes_index: int, modelo_carga: str,
        usar_cargas_bt: bool, usar_cargas_mt: bool, usar_gd_bt: bool, usar_gd_mt: bool,
        usar_geracao_hidraulica: bool, exibir_tensao: bool, exibir_corrente: bool,
        exibir_DEC: bool, exibir_FEC: bool, monitorar_subestacao: bool, gerar_grafico_circuito: bool):
        """ Recebe parâmetros de configuração dos cenários de simulação """



        class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
        class_Fluxo_de_Potencia.modelo_carga(modelo_carga)
        class_Fluxo_de_Potencia.config_gd(usar_gd_bt, usar_gd_mt)
        class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
        class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)


    @staticmethod
    def config_cargas(usar_cargas_bt, usar_cargas_mt):
        """ Este método configura as cargas quanto a quais vão ser simuladas """

        if usar_cargas_bt == False:
            dss.loads.first()
            for nome in dss.loads.names:
                if 'carga_baixa' in nome:
                    dss.text(f"disable load.{nome}")
                    dss.loads.next()

        if usar_cargas_mt == False:
            dss.loads.first()
            for nome in dss.loads.names:
                if 'carga_media' in nome:
                    dss.text(f"disable load.{nome}")
                    dss.loads.next()
        return 0


    @staticmethod
    def config_gd(usar_gd_bt, usar_gd_mt):
        """ Este método configura as cargas quanto a quais vão ser simuladas """

        if usar_gd_bt == False:
            dss.pvsystems.first()
            for nome in dss.pvsystems.names:
                if 'pv_baixa' in nome:
                    dss.text(f"disable pvsystem.{nome}")
                    dss.pvsystems.next()

        if usar_gd_mt == False:
            dss.pvsystems.first()
            for nome in dss.pvsystems.names:
                if 'pv_media' in nome:
                    dss.text(f"disable pvsystem.{nome}")
                    dss.pvsystems.next()
        return 0



    @staticmethod
    def modelo_carga(modelo_carga):
        """ 1: P constante e Q constante (padrão): comumente usados para estudos de fluxo de potência 
            2: Z constante (ou impedância constante)
            3: P constante e Q quadrático
            4: Exponencial:
            5: I constante (ou magnitude de corrente constante) Às vezes usado para carga retificadora
            6: P constante e Q fixo (no valor nominal)
            7: P constante e Q quadrático (ou seja, reatância fixa)
            8: CEP (ver ZIPV)"""

        dss.solution.solve()
        dss.loads.first()
        for nome in dss.loads.names:
            dss.loads.model = modelo_carga
            dss.loads.next()


    @staticmethod
    def limites_carga():
        """ Este método ele força o modelo de carga mudar para z constante,
        melhora a convergencia do fluxo de potência """

        dss.solution.solve()
        dss.loads.first()
        for nome in dss.loads.names:
            dss.text("load.{}.vminpu = 0.85".format(nome))
            dss.text("load.{}.vmaxpu = 1.15".format(nome))
            dss.loads.next()

    
    @staticmethod
    def gd_ufs_atualiza(ponto_simulacao):
        """ Atualiza a irradiância de cada painel fotovoltaico """

        irradiance_96 = [
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000,
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000,
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.168, 0.227, 0.288, 0.349,
        0.409, 0.468, 0.525, 0.581, 0.633, 0.684, 0.731, 0.775, 0.816, 0.852,
        0.886, 0.915, 0.940, 0.961, 0.977, 0.989, 0.997, 1.000, 0.999, 0.993,
        0.982, 0.968, 0.948, 0.925, 0.897, 0.866, 0.830, 0.791, 0.748, 0.702,
        0.653, 0.601, 0.547, 0.490, 0.432, 0.372, 0.311, 0.250, 0.190, 0.133,
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000,
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000,
        0.000, 0.000, 0.000, 0.000, 0.000, 0.000]


        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            dss.pvsystems.irradiance = irradiance_96[ponto_simulacao]
            dss.pvsystems.next()


    @staticmethod
    def cargas_atualiza(ponto_simulacao, mes):
        """ Atualiza a carga consumida """
        dss.loads.first()
        for nome in dss.loads.names:

            perfil_semanal = [float(v) for v in re.search(r'curva_diaria_[^_]+_([\d._]+)_curva_anual', nome).group(1).split('_')]
            curva_anual = [float(v) for v in re.search(r'curva_anual_([\d._]+)', nome).group(1).split('_')]

            dss.loads.kw = curva_anual[mes] * perfil_semanal[ponto_simulacao] 
            dss.loads.next()

    
    @staticmethod
    def run():
        class_Fluxo_de_Potencia.gd_ufs_atualiza()


