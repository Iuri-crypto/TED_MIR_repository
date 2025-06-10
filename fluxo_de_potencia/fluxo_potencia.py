import os
import re
from collections import defaultdict
import py_dss_interface
dss = py_dss_interface.DSS()
import os
import matplotlib.pyplot as plt
import py_dss_interface


""" Esta classe possui os métodos que configuram cenarios de simulação """

class class_Fluxo_de_Potencia:
    @staticmethod
    def config_run(
        modo_snapshot: bool, modo_daily: bool, modo_yearly: bool, mes_index: int, modelo_carga: str,
        usar_cargas_bt: bool, usar_cargas_mt: bool, usar_gd_bt: bool, usar_gd_mt: bool,
        usar_geracao_hidraulica: bool, exibir_tensao: bool, exibir_corrente: bool,
        exibir_DEC: bool, exibir_FEC: bool, monitorar_subestacao: bool, gerar_grafico_circuito: bool,
        caminho_base: str, nomes_que_devem_ser_carregados: list):
        """ Recebe parâmetros de configuração dos cenários de simulação """
        
        arquivos_dss = []
        for root, _, files in os.walk(caminho_base):
            for nome_arquivo in files:
                if nome_arquivo.lower() == "run_cargas_agregadas.dss":
                    arquivos_dss.append(os.path.join(root, nome_arquivo))

        for caminho in arquivos_dss:
            # Obtenção do nome do alimentador
            nome_alimentador = os.path.basename(os.path.dirname(caminho))

            dss.solution.max_iterations = 100
            dss.solution.max_control_iterations = 100

            class_Fluxo_de_Potencia.compilar(caminho)
            class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
            class_Fluxo_de_Potencia.modelo_carga(modelo_carga)
            class_Fluxo_de_Potencia.config_gd(usar_gd_bt, usar_gd_mt)
            class_Fluxo_de_Potencia.config_geradores(usar_geracao_hidraulica)
            class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)

            # Inicializa dicionários para armazenar potências e tensões
            potencias = defaultdict(lambda: defaultdict(float))

            for i in range(96):  # Iteração para 96 pontos do dia
                class_Fluxo_de_Potencia.cargas_atualiza(i, mes_index)
                class_Fluxo_de_Potencia.gd_ufs_atualiza(i)
                
                dss.solution.solve()
                
                # Coleta a potência ativa (kw) e reativa (kvar) para o horário
                potencias[i]['kw'] = round(dss.circuit.total_power[0], 4)  # Potência ativa
                potencias[i]['kvar'] = round(dss.circuit.total_power[1], 4)  # Potência reativa
                
                # Pode adicionar um print aqui para debugar, se necessário
                #print(f"Hora {i}: {potencias[i]['kw']} kW | {potencias[i]['kvar']} kVAr")
                #print("Iterações necessárias: {}".format(dss.solution.iterations))

                # Plotagem das potências ativas
            class_Fluxo_de_Potencia.plot_potencias(nome_alimentador, potencias)

            # plotagem final
            # Se for necessário, plotar as tensões também
            # if exibir_tensao:
            #    class_Fluxo_de_Potencia.plot_tensao(nome_alimentador)



    @staticmethod
    def plot_potencias(nome_alimentador, potencias):
        """Plota as potências ativas na saída da subestação"""
        
        # Horários do dia (00:00 até 23:59), divididos em 96 pontos (a cada 15 minutos)
        horas = list(range(96))
        horarios = [f"{h//4:02d}:{(h%4)*15:02d}" for h in horas]  # Formata as horas no formato HH:MM

        # Extrair apenas as potências ativas (kw) para o gráfico
        potencias_ativas = [-potencias[i]['kw'] for i in horas]

        # Plotando o gráfico
        plt.figure(figsize=(10, 6))
        plt.plot(horarios, potencias_ativas, label='Potência Ativa (kW)', color='b')

        # Definindo título e rótulos
        plt.title(f"Potência Ativa no Alimentador: {nome_alimentador}", fontsize=14)
        plt.xlabel("Hora do Dia", fontsize=12)
        plt.ylabel("Potência Ativa (kW)", fontsize=12)

        # Ajustando a exibição dos horários para que mostrem de forma clara
        plt.xticks(rotation=45, ha='right')  # Gira os rótulos para não sobrepor

        # Ajustar os limites do eixo Y para evitar distorções (se necessário)
        max_potencia = max(potencias_ativas)
        min_potencia = min(potencias_ativas)
        
        # Ajustar os limites do eixo Y com uma margem para melhor visualização
        margem = (max_potencia - min_potencia) * 0.1  # Ajuste de 10% para cima e para baixo
        plt.ylim(min_potencia - margem, max_potencia + margem)

        plt.grid(True)
        plt.legend()
        plt.tight_layout()

        # Exibindo o gráfico
        plt.show()



    @staticmethod
    def plot_tensao(nome_alimentador):
        """Este método gera os gráficos de tensão com o nome do alimentador"""

        # Simulando a interface com o DSS
        dss = py_dss_interface.DSS()
        barra_tensao = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag_pu))

        # Filtra os dados das tensões (barramentos que terminam com .1, .2 ou .3 e com vpu >= 0)
        barra_tensao_filtrada = [(nome, vpu) for nome, vpu in barra_tensao if nome.endswith((".1", ".2", ".3")) and vpu >= 0]

        # Filtra barramentos que estão com tensão entre 0.1 e 0.5 pu
        barras_problema = [(nome, vpu) for nome, vpu in barra_tensao_filtrada if 0.1 <= vpu <= 0.5]

        # Lista de barramentos fornecida (buses)
        buses = [
        "59869270827309837bt",
        "59816721827373865bt",
        "59834347827350861bt",
        "59837187827364308bt",
        "59831307827357505bt",
        "59806771827412586bt",
        "59852960827365697bt",
        "59894351827510767bt",
        "59849828827355127bt",
        "59829801827442955bt",
        "59892900827472156bt",
        "59864935827329823bt",
        "59879147827453626bt",
        "59863030827305908bt",
        "59897765827483304bt",
        "59863503827449003bt",
        "59886905827467189bt",
        "59871378827286390bt",
        "59891150827519205bt",
        "59835084827403507bt",
        "59859278827356268bt",
        "59857002827454179bt",
        "59847272827346484bt",
        "59847128827346409bt",
        "59851888827351241bt",
        "59852042827351245bt",
        "59866408827452735bt",
        "59866518827452616bt",
        "59859678827445111bt",
        "59859793827444976bt",
        "59865762827294149bt",
        "59865696827294287bt",
        "59836440827378773bt",
        "59836338827378901bt",
        "59834644827438399bt",
        "59834788827438540bt",
        "59835001827435637bt",
        "59834913827435519bt",
        "59889297827308163bt",
        "59889376827308368bt",
        "59832979827378654bt",
        "59832852827378737bt",
        "59847124827471117bt",
        "59847240827471015bt",
        "59881167827319813bt",
        "59880993827319736bt",
        "59829842827468227bt",
        "59883618827539576bt",
        "59882557827320141bt",
        "59886335827312811bt",
        "59842040827382203bt",
        "59816974827370723bt",
        "59856006827461915bt",
        "59884330827467831bt",
        "59888130827478215bt",
        "59812381827401884bt",
        "59855131827330866bt",
        "59850686827447026bt",
        "59860148827457728bt",
        "59879102827488027bt",
        "59890469827473419bt",
        "59821049827435402bt",
        "59827259827364050bt",
        "59843686827455679bt",
        "59884315827476323bt",
        "59828366827448277bt",
        "59832564827443623bt",
        "59852738827449377bt",
        "59827949827436205bt",
        "59817224827402829bt",
        "59886696827510160bt",
        "59853914827451036bt",
        "59820302827410218bt",
        "59835898827462697bt",
        "59820011827387516bt",
        "59853237827322426bt",
        "59821557827449968bt",
        "59848363827460724bt",
        "59860096827471134bt",
        "59825065827360534bt",
        "59838692827340314bt",
        "59881635827473398bt",
        "59831166827426293bt",
        "59839583827447620bt",
        "59862373827322763bt",
        "59890470827472268bt",
        "59831310827441167bt",
        "59882121827478908bt",
        "59871127827454925bt",
        "59849794827378412bt",
        "59827231827444990bt",
        "59817803827436948bt",
        "59874794827305937bt",
        "59887198827307512bt",
        "59824774827446532bt",
        "59866875827286905bt",
        "59833896827374958bt",
        "59845780827359187bt",
        "59838221827388447bt",
        "59875129827318542bt",
        "59866063827476194bt",
        "59861166827488570bt",
        "59860889827446191bt",
        "59823275827452258bt",
        "59885518827477097bt",
        "59818131827434338bt",
        "59867218827296719bt",
        "59869768827457895bt",
        "59824539827375792bt",
        "59810848827376698bt",
        "59829040827389910bt",
        "59836067827457294bt"


    ]



        # Criar lista de barramentos e suas tensões filtradas
        tensoes_barramentos_selecionados = [
            (nome.split('.')[0], vpu) for nome, vpu in barra_tensao_filtrada if nome.split('.')[0] in buses
        ]

        # Imprime barramentos com problemas
        if barras_problema:
            print(f"\nBarramentos com tensão entre 0.1 e 0.5 pu em {nome_alimentador}:")
            for nome, vpu in barras_problema:
                print(f"  - {nome}: {vpu:.3f} pu")

        # Geração do gráfico de dispersão
        if tensoes_barramentos_selecionados:
            nomes_nos, tensoes = zip(*tensoes_barramentos_selecionados)
            indices = list(range(len(tensoes)))

            # Gráfico de dispersão
            plt.figure(figsize=(14, 7))
            plt.scatter(indices, tensoes, color='royalblue', label='Tensão por nó', s=25)
            plt.axhline(y=1.05, color='green', linestyle='--', linewidth=1, label='Limite Superior (1.05 pu)')
            plt.axhline(y=0.95, color='red', linestyle='--', linewidth=1, label='Limite Inferior (0.95 pu)')
            plt.title(f"Tensões em pu - {nome_alimentador}", fontsize=16)
            plt.xlabel("Índice do nó", fontsize=12)
            plt.ylabel("Tensão (pu)", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle=':', linewidth=0.8)
            plt.tight_layout()
            plt.show()

        print(f"Simulação finalizada para {nome_alimentador}")


    @staticmethod
    def config_geradores(usar_geracao_hidraulica):
        """ Este método configura se os geradores vão ser considerados na simulação """

        if usar_geracao_hidraulica == False:
            dss.solution.solve()
            dss.generators.first()
            for nome in dss.generators.names:
                dss.text(f"disable generator.{nome}")
                dss.generators.next()
        else:
            return 0
        return 0
        
        

    @staticmethod
    def compilar(caminho):
        """ Este método compila o arquivo de modelagem do OpenDSS """
        dss.text("Clear")
        dss.text("Set DefaultBaseFrequency=60")
        dss.text(f"Compile {caminho}")

    
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
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.168, 0.227, 0.288, 0.349,
        0.409, 0.468, 0.525, 0.581, 0.633, 0.684, 0.731, 0.775, 0.816, 0.852,
        0.886, 0.915, 0.940, 0.961, 0.977, 0.989, 0.997, 1.000, 0.999, 0.993,
        0.982, 0.968, 0.948, 0.925, 0.897, 0.866, 0.830, 0.791, 0.748, 0.702,
        0.653, 0.601, 0.547, 0.490, 0.432, 0.372, 0.311, 0.250, 0.190, 0.133,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001]


        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            dss.pvsystems.irradiance = irradiance_96[ponto_simulacao]
            dss.pvsystems.next()


    @staticmethod
    def cargas_atualiza(ponto_simulacao, mes):
        """ Atualiza a carga consumida com base no perfil semanal e na curva anual. """
        nomes_cargas = dss.loads.names

        for nome in nomes_cargas:
            # Expressão regular para capturar o perfil semanal
            match = re.search(r'curva_diaria_[^_]+_([\d._-]+)_curva_anual', nome)
            if match:
                perfil_semanal = [float(v) for v in match.group(1).split('_')]
            else:
                print(f"Expressão regular não encontrou correspondência para: {nome}")
                perfil_semanal = [0.0] * 96  # Valor padrão em caso de erro

            # Expressão regular para capturar a curva anual
            match_curva_anual = re.search(r'curva_anual_([\d._]+)', nome)
            if match_curva_anual:
                curva_anual_str = match_curva_anual.group(1)
                if curva_anual_str:
                    try:
                        curva_anual = [float(v) for v in curva_anual_str.split('_') if v.strip() != '']
                        if len(curva_anual) != 12:
                            print(f"Erro: A curva anual em '{nome}' não tem 12 valores. Usando padrão.")
                            curva_anual = [1.0] * 12  # Atribuir um valor padrão caso a curva anual tenha erro
                    except ValueError as e:
                        print(f"Erro ao converter curva anual para números em '{nome}': {e}")
                        curva_anual = [1.0] * 12  # Atribuir valor padrão em caso de erro
                else:
                    print(f"Encontrado valor vazio para 'curva_anual' em '{nome}', atribuindo valor padrão.")
                    curva_anual = [1.0] * 12  # Atribuir valor padrão
            else:
                print(f"Expressão regular não encontrou correspondência para 'curva_anual' em: {nome}")
                curva_anual = [1.0] * 12  # Atribuir valor padrão

            # Verifique se o ponto de simulação está dentro do intervalo de perfil semanal
            if ponto_simulacao < len(perfil_semanal):
                valor_perfil_semanal = perfil_semanal[ponto_simulacao]
            else:
                print(f"Erro: Ponto de simulação {ponto_simulacao} fora do intervalo para 'perfil_semanal'. Usando valor padrão.")
                valor_perfil_semanal = 0.0

            # Verifique se o mês está dentro do intervalo de curva anual
            if mes < len(curva_anual):
                valor_curva_anual = curva_anual[mes]
            else:
                print(f"Erro: Mês {mes} fora do intervalo para 'curva_anual'. Usando valor padrão.")
                valor_curva_anual = 1.0  # Usando valor padrão

            # Aplique o valor de potência para a carga específica
            dss.loads.name = nome  # Selecione a carga pela identificação
            carga_kw = valor_curva_anual * valor_perfil_semanal
            dss.loads.kw = carga_kw  # Atualize a potência ativa para a carga

            # Avance para a próxima carga
            dss.loads.next()






    @staticmethod
    def tensoes():
        """ Este método coleta as tensões dos barramentos e retorna um dicionário com os nomes e tensões """
        barras_e_tensoes = defaultdict(float)
        list_barras_nodes = dss.circuit.nodes_names
        list_barras_vmag_pu = dss.circuit.buses_vmag_pu
        barra_tensao = list(zip(list_barras_nodes, list_barras_vmag_pu))
        for nome, vpu in barra_tensao:
            if nome.endswith((".1", ".2", ".3")) and vpu > 0:
                barras_e_tensoes[nome] = vpu
        return barras_e_tensoes

