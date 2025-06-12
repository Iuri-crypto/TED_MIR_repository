import os
import re
from collections import defaultdict
import py_dss_interface
dss = py_dss_interface.DSS()
import os
import matplotlib.pyplot as plt
import py_dss_interface
import matplotlib.pyplot as plt
import seaborn as sns
import heapq
from datetime import timedelta

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



            class_Fluxo_de_Potencia.compilar(caminho)
            class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
            class_Fluxo_de_Potencia.modelo_carga(modelo_carga)
            class_Fluxo_de_Potencia.config_gd(usar_gd_bt, usar_gd_mt)
            class_Fluxo_de_Potencia.config_geradores(usar_geracao_hidraulica)
            class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)

            # Inicializa dicionários para armazenar potências e tensões
            potencias = defaultdict(lambda: defaultdict(float))
            tensoes = defaultdict(lambda: defaultdict(float))


            for i in range(288):  # Iteração para 96 pontos do dia
                dss.solution.max_iterations = 40
                dss.solution.max_control_iterations = 40
                carga_dict = class_Fluxo_de_Potencia.carrega_curvas()
                class_Fluxo_de_Potencia.cargas_atualiza(i, mes_index, carga_dict)
                
                class_Fluxo_de_Potencia.gd_ufs_atualiza(i)
                
                dss.solution.solve()
                
                potencias[i]['kw'] = round(dss.circuit.total_power[0], 2)  # Potência ativa
                potencias[i]['kvar'] = round(dss.circuit.total_power[1], 2)  # Potência reativa

                barra_tensao = list(zip(dss.circuit.nodes_names, dss.circuit.buses_vmag_pu))

                # Filtra barramentos com sufixos de fase e tensão > 0
                barra_tensao_filtrada = [(nome, vpu) for nome, vpu in barra_tensao
                                        if nome.endswith((".1", ".2", ".3")) and vpu > 0]

                if barra_tensao_filtrada:
                    n_pontos = max(1, len(barra_tensao_filtrada) // 100)  # Garante pelo menos 1 valor

                    # 1% mais baixos
                    menores_tensoes = heapq.nsmallest(n_pontos, barra_tensao_filtrada, key=lambda x: x[1])
                    menores_valores_tensao = [vpu for nome, vpu in menores_tensoes]

                    # 1% mais altos
                    maiores_tensoes = heapq.nlargest(n_pontos, barra_tensao_filtrada, key=lambda x: x[1])
                    maiores_valores_tensao = [vpu for nome, vpu in maiores_tensoes]

                    # Juntando os dois conjuntos em uma lista só
                    tensoes[i] = menores_valores_tensao + maiores_valores_tensao
                print("Iterações necessárias: {}".format(dss.solution.iterations))

                if i % 5 == 0:
                    nome_alimentador = os.path.basename(os.path.dirname(caminho))


                    class_Fluxo_de_Potencia.compilar(caminho)
                    class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
                    class_Fluxo_de_Potencia.modelo_carga(modelo_carga)
                    class_Fluxo_de_Potencia.config_gd(usar_gd_bt, usar_gd_mt)
                    class_Fluxo_de_Potencia.config_geradores(usar_geracao_hidraulica)
                    class_Fluxo_de_Potencia.config_cargas(usar_cargas_bt, usar_cargas_mt)
                    #class_Fluxo_de_Potencia.limites_carga()

                # Plotagem das potências ativas
            #class_Fluxo_de_Potencia.plot_potencias(nome_alimentador, potencias)

            # plotagem final
            # Se for necessário, plotar as tensões também
            # if exibir_tensao:
            #class_Fluxo_de_Potencia.plot_tensao(nome_alimentador, tensoes)

            class_Fluxo_de_Potencia.plot_potencia_e_tensao(nome_alimentador, potencias, tensoes)
            print("here")






    @staticmethod
    def plot_potencias(nome_alimentador, potencias):
        sns.set_style("whitegrid")
        plt.rcParams.update({
            'axes.facecolor': '#f9f9f9',
            'grid.color': '#e5e5e5',
            'axes.edgecolor': '#cccccc',
            'font.size': 10,
            'axes.titlesize': 16,
            'axes.labelsize': 12,
            'xtick.labelsize': 9,
            'ytick.labelsize': 9
        })

        pontos_por_dia = 96  # 24h * 4 (15 minutos)
        total_pontos = pontos_por_dia * 3  # 3 dias

        # Extrair potências ativas para todos os pontos
        potencias_ativas = [-potencias[i]['kw'] for i in range(total_pontos)]

        plt.figure(figsize=(15, 6))
        plt.plot(range(total_pontos), potencias_ativas, label='Potência Ativa (kW)', color='#1f77b4', linewidth=2)
        plt.fill_between(range(total_pontos), potencias_ativas, color='#1f77b4', alpha=0.1)

        # Marcar ticks a cada 45 minutos (45 min = 3 intervalos de 15 min)
        tick_interval = 3
        xticks_pos = list(range(0, total_pontos, tick_interval))

        dias = ['Dia Útil', 'Sábado', 'Domingo']
        xticks_labels = []

        for pos in xticks_pos:
            dia = pos // pontos_por_dia
            ponto_dia = pos % pontos_por_dia
            hora = ponto_dia // 4  # 4 intervalos de 15 min = 1 hora
            minuto = (ponto_dia % 4) * 15
            label = f"{hora:02d}:{minuto:02d}"
            if ponto_dia == 0:
                label = dias[dia] + "\n" + label
            xticks_labels.append(label)

        plt.xticks(xticks_pos, xticks_labels, rotation=45, ha='right')

        ax = plt.gca()
        for label in ax.get_xticklabels():
            text = label.get_text()
            for dia in dias:
                if text.startswith(dia):
                    label.set_color('blue')
                    label.set_fontsize(12)
                    label.set_fontweight('bold')



        # Linhas verticais para separar os dias
        for separador in [pontos_por_dia, 2 * pontos_por_dia]:
            plt.axvline(x=separador, color='gray', linestyle='--', linewidth=1)

        plt.title(f"Potência Ativa no Alimentador: {nome_alimentador}", fontsize=16, weight='bold')
        plt.xlabel("Hora do Dia")
        plt.ylabel("Potência Ativa (kW)")

        max_pot = max(potencias_ativas)
        min_pot = min(potencias_ativas)
        margem = (max_pot - min_pot) * 0.1
        plt.ylim(min_pot - margem, max_pot + margem)

        sns.despine()
        plt.legend(loc='upper right', frameon=True, framealpha=0.9)
        plt.tight_layout()
        plt.show()


    @staticmethod
    def plot_tensao(nome_alimentador, tensoes_dict):
        if not tensoes_dict:
            print("Nenhuma tensão válida para exibir.")
            return

        # Configurações iguais ao gráfico de potências
        sns.set_style("whitegrid")
        plt.rcParams.update({
            'axes.facecolor': '#f9f9f9',
            'grid.color': '#e5e5e5',
            'axes.edgecolor': '#cccccc',
            'font.size': 10,
            'axes.titlesize': 16,
            'axes.labelsize': 12,
            'xtick.labelsize': 9,
            'ytick.labelsize': 9
        })

        pontos_por_dia = 96  # 24h * 4 (15 minutos)
        total_pontos = pontos_por_dia * 3  # 3 dias

        indices_tempo = sorted(tensoes_dict.keys())
        tensoes_plot = [min(tensoes_dict[i]) for i in indices_tempo]

        # Criar labels dos horários
        dias = ['Dia Útil', 'Sábado', 'Domingo']
        tick_interval = 3  # a cada 45 minutos (3 intervalos de 15 min)

        xticks_pos = list(range(0, total_pontos, tick_interval))
        xticks_labels = []

        for pos in xticks_pos:
            dia = pos // pontos_por_dia
            ponto_dia = pos % pontos_por_dia
            hora = ponto_dia // 4
            minuto = (ponto_dia % 4) * 15
            label = f"{hora:02d}:{minuto:02d}"
            if ponto_dia == 0:
                label = dias[dia] + "\n" + label
            xticks_labels.append(label)

        plt.figure(figsize=(15, 6))

        plt.plot(indices_tempo, tensoes_plot, marker='o', linestyle='-', linewidth=2, markersize=5,
                color='#1f77b4', alpha=0.8)
        plt.fill_between(indices_tempo, tensoes_plot, color='#1f77b4', alpha=0.1)

        # Linhas de limite
        plt.axhline(1.0, color='green', linestyle='--', linewidth=1, label='1.0 pu')
        plt.axhline(0.93, color='orange', linestyle='--', linewidth=1, label='Limite Inferior (0.93 pu)')
        plt.axhline(1.05, color='red', linestyle='--', linewidth=1, label='Limite Superior (1.05 pu)')

        plt.xticks(ticks=xticks_pos, labels=xticks_labels, rotation=45, ha='right')

        # Destacar nomes dos dias em azul e maior
        ax = plt.gca()
        for label in ax.get_xticklabels():
            text = label.get_text()
            for dia in dias:
                if text.startswith(dia):
                    label.set_color('blue')
                    label.set_fontsize(12)
                    label.set_fontweight('bold')

        # Linhas verticais para separar os dias
        for separador in [pontos_por_dia, 2 * pontos_por_dia]:
            plt.axvline(x=separador, color='gray', linestyle='--', linewidth=1)

        plt.title(f"Tensão Mínima (1%) por Tempo - {nome_alimentador}", fontsize=16, weight='bold')
        plt.xlabel("Hora do Dia", fontsize=12)
        plt.ylabel("Tensão (pu)", fontsize=12)

        plt.ylim(0.85, max(tensoes_plot) + 0.05)

        sns.despine()
        plt.legend(loc='upper right', frameon=True, framealpha=0.9)
        plt.tight_layout()
        plt.show()



    @staticmethod
    def plot_potencia_e_tensao(nome_alimentador, potencias, tensoes_dict):
        if not tensoes_dict:
            print("Nenhuma tensão válida para exibir.")
            return

        sns.set_style("whitegrid")
        plt.rcParams.update({
            'axes.facecolor': '#f9f9f9',
            'grid.color': '#e5e5e5',
            'axes.edgecolor': '#cccccc',
            'font.size': 10,
            'axes.titlesize': 16,
            'axes.labelsize': 12,
            'xtick.labelsize': 9,
            'ytick.labelsize': 9
        })

        pontos_por_dia = 96  # 24h * 4 (15 minutos)
        total_pontos = pontos_por_dia * 3  # 3 dias

        # Dados potência ativa (negativo conforme seu código)
        potencias_ativas = [-potencias[i]['kw'] for i in range(total_pontos)]

        # Tensões mínimas (menor valor para cada instante)
        indices_tempo = sorted(tensoes_dict.keys())
        tensoes_plot = [min(tensoes_dict[i]) for i in indices_tempo]

        # Criar labels do eixo x
        dias = ['Dia Útil', 'Sábado', 'Domingo']
        tick_interval = 3  # a cada 45 minutos (3 intervalos de 15 min)

        xticks_pos = list(range(0, total_pontos, tick_interval))
        xticks_labels = []
        for pos in xticks_pos:
            dia = pos // pontos_por_dia
            ponto_dia = pos % pontos_por_dia
            hora = ponto_dia // 4
            minuto = (ponto_dia % 4) * 15
            label = f"{hora:02d}:{minuto:02d}"
            if ponto_dia == 0:
                label = dias[dia] + "\n" + label
            xticks_labels.append(label)

        # Criar figura e eixo principal (potência)
        fig, ax1 = plt.subplots(figsize=(15, 6))

        # Plot potência no eixo y da esquerda
        ax1.plot(range(total_pontos), potencias_ativas, label='Potência Ativa (kW)', color='#1f77b4', linewidth=2)
        ax1.fill_between(range(total_pontos), potencias_ativas, color='#1f77b4', alpha=0.1)
        ax1.set_xlabel("Hora do Dia")
        ax1.set_ylabel("Potência Ativa (kW)", color='#1f77b4')
        ax1.tick_params(axis='y', labelcolor='#1f77b4')

        max_pot = max(potencias_ativas)
        min_pot = min(potencias_ativas)
        margem = (max_pot - min_pot) * 0.1
        ax1.set_ylim(min_pot - margem, max_pot + margem)

        # Configurar ticks do eixo x com labels
        ax1.set_xticks(xticks_pos)
        ax1.set_xticklabels(xticks_labels, rotation=45, ha='right')

        # Destacar nomes dos dias em azul e maior fonte no eixo x
        for label in ax1.get_xticklabels():
            text = label.get_text()
            for dia in dias:
                if text.startswith(dia):
                    label.set_color('blue')
                    label.set_fontsize(12)
                    label.set_fontweight('bold')

        # Linhas verticais para separar os dias
        for separador in [pontos_por_dia, 2 * pontos_por_dia]:
            ax1.axvline(x=separador, color='gray', linestyle='--', linewidth=1)

        # Criar eixo y secundário para a tensão
        ax2 = ax1.twinx()
        ax2.plot(indices_tempo, tensoes_plot, marker='o', linestyle='-', linewidth=2, markersize=5,
                color='orange', alpha=0.8, label='Tensão Mínima (pu)')
        ax2.fill_between(indices_tempo, tensoes_plot, color='orange', alpha=0.1)

        # Linhas de limite na tensão
        ax2.axhline(1.0, color='green', linestyle='--', linewidth=1, label='1.0 pu')
        ax2.axhline(0.93, color='red', linestyle='--', linewidth=1, label='Limite Inferior (0.93 pu)')
        ax2.axhline(1.05, color='red', linestyle='--', linewidth=1, label='Limite Superior (1.05 pu)')

        ax2.set_ylabel("Tensão (pu)", color='orange')
        ax2.tick_params(axis='y', labelcolor='orange')

        ax2.set_ylim(0.85, max(tensoes_plot) + 0.05)

        # Título geral
        plt.title(f"Potência e Tensão no Alimentador: {nome_alimentador}", fontsize=16, weight='bold')

        # Ajustes finais
        sns.despine(ax=ax1)
        sns.despine(ax=ax2, right=False)

        # Combinar legendas dos dois eixos
        lines_1, labels_1 = ax1.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper right', frameon=True, framealpha=0.9)

        plt.tight_layout()
        plt.show()
        
        

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
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.168, 0.227, 0.288, 0.349,
        0.409, 0.468, 0.525, 0.581, 0.633, 0.684, 0.731, 0.775, 0.816, 0.852,
        0.886, 0.915, 0.940, 0.961, 0.977, 0.989, 0.997, 1.000, 0.999, 0.993,
        0.982, 0.968, 0.948, 0.925, 0.897, 0.866, 0.830, 0.791, 0.748, 0.702,
        0.653, 0.601, 0.547, 0.490, 0.432, 0.372, 0.311, 0.250, 0.190, 0.133,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
        0.0001, 0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
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
        return

    @staticmethod
    def carrega_curvas():
        """ Carrega as curvas de carga """

        carga_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        dss.loads.first()

        for nome in dss.loads.names:
            partes = nome.split('_')

            extrair_du = False
            extrair_sa = False
            extrair_do = False

            dados_du = []
            dados_sa = []
            dados_do = []
            dados_curva_anual = []

            i = 0
            while i < len(partes):
                item = partes[i]

                if item == 'du':
                    extrair_du = True
                    extrair_sa = extrair_do = False
                    i += 1
                    continue
                elif item == 'sa':
                    extrair_sa = True
                    extrair_du = extrair_do = False
                    i += 1
                    continue
                elif item == 'do':
                    extrair_do = True
                    extrair_du = extrair_sa = False
                    i += 1
                    continue
                elif item == 'curva':
                    # Previne erro ao tentar converter 'curva' para float
                    extrair_du = extrair_sa = extrair_do = False
                    i += 1
                    continue
                elif item == 'anual':
                    # Previne erro ao tentar converter 'anual' para float
                    extrair_du = extrair_sa = extrair_do = False
                    # Extração dos 12 valores após 'curva_anual'
                    dados_curva_anual = [float(partes[j]) for j in range(i+1, i+13)]
                    i += 13  # pula 'anual' + 12 valores
                    continue

                # Armazena os dados nas listas corretas
                if extrair_du:
                    try:
                        dados_du.append(float(item))
                    except ValueError:
                        extrair_du = False
                elif extrair_sa:
                    try:
                        dados_sa.append(float(item))
                    except ValueError:
                        extrair_sa = False
                elif extrair_do:
                    try:
                        dados_do.append(float(item))
                    except ValueError:
                        extrair_do = False

                i += 1

            carga_dict[nome]['semana'] = dados_du + dados_sa + dados_do
            carga_dict[nome]['curva_anual'] = dados_curva_anual

            dss.loads.next()
        return carga_dict



    from collections import defaultdict

    # @staticmethod
    # def carrega_curvas():
    #     """ Carrega as curvas de carga """

    #     carga_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    #     dss.loads.first()

    #     for nome in dss.loads.names:
    #         partes = nome.split('_')

    #         extrair_du = False
    #         extrair_sa = False
    #         extrair_do = False

    #         dados_du = []
    #         dados_sa = []
    #         dados_do = []
    #         dados_curva_anual = []

    #         i = 0
    #         while i < len(partes):
    #             item = partes[i]

    #             if item == 'du':
    #                 extrair_du = True
    #                 extrair_sa = extrair_do = False
    #                 i += 1
    #                 continue
    #             elif item == 'sa':
    #                 extrair_sa = True
    #                 extrair_du = extrair_do = False
    #                 i += 1
    #                 continue
    #             elif item == 'do':
    #                 extrair_do = True
    #                 extrair_du = extrair_sa = False
    #                 i += 1
    #                 continue
    #             elif item == 'curva':
    #                 # Previne erro ao tentar converter 'curva' para float
    #                 extrair_du = extrair_sa = extrair_do = False
    #                 i += 1
    #                 continue
    #             elif item == 'anual':
    #                 # Previne erro ao tentar converter 'anual' para float
    #                 extrair_du = extrair_sa = extrair_do = False
    #                 # Extração dos 12 valores após 'curva_anual'
    #                 dados_curva_anual = [float(partes[j]) for j in range(i+1, i+13)]
    #                 i += 13  # pula 'anual' + 12 valores
    #                 continue

    #             # Armazena os dados nas listas corretas
    #             if extrair_du:
    #                 try:
    #                     dados_du.append(float(item))
    #                 except ValueError:
    #                     extrair_du = False
    #             elif extrair_sa:
    #                 try:
    #                     dados_sa.append(float(item))
    #                 except ValueError:
    #                     extrair_sa = False
    #             elif extrair_do:
    #                 try:
    #                     dados_do.append(float(item))
    #                 except ValueError:
    #                     extrair_do = False

    #             i += 1

    #         carga_dict[nome]['semana'] = dados_du + dados_sa + dados_do
    #         carga_dict[nome]['curva_anual'] = dados_curva_anual

    #         dss.loads.next()
    #     return carga_dict



    @staticmethod
    def cargas_atualiza(ponto_simulacao, mes, carga_dict):
        """ Atualiza a carga consumida com base no perfil semanal e na curva anual. """

        nomes = dss.loads.names
        dss.loads.first()
        for nome in nomes:
            min_15 = carga_dict[nome]['semana'][ponto_simulacao]
            mes_kw = carga_dict[nome]['curva_anual'][mes]
            dss.loads.kw = min_15 * mes_kw
            dss.loads.next()

        return






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

