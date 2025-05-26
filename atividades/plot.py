"""
    Autor: Iuri Lorenzo Quirino Moraes Silva
    Data: 2025-05-22
"""
import numpy as np
import matplotlib.pyplot as plt

class plot_class:
    def __init__(self, ctmt, ct_cod_op, curva_alim_temp, curvas_somadas):
        self.test = None
        self.curva_alim_temp = curva_alim_temp
        self.curvas_somadas = curvas_somadas
        self.ctmt = ctmt
        self.ct_cod_op = ct_cod_op

    def plotar_curvas_carga_comparadas(self):
        """
        Plota curvas de carga somadas (cargas) e ponderadas (alimentador) para todos CTMTs comuns.
        """
        try:
            # Percorre todas as chaves de curvas_somadas
            for ctmt in self.curvas_somadas.keys():
                # Busca a curva somada
                curva_cargas_somadas = self.curvas_somadas.get(ctmt)
                
                # Agora procuramos a curva ponderada em todas entradas de curva_alim_temp
                for ct_cod_op, alim_data in self.curva_alim_temp.items():
                    curva_ponderada = alim_data.get(ctmt)
                    
                    if curva_ponderada is not None:
                        # Se achar correspondente, plota ambas as curvas
                        tempo = np.arange(0, 24, 0.25)  # 96 pontos

                        fig, ax = plt.subplots(figsize=(14, 6))
                        ax.plot(tempo, curva_cargas_somadas, label="Curva das cargas somadas", linewidth=2, color="#1f77b4")
                        ax.plot(tempo, curva_ponderada, label="Curva do alimentador (ponderada)", linewidth=2.5, color="#eb1111")

                        # Eixo X com marcações de hora
                        x_ticks = np.arange(0, 24, 0.25)
                        x_labels = [f"{int(h):02d}:{int((h % 1) * 60):02d}" for h in x_ticks]
                        ax.set_xticks(x_ticks[::4])  # a cada 1h
                        ax.set_xticklabels(x_labels[::4], rotation=45, fontsize=8)
                        ax.set_xlim(0, 24)

                        # Detalhes do gráfico
                        ax.set_xlabel("Horário", fontsize=12)
                        ax.set_ylabel("Potência (kW)", fontsize=12)
                        ax.set_title(f"Curva de Carga - CTMT: {ctmt} | CT Nome: {ct_cod_op}", fontsize=14, weight='bold')
                        ax.grid(True, which='major', linestyle='--', linewidth=0.5, alpha=0.7)
                        ax.legend(fontsize=11)
                        plt.tight_layout()
                        plt.show()

                        break  # Plota apenas 1 curva do alimentador correspondente por CTMT

        except Exception as e:
            print(f"Ocorreu um erro ao plotar curvas: {e}")
