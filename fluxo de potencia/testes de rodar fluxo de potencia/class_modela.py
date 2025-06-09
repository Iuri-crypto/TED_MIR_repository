"""
        Esta classe é chamada quando é necessário modelar algum cenário específico
"""

from modela_bdgd_to_opendss import Modela 

class Modela_Cenarios:
    
    def __init__(self, cenario_1, cenario_2, cenario_3, cenario_4, cenario_1_alimentador,
                      cenario_2_alimentador, cenario_3_subestacao, cenario_4_regiao):
        """
            # Cenário 1: Carga média, cargas baixa e fluxo de potência por alimentador
            # Cenário 2: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por alimentador
            # Cenário 3: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por subestação
            # Cenário 4: Carga baixa agregadas aos trafos de média tensão e fluxo de potência por subestação considerando o sistema de subtransmissão
           
            # SE CENÁRIO 1 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UM ALIMENTADOR OU TODOS OS ALIMENTADORES
            # SE CENÁRIO 2 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UM ALIMENTADOR OU TODOS OS ALIMENTADORES
            # SE CENÁRIO 3 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UMA OU TODAS AS SUBESTAÇÕES
            # SE CENÁRIO 4 ESCOLHIDO: ENTÃO ESCOLHER SE VAI SIMULAR UMA OU MAIS REGIÕES 
        """
        self.cenario_1 = cenario_1
        self.cenario_2 = cenario_2
        self.cenario_3 = cenario_3
        self.cenario_4 = cenario_4
        self.cenario_1_alimentador = cenario_1_alimentador
        self.cenario_2_alimentador = cenario_2_alimentador
        self.cenario_3_subestacao = cenario_3_subestacao
        self.cenario_4_regiao = cenario_4_regiao
        
        
    
    def modelar(self):
        """ Este método é responsável por modelar o cenário escolhido"""
        
        # Caso o cenário 1 seja escolhido
        if self.cenario_1:
            
            pass
            
            
        elif self.cenario_2:
            pass
            
            
        elif self.cenario_3:
            pass
            
            
        elif self.cenario_4:
            pass
            
            
        elif self.cenario_1_alimentador:
            pass
            
            
        elif self.cenario_2_alimentador:
            pass
            
            
        elif self.cenario_3_subestacao:
            pass
            
            
        elif self.cenario_4_regiao:
            pass
        
        
        else:
            raise ValueError("Nenhum cenário válido foi selecionado.")
            
            
        
    
    