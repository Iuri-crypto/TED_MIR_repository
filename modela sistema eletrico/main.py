from bdgd_to_opendss.core.scenario_manager import ScenarioManager
from bdgd_to_opendss.core.model_runner import ModelRunner




def main():

    # 2. Escolha apenas um cenário para ser modelado
    # ----> scenario pode vir com ex: cenario_1, ou:
    # ----> scenario pode vir com 1, indicando erro !!!
    scenario = ScenarioManager().get_scenario(cenario_1 = True, cenario_2 = False, cenario_3 = False)
    
    # Caminho onde será salvo o cenário modelado
    caminho = 'bdgd_to_opendss\\cenarios\\cenario_1'

    # 3. Rodar a modelagem
    model_runner = ModelRunner(scenario)
    model_runner.run(caminho)

if __name__ == "__main__":
    main()
 