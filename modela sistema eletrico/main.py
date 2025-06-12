from bdgd_to_opendss.core.scenario_manager import ScenarioManager
from bdgd_to_opendss.core.model_runner import ModelRunner




def main():

    scenario = ScenarioManager().get_scenario(cenario_1 = False, cenario_2 = True, cenario_3 = False)
    
    # Caminho onde será salvo o cenário modelado
    caminho = r'C:\TED_MIDR\modelagens\cenario_2'

    # 3. Rodar a modelagem
    model_runner = ModelRunner(scenario)
    model_runner.run(caminho)

if __name__ == "__main__":
    main()
 
 
    