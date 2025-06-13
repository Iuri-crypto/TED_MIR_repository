import os
from bdgd_to_opendss.core.scenario_manager import ScenarioManager
from bdgd_to_opendss.core.model_runner import ModelRunner

def main():
    scenario = ScenarioManager().get_scenario(cenario_1=False, cenario_2=True, cenario_3=False)

    # Caminho onde será salvo o cenário modelado
    caminho = r"C:\TED_MIDR\Output_modelagem_DSS"

    # Criar a pasta caso não exista
    os.makedirs(caminho, exist_ok=True)

    # Rodar a modelagem
    model_runner = ModelRunner(scenario)
    model_runner.run(caminho)

if __name__ == "__main__":
    main()
