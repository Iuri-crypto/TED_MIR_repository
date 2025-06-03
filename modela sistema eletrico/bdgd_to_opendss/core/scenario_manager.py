import yaml

class ScenarioManager:
    def __init__(self, config_path="modela sistema eletrico/bdgd_to_opendss/config/scenario_config.yaml"):
        with open(config_path, "r") as file:
            self.scenarios = yaml.safe_load(file).get("cenarios", {})

    def get_scenario(self, **kwargs):
        for key, is_active in kwargs.items():
            if is_active:
                if key in self.scenarios:
                    print(f"Cenário ativado: {key} --> {self.scenarios[key]}")
                    return key
                else:
                    print(f"ERRO - Cenário '{key}' não encontrado no arquivo de configuração.")
                    return "error"
        print("ERRO - Nenhum cenário foi ativado.")
        return 1
