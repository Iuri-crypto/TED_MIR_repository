
import yaml
import os

def load_validation_config(section: str):
    config_path = os.path.join("modela sistema eletrico","bdgd_to_opendss", "validation", "validation_config.yaml")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        return config.get(section, {})  
