from bdgd_to_opendss.db.db_conector import get_connection
import yaml

def load_db_config(*nomes_bancos: str) -> dict:
    with open("bdgd_to_opendss/config/db_config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    bancos_config = {}
    for nome in nomes_bancos:
        if nome in config['bancos']:
            bancos_config[nome] = config['bancos'][nome]
        else:
            print(f"Aviso: Configuração para o banco '{nome}' não encontrada.")
    
    return bancos_config

