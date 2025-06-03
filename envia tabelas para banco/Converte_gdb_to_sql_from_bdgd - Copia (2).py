import subprocess
import os
import time

# Caminho do diretório do OSGeo4W
#osgeo4w_bin_path = r"C:\OSGeo4W\bin"

# Adiciona o diretório do OSGeo4W ao PATH
#os.environ["PATH"] += os.pathsep + osgeo4w_bin_path

# Adiciona o caminho do ogr2ogr ao PATH
os.environ["PATH"] += os.pathsep + r"C:\OSGeo4W\bin"


# =======================================================================================

# ANTES DE TUDO, DEVE-SE ENTRAR NO PG ADMIN  E NO TERMINAR JÁ LOGADO NO BANCO DE DADOS 
# QUE SE VAI TRABALHAR, DEVE-SE COMPILAR O SEGUINTE COMANDO:

"""     
        ALTER SCHEMA public OWNER TO iuri;
        GRANT ALL ON SCHEMA public TO iuri;
"""
# TROQUE O NOME iuri pelo nome do seu usuário do banco



#=======================================================================================
# CAMINHO DA BASE DE DADOS DO BDGD

# DE PREFERÊNCIA ENVIE SUA BASE DE DADOS "BDGD" PARA A ÁREA DE TRABALHO E PEGUE APENAS A SUBPASTA
caminho = "C:\\TED MIDR\\envia tabelas para base de dados postgresql\\base de dados da distribuidora\\Energisa_MT_405_2023-12-31_V11_20240612-1317.gdb"

# Lista de camadas a serem convertidas
layers = [
     "ARAT", "SEGCON", "BAR", "BASE", "BAY", "BE", "CONJ", "CRVCRG", "CTAT", "CTMT",
     "EP", "EQCR", "EQME", "EQRE", "EQSE", "EQTRAT", "EQTRM", "EQTRMT",
     "PIP", "PNT", "PONNOT", "PT", "RAMLIG", "SSDAT", "SSDBT",
     "SSDMT", "SUB", "UNCRAT", "UNCRBT", "UNCRMT", "UNREAT", "UNREMT",
     "UNSEAT", "UNSEBT", "UNSEMT", "UNTRMT", "UNTRAT", "UCAT_tab", "UCBT_tab",
     "UCMT_tab", "UGAT_tab", "UGBT_tab", "UGMT_tab"
]

# Configurações do banco de dados PostgreSQL
db_params = {
    "host": "localhost",
    "user": "iuri",
    "dbname": "BDGD_2023_ENERGISA_MT",   # Coloque aqui o nome do seu banco de dados que vc criou no pgadmin
    "password": "aa11bb22"
}


def convert_layer(layer_name):
    if layer_name == "SEGCON":
        ogr2ogr_cmd = (
            f'ogr2ogr -f "PostgreSQL" '
            f'PG:"host={db_params["host"]} user={db_params["user"]} dbname={db_params["dbname"]} password={db_params["password"]}" '
            f'"{caminho}" '
            f'-nln {layer_name} -lco COLUMN_TYPES=cmax_renamed=Float8 '
            f'-sql "SELECT cod_id, dist, geom_cab, form_cab, mat_fas_1, mat_fas_2, mat_fas_3, mat_neu, iso_fas_1, iso_fas_2, iso_fas_3, iso_neu, cnd_fas, r1, x1, ftrcnv, cnom, cmax AS cmax_renamed, tuc_fas, a1_fas, a2_fas, a3_fas, a4_fas, a5_fas, a6_fas, tuc_neu, a1_neu, a2_neu, a3_neu, a4_neu, a5_neu, a6_neu, descr, bit_fas_1, bit_fas_2, bit_fas_3, bit_neu, r_regul, uar FROM {layer_name}"'
        )
    else:
        ogr2ogr_cmd = (
            f'ogr2ogr -f "PostgreSQL" '
            f'PG:"host={db_params["host"]} user={db_params["user"]} dbname={db_params["dbname"]} password={db_params["password"]}" '
            f'"{caminho}" '
            f'-nln {layer_name} '
            f'-lco GEOMETRY_NAME=Shape '
            f'-lco OVERWRITE=YES '
            f'-lco COLUMN_TYPES=cmax_renamed=Float8 '
            f'-sql "SELECT * FROM {layer_name}"'
        )

    try:
        print(f'Convertendo camada {layer_name}...')
        result = subprocess.run(ogr2ogr_cmd, shell=True, text=True, capture_output=True)
        if result.returncode != 0:
            print(f'Erro ao converter a camada {layer_name}:')
            print(result.stderr)
        else:
            print(f'Camada {layer_name} convertida com sucesso.')
    except Exception as e:
        print(f'Erro ao executar o comando para a camada {layer_name}: {e}')




# Etapa 2: Iniciar a conversão
start_time = time.time()

for layer in layers:
    convert_layer(layer)

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Conversão de todas as camadas concluída em {elapsed_time:.2f} segundos.")
