import os
import re
import uuid
# import matplotlib.pyplot as plt  # Comentado para desabilitar gráficos
from shapely.geometry import Polygon, LineString
from shapely.ops import transform
import pyproj

def processa_arquivos_dss(caminho_base):
    dados = {}

    # Transformador: WGS84 → UTM (ajuste o EPSG conforme necessário)
    project = pyproj.Transformer.from_crs("epsg:4326", "epsg:32722", always_xy=True).transform

    for pasta_principal in os.listdir(caminho_base):
        caminho_pasta_principal = os.path.join(caminho_base, pasta_principal)
        if not os.path.isdir(caminho_pasta_principal):
            continue

        dados[pasta_principal] = {}

        for subpasta in os.listdir(caminho_pasta_principal):
            caminho_subpasta = os.path.join(caminho_pasta_principal, subpasta)
            if not os.path.isdir(caminho_subpasta):
                continue

            caminho_arquivo = os.path.join(caminho_subpasta, "run_cargas_agregadas.dss")
            if not os.path.isfile(caminho_arquivo):
                continue

            try:
                with open(caminho_arquivo, "r", encoding="utf-8") as f:
                    linhas = f.readlines()

                if any(l.strip().startswith("New Object") for l in linhas):
                    continue

                maior_kv = None
                for l in linhas:
                    vb_match = re.search(r'Set VoltageBases\s*=\s*"([^"]+)"', l)
                    if vb_match:
                        tensoes = [float(t.strip().replace(',', '.')) for t in vb_match.group(1).split(',')]
                        if tensoes:
                            maior_kv = max(tensoes)
                    if maior_kv:
                        break

                if maior_kv is None:
                    print(f"Tensões não encontradas em {caminho_arquivo}, pulando.")
                    continue

                subestacao_info = {}
                linhas_info = []

                for linha in linhas:
                    linha = linha.strip()

                    if linha.startswith("!coord_"):
                        coords_match = re.findall(r"\(([-\d\.]+), ([-\d\.]+)\)", linha)
                        nome_match = re.search(r"_nome_(.*?)_cod_([0-9]+)", linha)

                        if coords_match and nome_match:
                            coords = [{"lat": float(lat), "lon": float(lon)} for lat, lon in coords_match]
                            nome, cod = nome_match.groups()
                            subestacao_info = {
                                "nome": nome,
                                "codigo": cod,
                                "coordenadas": coords
                            }

                    if linha.startswith("New Line.coord_") and "linha_media" in linha:
                        coord_match = re.match(
                            r"New Line\.coord_([-\d\.]+)_([-\d\.]+)__([-\d\.]+)_([-\d\.]+)_([0-9]+)_linha_media", linha
                        )
                        bus1_match = re.search(r"Bus1=([^\s]+)", linha)
                        bus2_match = re.search(r"Bus2=([^\s]+)", linha)

                        if coord_match and bus1_match and bus2_match:
                            lat1, lon1, lat2, lon2, cod = coord_match.groups()
                            bus1 = bus1_match.group(1)
                            bus2 = bus2_match.group(1)

                            linhas_info.append({
                                "codigo": cod,
                                "bus1": {"lat": float(lat1), "lon": float(lon1), "nome": bus1},
                                "bus2": {"lat": float(lat2), "lon": float(lon2), "nome": bus2}
                            })

                if subestacao_info or linhas_info:
                    dados[pasta_principal][subpasta] = {
                        "subestacao": subestacao_info,
                        "linhas": linhas_info
                    }

                    # fig, ax = plt.subplots(figsize=(6, 6))  # Gráfico desabilitado
                    polygon = None
                    legendas = {"Dentro": [], "Cruza": [], "Proxima": []}

                    if subestacao_info.get("coordenadas") and len(subestacao_info["coordenadas"]) >= 3:
                        coords = subestacao_info["coordenadas"]
                        polygon_coords = [(p["lat"], p["lon"]) for p in coords]
                        polygon = Polygon(polygon_coords)
                        polygon_utm = transform(project, polygon)

                        # x_poly, y_poly = zip(*[(p["lon"], p["lat"]) for p in coords])
                        # ax.plot(x_poly, y_poly, 'blue', linewidth=1.5)
                        # ax.fill(x_poly, y_poly, 'blue', alpha=0.1)

                    for linha in linhas_info:
                        x = [linha['bus1']['lon'], linha['bus2']['lon']]
                        y = [linha['bus1']['lat'], linha['bus2']['lat']]
                        line_geom = LineString([
                            (linha['bus1']['lat'], linha['bus1']['lon']),
                            (linha['bus2']['lat'], linha['bus2']['lon'])
                        ])
                        line_geom_utm = transform(project, line_geom)

                        if polygon:
                            buffer_polygon = polygon_utm.buffer(3000)
                            nome_linha = f"{linha['bus1']['nome']} → {linha['bus2']['nome']}"

                            if polygon_utm.contains(line_geom_utm):
                                # ax.plot(x, y, 'green', linewidth=1.5)
                                legendas["Dentro"].append(nome_linha)
                            elif polygon_utm.intersects(line_geom_utm):
                                # ax.plot(x, y, 'orange', linewidth=1.5)
                                legendas["Cruza"].append(nome_linha)
                            elif buffer_polygon.contains(line_geom_utm):
                                # ax.plot(x, y, 'purple', linewidth=1.5)
                                legendas["Proxima"].append(nome_linha)

                    nome_slack = f"{pasta_principal}_{subpasta}_Barra_Slack"
                    linha_slack = f'New Object = Circuit.{nome_slack}\n~ basekv = {maior_kv} pu = 1.0 angle = 0\n'

                    barras_conectadas = set()
                    for linha in linhas_info:
                        nome_linha = f"{linha['bus1']['nome']} → {linha['bus2']['nome']}"
                        if (nome_linha in legendas["Dentro"] 
                            or nome_linha in legendas["Cruza"] 
                            or nome_linha in legendas["Proxima"]):
                            barras_conectadas.add(linha['bus1']['nome'])
                            barras_conectadas.add(linha['bus2']['nome'])

                    linhas_novas = []
                    for bus in sorted(barras_conectadas):
                        id_unico = uuid.uuid4().hex[:6]
                        nome = f"{nome_slack}_{id_unico}"
                        nova_linha = f'New Line.{nome} phases=3 bus1= SourceBus bus2={bus} switch=y\n'
                        linhas_novas.append(nova_linha)

                    conteudo_novo = [linha_slack] + linhas_novas + ["\n"] + linhas
                    try:
                        with open(caminho_arquivo, "w", encoding="utf-8") as f:
                            f.writelines(conteudo_novo)
                        print(f"Slack inserida e conectada em: {caminho_arquivo}")
                    except Exception as e:
                        print(f"Erro ao inserir barra slack em {caminho_arquivo}: {e}")

                    # título opcional que apareceria no gráfico (comentado)
                    # titulo_status = f'{pasta_principal} / {subpasta}'
                    # for tipo, linhas_legenda in legendas.items():
                    #     if linhas_legenda:
                    #         titulo_status += f"\n{tipo.upper()}:"
                    #         for linha_leg in linhas_legenda:
                    #             titulo_status += f"\n↳ {linha_leg}"

                    # ax.set_title(titulo_status, fontsize=8)
                    # ax.set_xlabel('Longitude')
                    # ax.set_ylabel('Latitude')
                    # ax.grid(True)
                    # plt.tight_layout()
                    # plt.show()

            except Exception as e:
                print(f"Erro ao processar {caminho_arquivo}: {e}")

    return dados
