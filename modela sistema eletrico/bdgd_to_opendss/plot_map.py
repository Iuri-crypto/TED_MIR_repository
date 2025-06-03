import geopandas as gpd
import folium
import tempfile
import webbrowser
import os

# Caminho para o shapefile
shapefile_path = r"C:\Users\muril\Downloads\MT_setores_CD2022\MT_setores_CD2022.shp"

# Carrega os dados
gdf = gpd.read_file(shapefile_path)

# Converte para EPSG:4326 (lat/lon) se necessário
if gdf.crs != "EPSG:4326":
    gdf = gdf.to_crs("EPSG:4326")

# Calcula centro
centro = gdf.geometry.centroid.unary_union.centroid
mapa = folium.Map(location=[centro.y, centro.x], zoom_start=6, control_scale=True)

# Adiciona a camada dos setores
folium.GeoJson(
    gdf,
    name="Setores Censitários",
    style_function=lambda x: {
        "fillColor": "lightgray",
        "color": "black",
        "weight": 0.3,
        "fillOpacity": 0.5
    }
).add_to(mapa)

# Controles
folium.LayerControl().add_to(mapa)

# Salva em arquivo temporário e abre no navegador
with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f:
    mapa.save(f.name)
    webbrowser.open('file://' + os.path.realpath(f.name))
