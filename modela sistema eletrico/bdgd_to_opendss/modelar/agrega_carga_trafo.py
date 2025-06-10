import os
import re
import pandas as pd
from collections import defaultdict
import numpy as np  # necessário para vetores
import hashlib
from tqdm import tqdm



class Agrega_Carga_Trafos:
    def __init__(self, caminho, df_cargas_Agregadas, df_GD_FV_Agregadas):
        self.caminho = caminho
        self.df_cargas_Agregadas = df_cargas_Agregadas 
        self.df_GD_FV_Agregadas = df_GD_FV_Agregadas


    def agrupar_por_nome(self, df: pd.DataFrame) -> dict:
        """
        Agrupa o DataFrame por 'nome' e retorna um dicionário onde:
        - chave = nome do trafo (coluna 'nome')
        - valor = DataFrame com todas as linhas associadas a esse nome
        """
        grupos = {}
        for nome, grupo in df.groupby('nome'):
            grupos[nome] = grupo.reset_index(drop=True)
        return grupos



    def soma_cargas_e_gd_nos_trafos(self, dicionario_cargas_por_nome, dicionario_gd_por_nome):
        """
        Processa as cargas e as unidades de geração distribuída (GD) associadas a cada transformador.

        Objetivos:
        - Associar cargas e GDs aos transformadores com base no código de identificação (cod_id).
        - Ignorar as curvas de GD (xycurve, loadshape, etc.).
        - Extrair informações elétricas dos transformadores (tensão e barramento secundário).
        - Retornar uma estrutura organizada por transformador.

        Parâmetros:
        - dicionario_cargas_por_nome: dict com DataFrames contendo as cargas por nome de pasta.
        - dicionario_gd_por_nome: dict com DataFrames contendo GDs por nome de pasta.

        Retorna:
        - trafos_cargas: dict estruturado por pasta e por transformador, contendo:
            - cargas associadas
            - GDs associadas
            - informações do trafo (tensão secundária e barramento secundário)
        """
        trafos_cargas = {}

        # Padrões regex para encontrar elementos no run.dss
        padrao_carga = re.compile(r"(?i)new\s+load\.nome_([^_\s]+).*?(carga_pip|carga_baixa)\b")
        padroes_gd = ("New pvsystem", "New xycurve", "New loadshape", "New tshape")
        padrao_trafo = re.compile(r"(?i)new\s+transformer\.(\S+)")
        padrao_kv2 = re.compile(r"(?i)~\s*wdg\s*=\s*2.*?kv\s*=\s*([\d.]+)")
        padrao_bus2 = re.compile(r"(?i)~\s*wdg\s*=\s*2.*?bus\s*=\s*(\S+)")

        # Percorre os diretórios que contêm os arquivos run.dss
        for root, dirs, files in os.walk(self.caminho):
            if 'run.dss' not in files:
                continue  # pula pastas que não contêm o arquivo principal

            nome_pasta = os.path.basename(root)
            if nome_pasta not in dicionario_cargas_por_nome:
                continue  # ignora pastas sem carga mapeada

            # Cria mapeamento cod_id -> trafo a partir do DataFrame de cargas
            df_cargas = dicionario_cargas_por_nome[nome_pasta]
            cod_id_para_trafo = dict(zip(df_cargas["cod_id"].astype(str), df_cargas["uni_tr_mt"]))

            # Faz o mesmo para GD, se houver
            df_gd = dicionario_gd_por_nome.get(nome_pasta)
            cod_id_para_trafo_gd = {}
            if df_gd is not None:
                cod_id_para_trafo_gd = dict(zip(df_gd["cod_id"].astype(str), df_gd["uni_tr_mt"]))

            caminho_arquivo = os.path.join(root, 'run.dss')
            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                linhas = f.readlines()

            # Inicializa listas de resultados temporários
            linhas_sem_gd = []         # armazenará linhas do run.dss sem as curvas de GD
            cargas_encontradas = []    # armazenará tuplas (trafo, linha) para cargas
            gd_encontradas = []        # armazenará tuplas (trafo, linha) para GDs
            trafos_info = {}           # armazenará info extraída dos trafos

            # Armazena blocos de curvas auxiliares temporariamente por cod_id (fora do loop!)
            curvas_aux_por_cod_id = {}

            # Leitura linha a linha
            i = 0
            while i < len(linhas):
                linha = linhas[i]


                linha_strip = linha.strip().lower()

                # Detecta curvas auxiliares e armazena temporariamente
                if linha_strip.startswith("new xycurve.") or \
                linha_strip.startswith("new loadshape.") or \
                linha_strip.startswith("new tshape."):

                    curva_linhas = [linha.strip()]
                    i += 1

                    # Coleta linhas de continuação
                    while i < len(linhas) and linhas[i].strip().startswith("~"):
                        curva_linhas.append(linhas[i].strip())
                        i += 1

                    curva_bloco = "\n".join(curva_linhas)

                    # Tenta identificar cod_id
                    for cod_id in cod_id_para_trafo_gd:
                        if cod_id in curva_bloco:
                            curvas_aux_por_cod_id.setdefault(cod_id, []).append(curva_bloco)
                            break

                    continue  # pula para a próxima linha

                # Detecta um novo PVSystem
                if linha_strip.startswith("new pvsystem."):
                    bloco_gd = [linha.strip()]
                    i += 1

                    # Coleta linhas ~ de continuação
                    while i < len(linhas) and linhas[i].strip().startswith("~"):
                        bloco_gd.append(linhas[i].strip())
                        i += 1

                    bloco_texto = " ".join(bloco_gd)

                    for cod_id, trafo in cod_id_para_trafo_gd.items():
                        if cod_id in bloco_texto:
                            # Inclui curvas auxiliares associadas antes do pvsystem
                            curvas_aux = curvas_aux_por_cod_id.get(cod_id, [])
                            bloco_completo = curvas_aux + bloco_gd  # mantém ordem correta
                            gd_encontradas.append((trafo, "\n".join(bloco_completo)))
                            break

                    continue  # já avançou 'i', então próxima linha


                # Caso seja um transformador
                match_trafo = padrao_trafo.search(linha)
                if match_trafo:
                    nome_trafo = match_trafo.group(1)
                    tensao_secundario = None
                    bus2 = None

                    # Lê as próximas linhas (~) para extrair kv e bus2 do enrolamento secundário
                    j = i + 1
                    while j < len(linhas) and linhas[j].strip().startswith("~"):
                        linha_sub = linhas[j]
                        if "wdg=2" in linha_sub.lower():
                            match_kv2 = padrao_kv2.search(linha_sub)
                            match_bus2 = padrao_bus2.search(linha_sub)
                            if match_kv2:
                                tensao_secundario = float(match_kv2.group(1))
                            if match_bus2:
                                bus2 = match_bus2.group(1)
                        j += 1

                    # Armazena as infos do trafo se encontradas
                    if tensao_secundario is not None and bus2 is not None:
                        trafos_info[nome_trafo] = {
                            "tensao_secundario_kv": tensao_secundario,
                            "bus2": bus2
                        }

                    # Mantém as linhas do trafo no processamento interno
                    linhas_sem_gd.append(linha)
                    linhas_sem_gd.extend(linhas[i + 1:j])
                    i = j
                    continue

                # Caso seja uma carga válida
                match_carga = padrao_carga.search(linha)
                if match_carga:
                    cod_id_sujo = match_carga.group(1)
                    cod_id = cod_id_sujo.strip('"')
                    trafo = cod_id_para_trafo.get(cod_id)
                    if trafo:
                        cargas_encontradas.append((trafo, linha.strip()))

                # Mantém a linha (não é GD)
                linhas_sem_gd.append(linha)
                i += 1

            # → Não salva mais o run_clean.dss (removido conforme solicitado)

            # Inicializa estrutura para a pasta, se necessário
            if nome_pasta not in trafos_cargas:
                trafos_cargas[nome_pasta] = {}

            # Agrupa as cargas por transformador
            for trafo, linha_carga in cargas_encontradas:
                trafos_cargas[nome_pasta].setdefault(trafo, {"cargas": [], "gd": []})
                trafos_cargas[nome_pasta][trafo]["cargas"].append(linha_carga)

            # Agrupa as GDs por transformador
            for trafo, linha_gd in gd_encontradas:
                trafos_cargas[nome_pasta].setdefault(trafo, {"cargas": [], "gd": []})
                trafos_cargas[nome_pasta][trafo]["gd"].append(linha_gd)

            # Adiciona informações dos transformadores (tensão e bus2)
            for trafo, info in trafos_info.items():
                if trafo not in trafos_cargas[nome_pasta]:
                    trafos_cargas[nome_pasta][trafo] = {"cargas": [], "gd": []}
                trafos_cargas[nome_pasta][trafo].update(info)

        # Se nada for encontrado, notifica
        if not trafos_cargas:
            print("Nenhuma carga ou GD vinculada a trafos foi encontrada.")

        return trafos_cargas



    def carregar_curvas_carga(self, caminho_curvas):
        curvas = {}
        padrao = re.compile(r"(\w+-Tipo\d+):(\w+)\s*=\s*\[([^\]]+)\]")

        with open(caminho_curvas, 'r', encoding='utf-8') as f:
            for linha in f:
                match = padrao.search(linha)
                if match:
                    chave = match.group(1)  # Ex: AT-2-Tipo5
                    tipo = match.group(2)   # Ex: DO, DU, SA
                    valores = list(map(float, match.group(3).split(',')))
                    curvas.setdefault(chave, {})[tipo] = valores

        return curvas

            


    def criar_run_cargas_agregadas(self, trafos_cargas):
        caminho_curvas = os.path.join(self.caminho, 'curvas_de_carga.txt')
        curvas = self.carregar_curvas_carga(caminho_curvas)

        re_pv = re.compile(r"(?i)new\s+pvsystem\.")
        re_kv = re.compile(r"(?i)\bkv\s*=\s*([\d.]+)")
        re_aux = re.compile(r"(?i)new\s+(xycurve|loadshape|tshape)\.")
        re_carga_baixa = re.compile(r"(?i)new\s+load\.[^\s]+.*?(carga_baixa|iluminacao)\b")
        re_nome_carga = re.compile(r"New Load\.nome_([^\s]+)")
        re_tipcc = re.compile(r"curva_diaria_([^_]+)")
        re_curva_anual = re.compile(r"curva_anual_((?:\d+(?:\.\d+)?_?){11}\d+(?:\.\d+)?)")
        re_carga_no_pv = re.compile(r"(?i)new\s+load\..*?_carga_no_pv\b")
        re_carga_agregada_existente = re.compile(r"(?i)new\s+load\.nome_.*?_carga_pip\b")

        lista_pastas = list(os.walk(self.caminho))
        r=0
        for root, _, files in tqdm(lista_pastas, desc="Pastas processadas"):
            r=r+1
            if r == 10:
                break
            ...
            if 'run.dss' not in files:
                continue

            nome_pasta = os.path.basename(root)
            if nome_pasta not in trafos_cargas:
                continue

            caminho_arquivo = os.path.join(root, 'run.dss')
            novo_arquivo = os.path.join(root, 'run_cargas_agregadas.dss')

            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                linhas_originais = [linha.rstrip() for linha in f]

            # 1) Filtrar linhas: remover cargas ligadas diretamente a PV
            linhas_sem_pv_carga = []
            i = 0
            while i < len(linhas_originais):
                linha = linhas_originais[i]
                if re_carga_no_pv.search(linha):
                    i += 1
                    while i < len(linhas_originais) and linhas_originais[i].lstrip().startswith("~"):
                        i += 1
                    continue
                linhas_sem_pv_carga.append(linha)
                i += 1

            # 2) Separar curvas auxiliares e PVs válidos
            blocos_curvas_auxiliares = {}
            blocos_pv = {}
            linhas_filtradas = []

            i = 0
            while i < len(linhas_sem_pv_carga):
                linha = linhas_sem_pv_carga[i]

                # Blocos PV com kv > 0
                if re_pv.search(linha):
                    bloco_pv = [linha]
                    i += 1
                    while i < len(linhas_sem_pv_carga) and linhas_sem_pv_carga[i].lstrip().startswith("~"):
                        bloco_pv.append(linhas_sem_pv_carga[i])
                        i += 1

                    bloco_str = " ".join(bloco_pv)
                    nome_pv_match = re.search(r"(?i)new\s+pvsystem\.([^\s]+)", bloco_str)
                    nome_pv = nome_pv_match.group(1).lower() if nome_pv_match else f"pv_{i}"

                    kv_match = re_kv.search(bloco_str)
                    kv_valor = float(kv_match.group(1)) if kv_match else 0.0

                    if kv_valor > 1:
                        blocos_pv[nome_pv] = bloco_pv
                    continue

                # Ignorar cargas baixas e agregadas existentes
                if re_carga_baixa.search(linha) or re_carga_agregada_existente.search(linha):
                    i += 1
                    while i < len(linhas_sem_pv_carga) and linhas_sem_pv_carga[i].lstrip().startswith("~"):
                        i += 1
                    continue

                # Curvas auxiliares
                if re_aux.search(linha):
                    nome_curva_match = re.search(r"(?i)new\s+(?:xycurve|loadshape|tshape)\.([^\s]+)", linha)
                    if nome_curva_match:
                        nome_curva = nome_curva_match.group(1).lower()
                        bloco_curva = [linha]
                        i += 1
                        while i < len(linhas_sem_pv_carga) and linhas_sem_pv_carga[i].lstrip().startswith("~"):
                            bloco_curva.append(linhas_sem_pv_carga[i])
                            i += 1
                        blocos_curvas_auxiliares[nome_curva] = bloco_curva
                        continue

                linhas_filtradas.append(linha)
                i += 1

            # 3) Criar cargas agregadas
            linhas_agg = []
            for trafo, dados in trafos_cargas[nome_pasta].items():
                if not isinstance(dados, dict) or "cargas" not in dados:
                    continue

                cargas = dados["cargas"]
                tensao_trafo = dados.get("tensao_secundario_kv")
                bus2 = dados.get("bus2")

                if not cargas or tensao_trafo is None:
                    continue

                curva_diaria_total = np.zeros(96)
                curva_anual_total = np.zeros(12)
                tip_cc = None
                n_validas = 0

                for linha_carga in cargas:
                    if not re_carga_baixa.search(linha_carga):
                        continue

                    nome_part = re_nome_carga.search(linha_carga)
                    if not nome_part:
                        continue

                    nome_completo = nome_part.group(1)
                    tip_cc_match = re_tipcc.search(nome_completo)
                    tip_cc = tip_cc_match.group(1) if tip_cc_match else None
                    if tip_cc not in curvas:
                        continue

                    curva_do = curvas[tip_cc].get("DO")
                    curva_du = curvas[tip_cc].get("DU")
                    curva_sa = curvas[tip_cc].get("SA")

                    if not (curva_do and curva_du and curva_sa):
                        continue

                    curva_diaria = (np.array(curva_do) + 5 * np.array(curva_du) + np.array(curva_sa)) / 7
                    curva_diaria_total += curva_diaria

                    curva_anual_match = re_curva_anual.search(nome_completo)
                    if not curva_anual_match:
                        continue

                    curva_anual = np.fromiter(map(float, curva_anual_match.group(1).split('_')), dtype=float)
                    curva_anual_total += curva_anual

                    n_validas += 1

                if n_validas:
                    curva_diaria_media = curva_diaria_total / n_validas
                    curva_diaria_str = "_".join(f"{x:.4f}" for x in curva_diaria_media)
                    curva_anual_str = "_".join(f"{x:.2f}" for x in curva_anual_total)

                    linhas_agg.append(
                        f"New Load.nome_{trafo}_curva_diaria_{tip_cc}_{curva_diaria_str}_curva_anual_{curva_anual_str}_carga_agregada "
                        f"Bus1={bus2} Phases=3\n"
                        f"~ Conn=wye Model=1 Kv={tensao_trafo} Kw=1 pf=0.92"
                    )

            # ... código anterior permanece igual até a etapa 3

            # 4) Montar arquivo final: linhas filtradas + curvas auxiliares usadas por PVs de média tensão + cargas agregadas + blocos PV válidos

            # Extrair curvas usadas pelos PVs de média tensão
            curvas_usadas_pvs_mt = set()
            re_curvas_ref = re.compile(r"(?i)(effcurve|p-tcurve|daily|tdaily|xycurve|loadshape|tshape)\s*=\s*([^\s~]+)")

            for bloco_pv in blocos_pv.values():
                bloco_str = " ".join(bloco_pv)
                referencias = re_curvas_ref.findall(bloco_str)
                for _, curva in referencias:
                    curvas_usadas_pvs_mt.add(curva.lower())

            # Construir lista de blocos de curvas auxiliares para as curvas usadas pelos PVs de média tensão
            blocos_curvas_para_pvs_mt = []
            for curva_nome in curvas_usadas_pvs_mt:
                if curva_nome in blocos_curvas_auxiliares:
                    blocos_curvas_para_pvs_mt.extend(blocos_curvas_auxiliares[curva_nome])

            resultado_final = (
                linhas_filtradas
                + blocos_curvas_para_pvs_mt
                + linhas_agg
                + sum(blocos_pv.values(), [])
            )

            # Remover linhas em branco
            resultado_final = [linha for linha in resultado_final if linha.strip()]

            with open(novo_arquivo, 'w', encoding='utf-8') as f:
                f.write("\n".join(resultado_final))





  
    def criar_run_cargas_e_gd_agregadas(self, caminho, trafos_cargas):
        def gerar_hash(texto):
            return hashlib.sha256(texto.encode()).hexdigest()

        # Lê o arquivo de curvas de carga uma vez
        curvas_de_carga = {}
        caminho_curvas = os.path.join(caminho, 'curvas_de_carga.dss')  # ajuste conforme necessário
        if os.path.exists(caminho_curvas):
            with open(caminho_curvas, 'r', encoding='utf-8') as f:
                for linha in f:
                    match = re.match(r"(\w+(?:-\w+)*):\w+\s*=\s*\[(.*?)\]", linha)
                    if match:
                        nome, valores = match.groups()
                        lista = [float(v.strip()) for v in valores.split(',') if v.strip()]
                        curvas_de_carga[nome.strip()] = lista

        r = 0
        for root, dirs, files in os.walk(caminho):
            r+=1
            if r == 10:
                break
            if 'run_cargas_agregadas.dss' not in files:
                continue

            nome_pasta = os.path.basename(root)
            if nome_pasta not in trafos_cargas:
                continue

            caminho_arquivo = os.path.join(root, 'run_cargas_agregadas.dss')
            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                linhas_existentes = f.readlines()

            linhas_pvsystems = []
            linhas_cargas_media = []

            for trafo, dados in trafos_cargas[nome_pasta].items():
                if "gd" in dados:
                    # Lógica de GD agregada (mesmo código anterior)
                    lista_gd = dados["gd"]
                    kva_total = 0.0
                    pmpp_total = 0.0
                    bus = dados.get("bus2")
                    tensao = dados.get("tensao_secundario_kv", 0.22)
                    phases = 3
                    hash_id = gerar_hash(trafo)

                    for bloco in lista_gd:
                        if not isinstance(bloco, str):
                            continue

                        match_kv = re.search(r"~\s*kv\s*=\s*([\d\.]+)", bloco)
                        if not match_kv or float(match_kv.group(1)) >= 1.0:
                            continue

                        match_kva = re.search(r"\bkva\s*=\s*([\d\.]+)", bloco, re.IGNORECASE)
                        match_pmpp = re.search(r"\bpmpp\s*=\s*([\d\.]+)", bloco, re.IGNORECASE)

                        if match_kva:
                            kva_total += float(match_kva.group(1))
                        if match_pmpp:
                            pmpp_total += float(match_pmpp.group(1))

                    if kva_total > 0 and pmpp_total > 0:
                        if kva_total > 200 or pmpp_total > 200:
                            print(f"[{nome_pasta}] GD com potência alta (kVA: {kva_total:.1f}, Pmpp: {pmpp_total:.1f}) → limitando para 200.")
                            kva_total = min(kva_total, 200)
                            pmpp_total = min(pmpp_total, 200)

                        nome_pv = f"pv_{hash_id}"
                        nome_ptcurve = f"mypvst_{hash_id}"
                        nome_effcurve = f"myeff_{hash_id}"
                        nome_irrad = f"myirrad_{hash_id}"
                        nome_temp = f"mytemp_{hash_id}"

                        bloco_curvas = (
                            f"New xycurve.{nome_ptcurve} npts=4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]\n"
                            f"New xycurve.{nome_effcurve} npts=4 xarray=[.1 .2 .4 1.0] yarray=[.86 .9 .93 .97]\n"
                            f"New loadshape.{nome_irrad} npts=1 interval=1 mult=[1]\n"
                            f"New tshape.{nome_temp} npts=1 interval=1 temp=[25]\n\n"
                        )

                        bloco_pv = (
                            f"New PVSystem.{nome_pv}_gd_baixa_tensao phases={phases} conn=wye bus1={bus}\n"
                            f"~ kv={tensao} kva={kva_total:.2f} pmpp={pmpp_total:.2f}\n"
                            f"~ pf=0.92 %cutin=0.00005 %cutout=0.00005 varfollowinverter=Yes effcurve={nome_effcurve}\n"
                            f"~ p-tcurve={nome_ptcurve} daily={nome_irrad} tdaily={nome_temp}\n\n"
                        )

                        linhas_pvsystems.append(bloco_curvas + bloco_pv)
               

            with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                f.writelines(linhas_existentes)
                if linhas_pvsystems:
                    f.write('\n')
                    f.writelines(linhas_pvsystems)






    def adicionar_curvas_normalizadas_medias(self, caminho, trafos_cargas):
        # Lê o arquivo de curvas de carga uma vez
        curvas_de_carga = {}
        caminho_curvas = os.path.join(caminho, 'curvas_de_carga.txt')
        if os.path.exists(caminho_curvas):
            with open(caminho_curvas, 'r', encoding='utf-8') as f:
                for linha in f:
                    match = re.match(r"(\w+(?:-\w+)*):\w+\s*=\s*\[(.*?)\]", linha)
                    if match:
                        nome, valores = match.groups()
                        lista = [float(v.strip()) for v in valores.split(',') if v.strip()]
                        curvas_de_carga[nome.strip()] = lista

        r = 0
        for root, dirs, files in os.walk(caminho):
            r +=1
            if r == 10:
                break
            if 'run_cargas_agregadas.dss' not in files:
                continue

            nome_pasta = os.path.basename(root)
            if nome_pasta not in trafos_cargas:
                continue

            caminho_arquivo = os.path.join(root, 'run_cargas_agregadas.dss')
            with open(caminho_arquivo, 'r', encoding='utf-8') as f:
                linhas_existentes = f.readlines()

            novas_linhas = []
            for linha in linhas_existentes:
                if 'carga_media ' not in linha:
                    novas_linhas.append(linha)
                    continue

                match_nome_curva = re.search(r"curva_diaria_([A-Za-z0-9\-]+)", linha)
                if not match_nome_curva:
                    novas_linhas.append(linha)
                    continue

                nome_curva = match_nome_curva.group(1)
                if nome_curva not in curvas_de_carga:
                    print(f"⚠️ Curva '{nome_curva}' não encontrada para a linha: {linha.strip()}")
                    novas_linhas.append(linha)
                    continue

                curva = curvas_de_carga[nome_curva]
                max_val = max(curva)
                curva_norm = [round(v / max_val, 4) if max_val > 0 else 0.0 for v in curva]
                curva_str = "_".join(f"{v:.4f}" for v in curva_norm)

                match_insercao = re.search(
                    r"(curva_diaria_" + re.escape(nome_curva) + r")(_curva_anual_)", linha
                )
                if match_insercao:
                    ponto_insercao = match_insercao.end(1)
                    nova_linha = linha[:ponto_insercao] + f"_{curva_str}" + linha[ponto_insercao:]
                    novas_linhas.append(nova_linha)
                else:
                    novas_linhas.append(linha)

            with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                f.writelines(novas_linhas)


    def run(self):
        dicionario_cargas_por_nome = self.agrupar_por_nome(self.df_cargas_Agregadas)
        dicionario_gd_por_nome = self.agrupar_por_nome(self.df_GD_FV_Agregadas)
        trafos_cargas_e_gd = self.soma_cargas_e_gd_nos_trafos(dicionario_cargas_por_nome, dicionario_gd_por_nome)

        self.criar_run_cargas_agregadas(trafos_cargas_e_gd)
        self.criar_run_cargas_e_gd_agregadas(self.caminho, trafos_cargas_e_gd)
        self.adicionar_curvas_normalizadas_medias(self.caminho, trafos_cargas_e_gd)

        return 0

