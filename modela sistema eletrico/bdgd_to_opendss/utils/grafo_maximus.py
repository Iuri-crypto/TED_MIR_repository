from collections import defaultdict
import pandas as pd
import networkx as nx

path = r"C:\Users\Lucas Maximus\Desktop\BDGD\csv"

#GERA O GRAFO ONDE OS NOS SAO COD_ID


CTMT = '2014294'

df_1 = pd.read_csv(path + r"\SSDMT.csv",dtype=str) #segmentos
df_2 = pd.read_csv(path + r"\UNTRMT.csv",dtype=str) #trafos
df_3 = pd.read_csv(path + r"\UNSEMT.csv",dtype=str) #chaves

df_filtrado_1 = df_1[df_1['CTMT'] == CTMT]
df_filtrado_2 = df_2[df_2['CTMT'] == CTMT]
df_filtrado_3 = df_3[df_3['CTMT'] == CTMT]

df_1 = df_2 = df_3 = None

dict_ssdmt = df_filtrado_1.to_dict(orient='records') #lista de dicionarios
dict_untrmt = df_filtrado_2.to_dict(orient='records') #lista de dicionarios
dict_unsemt = df_filtrado_3.to_dict(orient='records') #lista de dicionarios

conjunto_todos_pac = set()

# Associa um PAC com os COD_ID dos objetos conectados a ele
dicionario_pac_cod_id = defaultdict(list)

# Associa o cod_id do objeto com o pac_1 e pac_2
cod_id_pac_1_pac_2 = defaultdict(set)

for ssdmt in dict_ssdmt:
    conjunto_todos_pac.add(ssdmt['PAC_1'])
    conjunto_todos_pac.add(ssdmt['PAC_2'])

    dicionario_pac_cod_id[ssdmt['PAC_1']].append(ssdmt['COD_ID'])
    dicionario_pac_cod_id[ssdmt['PAC_2']].append(ssdmt['COD_ID'])

    cod_id_pac_1_pac_2[ssdmt['COD_ID']].update([ssdmt['PAC_1'],ssdmt['PAC_2']])



for untrmt in dict_untrmt:
    conjunto_todos_pac.add(untrmt['PAC_1'])
    conjunto_todos_pac.add(untrmt['PAC_2'])

    dicionario_pac_cod_id[untrmt['PAC_1']].append(untrmt['COD_ID'])
    dicionario_pac_cod_id[untrmt['PAC_2']].append(untrmt['COD_ID'])

    cod_id_pac_1_pac_2[untrmt['COD_ID']].update([untrmt['PAC_1'],untrmt['PAC_2']])


for unsemt in dict_unsemt:
    conjunto_todos_pac.add(unsemt['PAC_1'])
    conjunto_todos_pac.add(unsemt['PAC_2'])

    dicionario_pac_cod_id[unsemt['PAC_1']].append(unsemt['COD_ID'])
    dicionario_pac_cod_id[unsemt['PAC_2']].append(unsemt['COD_ID'])

    cod_id_pac_1_pac_2[unsemt['COD_ID']].update([unsemt['PAC_1'],unsemt['PAC_2']])


lista_arestas = list()

for x,y in dicionario_pac_cod_id.items():
    
    for id in y:
        copia_lista = y.copy()
        copia_lista.remove(id)

        for obj in copia_lista:
            lista_arestas.append((id,obj))



    G = nx.Graph()
    G.add_edges_from(lista_arestas)


    B = nx.bfs_tree(G,"584202") #Objeto raiz da arvore radial


    A = nx.nx_agraph.to_agraph(B)

    A.graph_attr["nodesep"] = "3" #separacao horizontal dos nos no grafo
    A.graph_attr["ranksep"] = "3" #separacao vertical dos nos no grafo


    A.node_attr["shape"] = "circle"
    A.edge_attr["arrowsize"] = "1"
    A.edge_attr["weight"] = "2"


    A.layout(prog="dot")
    A.draw('teste.svg')



'''



G = nx.Graph()

for x,y in dicionario_pac_cod_id.items():
    
    arestas = set()

    for obj_cod_id in y:    
            arestas.update(cod_id_pac_1_pac_2[obj_cod_id])

    arestas.remove(x)

    for id in arestas:
        G.add_edge(x,id)





B = nx.bfs_tree(G,"60759020827212433MT")




A = nx.nx_agraph.to_agraph(B)

A.graph_attr["nodesep"] = "3" #separacao horizontal dos nos no grafo
A.graph_attr["ranksep"] = "3" #separacao vertical dos nos no grafo


A.node_attr["shape"] = "circle"
A.edge_attr["arrowsize"] = "1"
A.edge_attr["weight"] = "2"


A.layout(prog="dot")
A.draw('teste.svg')


'''





