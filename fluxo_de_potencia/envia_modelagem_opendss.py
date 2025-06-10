"""
    Esta classe envia modelagens para o Opendss
"""


class send_models_to_opendss:
    """ Carrega as modelagens e envia ao OpenDSS """

    def __init__(self, dss, os, caminho, alimentador):
        self.dss = dss
        self.os = os
        self.caminho = caminho
        self.alimentador = alimentador


    def compile(self):
        """ Envia compilando o arquivo """
        diretorio = self.os.path.join(self.caminho, str(self.alimentador))
        [self.dss.text("Compile {}".format(self.os.path.join(diretorio, f)))
         for f in self.os.listdir(diretorio) if f.endswith('.dss')]



    def curvas_carga(self):
        """ Carrega curvas de carga """
        curva_de_carga = {}
        diretorio = self.os.path.join(self.caminho, str(self.alimentador))
        [self.dss.text("Compile {}".format(self.os.path.join(diretorio, f)))
         for f in self.os.listdir(diretorio) if f.endswith('.dss')]



    def curvas_geracao(self):
        """ Carrega curvas de geração """
        diretorio = self.os.path.join(self.caminho, str(self.alimentador))
        [self.dss.text("Compile {}".format(self.os.path.join(diretorio, f)))
         for f in self.os.listdir(diretorio) if f.endswith('.dss')]
