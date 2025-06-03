"""
    Este software consegue se conectar a um servidor local PostgreSQL
    e carregar as tabelas da base de dados geográfica da distribuidora
    : BDGD de qualquer estado, e na sequência ela modela o circuito elétrico
    inteiro do estado inteiro, considerando diferentes tipos de modelagens
    que são escolhidas pelo operador

    --- Feito por Iuri Lorenzo no 8 semestre
"""

class bdgd_to_opendss:
    """ Classe que contém todos os métodos para modelar cada componente
        do sistema elétrico de potência considerando diversos cenários """

    def _init__(self):
        self.t=None



