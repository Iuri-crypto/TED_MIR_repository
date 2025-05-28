

class connect_class:
    """ Analise de dados """
    def __init__(self, psycopg2, dbname:str, user:str, password:str, host:str, port:str):
        self.cur = None
        self.conn = None
        self.user = user
        self.dbname = dbname
        self.password = password
        self.host = host
        self.port = port
        
        self.psycopg2 = psycopg2
   

    def conecta(self):
        """ Conecta a base de dados local SERVIDOR --> POSTGRESQL """
        self.conn = self.psycopg2.connect(dbname='BDGD_2023_ENERGISA_MT',user='iuri',password='aa11bb22',host='localhost',port='5432')
        self.cur = self.conn.cursor()
        print("======================================")
        print("\nConexão Estabelecida com sucesso. !!!\n")
        print("======================================")
        return self.conn, self.cur

   
    def desconecta(self):
        """ Fecha a conexão com o banco de dados """
        if self.cur: self.cur.close()
        if self.conn:self.conn.close()
        print("=====================================")
        print("Conexão com o banco de dados fechada.")
        print("=====================================")
        



