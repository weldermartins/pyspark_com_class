from pyspark.shell import spark


class IngestaoBanco:
    """A classe faz leitura e escrita em databases, csv e json. """

    def __init__(self, qualbanco, porta, database, usuario, senha, tabela):
        """Inicializa os atributos para leitura/escrita"""
        self.qualbanco = qualbanco
        self.porta = porta
        self.database = database
        self.usuario = usuario
        self.senha = senha
        self.tabela = tabela

    def read_banco(self):
        """Realiza a leitura da tabela em um banco de dados."""
        url = f'jdbc:{self.qualbanco}://localhost:{self.porta}/{self.database}'
        properties = {'user': self.usuario, 'password': self.senha}
        df = spark.read.jdbc(url=url, table=self.tabela, properties=properties)
        return df

    def write_banco(self, df):
        """O metodo realiza a escrita e sobrescrita da tabela em um banco de dados."""
        url = f'jdbc:{self.qualbanco}://localhost:{self.porta}/{self.database}'
        properties = {'user': self.usuario, 'password': self.senha}
        df.write.mode('overwrite').jdbc(url=url, table=self.tabela, properties=properties)


class IngestaoCsv:
    """A classe faz leitura e escrita em arquivos CSV."""

    def __init__(self, path, delimitador, cabecalho):
        """Inicializa os atributos para leitura/escrita"""
        self.path = path
        self.delimitador = delimitador
        self.cabecalho = cabecalho

    def read_csv(self):
        """O metodo realiza a leitura e verifica se séra necessário a inclusão do cabeçalho."""
        if self.cabecalho == True:
            df = spark.read.csv(self.path, header=self.cabecalho, sep=f'{self.delimitador}')
        else:
            df = spark.read.csv(self.path, header=self.cabecalho, sep=f'{self.delimitador}')
        return df

    def write_csv(self, df):
        """O metodo realiza a escrita e sobrescreve, verifica se séra necessário a inclusão do cabeçalho."""
        if self.cabecalho == True:
            df.write.mode('overwrite').csv(self.path, header=self.cabecalho, sep=f'{self.delimitador}')
        else:
            df.write.mode('overwrite').csv(self.path, header=self.cabecalho, sep=f'{self.delimitador}')

