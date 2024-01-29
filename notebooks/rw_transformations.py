from pyspark.sql.functions import from_utc_timestamp, current_timestamp, col

from functions.ingestion_class import IngestaoBanco
from functions.ingestion_class import IngestaoCsv
import keyvalt as k


# Cria a instância da classe
# Realiza a leitura do arquivo csv com o cabeçalho
path = '../data'
escrever_csv = IngestaoCsv(path, ';', True)
df = escrever_csv.read_csv()

# Cria a instância da classe.
# Realiza a conexão com o banco Mysql
ler_mysql = IngestaoBanco('mysql', '3306', 'financeiro', k.usuario_mysql, k.senha_mysql, 'cadastro_de_nomes')
ler_mysql.write_banco(df)  # Cria a tabela com os dados a partir do csv no mysql

# Realiza a leitura da tabela no mysql e atribui ao Dataframe Spark
df = ler_mysql.read_banco()

# Realiza a transformação nos tipos de dados e inseri uma nova coluna chamada data_ingestao
df1 = df \
    .withColumn("id", col("id").cast("int")) \
    .withColumn("idade", col("idade").cast("int")) \
    .withColumn('data_ingestao', from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))

# Realiza a conexão com o banco Postgres
ler_postgres = IngestaoBanco('postgresql', '5432', 'teste-airflow-db', k.usuario_postgres, k.senha_postgres,
                             'cadastro_de_nomes')


# Cria a tabela cadastro_de_nomes com os dados a partir da tabela do Mysql
ler_postgres.write_banco(df1)

