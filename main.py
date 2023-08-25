import psycopg2
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq

lista_add_colunas = []
colunas = ['customers','employees','offices','orderdetails','orders','payments','product_lines','products']

def conexao():
    con = psycopg2.connect(host='psql-mock-database-cloud.postgres.database.azure.com', database='ecom1692848222792bypjwxeymatmczia',
    user='gkwimjzggfdatutzxsdmvjzq@psql-mock-database-cloud', password='ntikuznbgqhgqqbsydxnycdu')
    cur = con.cursor()
    extrair_tabelas(con)

def extrair_tabelas(con):
    for colunas_lista in colunas: 
        df = pd.read_sql_query(f'select *from {colunas_lista}',con)
        df.to_csv(f'{colunas_lista}.csv',index=False)
        print(f'arquivo{colunas_lista} salvo')
        converte_arquivos(df,colunas_lista)

def converte_arquivos(df,colunas_lista):
     df_paquet = pa.Table.from_pandas(df)
     pq.write_table(df_paquet, f'{colunas_lista}.parquet')

if __name__=="__main__":
    conexao()