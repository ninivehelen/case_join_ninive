# Case técnico para a vaga para Analista de Dados Jr (python) na Join 💻

⚠️ OBS: Os arquivos gerados das tabelas nos formatos parquet e os arquivos das respostas em formato delta vão ser salvos em seu 
dbs/filestore do databricks. Como utilizei o databricks e minha conte não é premium, não conseguir configurar o CLI para baixar os arquivos gerados parquet e delta
e também não tenho acesso a armazenamento em nuvem para passar os arquivos para nuvem para baixar. 
Porém, quando seguir as etapas, esses arquivos serão salvos no seu dbfs:filestore e você terá acesso, e conseguirá compilar:
Segue as etapas abaixo para a aplicação. 

📋 Este repositório foi criado para armazena os codigos desenvolvidos para o case técnico. 

O que contém no repositório quais tarefas são realizadas. ⚒️

Este case utiliza um banco de dados na nuvém chamado Ecommerce website database, que pode ser acessado no link: https://uibakery.io/sql-playground. 
A parti desse banco de dados foi criado um DER utilizando uma ferramenta gratuita que se chama br_modelo para fazer o diagrama entidade e relacionamento, foi feito o diagrama logico também.  

* O DER esta salvo dentro de uma pasta do repositório  chamada DER. Nesta pasta tem 4 arquivos. 

* DER.brM3, DER_ecommerce_logico.brM3  ambos foram criados na ferramenta chamada br_modelo. Após a criação dos DER, foi exportado em formato png que são cada arquivo correspondente ao .brM3 que se são:
* DER.png e DER_ecommerce_logico.png. Realizei a exportação nesse formato, para que não seja necessáro baixar o br_modelo que é um executável para abrir os DER. Neste caso pode apenas abrir os dois arquivos com extensao png e observa os dois DER.png o DER e o DER_ecommerce_logico.png em seu computador. 

* O repositório também  contém um  arquivo chamado desafio_join_notebook.ipynb 💻📋📎📁

* O que esse arquivo faz: desafio_join_notebook.ipynb 
  neste notebook criado no Databricks ele contém códigos  para realizar a conexão com um banco de dados da nuvém citados acima. Com a utilização do driver JDBC foi realizado a conexão com o banco de dados. Após fazer a conexão, foi realizado a etapa de codificação para extrair dados das tabelas dentro do notebook que contém os códigos. 

* Na etapa de extração das tabelas, foi utilizado 2 funções. Uma para fazer a conexão com o banco de dados chamada: conexao, que foi responsável pela conexão com o banco de dados acessando suas credenciais e pecorrendo as tabelas existentes, para a extração e fazer a chamada da função: mostrar_tabelas_extrair passando as colunas existentes como parametro e as credencias do banco.
* A outra função chamada: mostrar_tabelas_extrair é responsavel por receber as credenciis da função anterior, receber as tabelas pecorridas e salvar elas utilizando o pyspark read.  O salvamento delas foi em formato parquet. 
* Para realizar a chamada dessas funções foi utilizado if __name__=="__main__": que é responsável para realizar a chamar da função: conexao.
#### Etapa Seguintes 📋
* Após o salvamento das tabelas em formato parquet foi iniciado o processo de consulta de informações solicitadas a respeito das colunas. Foram:
- Qual país possui a maior quantidade de itens cancelados?
- Qual o faturamento da linha de produto mais vendido, considere como os itens Shipped, cujo o pedido foi realizado no ano de 2005?
- Nome, sobrenome e e-mail dos vendedores do Japão, o local-part do e-mail deve estar mascarado.
* utilizando o pyspark com sql, foi realizada as consultas para extrair as respostas das perguntas acima. Com pyspark read foi realizado a abertura dos aquivos parquet, apos abrir os arquivos foi criado  utilizando a função createOrReplaceTempView no pyspark para transformar cada arquivo aberto no formato parquet, em um template temporário. 
* Após essa transformação, foi realizar as consultas sql para responder as perguntas, e salvar as consultas em formato delta utilizando pyspark write. 
* As consultas foram salvas em formato delta e também exibidas na tela do notebook com a função display do pyspark. Podendo assim, ver o resultado das informações das respostas das perguntas. 
#### Como rodar os códigos ⚒️
* 1-Dentro do projeto no github clone o repositório. 
* 2-Após fazer o clone, utilize notebook: desafio_join_notebook.ipynb que contém os codigos dos procedimentos explicados neste README e importe o  notebook desafio_join_notebook.ipynb para o databricks.  
* 3-Após isso, compile os codigos do notebook e aguarde até o fim do processamento.
* 4- Você terá as tabelas extraidas no formato parquet salvas no seu dbfs:file/store do databricks e também as consultas das respostas salvas no formato delta. 

Ferramentas e linguagens utilizadas para o desenvolvimento.
* ![Python](https://img.shields.io/badge/-Python-yellow?style=for-the-badge&logo=Python)
* ![Databricks](https://img.shields.io/badge/-databricks-blue?style=for-the-badge&logo=databricks)
* ![Pandas](https://img.shields.io/badge/-Pandas-222222?style=for-the-badge&logo=Pandas)
* ![PySpark](https://img.shields.io/badge/-PySpark-green?style=for-the-badge&logo=PySpark)
