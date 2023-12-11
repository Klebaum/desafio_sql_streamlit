# PROJETO SQL TRIGGO.AI

Repositório contendo o código do desafio e explicações de como rodar.
Também contenco algumas explicações do código.

## Instalação do pip, criação de um venv e instalação dos requirements

Foi escolhido o uso do Streamlit onde podemos criar sites/dashboards interativas de maneira mais fácil utilizando python, ideal para unir a apresentação dos dados com alguns modelos de ML ou AI por exemplo.

- Instale o "pip":
```shell
sudo apt-get install python3-pip
```
- Entre na pasta raiz do projeto:
```shell
cd desafio_sql_streamlit
```
- Criar um ambiente virtual (Este passo é importante pois o streamlit recomenda o uso de um venv):
```shell
python -m venv .venv
```
- Instalar as dependências por meio do arquivo requirements.txt, o -r serve para ler de maneira recursiva o documento assim instalando todas as dependências contidas no arquivo:
```shell
pip install -r requirements.txt
```

## Configurando a conexão ao servidor do PostgreSQL

Na conexão com o PostgreSQL é necessário a instalação do do mesmo no computador, já que o database é tratado como "localhost".

- Abra o arquivo connect.py, aqui está indicando o vim para ser aberto, mas qualquer editor de texto pode ser utilizado:
```shell
cd services
vim connect.py
```
O Arquivo terá as seguintes informações, deve-se realizar modificações na configuração da conexão:
```shell
import psycopg2

def init_connection():
    """_summary_ : Connect to PostgreSQL database

    Returns:
        _type_: psycopg2.connection
    """
    conn = psycopg2.connect(
        host="localhost",
        database="DB_teste",
        user="postgres",
        password="123456",
        port=5432
    )
    return conn
```

## Execução do código
- Para executar o código deve-se rodar o arquivo Homepage.py, que neste caso funcionará como se fosse uma função main.
```shell
streamlit run Homepage.py
```

# Explicação das atividades desenvolvidas

O modo como foi construído o projeto é em criar funções para retornar os resultados esperados e assim mostrá-los por meio do Streamlit, que permite a apresentação em dataframes, gráficos, etc. Está abordagem é muito confortável para a utilização conjunta do pandas.

## Homepage
- Na primeira página temos apenas uma tabela (tratada como dataframe) que representa o arquivo CSV;

## CSV tasks
Nesta página foram realizadas as primeiras atividades pedidas, foram implementadas funções no arquivo functions_tasks.py que retornam os dataframes referentes aos objetivos.
- A função best_seller lista um ranking de vendedores do que mais vendeu para o que menos vendeu.
- A função best_client lista o "melhor cliente" e "pior cliente", neste caso levando em conta que o melhor cliente é aquele que mais gasta e o pior aquele que menos gasta.
- A função mean_type_sale lista o valor médio de cada tipo de venda.
- A função sale_per_client calcula o número de vendas registradas a cada cliente.

## SQL scenarios
Nesta página foram realizadas a segunda parte das atividades perdidas que envolviam a criação de QUERYS, neste projeto o banco de dados relacional escolhido foi o PostgreSQL como mencionado anteriormente.
- Primeiro é apresentado o DER do banco de dados, onde a PK de CLIENTE e VENDEDOR são os próprios nomes vistos que são características únicas neste caso, embora poderia ter sido criado uma PK ID preferiu-se está abordagem. Na tabela VENDA após uma análise exploratória notou-se que o ID das vendas que deveria ser único se repetia algumas vezes, portanto, decidiu-se criar uma nova PK para identificação está sendo id_venda. Em questão de relacionamento temos uma relação de 1 para muitos (1..N) de VENDEDOR para VENDA e de CLIENTE para VENDA. 
- A função get_sales2020 utiliza uma QUERY selecionando ID da venda e o nome do cliente, presentes na tabela VENDA, no entanto, é criada uma limitação para apenas "pegar" valores no ano de 2020.
- A função get_team seleciona o nome e a equipe do vendedor da tabela VENDEDOR.
- A função quarterly_sales extraí os valores de da coluna ano de DataDaVenda (DATE) e utiliza o alias Ano, assim acontece também para o Trimestre, já no atributo valor é realizado a soma dos valores referentes a ano e trimestre. Após estas tarefas é realizado um agrupamento por ano e trimestre, sendo assim a soma dos valores será calculado para este agrupamento, depois é realizado uma ordenação para representar um histórico.
- Por último é realizado a plotagem de um gráfico de barras que representa o histórico de vendas trimestrais
da empresa.


