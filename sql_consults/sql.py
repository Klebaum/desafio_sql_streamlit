import streamlit as st
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer


def create_tables(conn):
    """_summary_ : Create the tables in the database

    Args:
        conn (_type_): psycopg2.connection
    """
    script_sql = """
    CREATE TABLE IF NOT EXISTS CLIENTE (
	    Nome VARCHAR (50) PRIMARY KEY NOT NULL
    );

    CREATE TABLE IF NOT EXISTS VENDEDOR (
        Nome VARCHAR (50) PRIMARY KEY NOT NULL,
        Timee VARCHAR (20) NOT NULL
    );

    CREATE TABLE IF NOT EXISTS VENDA (
        id_venda SERIAL PRIMARY KEY,
        Nome_cliente VARCHAR NOT NULL,
        ID VARCHAR(10) NOT NULL,
        Tipo VARCHAR(50) NOT NULL,
        DataDaVenda DATE NOT NULL,
        Categoria VARCHAR(50) NOT NULL,
        Nome_vendedor VARCHAR NOT NULL,
        Regional VARCHAR(50) NOT NULL,
        DuracaoDoContrato INT NOT NULL,
        Equipe VARCHAR(50) NOT NULL,
        Valor DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (Nome_cliente) REFERENCES CLIENTE (Nome),
        FOREIGN KEY (Nome_vendedor) REFERENCES VENDEDOR (Nome)
    );
    """
    execute_sql_instruction(script_sql, conn)


def execute_sql_instruction(query, conn):
    """_summary_ : Execute a SQL query

    Args:
        query (_type_): String with the SQL query
        conn (_type_): psycopg2.connection
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
    except Exception as e:
        st.error(f"Erro ao executar a instrução SQL: {e}")


def check_empty_table(table_name, cursor):
    query_check_empty = f"SELECT COUNT(*) FROM {table_name};"
    cursor.execute(query_check_empty)
    result_empty_check = cursor.fetchone()
    
    return result_empty_check


def fill_database(data, conn):
    """_summary_ : Fill the database with the data from the dataframe

    Args:
        data (_type_): Pandas dataframe
        conn (_type_): psycopg2.connection
    """
    cursor = conn.cursor()

    client_names = data['Cliente'].unique()
    sellers_team = data.groupby('Vendedor')['Equipe'].agg(lambda x: ', '.join(x.unique())).reset_index() 

    if conn:
        result_empty_check = check_empty_table('CLIENTE', cursor)

        if result_empty_check is not None and result_empty_check[0] == 0:
            st.warning("A tabela 'CLIENTE' está vazia. Inserindo dados.")
            client_values = [(client,) for client in client_names]
            query_client = "INSERT INTO CLIENTE (Nome) VALUES (:1);"
            cursor.executemany(query_client, client_values)

        result_empty_check = check_empty_table('VENDEDOR', cursor)

        if result_empty_check is not None and result_empty_check[0] == 0:
            st.warning("A tabela 'VENDEDOR' está vazia. Inserindo dados.")
            seller_values = [(row['Vendedor'], row['Equipe']) for index, row in sellers_team.iterrows()]
            query_seller = "INSERT INTO VENDEDOR (Nome, Timee) VALUES (:1, :2);"
            cursor.executemany(query_seller, seller_values)

        result_empty_check = check_empty_table('VENDA', cursor)
        if result_empty_check is not None and result_empty_check[0] == 0:
            st.warning("A tabela 'VENDA' está vazia. Inserindo dados.")
            data['Data da Venda'] = pd.to_datetime(data['Data da Venda'])

            # Then convert 'Data da Venda' to string
            data['Data da Venda'] = data['Data da Venda'].dt.date.astype(str)
            sale_values = [
                    (row['Cliente'], row['ID'], row['Tipo'], row['Data da Venda'], row['Categoria'], row['Vendedor'], 
                    row['Regional'], row['Duração do Contrato (Meses)'], row['Equipe'], row['Valor']) 
                    for index, row in data.iterrows()
                ]
            
            query_sale = ("INSERT INTO VENDA (Nome_Cliente, ID, Tipo, DataDaVenda, Categoria, Nome_vendedor, Regional, DuracaoDoContrato, Equipe, Valor) " 
            "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10);")
            cursor.executemany(query_sale, sale_values)

            st.success("Dados inseridos com sucesso.")
    else:
        st.warning("A conexão com o banco de dados não foi bem-sucedida.")