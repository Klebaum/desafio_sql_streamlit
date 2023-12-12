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

        if result_empty_check is not None and result_empty_check.iloc[0, 0] == 0:
            st.warning("A tabela 'VENDEDOR' está vazia. Inserindo dados.")
            for index, row in sellers_team.iterrows():
                seller = row['Vendedor']
                team = row['Equipe']
                query_seller = f"INSERT INTO VENDEDOR (Nome, Timee) VALUES ('{seller}', '{team}');"
                execute_sql_instruction(query_seller, conn)

        query_check_empty = "SELECT COUNT(*) FROM VENDA;"
        result_empty_check = conn.query(query_check_empty)
        if result_empty_check is not None and result_empty_check.iloc[0, 0] == 0:
            st.warning("A tabela 'VENDA' está vazia. Inserindo dados.")
            for index, row in data.iterrows():
                nome_cliente = row['Cliente']
                id_venda = row['ID']
                tipo = row['Tipo']
                data_da_venda = row['Data da Venda']
                categoria = row['Categoria']
                nome_vendedor = row['Vendedor']
                regional = row['Regional']
                duracao_do_contrato = row['Duração do Contrato (Meses)']
                equipe = row['Equipe']
                valor = row['Valor']

                query_insercao_venda = f"INSERT INTO VENDA (Nome_cliente, ID, Tipo, DataDaVenda, Categoria, Nome_vendedor, Regional, DuracaoDoContrato, Equipe, Valor) " \
                                    f"VALUES ('{nome_cliente}', '{id_venda}', '{tipo}', '{data_da_venda}', '{categoria}', '{nome_vendedor}', '{regional}', {duracao_do_contrato}, '{equipe}', {valor});"
                
                execute_sql_instruction(query_insercao_venda, conn)

            st.success("Dados inseridos com sucesso.")
    else:
        st.warning("A conexão com o banco de dados não foi bem-sucedida.")