import pandas as pd


def get_sales_2020(conn):
    """_summary_ : List all sales (ID) and their respective customers only in 2020

    Args:
        conn (_type_): psycopg2.connection

    Returns:
        _type_: Pandas dataframe
    """
    query = "SELECT ID, Nome_cliente FROM VENDA WHERE DataDaVenda BETWEEN '2020-01-01' AND '2020-12-31';"
    return pd.read_sql_query(query, conn).reset_index(drop=True)


def get_team(conn):
    """_summary_ : List the team(s) of each seller

    Args:
        conn (_type_): psycopg2.connection

    Returns:
        _type_: Pandas dataframe
    """
    query = "SELECT Nome, Timee FROM VENDEDOR;"
    return pd.read_sql_query(query, conn).reset_index(drop=True)


def quarterly_sales(conn):
    """_summary_ : Build a table that quarterly evaluates the sales result and plot a graph of this history
    
    Args:
        conn (_type_): psycopg2.connection
        
        Returns:
            _type_: Pandas dataframe
    """

    query = """
    SELECT
        EXTRACT(YEAR FROM DataDaVenda) AS Ano,
        EXTRACT(QUARTER FROM DataDaVenda) AS Trimestre,
        SUM(Valor) AS Valor
    FROM VENDA
    GROUP BY Ano, Trimestre
    ORDER BY Ano, Trimestre;
    """

    return pd.read_sql_query(query, conn)
