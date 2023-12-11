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
