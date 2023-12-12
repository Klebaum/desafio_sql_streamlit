import streamlit as st
from sql_consults.sql import fill_database, create_tables
from Homepage import data
from functions_sql import get_sales_2020, get_team, quarterly_sales
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# Create the connection with the database SNOWFLAKE
conn = st.connection("snowflake")
st.title("SQL Scenarios")

# Create the tables in the database
#create_tables(conn)

# Fill the tables with the data
fill_database(data, conn)

st.subheader("Diagrama Entidade Relacionamento")
st.image("/home/kleber/desafio_sql_streamlit/pages/erd_diagram.png", width=1200)

# Define the columns to be used in the layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("Vendas e seus respectivos clientes (2020)")
    # List all the values of sales and their respective clients in 2020
    st.dataframe(get_sales_2020(conn), width=600)

with col2:
    st.subheader("Equipe de cada vendedor")
    # List each seller and their respective team
    st.dataframe(get_team(conn), width=600)

col3, col4 = st.columns(2)

with col3:
    # Construct a table with the sales per quarter
    st.subheader("Vendas por trimestre")
    df = quarterly_sales(conn)
    df['ANO'] = df['ANO'].astype(int).astype(str)
    df['TRIMESTRE'] = df['TRIMESTRE'].astype(int)
    df['VALOR'] = df['VALOR'].astype(str)
    df['VALOR'] = 'R$ ' + df['VALOR'].apply(lambda x: '{:,.2f}'.format(float(x)))

    st.dataframe(df, width=600)

with col4:
    # Plot the graph of sales per quarter.
    st.subheader("Gráfico de vendas por trimestre")
    fig, ax = plt.subplots(figsize=(10, 6))

    # Offset to separate the bars of each year
    offset = 0.2

    for i, ano in enumerate(df['ANO'].unique()):
        df_ano = df[df['ANO'] == ano]

        plt.bar(df_ano['TRIMESTRE'] + i * offset, df_ano['VALOR'], width=0.2, label=str(ano))

    plt.xlabel('Trimestres') 
    plt.ylabel('Valor de Vendas')
    plt.title('Resultado de Vendas por Trimestre')

    formatter = mticker.FuncFormatter(lambda x, _: f'{x / 1e6:.0f}M')
    plt.gca().yaxis.set_major_formatter(formatter)

    quarters_labels = [f"{int(trimestre)}º TRIMESTRE" for trimestre in df['TRIMESTRE']]
    plt.xticks(ticks=df['TRIMESTRE'] + ((len(df['ANO'].unique()) - 1) / 2) * offset, labels=quarters_labels)

    plt.legend()
    plt.tight_layout() 
    st.pyplot(plt)
