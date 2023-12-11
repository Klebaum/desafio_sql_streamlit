import streamlit as st
import pandas as pd

# Defining the page config for the homepage
st.set_page_config(page_title="Streamlit Homepage", page_icon=":smiley:", layout="wide")

# Defining the sidebar
st.title("Tabela da base de dados")
st.sidebar.success("Selecione uma página no menu lateral.")

# Read the CSV file and organize the data with pré-processing
data = pd.read_csv("DB_Teste.csv", sep=";", decimal=",")
data = data.dropna(axis=1, how='all')
data['Duração do Contrato (Meses)'] = data['Duração do Contrato (Meses)'].astype(int)
data['Data da Venda'] = pd.to_datetime(data['Data da Venda'], format='%d/%m/%Y')
data['Data da Venda'] = data['Data da Venda'].dt.date

st.dataframe(data)
data['Valor'] = data['Valor'].str.replace(' ', '').str.replace('R\$', '', regex=True).str.replace('.', '').str.replace(',', '.').astype(float)
