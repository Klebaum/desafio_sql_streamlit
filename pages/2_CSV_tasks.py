import streamlit as st
from Homepage import data
from functions_tasks import best_seller, best_client, mean_type_sale, sale_per_client

st.title("CSV Tasks")

# Print the seller responsible for the highest sale in descending order
data_best_seller = best_seller(data)

# Print the client responsible for the highest and lowest sale
data_best_cliente = best_client(data)

# Print the mean of each type of sale
data_mean = mean_type_sale(data)

# Print the number of sales per client
data_sale_per_client = sale_per_client(data)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Vendas por vendedor")
    st.dataframe(data_best_seller, width=600)

with col2:
    st.subheader("Clientes responsáveis pela maior e menor venda")
    st.dataframe(data_best_cliente, hide_index=True, width=600)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Valor Médio por Tipo de Venda")
    st.dataframe(data_mean, hide_index=True, width=600)

with col4:
    st.subheader("Número de Vendas por Cliente")
    st.dataframe(data_sale_per_client,width=600)



