def format_float_value(data):
    """_summary_ : Fromat the float values to string with R$ and two decimal places

    Args:
        data (_type_): Pandas dataframe

    Returns:
        _type_: Pandas dataframe
    """
    data = data.apply(lambda x: "R$ {:,.2f}".format(float(x)))
    data = data.str.replace('.', 'h').str.replace(',','.').str.replace('h',',')
    
    return data


def best_seller(data):
    """_summary_ : List the best seller and the total value sold

    Args:
        data (_type_): Pandas dataframe

    Returns:
        _type_: Pandas dataframe
    """
    data_best_seller = data.groupby(['Vendedor'])['Valor'].sum().sort_values(ascending=False).reset_index()
    data_best_seller.columns = ['Vendedor', 'Valor Total Vendido']

    data_best_seller['Valor Total Vendido'] = format_float_value(data_best_seller['Valor Total Vendido'])

    return data_best_seller


def best_client(data):
    """_summary_ : List the best client and the total value bought

    Args:
        data (_type_): Pandas dataframe

    Returns:
        _type_: Pandas dataframe
    """
    data_best_client = data.groupby(['Cliente'])['Valor'].sum().sort_values(ascending=False).reset_index()

    max_value = data_best_client['Valor'].max()
    min_value = data_best_client['Valor'].min()

    # Filtra apenas as linhas correspondentes ao maior e ao menor valor
    data_best_client = data_best_client[(data_best_client['Valor'] == max_value) | (data_best_client['Valor'] == min_value)]

    # Formata o valor para exibição
    data_best_client['Valor Total Vendido'] = format_float_value(data_best_client['Valor'])
    data_best_client = data_best_client.drop(columns=['Valor'])

    return data_best_client


def mean_type_sale(data):
    """_summary_ : Calculate the mean value of each type of sale

    Args:
        data (_type_): Pandas dataframe

    Returns:
        _type_: Pandas dataframe
    """
    data_mean = data.groupby(['Tipo'])['Valor'].mean().reset_index()
    data_mean.columns = ['Tipo  de Venda', 'Valor Médio']
    data_mean['Valor Médio'] = format_float_value(data_mean['Valor Médio'])

    return data_mean


def sale_per_client(data):
    """_summary_ : Calculate the number of sales per client

    Args:
        data (_type_): Pandas dataframe

    Returns:
        _type_: Pandas dataframe
    """
    data_sale_per_client = data.groupby(['Cliente'])['Valor'].count().sort_values(ascending=False).reset_index()
    data_sale_per_client.columns = ['Cliente', 'Número de Vendas']

    return data_sale_per_client