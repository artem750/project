import pandas as pd
import numpy as np
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
import sqlite3
import random
from datetime import datetime, timedelta
import io
import base64


#ПОЛУЧЕНИЕ СПИСКА ДОСТУПНЫХ БУМАГ
def secs(currency):
    if currency == "rub":
        conn = sqlite3.connect("base_rub")
    elif currency=="usd":
        conn = sqlite3.connect("base_usd")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM stock_potential")
    data = cursor.fetchall()
    data = pd.DataFrame(data)
    data = data.iloc[:, 0]
    data = data.tolist()
    return data


#ПОЛУЧЕНИЕ ВХОДНЫХ ДАННЫХ ПО ВЫБРАННЫМ БУМАГАМ ЗА ПОСЛЕДНИЙ ГОД
def inputs(securities,currency):#предлагаю здесь добавить доп параметр определения руб или дол
    #предлагаю здесь добавить разделение на базы данных руб и дол
    
    now=datetime.now()
    today=now.strftime('%Y-%m-%d')

    one_year_ago = now.replace(year=now.year - 1)
    one_year_ago=one_year_ago.strftime('%Y-%m-%d')
    
    if currency == "rub":
        conn = sqlite3.connect("base_rub")
    elif currency=="usd":
        conn = sqlite3.connect("base_usd")
    cursor = conn.cursor()
    
    securities = [(item,) for item in securities]
    all_data = []
    for table_name in securities:
        cursor.execute(f"""
            SELECT *
            FROM {table_name[0]}
            WHERE Date BETWEEN '{one_year_ago}' AND '{today}'
        """)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)
        all_data.append(df)
    
    transformed_data=[]
    
    for df in all_data:
        df = df.rename(columns={'Close': df['Ticker'].iloc[0]})
        df = df.drop(['Dividends', 'Stock_Splits', 'Ticker'], axis=1)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        transformed_data.append(df)
    
    merged_df = transformed_data[0]
    
    for df in transformed_data[1:]:
        merged_df = merged_df.join(df, how='inner')
    df=merged_df
    
    daily_returns = df.pct_change()
    daily_returns.reset_index(inplace=True)
    daily_returns = daily_returns.drop(index=0)
    daily_returns_no_date = daily_returns.drop(columns='Date')
    cov_matrix = daily_returns_no_date.cov()
    
    #предлагаю здесь добавить разделение на базы данных руб и дол
    cursor.execute("SELECT * FROM stock_potential")
    data = cursor.fetchall()
    returns = pd.DataFrame(data)
    returns = returns.T
    returns.columns = returns.iloc[0]
    returns = returns.drop(0)
    returns.reset_index(drop=True, inplace=True)
    returns = returns.replace({'%': '', ',': '.'}, regex=True).astype(float)
    returns = returns[returns.columns.intersection(df.columns)]
    returns = returns / 100
    column_order = df.columns
    returns = returns[column_order]

    # Безрисковая ставка
    if currency == "rub":
        risk_free = 0.2231
    elif currency=="usd":
        risk_free = 0.043
    
    return returns,cov_matrix,risk_free, daily_returns,df

#БЕСПЛАТНАЯ ОПТИМИЗАЦИЯ
def optimize_else(target,returns,cov_matrix,risk_free,df):
    cov_matrix_values=cov_matrix.values
    # Функция для вычисления отрицательного коэффициента Шарпа
    def negative_sharpe_ratio(weights, returns, cov_matrix, risk_free):
        weights = weights.reshape(1, -1)
        portfolio_return = np.dot(weights, returns)
        portfolio_risk = np.sqrt(np.dot(np.dot(weights, cov_matrix_values), weights.T) * 252)
        sharpe_ratio = (portfolio_return - risk_free) / portfolio_risk
        return -sharpe_ratio
    
    # Функция для вычисления доходности портфеля
    def portfolio_return(weights, returns):
        return -np.dot(weights, returns)
    
    # Функция для вычисления риска портфеля
    def portfolio_risk(weights, cov_matrix):
        weights = weights.reshape(1, -1)
        return np.sqrt(np.dot(np.dot(weights, cov_matrix_values), weights.T) * 252)
    
    # Ограничения: сумма весов должна быть равна 1
    def check_sum(weights):
        return np.sum(weights) - 1
    
    num_weights = len(returns.columns)
    
    # Создаем массив initial_weights с одинаковыми весами
    initial_weights = np.full(num_weights, 1 / num_weights)
    #initial_weights = initial_weights.reshape(1, -1)
    
    # Ограничения: веса должны быть в диапазоне от 0 до 1
    bounds = [(0.05, 1) for _ in range(len(returns.columns))]
    
    # Ограничения: сумма весов должна быть равна 1
    constraints = ({'type': 'eq', 'fun': check_sum})
    
    if target == 'Максимизация доходности':
        # Целевая функция: максимизация доходности портфеля
        result = minimize(portfolio_return, initial_weights, args=(returns.values.flatten(),), method='SLSQP', bounds=bounds, constraints=constraints)
    elif target == 'Минимизация риска':
        # Целевая функция: минимизация риска портфеля
        result = minimize(portfolio_risk, initial_weights, args=(cov_matrix_values,), method='SLSQP', bounds=bounds, constraints=constraints)
    elif target == 'Максимизация коэффициента Шарпа':
        # Целевая функция: максимизация коэффициента Шарпа
        result = minimize(negative_sharpe_ratio, initial_weights, args=(returns.values.flatten(), cov_matrix_values, risk_free), method='SLSQP', bounds=bounds, constraints=constraints)
    
    # Оптимальные веса
    optimal_weights = result.x
    optimal_weights = optimal_weights.reshape(1, -1)
    
    # Оптимальная доходность портфеля
    optimal_portfolio_return = np.dot(optimal_weights, returns.values.flatten())
    
    # Вычисление риска портфеля
    port_risk = np.sqrt(np.dot(np.dot(optimal_weights,cov_matrix_values), optimal_weights.T) * 252)
    
    # Вычисление коэффициента Шарпа
    sharpe_ratio_value = (optimal_portfolio_return - risk_free) / port_risk
    
    optimal_weights_df = pd.DataFrame(optimal_weights, columns=cov_matrix.columns)
    
    port_risk = port_risk.item()
    optimal_portfolio_return = optimal_portfolio_return.item()
    
    data = {'Optimization': [target]}
    for col in optimal_weights_df.columns:
        data[col] = [optimal_weights_df[col].iloc[0]]
    data['Portfolio_Risk'] = [port_risk]
    data['Portfolio_Return'] = [optimal_portfolio_return]
    data = pd.DataFrame(data)
    
    #расчет штук бумаг в портфеле
    last_row_df = df.tail(1)
    max_value = last_row_df.max().max()
    max_column = last_row_df.idxmax(axis=1).values[0]
    optimal_value = optimal_weights_df.at[0, max_column]
    min_sum_port = max_value / optimal_value
    scaled_weights = optimal_weights_df * min_sum_port
    last_row_df.reset_index(drop=True, inplace=True)
    scaled_weights.reset_index(drop=True, inplace=True)
    quantity_df = scaled_weights / last_row_df
    quantity_df = quantity_df.round(0).astype(int)
    first_column = data.iloc[:, 0]
    quantity_df.insert(0, first_column.name, first_column)
    last_column = data.iloc[:, -1]
    quantity_df.insert(len(quantity_df.columns), last_column.name, last_column)
    penultimate_column = data.iloc[:, -2]
    quantity_df.insert(len(quantity_df.columns) - 1, penultimate_column.name, penultimate_column)
    
    return data,quantity_df


#ПЛАТНАЯ ОПТИМИЗАЦИЯ
def optimize_profile(target,returns,cov_matrix,df):
    prices=df
    
    cov_matrix_values=cov_matrix.values
    returns_values=returns.values
    # Функция для генерации случайных весов
    def generate_random_weights(n):
        min_value = 0.05
        total_sum = 1.0
        remaining_sum = total_sum - min_value * n
        numbers = [min_value] * n

        for i in range(n-1):
            max_add = remaining_sum - min_value * (n - i)
            add = random.uniform(0, max_add)
            numbers[i] += add
            remaining_sum -= add

        numbers[n-1] += remaining_sum
        weights = np.array(numbers)
        weights = weights.reshape(1, -1)
        return weights

    # Функция для расчета риска портфеля
    def calculate_portfolio_risk(weights, cov_matrix_values):
        return np.sqrt(np.dot(np.dot(weights, cov_matrix_values), weights.T) * 252)

    # Функция для расчета доходности портфеля
    def calculate_portfolio_return(weights, returns_values):
        return np.dot(weights, returns_values.T)


    # Функция для генерации сценариев и расчета риска и доходности
    def generate_portfolio_scenarios(returns_values, cov_matrix_values, num_scenarios):
        num_assets = returns_values.shape[1]
        weights_list = []
        risks_list = []
        returns_list = []

        for _ in range(num_scenarios):
            weights = generate_random_weights(num_assets)
            weights = weights.flatten()
            portfolio_risk = calculate_portfolio_risk(weights, cov_matrix_values)
            portfolio_risk = portfolio_risk.flatten()
            portfolio_risk = portfolio_risk.item()
            portfolio_return = calculate_portfolio_return(weights, returns_values)
            portfolio_return = portfolio_return.flatten()
            portfolio_return = portfolio_return.item()

            weights_list.append(weights)
            risks_list.append(portfolio_risk)
            returns_list.append(portfolio_return)

        return weights_list, risks_list, returns_list

    num_scenarios = 600000

    weights_list, risks_list, returns_list = generate_portfolio_scenarios(returns_values, cov_matrix_values, num_scenarios)
                         
    df = pd.DataFrame(weights_list, columns=returns.columns)
    df['Portfolio_Risk'] = risks_list
    df['Portfolio_Return'] = returns_list
    data=df

    min_risk_index = data['Portfolio_Risk'].idxmin()
    min_risk = data.loc[min_risk_index, 'Portfolio_Risk']
    min_risk_yield = data.loc[min_risk_index, 'Portfolio_Return']
    data = data[(data['Portfolio_Risk'] >= min_risk) & (data['Portfolio_Return'] >= min_risk_yield)]

    def graph(data):
        # Построение графика
        plt.figure(figsize=(10, 6))
        plt.scatter(data['Portfolio_Risk'], data['Portfolio_Return'], alpha=0.5, s=3, c='darkblue')
        plt.xlabel('Portfolio Risk')
        plt.ylabel('Portfolio Return')
        plt.title('Portfolio Risk vs Return')
        plt.grid(True)
        plt.show()

    #graph(data)

    #построение кривой
    data = data.sort_values(by='Portfolio_Risk', ascending=False)
    data=data.reset_index(drop=True)

    split_data = np.array_split(data, 100)

    for i in range(len(split_data)):
        max_index = split_data[i]['Portfolio_Return'].idxmax()
        split_data[i] = split_data[i].loc[[max_index]]

    data = pd.concat(split_data)
    #graph(data)
    data=data.reset_index(drop=True)

    split_data = np.array_split(data, 5)

    for i in range(len(split_data)):
        middle_index = len(split_data[i]) // 2
        split_data[i] = split_data[i].iloc[[middle_index]]

    data = pd.concat(split_data)
    data=data.reset_index(drop=True)

    data['Optimization'] = data.index.map(lambda x: [
    "Агрессивный",
    "Умеренно-агрессивный",
    "Рациональный",
    "Умеренно-консервативный",
        "Консервативный"][x % 5])
        
        
    cols = data.pop('Optimization')
    data.insert(0, 'Optimization', cols)
    data = data[data['Optimization'] == target]
    data = data.reset_index(drop=True)
    
    #расчет штук бумаг в портфеле
    df=prices
    optimal_weights_df = data.drop(columns=['Optimization', 'Portfolio_Risk', 'Portfolio_Return'])
    last_row_df = df.tail(1)
    max_value = last_row_df.max().max()
    max_column = last_row_df.idxmax(axis=1).values[0]
    optimal_value = optimal_weights_df.at[0, max_column]
    min_sum_port = max_value / optimal_value
    scaled_weights = optimal_weights_df * min_sum_port
    last_row_df.reset_index(drop=True, inplace=True)
    scaled_weights.reset_index(drop=True, inplace=True)
    quantity_df = scaled_weights / last_row_df
    quantity_df = quantity_df.round(0).astype(int)
    first_column = data.iloc[:, 0]
    quantity_df.insert(0, first_column.name, first_column)
    last_column = data.iloc[:, -1]
    quantity_df.insert(len(quantity_df.columns), last_column.name, last_column)
    penultimate_column = data.iloc[:, -2]
    quantity_df.insert(len(quantity_df.columns) - 1, penultimate_column.name, penultimate_column)
    
    return data,quantity_df


#ФУНКЦИЯ РАСЧЕТА ГРАФИКА ДОХОДНОСТИ ПОРТФЕЛЯ
def portfolio_return(data,daily_returns,df):
    
    Risk_Return_Ratio = data['Portfolio_Risk'] / data['Portfolio_Return']
    Risk_Return_Ratio = Risk_Return_Ratio.iloc[0]

    risk_portfolio = data.at[0, 'Portfolio_Risk']
    return_portfolio = data.at[0, 'Portfolio_Return']
    
    data = data.drop(columns=['Optimization',"Portfolio_Risk","Portfolio_Return"])
    data['Date']=None
    data.insert(0, 'Date', data.pop('Date'))
    data.loc[0, 'Date'] = daily_returns.loc[daily_returns.index[-1], 'Date']
    daily_returns['Date'] = pd.to_datetime(daily_returns['Date'])
    data['Date'] = pd.to_datetime(data['Date'])
    def add_suffix(col_name):
        return col_name + '_weight' if col_name != 'Date' else col_name
    # Применяем функцию к именам столбцов
    data = data.rename(columns=add_suffix)
    merged_df = pd.merge(daily_returns, data, on='Date', how='left')
    
    
    tickers_df = merged_df.loc[:, ~merged_df.columns.str.contains('_weight')]
    weights_df = merged_df.loc[:, merged_df.columns.str.contains('_weight') | (merged_df.columns == 'Date')]
    prices_df=df
    
    
    tickers_df.set_index('Date', inplace=True)
    weights_df.set_index('Date', inplace=True)
    
    
    def remove_suffix(col_name):
        return col_name.replace('_weight', '')
    
    # Применяем функцию к именам столбцов
    weights_df = weights_df.rename(columns=remove_suffix)
    
    
    last_row_prices = prices_df.iloc[-1]
    max_value_prices = last_row_prices.max()
    max_index_prices = last_row_prices.idxmax()
    
    
    max_column_index = prices_df.columns.get_loc(max_index_prices)
    last_row_weights = weights_df.iloc[-1]
    weight_value = last_row_weights.iloc[max_column_index]
    portfolio_sum=max_value_prices/weight_value
    
    new_row = {}
    for col in prices_df.columns:
        if col != 'Date':
            new_row[col] = [(portfolio_sum  * last_row_weights[col]) / last_row_prices[col]]
    
    # Создаем новый DataFrame
    amount_of_sec = pd.DataFrame(new_row)
    
    first_row_prices = prices_df.iloc[0]
    first_row_amount_of_sec = amount_of_sec.iloc[0]
    
    new_row = {}
    for col in amount_of_sec.columns:
        if col != 'Date':
            new_row[col] = [first_row_amount_of_sec[col] * first_row_prices[col]]
            
    positions_1y_ago = pd.DataFrame(new_row)
    portfolio_sum_1y_ago = positions_1y_ago.sum(axis=1)
    portfolio_sum_1y_ago=portfolio_sum_1y_ago.iloc[0]
    
    first_index_prices_df = prices_df.index[0]
    # Добавляем пустую строку в weights_df с индексом, равным первому индексу prices_df
    weights_df.loc[first_index_prices_df] = [None] * weights_df.shape[1]
    weights_df.sort_index(inplace=True)
    
    for col in weights_df.columns:
        weights_df.iloc[0, weights_df.columns.get_loc(col)] = positions_1y_ago.iloc[0, positions_1y_ago.columns.get_loc(col)] / portfolio_sum_1y_ago
    
    tickers_df = tickers_df + 1
    
    port_return_df = pd.DataFrame(index=tickers_df.index, columns=['port_return'])
    
    for x in range(len(tickers_df.index)):
    
        tickers_row=tickers_df.iloc[x]
    
        if x==0:
            weights_row_last=weights_df.iloc[x]
        else:
            weights_row_last=weights_df.iloc[x-1]
        port_return = (tickers_row * weights_row_last).sum()
        port_return_df.iloc[x] = port_return
        
        for y in range(len(tickers_df.columns)):
            ticker=tickers_row.iloc[y]
            weight_last=weights_row_last.iloc[y]
            weights_df.iloc[x+1, y]= ticker*weight_last/port_return
    
    port_return_df['Cummulative']=None
    port_return_df.iloc[0,1]=port_return_df.iloc[0,0]
    
    for x in range(1, len(port_return_df.index)):
            last_cummulative=port_return_df.iloc[x-1,1]
            port_return=port_return_df.iloc[x,0]
            port_return_df.iloc[x,1]=last_cummulative*port_return
    
    port_return_df=port_return_df['Cummulative']
    last_date = port_return_df.index[-1]
    last_date = str(last_date)
    last_date = datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S")
    new_date = last_date.replace(year=last_date.year + 1)
    new_date_str = new_date.strftime("%Y-%m-%d %H:%M:%S")
    
    return_portfolio=return_portfolio+1
    last_port_ret=port_return_df.iloc[-1]
    new_row = pd.DataFrame({'Cummulative': [last_port_ret*return_portfolio]}, index=[new_date_str])
    port_return_df = pd.concat([port_return_df, new_row])
    
    port_return_df=port_return_df-1

    
    # Строим график
    plt.figure(figsize=(10, 6))
    plt.plot(port_return_df.index, port_return_df['Cummulative'], linestyle='-', linewidth=2, color='black')
    plt.title('Cummulative Return Over Time')
    plt.xlabel('Date')
    plt.ylabel('Cummulative Return')
    plt.grid(True)
    
    # Координаты для прямой отрицательного отклонения
    x1 = port_return_df.index[-2]  # Предпоследняя строка
    y1 = port_return_df['Cummulative'].iloc[-2]  # Значение Cummulative в предпоследней строке
    x2 = port_return_df.index[-1]  # Последняя строка
    y2 = port_return_df['Cummulative'].iloc[-1] * (1-risk_portfolio)  # Значение Cummulative в последней строке, умноженное на 0.2
    
    # Добавляем прямую на график
    plt.plot([x1, x2], [y1, y2], linestyle='--', linewidth=2, color='red')
    
    # Координаты для прямой положительного отклонения
    x1 = port_return_df.index[-2]  # Предпоследняя строка
    y1 = port_return_df['Cummulative'].iloc[-2]  # Значение Cummulative в предпоследней строке
    x2 = port_return_df.index[-1]  # Последняя строка
    y2 = port_return_df['Cummulative'].iloc[-1] * (1+risk_portfolio)  # Значение Cummulative в последней строке, умноженное на 0.2
    
    # Добавляем прямую на график
    plt.plot([x1, x2], [y1, y2], linestyle='--', linewidth=2, color='green')
    
    value_to_display = (return_portfolio - 1) *100
    plt.text(0.10, 0.85, f'+{value_to_display:.2f}% за год при базовом сценарии', transform=plt.gca().transAxes,
    fontsize=12, bbox=dict(facecolor='white', alpha=0.5))
    
    plt.text(0.10, 0.75, f'коэффициент риск/доходность:{Risk_Return_Ratio:.2f}', transform=plt.gca().transAxes,
    fontsize=12, bbox=dict(facecolor='green', alpha=0.5))
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', backend="agg")
    buf.seek(0)
    image : bytes = base64.b64encode(buf.read()).decode('utf-8');
    buf.close()
    return image

#ФУКНЦИЯ РАСЧЕТА КЛИЕНТСКОГО ПОРТФЕЛЯ
def client_port(securities,weights,сurrency):
    returns,cov_matrix,risk_free, daily_returns,df = inputs(securities,сurrency)
    weights= {
        'Security': securities,
        'Weight': weights
    }
    weights = pd.DataFrame(weights)
    weights = weights.set_index('Security').T
    weights_ar = weights.values
    
    cov_matrix_values = cov_matrix.values
    returns=returns.values
    risk=np.sqrt(np.dot(np.dot(weights, cov_matrix_values), weights_ar.T) * 252)
    risk = risk.item()
    return_p=np.dot( weights_ar, returns.T)
    return_p = return_p.item()
    
    
    data = {'Optimization': None}
    for col in weights.columns:
        data[col] = [weights[col].iloc[0]]
    data['Portfolio_Risk'] = [risk]
    data['Portfolio_Return'] = [return_p]
    data = pd.DataFrame(data)
    
    image = portfolio_return(data,daily_returns,df)

    return (data, image)
    
    
#ПОЛУЧЕНИЕ ВХОДНЫХ ДАННЫХ ПО ВЫБРАННЫМ БУМАГАМ ЗА ПОСЛЕДНИЙ ГОД (ДЛЯ БЭКТЕСТА)
def inputs_backtest(securities,currency):#предлагаю здесь добавить доп параметр определения руб или дол
    #предлагаю здесь добавить разделение на базы данных руб и дол
    
    now=datetime.now()

    one_year_ago = now.replace(year=now.year - 1)
    one_year_ago=one_year_ago.strftime('%Y-%m-%d')
    
    two_years_ago = now.replace(year=now.year - 2)
    two_years_ago=two_years_ago.strftime('%Y-%m-%d')

    
    if currency == "rub":
        conn = sqlite3.connect("base_rub")
    elif currency=="usd":
        conn = sqlite3.connect("base_usd")
    cursor = conn.cursor()
    
    
    securities = [(item,) for item in securities]
    all_data = []
    for table_name in securities:
        cursor.execute(f"""
            SELECT *
            FROM {table_name[0]}
            WHERE Date BETWEEN '{two_years_ago}' AND '{one_year_ago}'
        """)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=column_names)
        all_data.append(df)
    
    transformed_data=[]
    
    for df in all_data:
        df = df.rename(columns={'Close': df['Ticker'].iloc[0]})
        df = df.drop(['Dividends', 'Stock_Splits', 'Ticker'], axis=1)
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        transformed_data.append(df)
    
    merged_df = transformed_data[0]
    
    for df in transformed_data[1:]:
        merged_df = merged_df.join(df, how='inner')
    df=merged_df
    
    daily_returns = df.pct_change()
    daily_returns.reset_index(inplace=True)
    daily_returns = daily_returns.drop(index=0)
    daily_returns_no_date = daily_returns.drop(columns='Date')
    cov_matrix = daily_returns_no_date.cov()
    
    """
    #предлагаю здесь добавить разделение на базы данных руб и дол
    cursor.execute("SELECT * FROM stock_potential")
    data = cursor.fetchall()
    returns = pd.DataFrame(data)
    returns = returns.T
    returns.columns = returns.iloc[0]
    returns = returns.drop(0)
    returns.reset_index(drop=True, inplace=True)
    returns = returns.replace({'%': '', ',': '.'}, regex=True).astype(float)

    
    """
    if currency == "rub":
        returns = {
        'GAZP': [-29.23],#[24.65],
        'SBER': [-13.07],#[15.8],
        'LKOH': [-4.32],#[18.22],
        'NVTK': [-44.34],#[28.86],
        'ROSN': [-15.43],#[23.57],
        'PLZL': [29.04],#[41.66],
        'GMKN': [-35.23],#[-25],
        'SNGSP': [0],#[-10],
        'TATN': [-11],#[10],
        'CHMF': [-12.7]}#[0]}
        
    elif currency=="usd":
        returns = {
        'JPM': [19.07],
        'WMT': [13.65],
        "MSFT":[15.86],
        'NVDA': [52.89],
        'AAPL': [11.62],
        'AMZN':[19.73],
        'V':[22.65],
        'UNH':[15.85],
        'PG':[12.87],
        'CRM':[21.31]}
        

    
    returns = pd.DataFrame(returns)
    
    
    returns = returns[returns.columns.intersection(df.columns)]
    returns = returns[returns.columns.intersection(df.columns)]
    returns = returns / 100
    column_order = df.columns
    returns = returns[column_order]

    # Безрисковая ставка
    if currency == "rub":
        risk_free = 0.15
    elif currency=="usd":
        risk_free = 0.043
        
    
    return returns,cov_matrix,risk_free, daily_returns,df