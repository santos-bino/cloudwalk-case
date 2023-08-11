import requests
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
from datetime import datetime
from threshold import sql_query_to_pd, pd_to_sqlDB, insert_into_table_monitoramento, delete_table
from collections import deque


delete_table('monitoramento')
# URL do endpoint
url = 'http://localhost:5000/monitorar'

# Parâmetros da chamada
df2 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_1.csv')

time_count = df2[['time','f0_']].values.tolist()
# Criar um DataFrame vazio
data = {'time': [], 'count': []}
df_denied = pd.DataFrame(data)
df_failed = pd.DataFrame(data)
df_reversed = pd.DataFrame(data)

# Fila para manter o histórico dos últimos pontos

fig, ax = plt.subplots()
line, = ax.plot([], [], marker='o')
ax.set_xlabel('Time')
ax.set_ylabel('Count')
ax.set_title('Real-Time Data Denied')

fig2, ax2 = plt.subplots()
line2, = ax2.plot([], [], marker='o')
ax2.set_xlabel('Time')
ax2.set_ylabel('Count')
ax2.set_title('Real-Time Data Failed')

fig3, ax3 = plt.subplots()
line3, = ax3.plot([], [], marker='o')
ax3.set_xlabel('Time')
ax3.set_ylabel('Count')
ax3.set_title('Real-Time Data Reversed')

# Converta os horários para o formato de datetime
time_count = [(datetime.strptime(tc[0], '%Hh %M'), tc[1]) for tc in time_count]

def add_data_to_graph_denied(time, count):
    # Atualiza o DataFrame
    global df_denied 
    df_denied = df_denied.append({'time': time, 'count': count}, ignore_index=True)
    
    # Defina os limites e rótulos do eixo x a cada atualização
    ax.set_xlim(df_denied['time'].min(), df_denied['time'].max())
    ax.set_xticks(df_denied['time'])
    
    ax.set_xlim(df_denied['time'].min(), df_denied['time'].max())
    ax.set_xticks(df_denied['time'])
    
    line.set_data(df_denied['time'], df_denied['count'][:len(df_denied)])
    ax.relim()
    ax.autoscale_view()
    fig.autofmt_xdate()  # Formatação automática dos rótulos de data/hora

def add_data_to_graph_failed(time, count):
    # Atualiza o DataFrame
    global df_failed 
    df_failed = df_failed.append({'time': time, 'count': count}, ignore_index=True)
    
    # Defina os limites e rótulos do eixo x a cada atualização
    ax2.set_xlim(df_failed['time'].min(), df_failed['time'].max())
    ax2.set_xticks(df_failed['time'])
    
    line2.set_data(df_failed['time'], df_failed['count'][:len(df_failed)])
    ax2.relim()
    ax2.autoscale_view()
    fig2.autofmt_xdate()  # Formatação automática dos rótulos de data/hora

def add_data_to_graph_reversed(time, count):
    # Atualiza o DataFrame
    global df_reversed 
    df_reversed = df_reversed.append({'time': time, 'count': count}, ignore_index=True)
    
    # Defina os limites e rótulos do eixo x a cada atualização
    ax3.set_xlim(df_reversed['time'].min(), df_reversed['time'].max())
    ax3.set_xticks(df_reversed['time'])
    
    line2.set_data(df_reversed['time'], df_reversed['count'][:len(df_reversed)])
    ax3.relim()
    ax3.autoscale_view()
    fig3.autofmt_xdate()  # Formatação automática dos rótulos de data/hora

def animate(i):
    parametro = {
        'time': time_count[i][0].strftime('%Hh %M'),
        'count': time_count[i][1]
    }
    print(parametro['time'])

    # Faz a chamada POST para o servidor
    response = requests.post(url, json=parametro)
    # Verifica a resposta
    data = response.json()
    resultado = data['resultado']
    print(f"Resultado da função: {resultado}")
    valores = (parametro['time'],resultado,parametro['count'])
    insert_into_table_monitoramento(valores)
    
    sql_graph = """
    SELECT *
    FROM monitoramento
    """
    df1 = sql_query_to_pd(sql_graph, db_name='default.db')
    print(df1)
    
    new_time = df1.loc[df1.index[-1]]['time']
    new_count = df1.loc[df1.index[-1]]['count']
    if df1.loc[df1.index[-1]]['status'] == 'denied':
        add_data_to_graph_denied(new_time, new_count)
    elif df1.loc[df1.index[-1]]['status'] == 'failed':
        add_data_to_graph_failed(new_time, new_count)
    elif df1.loc[df1.index[-1]]['status'] == 'reversed':
        add_data_to_graph_reversed(new_time, new_count)
    else:
        pass

    check_alert = """
    WITH agrupamento AS (
    SELECT 
        substr(time, 1, 4) || 
        CASE
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 0 AND 9 THEN '00'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 10 AND 19 THEN '10'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 20 AND 29 THEN '20'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 30 AND 39 THEN '30'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 40 AND 49 THEN '40'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 50 AND 59 THEN '50'
        END AS hora
        ,status
        ,count
    FROM monitoramento
    )
    SELECT agrupamento.*, threshold, CASE WHEN agrupamento.count >= threshold THEN 'alerta' ELSE 'normal' END AS flag
    FROM agrupamento
    JOIN threshold
    ON agrupamento.hora = threshold.hora AND agrupamento.status = threshold.status
    """
    df_check_alert = sql_query_to_pd(check_alert, db_name='default.db')
    print(df_check_alert)
    if df_check_alert.loc[df_check_alert.index[-1]]['flag'] == 'alerta':
        url2 = 'http://localhost:5000/enviar_email'
        parametro = {'body':"""Status: """+df_check_alert.loc[df_check_alert.index[-1]]['status']+""" reportou """+str(df_check_alert.loc[df_check_alert.index[-1]]['count'])+""" transações às """+df_check_alert.loc[df_check_alert.index[-1]]['hora']+""". O máximo considerado normal é """+str(df_check_alert.loc[df_check_alert.index[-1]]['threshold'])+""". """}
        response = requests.post(url2, json=parametro)
    

ani = FuncAnimation(fig, animate, frames=len(time_count), interval=5000)
ani2 = FuncAnimation(fig2, animate, frames=len(time_count), interval=5000)
ani3 = FuncAnimation(fig3, animate, frames=len(time_count), interval=5000)
plt.show()
