import requests
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import pandas as pd
from datetime import datetime
from threshold import sql_query_to_pd, pd_to_sqlDB, insert_into_table_monitoramento, delete_table

delete_table('monitoramento')
# URL do endpoint
url = 'http://localhost:5000/monitorar'

# Parâmetros da chamada
df2 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_2.csv')

time_count = df2[['time','count']].values.tolist()
# Criar um DataFrame vazio
data = {'time': [], 'count': []}
df = pd.DataFrame(data)

fig, ax = plt.subplots()
line, = ax.plot([], [], marker='o')
ax.set_xlabel('Time')
ax.set_ylabel('Count')
ax.set_title('Real-Time Data Plot')

# Converta os horários para o formato de datetime
time_count = [(datetime.strptime(tc[0], '%Hh %M'), tc[1]) for tc in time_count]

def add_data_to_graph(time, count):
    # Atualiza o DataFrame
    global df 
    df = df.append({'time': time, 'count': count}, ignore_index=True)
    
    # Defina os limites e rótulos do eixo x a cada atualização
    ax.set_xlim(df['time'].min(), df['time'].max())
    ax.set_xticks(df['time'])
    
    line.set_data(df['time'], df['count'][:len(df)])
    ax.relim()
    ax.autoscale_view()
    fig.autofmt_xdate()  # Formatação automática dos rótulos de data/hora

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
    df1 = df1[df1['status'] == 'denied']
    print(df1)
    
    new_time = df1.loc[df1.index[-1]]['time']
    new_count = df1.loc[df1.index[-1]]['count']
    
    add_data_to_graph(new_time, new_count)

ani = FuncAnimation(fig, animate, frames=len(time_count), interval=1000)

plt.show()
