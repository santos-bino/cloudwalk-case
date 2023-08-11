import pandas as pd
import sqlite3
from matplotlib import pyplot as plt

def pd_to_sqlDB(input_df: pd.DataFrame,
                table_name: str,
                db_name: str = 'default.db') -> None:
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    cols = input_df.columns
    cols_string = ','.join(cols)
    val_wildcard_string = ','.join(['?'] * len(cols))
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    logging.info(f'SQL DB {db_name} created')
    sql_string = f"""CREATE TABLE {table_name} ({cols_string});"""
    cur.execute(sql_string)
    logging.info(f'SQL Table {table_name} created with {len(cols)} columns')
    rows_to_upload = input_df.to_dict(orient='split')['data']
    sql_string = f"""INSERT INTO {table_name} ({cols_string}) VALUES ({val_wildcard_string});"""
    cur.executemany(sql_string, rows_to_upload)
    logging.info(f'{len(rows_to_upload)} rows uploaded to {table_name}')
    con.commit()
    con.close()


def sql_query_to_pd(sql_query_string: str, db_name: str ='default.db') -> pd.DataFrame:
    con = sqlite3.connect(db_name)
    cursor = con.execute(sql_query_string)
    result_data = cursor.fetchall()
    cols = [description[0] for description in cursor.description]
    con.close()
    return pd.DataFrame(result_data, columns=cols)


def drop_table(table_name, db_name='default.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    conn.commit()
    conn.close()

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

# Carregar os dados e fazer as transformações necessárias
df1 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_1.csv')
df1 = df1.rename(columns={'f0_':'count'})
df1 = df1[['time','status','count']]
df2 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_2.csv')
df2 = df2[['time','status','count']]

# Remover linhas com o status "refunded"
df1 = df1[df1['status'] != 'refunded']
df2 = df2[df2['status'] != 'refunded']

df1 = df1[df1['status'] != 'backend_reversed']
df2 = df2[df2['status'] != 'backend_reversed']

df1 = df1[df1['status'] != 'processing']
df2 = df2[df2['status'] != 'processing']

# Combinação dos dataframes
df_train = df1[['time', 'status', 'count']]
df_test = df2[['time', 'status', 'count']]

# Converter o horário para minutos
def time_to_minutes(time_str):
    hours, minutes = map(int, time_str.split('h '))
    total_minutes = hours * 60 + minutes
    return total_minutes

df_train['time'] = df_train['time'].apply(time_to_minutes)
df_test['time'] = df_test['time'].apply(time_to_minutes)

# Codificar os labels de status
label_encoder = LabelEncoder()
label_encoder.fit(df_train['status'])

# Transformar os rótulos nos conjuntos de treinamento e teste
df_train['status'] = label_encoder.transform(df_train['status'])
df_test['status'] = label_encoder.transform(df_test['status'])

# Dividir os dados de treinamento e teste em features e target
X_train = df_train.drop('status', axis=1)
y_train = df_train['status']
X_test = df_test.drop('status', axis=1)
y_test = df_test['status']

# Treinar um Random Forest Classifier
clf = RandomForestClassifier(random_state=42)
clf.fit(X_train, y_train)

# Mapear labels numéricos para labels de status
status_mapping = {0: 'approved', 1: 'denied', 2: 'failed', 3: 'reversed'}


# Função para fazer previsões
def predict_status(time, count):
    time_in_minutes = time_to_minutes(time)
    input_data = [[time_in_minutes, count]]
    prediction = clf.predict(input_data)[0]
    predicted_status = status_mapping[prediction]
    return predicted_status

