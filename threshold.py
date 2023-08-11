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

def insert_into_table_monitoramento(values,table_name='monitoramento', db_name='default.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    placeholders = ', '.join(['?'] * len(values))
    query = f"INSERT INTO {table_name} VALUES ({placeholders});"
    cursor.execute(query, values)

    conn.commit()
    conn.close()

def delete_table(table_name, db_name='default.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name};")
    conn.commit()
    conn.close()

if __name__ == '__main__':
    df1 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_1.csv')
    df1 = df1.rename(columns={'f0_':'count'})
    df1 = df1[['time','status','count']]
    df2 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/transactions_2.csv')
    df2 = df2[['time','status','count']]
    df1 = df1[df1['status'] != 'refunded']
    df2 = df2[df2['status'] != 'refunded']

    df1 = df1[df1['status'] != 'backend_reversed']
    df2 = df2[df2['status'] != 'backend_reversed']

    df1 = df1[df1['status'] != 'processing']
    df2 = df2[df2['status'] != 'processing']
    df = pd.concat([df1,df2])
    dados = df.reset_index(drop=True)


    drop_table('transacoes')

    pd_to_sqlDB(dados,
                table_name='transacoes',
                db_name='default.db')

    sql_statistics = """
    WITH media AS (
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
        ,AVG(count) AS media_transacoes
        ,MAX(count) AS max_transacoes
        ,MIN(count) AS min_transacoes
        ,COUNT(*) AS n

    FROM transacoes
    GROUP BY hora, status
    ORDER BY time DESC
    ),

    setting_thresholds AS (
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
    ,transacoes.status
    ,media_transacoes
    ,min_transacoes
    ,max_transacoes
    ,SQRT(SUM((count-media_transacoes)*(count-media_transacoes))/(n-1)) AS std_deviation
    ,media_transacoes + CASE WHEN 1*SQRT(SUM((count-media_transacoes)*(count-media_transacoes))/(n-1)) IS NULL THEN 0 ELSE 1*SQRT(SUM((count-media_transacoes)*(count-media_transacoes))/(n-1)) END AS media_plus_n_std_deviation
    FROM transacoes
    JOIN media
    ON substr(time, 1, 4) || 
        CASE
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 0 AND 9 THEN '00'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 10 AND 19 THEN '10'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 20 AND 29 THEN '20'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 30 AND 39 THEN '30'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 40 AND 49 THEN '40'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 50 AND 59 THEN '50'
        END = media.hora
    AND transacoes.status = media.status
    GROUP BY substr(time, 1, 4) || 
        CASE
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 0 AND 9 THEN '00'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 10 AND 19 THEN '10'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 20 AND 29 THEN '20'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 30 AND 39 THEN '30'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 40 AND 49 THEN '40'
            WHEN CAST(substr(time, 5, 2) AS INTEGER) BETWEEN 50 AND 59 THEN '50'
        END, transacoes.status
    ORDER BY hora ASC
    )

    SELECT 
    hora
    ,status
    ,ROUND(media_plus_n_std_deviation,0) AS threshold
    FROM setting_thresholds
    """

    threshold = sql_query_to_pd(sql_statistics, db_name='default.db')

    drop_table('threshold')

    pd_to_sqlDB(threshold,
                table_name='threshold',
                db_name='default.db')

    tabela_monitoramento = pd.DataFrame(columns=['time', 'status', 'count'])

    pd_to_sqlDB(tabela_monitoramento,
                table_name='monitoramento',
                db_name='default.db')