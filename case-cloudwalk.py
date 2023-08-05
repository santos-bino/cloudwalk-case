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

checkout_1 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/checkout_1.csv')
checkout_2 = pd.read_csv('https://raw.githubusercontent.com/thais-menezes/monitoring/main/checkout_2.csv')

pd_to_sqlDB(checkout_1,
            table_name='sales_data1',
            db_name='default.db')
pd_to_sqlDB(checkout_2,
            table_name='sales_data2',
            db_name='default.db')

sql_query_string_1 = """
    SELECT time,
       today,
       yesterday,
       same_day_last_week,
       avg_last_week,
       avg_last_month,
       today - yesterday AS difference_from_yesterday,
       today - same_day_last_week AS difference_from_same_day_last_week,
       today - avg_last_week AS difference_from_avg_last_week,
       today - avg_last_month AS difference_from_avg_last_month
FROM sales_data1
"""

sql_query_string_2 = """
    SELECT time,
       today,
       yesterday,
       same_day_last_week,
       avg_last_week,
       avg_last_month,
       today - yesterday AS difference_from_yesterday,
       today - same_day_last_week AS difference_from_same_day_last_week,
       today - avg_last_week AS difference_from_avg_last_week,
       today - avg_last_month AS difference_from_avg_last_month
FROM sales_data2
"""

result_df_1 = sql_query_to_pd(sql_query_string_1, db_name='default.db')
result_df_2 = sql_query_to_pd(sql_query_string_2, db_name='default.db')

plt.figure()
result_df_1['difference_from_yesterday'].plot(kind='line')
result_df_1['difference_from_same_day_last_week'].plot(kind='line')
result_df_1['difference_from_avg_last_week'].plot(kind='line')
result_df_1['difference_from_avg_last_month'].plot(kind='line')
plt.legend(loc = 'lower left', shadow=True, fontsize = '8')
plt.xlabel('Hour', fontsize = 8, color = 'black')
plt.ylabel('Number of sales', fontsize = 8, color = 'black')
plt.grid(True)
plt.suptitle('Difference between today and the other data points', fontsize=14, color='black')

plt.figure()
result_df_1['today'].plot(kind='line')
result_df_1['yesterday'].plot(kind='line')
result_df_1['same_day_last_week'].plot(kind='line')
result_df_1['avg_last_week'].plot(kind='line')
result_df_1['avg_last_month'].plot(kind='line')
plt.legend(loc = 'lower left', shadow=True, fontsize = '8')
plt.xlabel('Hour', fontsize = 8, color = 'black')
plt.ylabel('Number of sales', fontsize = 8, color = 'black')
plt.grid(True)
plt.suptitle('Comparison between today and the other data points', fontsize=14, color='black')
