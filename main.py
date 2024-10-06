from flask import Flask,request
import pandas as pd
import sqlite3 
import re


app = Flask(__name__)

@app.route('/migrate')
def migrate():
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    df = pd.read_excel('./20210309_2020_1 .xls')
    df.columns = [
        re.sub(r'\W+', '_', col).strip('_') for col in df.columns
    ]
    table_name='excel_data'
    columns = df.columns
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} TEXT' for col in columns])})"
    cursor.execute(create_table_query)

    for index, row in df.iterrows():
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for _ in columns])})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    conn.close()

    


def db_connect():
    DATABASE = 'data.db'
    db = sqlite3.connect(DATABASE)
    return db

@app.route('/data')
def main():
    api_well_number = request.args.get('well')
    cur = db_connect().cursor()
    cur.execute("PRAGMA table_info(excel_data)")
    rows = cur.fetchall()
    
    cur.execute("select SUM(OIL) as oil_sum,SUM(BRINE) as brine_sum,SUM(GAS) as gas_sum FROM excel_data where API_WELL_NUMBER={} ".format(api_well_number))
    row = cur.fetchone()
    return {
        'oil': row[0],
        'gas': row[2],
        'brine': row[1]
    }

    




