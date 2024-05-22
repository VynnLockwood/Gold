import pandas as pd
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import mysql.connector
import requests
from datetime import timedelta, datetime
import schedule
import time
import threading
import pytz

app = Flask(__name__)
CORS(app)

def get_db_connection():
    conn = mysql.connector.connect( 
            host="###", 
            user="###", 
            password="###", 
            port=8000, 
            database="###")
    return conn

def fetch_data():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = "SELECT DISTINCT Symbol FROM db_ideatrade.vix_dxy_10yy_xauusd ORDER BY Symbol ASC"
    cursor.execute(sql_query)

    unique_symbols = [row['Symbol'] for row in cursor.fetchall()]

    for symbol in unique_symbols:
        sql_query = "SELECT DISTINCT Date, `Close*` FROM db_ideatrade.vix_dxy_10yy_xauusd WHERE Symbol = %s ORDER BY Date ASC"
        cursor.execute(sql_query, (symbol,))
        data = cursor.fetchall()

        df = pd.DataFrame(data)
        df = df[df['Close*'] != 0]
        df.drop_duplicates(subset='Date', keep='first', inplace=True)

        file_name = f"{symbol}.csv"
        df.to_csv(file_name, index=False, columns=['Date', 'Close*'])

    cursor.close()
    conn.close()

def fetch_spdr_data(start_date):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query_spdr = "SELECT DISTINCT Date, GLD_Close FROM db_ideatrade.spdr WHERE Date >= %s ORDER BY Date ASC"
    cursor.execute(sql_query_spdr, (start_date,))
    data_spdr = cursor.fetchall()

    df_spdr = pd.DataFrame(data_spdr)
    df_spdr = df_spdr[df_spdr['GLD_Close'] != 0]
    df_spdr.drop_duplicates(subset='Date', keep='first', inplace=True)

    df_spdr.rename(columns={'Date': 'Date', 'GLD_Close': 'Close*'}, inplace=True)

    file_name_spdr = "SPDR.csv"
    df_spdr.to_csv(file_name_spdr, index=False)

    cursor.close()
    conn.close()

    return file_name_spdr

def fetch_xau_spdr_data(start_date):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query_spdr = "SELECT DISTINCT Date, LBMA_Gold_Price FROM db_ideatrade.spdr WHERE Date >= %s ORDER BY Date ASC"
    cursor.execute(sql_query_spdr, (start_date,))
    data_spdr = cursor.fetchall()

    df_spdr = pd.DataFrame(data_spdr)
    df_spdr = df_spdr[df_spdr['LBMA_Gold_Price'] != 0]
    df_spdr.drop_duplicates(subset='Date', keep='first', inplace=True)

    df_spdr.rename(columns={'Date': 'Date', 'LBMA_Gold_Price': 'Close*'}, inplace=True)

    file_name_spdr = "XAUUSD.csv"
    df_spdr.to_csv(file_name_spdr, index=False)

    cursor.close()
    conn.close()

    return file_name_spdr

def fill_gap_and_save(symbol):
    file_name = f"{symbol}.csv"
    df = pd.read_csv(file_name, parse_dates=['Date'])

    df = df.sort_values(by='Date')

    gap_data = []

    for i in range(len(df) - 1):
        date_diff = (df.iloc[i + 1]['Date'] - df.iloc[i]['Date']).days
        if date_diff > 1:
            start_date = df.iloc[i]['Date'] + timedelta(days=1)
            end_date = df.iloc[i + 1]['Date'] - timedelta(days=1)
            gap_dates = pd.date_range(start=start_date, end=end_date)
            prev_close = df.iloc[i]['Close*']
            gap_data.extend([(date, prev_close) for date in gap_dates])

    gap_df = pd.DataFrame(gap_data, columns=['Date', 'Close*'])
    filled_df = pd.concat([df, gap_df], ignore_index=True)
    filled_df = filled_df.sort_values(by='Date')

    filled_file_name = f"{symbol}_filled.csv"
    filled_df.to_csv(filled_file_name, index=False)

def fill_gap_and_save_all():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = "SELECT DISTINCT Symbol FROM db_ideatrade.vix_dxy_10yy_xauusd UNION SELECT DISTINCT 'SPDR' FROM db_ideatrade.spdr ORDER BY Symbol ASC"
    cursor.execute(sql_query)
    unique_symbols = [row['Symbol'] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    for symbol in unique_symbols:
        fill_gap_and_save(symbol)

def change_column_names(symbol):
    filled_file_name = f"{symbol}_filled.csv"
    try:
        df = pd.read_csv(filled_file_name)
        df = df.rename(columns={'Date': 'time', 'Close*': 'value'})
        df.to_csv(filled_file_name, index=False)
        return True
    except FileNotFoundError:
        return False

def check_and_fill_gaps():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = "SELECT DISTINCT Symbol FROM db_ideatrade.vix_dxy_10yy_xauusd ORDER BY Symbol ASC"
    cursor.execute(sql_query)
    unique_symbols = [row['Symbol'] for row in cursor.fetchall()]
    unique_symbols.append("SPDR")

    cursor.close()
    conn.close()

    for symbol in unique_symbols:
        filled_file_name = f"{symbol}_filled.csv"
        df = pd.read_csv(filled_file_name, parse_dates=['Date'])

        last_date = df['Date'].iloc[-1].date()
        current_date = datetime.now().date()

        if last_date < current_date:
            missing_dates = pd.date_range(start=last_date + timedelta(days=1), end=current_date)
            last_close = df['Close*'].iloc[-1]
            gap_df = pd.DataFrame({'Date': missing_dates, 'Close*': last_close})
            df = pd.concat([df, gap_df], ignore_index=True)
            df.to_csv(filled_file_name, index=False)

def change_column_names_all():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = "SELECT DISTINCT Symbol FROM db_ideatrade.vix_dxy_10yy_xauusd ORDER BY Symbol ASC"
    cursor.execute(sql_query)
    unique_symbols = [row['Symbol'] for row in cursor.fetchall()]
    unique_symbols.append("SPDR")

    cursor.close()
    conn.close()

    for symbol in unique_symbols:
        change_column_names(symbol)

import requests
import schedule
import time
import threading
import pytz
from datetime import datetime, timedelta

def make_request(endpoint):
    url = f"http://127.0.0.1:5040{endpoint}"
    try:
        response = requests.get(url)
        print(f"Requested {endpoint}, Status Code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error requesting {endpoint}: {e}")

def schedule_tasks():
    # Set the timezone to Asia/Bangkok
    bangkok_tz = pytz.timezone('Asia/Bangkok')

    # Schedule tasks in Asia/Bangkok timezone
    schedule.every().day.at("10:05").do(make_request, endpoint='/fetch').tag('scheduled_task')
    schedule.every().day.at("10:06").do(make_request, endpoint='/fetch_spdr').tag('scheduled_task2')
    schedule.every().day.at("10:06").do(make_request, endpoint='/fetch_spdr_xau').tag('scheduled_task20')
    schedule.every().day.at("10:07").do(make_request, endpoint='/fill_gap').tag('scheduled_task3')
    schedule.every().day.at("10:08").do(make_request, endpoint='/check_and_fill_gaps').tag('scheduled_task4')
    schedule.every().day.at("10:09").do(make_request, endpoint='/change_column_names_all').tag('scheduled_task5')

    schedule.every().day.at("14:35").do(make_request, endpoint='/fetch').tag('scheduled_task6')
    schedule.every().day.at("14:36").do(make_request, endpoint='/fetch_spdr').tag('scheduled_task7')
    schedule.every().day.at("10:06").do(make_request, endpoint='/fetch_spdr_xau').tag('scheduled_task21')
    schedule.every().day.at("14:37").do(make_request, endpoint='/fill_gap').tag('scheduled_task8')
    schedule.every().day.at("14:38").do(make_request, endpoint='/check_and_fill_gaps').tag('scheduled_task9')
    schedule.every().day.at("14:39").do(make_request, endpoint='/change_column_names_all').tag('scheduled_task10')

    schedule.every().day.at("17:05").do(make_request, endpoint='/fetch').tag('scheduled_task11')
    schedule.every().day.at("17:06").do(make_request, endpoint='/fetch_spdr').tag('scheduled_task12')
    schedule.every().day.at("10:06").do(make_request, endpoint='/fetch_spdr_xau').tag('scheduled_task22')
    schedule.every().day.at("17:07").do(make_request, endpoint='/fill_gap').tag('scheduled_task13')
    schedule.every().day.at("17:08").do(make_request, endpoint='/check_and_fill_gaps').tag('scheduled_task14')
    schedule.every().day.at("17:09").do(make_request, endpoint='/change_column_names_all').tag('scheduled_task15')



    

    while True:
        # Run scheduled tasks
        schedule.run_pending()
        time.sleep(1)

@app.route('/fetch')
def api_fetch():
    fetch_data()
    return 'Data fetched and saved to CSV files.'

@app.route('/fetch_spdr')
def api_fetch_spdr():
    dxy_df = pd.read_csv('DXY.csv')
    start_date = dxy_df['Date'].iloc[0]
    fetch_spdr_data(start_date)
    return 'SPDR Data fetched and saved to CSV files.'

@app.route('/fetch_spdr_xau')
def api_fetch_spdr_xau():
    dxy_df = pd.read_csv('DXY.csv')
    start_date = dxy_df['Date'].iloc[0]
    fetch_xau_spdr_data(start_date)
    return 'XAUUSD Data fetched and saved to CSV files.'

@app.route('/fill_gap')
def api_fill_gap():
    fill_gap_and_save_all()
    return 'Gaps filled and saved to CSV file for all symbols.'

@app.route('/check_and_fill_gaps')
def api_check_and_fill_gaps():
    check_and_fill_gaps()
    return 'Done'

@app.route('/change_column_names_all')
def api_change_column_names_all():
    change_column_names_all()
    return jsonify({"message": "Column names changed for all symbols"})

@app.get('/get_data/<symbol>')
def get_data(symbol):
    filled_file_name = f"{symbol}_filled.csv"
    try:
        df = pd.read_csv(filled_file_name)
        data = df.to_dict(orient='records')
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "Symbol not found"}), 404

if __name__ == '__main__':
    # Start the scheduling thread
    scheduler_thread = threading.Thread(target=schedule_tasks)
    scheduler_thread.start()

    # Start the Flask app
    app.run(debug=True, port=5040, host='0.0.0.0')
