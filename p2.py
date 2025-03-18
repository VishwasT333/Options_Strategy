import requests
import pandas as pd
import datetime
import mysql.connector

# ✅ API Credentials
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzQyMzExOTI0LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwNDIxNDc1MCJ9.UCxgwyHMBu5Q0MwvFjcwgW94rjf627bvVdindU2fdu7PIuy7WBj1QQ-s9duVte53mKeuOg0s8Zr-iBHSL4lqbQ"
CLIENT_ID = "1104214750"

# ✅ MySQL Database Connection
DB_CONFIG = {
    "host": "localhost",
    "user": "sqluser",
    "password": "vishwast3",
    "database": "trading_db"
}

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def create_trades_table():
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS trades;")  # Ensure old incorrect table is removed
    db.commit()
    cursor.execute("""
        CREATE TABLE trades (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trade_time DATETIME NOT NULL,
            price FLOAT NOT NULL
        )
    """)
    db.commit()
    cursor.close()
    db.close()

def insert_trade(trade_time, price):
    db = connect_db()
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO trades (trade_time, price) VALUES (%s, %s)
    """, (trade_time, price))
    db.commit()
    cursor.close()
    db.close()

# 🔹 Fetch Intraday Data
def fetch_nifty_data():
    url = "https://api.dhan.co/v2/charts/intraday"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN
    }

    # Fetch data for the last 7 days including today
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    payload = {
        "securityId": "13",  
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": "1",  # 1-minute interval
        "fromDate": start_date,
        "toDate": end_date
    }

    print(f"🔍 Sending Payload: {payload}")

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f"❌ API request failed with status code {response.status_code}")
        print(f"🔍 API Response: {response.text}")
        return pd.DataFrame()

    try:
        data = response.json()
        if all(key in data for key in ['open', 'high', 'low', 'close', 'volume', 'timestamp']):
            df = pd.DataFrame({
                'datetime': [datetime.datetime.fromtimestamp(ts) for ts in data['timestamp']],
                'close': data['close']
            })
            df.set_index('datetime', inplace=True)
            df.sort_index(inplace=True)
            return df
        else:
            print(f"⚠️ API Response missing expected keys: {list(data.keys())}")
    except Exception as e:
        print(f"❌ Error parsing API response: {e}")

    return pd.DataFrame()

# 🔹 Calculate EMA
def calculate_ema(df, span):
    return df['close'].ewm(span=span, adjust=False).mean()

# 🔹 Detect EMA Crossovers and Trigger Buys/Sells
def count_today_ema_crossovers(df):
    df['EMA_5'] = calculate_ema(df, span=5)
    df['EMA_20'] = calculate_ema(df, span=20)
    
    today = datetime.datetime.now().date()
    df_today = df[df.index.date == today].copy()
    
    # Detect bullish crossover (EMA 5 crosses above EMA 20)
    df_today.loc[:, 'Bullish_Crossover'] = (df_today['EMA_5'] > df_today['EMA_20']) & \
                                           (df_today['EMA_5'].shift(1) <= df_today['EMA_20'].shift(1))
    
    # Detect bearish crossover (EMA 5 crosses below EMA 20)
    df_today.loc[:, 'Bearish_Crossover'] = (df_today['EMA_5'] < df_today['EMA_20']) & \
                                           (df_today['EMA_5'].shift(1) >= df_today['EMA_20'].shift(1))
    
    crossover_count = df_today[['Bullish_Crossover', 'Bearish_Crossover']].sum().sum()
    
    for index, row in df_today[df_today['Bullish_Crossover']].iterrows():
        print(f"✅ Bullish Buy Triggered at {index} with price {row['close']}")
        insert_trade(index, row['close'])
    
    for index, row in df_today[df_today['Bearish_Crossover']].iterrows():
        print(f"❌ Bearish Sell Triggered at {index} with price {row['close']}")
        insert_trade(index, row['close'])
    
    return df_today, crossover_count

# 🔹 Main Execution
create_trades_table()
df = fetch_nifty_data()

if not df.empty:
    df_today, crossover_count = count_today_ema_crossovers(df)
    print(f"\n✅ Today's EMA(5) > EMA(20) and EMA(5) < EMA(20) Crossovers: {crossover_count}\n")
    print(df_today[['close', 'EMA_5', 'EMA_20']].tail())
else:
    print("❌ No data available for the given date range.")
