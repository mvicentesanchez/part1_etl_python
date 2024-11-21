## Libraries
import pandas as pd
import numpy as np
from faker import Faker
import random
import sqlite3
import logging
from cryptography.fernet import Fernet

# Generar una clave para encryption y decryption
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Loggin file
logging.basicConfig(filename='transaction_logs.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

## Funciones

### 1. Cree datos simulados para completar el ejercicio, asegurándose de que reflejan escenarios realistas de transacciones de venta.

def data_transaction_generator(no_rows=int, export=bool):
    fake = Faker()
    num_rows = no_rows

    # Max 30 customer ID, Max 5 product id
    unique_customer_ids = [fake.uuid4() for _ in range(30)]
    unique_product_ids = [fake.uuid4() for _ in range(5)]

    # Random data
    data = {
        "transaction_id": [fake.uuid4() for _ in range(num_rows)],
        "customer_id": [random.choice(unique_customer_ids) for _ in range(num_rows)],
        "product_id": [random.choice(unique_product_ids) for _ in range(num_rows)],
        "quantity": [random.randint(1, 10) for _ in range(num_rows)],
        "timestamp": [fake.date_time_this_year() for _ in range(num_rows)]
    }

    df = pd.DataFrame(data)

    if export:
        df.to_csv('./data_mocked.csv', sep=',', index=False, encoding='utf-8')

    return df

### 2. Encrypt información sensible

def encrypt_sensitive_data(df):
    df['customer_id'] = df['customer_id'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode())
    df['product_id'] = df['product_id'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode())
    return df

### 3. Data quality

def data_quality(dataframe):
    if dataframe.isnull().values.any():
        null_rows = dataframe[dataframe.isnull().any(axis=1)]
        logging.info(f"Rows with null values excluded: {null_rows}")
        dataframe = dataframe.dropna()

    def is_valid_date(date):
        try:
            pd.to_datetime(date)
            return True
        except ValueError:
            return False

    invalid_dates = dataframe[~dataframe['timestamp'].apply(is_valid_date)]
    if not invalid_dates.empty:
        logging.info(f"Rows with invalid dates excluded: {invalid_dates}")
        dataframe = dataframe[dataframe['timestamp'].apply(is_valid_date)]

    return dataframe

### 4. DB

def insert_transaction_records_db(df_to_process):
    conn = sqlite3.connect('./sales_transactions.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        product_id TEXT,
                        quantity INTEGER,
                        sale_date DATE
        )
    ''')

    inserted_count = 0
    not_inserted_count = 0

    for index, row in df_to_process.iterrows():
        cursor.execute('SELECT COUNT(*) FROM transactions WHERE transaction_id = ?', (row['transaction_id'],))
        if cursor.fetchone()[0] == 0:
            timestamp_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO transactions (transaction_id, customer_id, product_id, quantity, sale_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['transaction_id'], row['customer_id'], row['product_id'], row['quantity'], timestamp_str))
            inserted_count += 1
            logging.info(f"Inserted transaction {row['transaction_id']}")
        else:
            not_inserted_count += 1
            logging.warning(f"Transaction {row['transaction_id']} already exists, not inserted.")

    conn.commit()
    conn.close()
    logging.info(f"Total inserted: {inserted_count}, Total not inserted: {not_inserted_count}")

if __name__ == "__main__":

    # df = data_transaction_generator(no_rows=100, export=True)
    df = pd.read_csv('./data_mocked.csv', encoding='utf-8', sep=',')
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')

    #Quality

    df_quality = data_quality(dataframe=df)
    
    # Encrypt sensitive data
    df = encrypt_sensitive_data(df_quality)

    insert_transaction_records_db(df_to_process=df)
