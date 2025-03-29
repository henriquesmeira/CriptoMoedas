import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Credenciais do banco
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Função para criar conexão com o banco
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

# Função para criar a tabela se não existir
def create_table():
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS historico_criptos (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        date TIMESTAMP,
        priceUsd NUMERIC
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tabela criada/verificada com sucesso!")

# Função para inserir os dados
def insert_data(df):
    conn = connect_db()
    if conn is None:
        return
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO historico_criptos (crypto, date, priceUsd)
    VALUES (%s, %s, %s)
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_query, (row['crypto'], row['date'], row['priceUsd']))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Dados inseridos com sucesso!")

# Execução principal
def main():
    from historicocriptos import get_crypto_historical_data
    
    df = get_crypto_historical_data()
    if df is not None:
        create_table()
        insert_data(df)
    else:
        print("❌ Nenhum dado para inserir.")

if __name__ == "__main__":
    main()
