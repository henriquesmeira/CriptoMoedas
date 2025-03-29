import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Importando os módulos necessários
from cadastro import get_top_cryptocurrencies
from candle import get_crypto_historical_data
from historicocriptos import get_crypto_historical_data as get_historico_data
from volume import get_crypto_data

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

# Função para criar todas as tabelas necessárias
def create_tables():
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    
    # Tabela para dados de historicocriptos.py
    create_historico_table = """
    CREATE TABLE IF NOT EXISTS historicocriptos (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        date TIMESTAMP,
        priceUsd NUMERIC,
        time BIGINT,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de cadastro.py
    create_cadastro_table = """
    CREATE TABLE IF NOT EXISTS cadastro (
        id SERIAL PRIMARY KEY,
        rank INTEGER,
        nome VARCHAR(100),
        simbolo VARCHAR(20),
        preco_usd NUMERIC,
        cap_mercado_usd NUMERIC,
        variacao_24h NUMERIC,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de candle.py
    create_candle_table = """
    CREATE TABLE IF NOT EXISTS candle (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        date TIMESTAMP,
        priceUsd NUMERIC,
        time BIGINT,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de volume.py
    create_volume_table = """
    CREATE TABLE IF NOT EXISTS volume (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        rank INTEGER,
        symbol VARCHAR(20),
        name VARCHAR(100),
        supply NUMERIC,
        maxSupply NUMERIC,
        marketCapUsd NUMERIC,
        volumeUsd24Hr NUMERIC,
        priceUsd NUMERIC,
        changePercent24Hr NUMERIC,
        vwap24Hr NUMERIC,
        timestamp TIMESTAMP,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor.execute(create_historico_table)
        cursor.execute(create_cadastro_table)
        cursor.execute(create_candle_table)
        cursor.execute(create_volume_table)
        conn.commit()
        print("✅ Todas as tabelas criadas/verificadas com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao criar tabelas: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de historicocriptos.py
def insert_historico_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de histórico para inserir.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO historicocriptos (crypto, date, priceUsd, time)
    VALUES (%s, %s, %s, %s)
    """
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['crypto'],
                row['date'],
                float(row['priceUsd']),
                int(row['time'])
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} registros históricos inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados históricos: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de cadastro.py
def insert_cadastro_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de cadastro para inserir.")
        return False
    
    # Limpar valores formatados para armazenar como numéricos
    df_clean = df.copy()
    for col in ['Preço (USD)', 'Cap. Mercado (USD)', 'Variação 24h (%)']:
        if col in df_clean.columns:
            if col == 'Preço (USD)' or col == 'Cap. Mercado (USD)':
                df_clean[col] = df_clean[col].str.replace('$', '').str.replace(',', '').astype(float)
            elif col == 'Variação 24h (%)':
                df_clean[col] = df_clean[col].str.replace('%', '').astype(float)
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO cadastro (rank, nome, simbolo, preco_usd, cap_mercado_usd, variacao_24h)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    try:
        data_to_insert = []
        for _, row in df_clean.iterrows():
            data_to_insert.append((
                row['Rank'], 
                row['Nome'], 
                row['Símbolo'], 
                float(row['Preço (USD)']), 
                float(row['Cap. Mercado (USD)']), 
                float(row['Variação 24h (%)'])
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} registros de cadastro inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados de cadastro: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de candle.py
def insert_candle_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de candle para inserir.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO candle (crypto, date, priceUsd, time)
    VALUES (%s, %s, %s, %s)
    """
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['crypto'],
                row['date'],
                float(row['priceUsd']),
                int(row['time'])
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} registros de candle inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados de candle: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de volume.py
def insert_volume_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de volume para inserir.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO volume (
        crypto, rank, symbol, name, supply, maxSupply, 
        marketCapUsd, volumeUsd24Hr, priceUsd, 
        changePercent24Hr, vwap24Hr, timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['crypto'],
                row['rank'],
                row['symbol'],
                row['name'],
                float(row['supply']),
                float(row['maxSupply']) if pd.notna(row['maxSupply']) else None,
                float(row['marketCapUsd']),
                float(row['volumeUsd24Hr']),
                float(row['priceUsd']),
                float(row['changePercent24Hr']),
                float(row['vwap24Hr']),
                row['timestamp']
            ))
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} registros de volume inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados de volume: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Execução principal
def main():
    print("🏦 Iniciando gerenciador de banco de dados para criptomoedas...")
    
    # Criar todas as tabelas
    if not create_tables():
        print("❌ Falha ao criar tabelas. Abortando operação.")
        return
    
    # Obter e inserir dados de historicocriptos.py
    print("\n📊 Processando dados de historicocriptos.py...")
    df_historico = get_historico_data()
    insert_historico_data(df_historico)
    
    # Obter e inserir dados de cadastro.py
    print("\n🏆 Processando dados de cadastro.py...")
    df_cadastro = get_top_cryptocurrencies(10)
    insert_cadastro_data(df_cadastro)
    
    # Obter e inserir dados de candle.py
    print("\n🕯️ Processando dados de candle.py...")
    df_candle = get_crypto_historical_data()
    insert_candle_data(df_candle)
    
    # Obter e inserir dados de volume.py
    print("\n💰 Processando dados de volume.py...")
    df_volume = get_crypto_data()
    insert_volume_data(df_volume)
    
    print("\n✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    main()