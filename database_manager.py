import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Importando os módulos necessários
from cadastro import get_top_cryptocurrencies
from candle import get_crypto_historical_data
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
    
    # Tabela para os dados históricos
    create_historical_table = """
    CREATE TABLE IF NOT EXISTS historico_criptos (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        date TIMESTAMP,
        priceUsd NUMERIC
    );
    """
    
    # Tabela para os dados de cadastro (top criptomoedas)
    create_ranking_table = """
    CREATE TABLE IF NOT EXISTS ranking_criptos (
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
    
    # Tabela para os dados completos/volume
    create_volume_table = """
    CREATE TABLE IF NOT EXISTS volume_criptos (
        id SERIAL PRIMARY KEY,
        crypto VARCHAR(50),
        rank INTEGER,
        symbol VARCHAR(20),
        name VARCHAR(100),
        supply NUMERIC,
        max_supply NUMERIC,
        market_cap_usd NUMERIC,
        volume_usd_24h NUMERIC,
        price_usd NUMERIC,
        change_percent_24h NUMERIC,
        vwap_24h NUMERIC,
        timestamp TIMESTAMP
    );
    """
    
    try:
        cursor.execute(create_historical_table)
        cursor.execute(create_ranking_table)
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

# Função para inserir dados históricos
def insert_historical_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado histórico para inserir.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO historico_criptos (crypto, date, priceUsd)
    VALUES (%s, %s, %s)
    """
    
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_query, (row['crypto'], row['date'], row['priceUsd']))
        
        conn.commit()
        print("✅ Dados históricos inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados históricos: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de ranking
def insert_ranking_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de ranking para inserir.")
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
    INSERT INTO ranking_criptos (rank, nome, simbolo, preco_usd, cap_mercado_usd, variacao_24h)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    try:
        for _, row in df_clean.iterrows():
            cursor.execute(insert_query, (
                row['Rank'], 
                row['Nome'], 
                row['Símbolo'], 
                row['Preço (USD)'], 
                row['Cap. Mercado (USD)'], 
                row['Variação 24h (%)']
            ))
        
        conn.commit()
        print("✅ Dados de ranking inseridos com sucesso!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados de ranking: {e}")
        success = False
    finally:
        cursor.close()
        conn.close()
        return success

# Função para inserir dados de volume
def insert_volume_data(df):
    if df is None or df.empty:
        print("❌ Nenhum dado de volume para inserir.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO volume_criptos (
        crypto, rank, symbol, name, supply, max_supply, 
        market_cap_usd, volume_usd_24h, price_usd, 
        change_percent_24h, vwap_24h, timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row['crypto'],
                row['rank'],
                row['symbol'],
                row['name'],
                row['supply'],
                row['maxSupply'] if pd.notna(row['maxSupply']) else None,
                row['marketCapUsd'],
                row['volumeUsd24Hr'],
                row['priceUsd'],
                row['changePercent24Hr'],
                row['vwap24Hr'],
                row['timestamp']
            ))
        
        conn.commit()
        print("✅ Dados de volume inseridos com sucesso!")
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
    
    # Obter e inserir dados históricos
    print("\n📊 Processando dados históricos...")
    df_historical = get_crypto_historical_data()
    insert_historical_data(df_historical)
    
    # Obter e inserir dados de ranking
    print("\n🏆 Processando dados de ranking...")
    df_ranking = get_top_cryptocurrencies(10)  # Aumentei para 10 criptomoedas
    insert_ranking_data(df_ranking)
    
    # Obter e inserir dados de volume
    print("\n💰 Processando dados de volume...")
    df_volume = get_crypto_data()
    insert_volume_data(df_volume)
    
    print("\n✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    main()