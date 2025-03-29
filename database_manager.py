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
        crypto TEXT,
        date TEXT,
        priceUsd TEXT,
        time TEXT,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de cadastro.py
    create_cadastro_table = """
    CREATE TABLE IF NOT EXISTS cadastro (
        id SERIAL PRIMARY KEY,
        rank TEXT,
        nome TEXT,
        simbolo TEXT,
        preco_usd TEXT,
        cap_mercado_usd TEXT,
        variacao_24h TEXT,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de candle.py
    create_candle_table = """
    CREATE TABLE IF NOT EXISTS candle (
        id SERIAL PRIMARY KEY,
        crypto TEXT,
        date TEXT,
        priceUsd TEXT,
        time TEXT,
        data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela para dados de volume.py
    create_volume_table = """
    CREATE TABLE IF NOT EXISTS volume (
        id SERIAL PRIMARY KEY,
        crypto TEXT,
        rank TEXT,
        symbol TEXT,
        name TEXT,
        supply TEXT,
        maxSupply TEXT,
        marketCapUsd TEXT,
        volumeUsd24Hr TEXT,
        priceUsd TEXT,
        changePercent24Hr TEXT,
        vwap24Hr TEXT,
        timestamp TEXT,
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

# Função genérica para inserir dados em qualquer tabela
def insert_data(df, table_name, columns):
    if df is None or df.empty:
        print(f"❌ Nenhum dado para inserir na tabela {table_name}.")
        return False
    
    conn = connect_db()
    if conn is None:
        return False
    
    cursor = conn.cursor()
    
    # Construir a query dinamicamente
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES ({placeholders})
    """
    
    try:
        data_to_insert = []
        for _, row in df.iterrows():
            # Converte todos os valores para string
            values = [str(row[col]) if pd.notna(row[col]) else None for col in columns]
            data_to_insert.append(tuple(values))
        
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"✅ {len(data_to_insert)} registros inseridos com sucesso na tabela {table_name}!")
        success = True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir dados na tabela {table_name}: {e}")
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
    insert_data(df_historico, 'historicocriptos', ['crypto', 'date', 'priceUsd', 'time'])
    
    # Obter e inserir dados de cadastro.py
    print("\n🏆 Processando dados de cadastro.py...")
    df_cadastro = get_top_cryptocurrencies(10)
    # Renomear colunas para corresponder à tabela
    df_cadastro = df_cadastro.rename(columns={
        'Rank': 'rank',
        'Nome': 'nome',
        'Símbolo': 'simbolo',
        'Preço (USD)': 'preco_usd',
        'Cap. Mercado (USD)': 'cap_mercado_usd',
        'Variação 24h (%)': 'variacao_24h'
    })
    insert_data(df_cadastro, 'cadastro', ['rank', 'nome', 'simbolo', 'preco_usd', 'cap_mercado_usd', 'variacao_24h'])
    
    # Obter e inserir dados de candle.py
    print("\n🕯️ Processando dados de candle.py...")
    df_candle = get_crypto_historical_data()
    insert_data(df_candle, 'candle', ['crypto', 'date', 'priceUsd', 'time'])
    
    # Obter e inserir dados de volume.py
    print("\n💰 Processando dados de volume.py...")
    df_volume = get_crypto_data()
    insert_data(df_volume, 'volume', [
        'crypto', 'rank', 'symbol', 'name', 'supply', 'maxSupply', 
        'marketCapUsd', 'volumeUsd24Hr', 'priceUsd', 
        'changePercent24Hr', 'vwap24Hr', 'timestamp'
    ])
    
    print("\n✅ Processo concluído com sucesso!")

if __name__ == "__main__":
    main()