import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np

# Importando módulos que já geram os dataframes
from cadastro import get_top_cryptocurrencies
from candle import get_crypto_historical_data
from historicocriptos import get_crypto_historical_data as get_historico_data
from volume import get_crypto_data

# Carregar variáveis do .env
load_dotenv()

# Função para conectar ao banco
def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return None

# Função para criar tabelas
def create_tables():
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS volume (
            id SERIAL PRIMARY KEY,
            crypto VARCHAR(50),
            rank INTEGER,
            symbol VARCHAR(10) NOT NULL,
            name VARCHAR(100) NOT NULL,
            supply NUMERIC,
            max_supply NUMERIC,
            market_cap_usd NUMERIC,
            volume_24h NUMERIC,
            price_usd NUMERIC(20,8),
            change_24h NUMERIC,
            vwap_24h NUMERIC,
            timestamp TIMESTAMP,
            data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS historicocriptos (
            id SERIAL PRIMARY KEY,
            crypto VARCHAR(50) NOT NULL,
            date TIMESTAMP NOT NULL,
            price_usd NUMERIC(20,8) NOT NULL,
            time TIMESTAMP,
            data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cadastro (
            id SERIAL PRIMARY KEY,
            rank INTEGER,
            nome VARCHAR(100) NOT NULL,
            simbolo VARCHAR(10) NOT NULL,
            preco_usd NUMERIC(20,8),
            cap_mercado_usd NUMERIC,
            variacao_24h NUMERIC,
            data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS candle (
            id SERIAL PRIMARY KEY,
            crypto VARCHAR(50) NOT NULL,
            date TIMESTAMP NOT NULL,
            price_usd NUMERIC(20,8) NOT NULL,
            time TIMESTAMP,
            data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        print("✅ Tabelas criadas com sucesso")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao criar tabelas: {e}")
        return False
    finally:
        conn.close()

# Função para limpar a tabela antes de inserir novos dados
def clear_table(table_name):
    conn = connect_db()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        # Usando TRUNCATE que é mais rápido que DELETE e reseta a sequência do ID
        cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
        conn.commit()
        print(f"✅ Tabela {table_name} limpa com sucesso")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao limpar tabela {table_name}: {e}")
        return False
    finally:
        conn.close()

# Função para converter tipos numpy para python nativos
def convert_numpy_types(value):
    if pd.isna(value):
        return None
    if isinstance(value, (np.int64, np.int32)):
        return int(value)
    if isinstance(value, (np.float64, np.float32)):
        return float(value)
    if isinstance(value, np.datetime64):
        return pd.to_datetime(value).to_pydatetime()
    return value

# Função para inserir dataframes no banco
def insert_dataframe(df, table_name, replace_data=False):
    if df is None or df.empty:
        print(f"❌ DataFrame vazio para {table_name}")
        return False
    
    # Verificar o número de linhas antes de inserir
    print(f"🔍 Inserindo {len(df)} registros na tabela {table_name}")
    
    conn = connect_db()
    if not conn:
        return False
    
    try:
        # Para cadastro e volume, limpar tabela antes de inserir
        if replace_data:
            if not clear_table(table_name):
                return False
        
        cursor = conn.cursor()
        columns = list(df.columns)
        
        # Converter DataFrame para lista de tuplas com tipos Python nativos
        data = [
            tuple(convert_numpy_types(x) for x in record) 
            for record in df.to_records(index=False)
        ]
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        cursor.executemany(query, data)
        conn.commit()
        print(f"✅ {len(data)} registros {'atualizados' if replace_data else 'inseridos'} em {table_name}")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao inserir em {table_name}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# Função para transformar o dataframe de historicocriptos
def transform_historicocriptos_df(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['priceUsd'] = pd.to_numeric(df['priceUsd'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df.rename(columns={'priceUsd': 'price_usd'})

# Função para transformar o dataframe de cadastro
def transform_cadastro_df(df):
    df = df.copy()
    df = df.rename(columns={
        'Rank': 'rank',
        'Nome': 'nome',
        'Símbolo': 'simbolo',
        'Preço (USD)': 'preco_usd',
        'Cap. Mercado (USD)': 'cap_mercado_usd',
        'Variação 24h (%)': 'variacao_24h'
    })
    
    df['rank'] = pd.to_numeric(df['rank'])
    df['preco_usd'] = pd.to_numeric(df['preco_usd'].str.replace('[^\d.]', '', regex=True))
    df['cap_mercado_usd'] = pd.to_numeric(df['cap_mercado_usd'].str.replace('[^\d.]', '', regex=True))
    df['variacao_24h'] = pd.to_numeric(df['variacao_24h'].str.replace('%', ''))
    
    return df[['rank', 'nome', 'simbolo', 'preco_usd', 'cap_mercado_usd', 'variacao_24h']]

# Função para transformar o dataframe de candle
def transform_candle_df(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['priceUsd'] = pd.to_numeric(df['priceUsd'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    return df.rename(columns={'priceUsd': 'price_usd'})

# Função para transformar o dataframe de volume
def transform_volume_df(df):
    df = df.copy()
    
    # Verificar tamanho do DataFrame original
    print(f"🔍 Volume: DataFrame original tem {len(df)} registros")
    
    # Garantir que não há valores nulos em colunas obrigatórias
    if 'symbol' in df.columns and 'name' in df.columns:
        df_filtered = df.dropna(subset=['symbol', 'name'])
        if len(df_filtered) != len(df):
            print(f"⚠️ Removidos {len(df) - len(df_filtered)} registros com valores nulos em symbol ou name")
        df = df_filtered
    
    # Conversões seguras com tratamento para valores nulos
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
    df['supply'] = pd.to_numeric(df['supply'], errors='coerce').round(2)
    df['maxSupply'] = pd.to_numeric(df['maxSupply'], errors='coerce').round(2)
    df['marketCapUsd'] = pd.to_numeric(df['marketCapUsd'], errors='coerce')
    df['volumeUsd24Hr'] = pd.to_numeric(df['volumeUsd24Hr'], errors='coerce').round(2)
    df['priceUsd'] = pd.to_numeric(df['priceUsd'], errors='coerce')
    df['changePercent24Hr'] = pd.to_numeric(df['changePercent24Hr'], errors='coerce')
    df['vwap24Hr'] = pd.to_numeric(df['vwap24Hr'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Renomear colunas
    renamed_df = df.rename(columns={
        'marketCapUsd': 'market_cap_usd',
        'volumeUsd24Hr': 'volume_24h',
        'priceUsd': 'price_usd',
        'changePercent24Hr': 'change_24h',
        'vwap24Hr': 'vwap_24h',
        'maxSupply': 'max_supply'
    })
    
    print(f"🔍 Volume: DataFrame após transformação tem {len(renamed_df)} registros")
    return renamed_df

# Função principal do ETL
def run_etl():
    print("🚀 Iniciando ETL...")
    
    # Criar tabelas
    if not create_tables():
        print("❌ Falha ao criar tabelas, abortando ETL")
        return
    
    print("📊 Obtendo e processando dataframes...")
    
    # Obter dataframes direto das funções importadas
    try:
        # 1. Volume (com substituição de dados)
        print("📊 Processando volume...")
        volume_df = get_crypto_data()
        if volume_df is not None and not volume_df.empty:
            transformed_df = transform_volume_df(volume_df)
            insert_dataframe(transformed_df, 'volume', replace_data=True)
        else:
            print("⚠️ Sem dados para volume")
        
        # 2. Historicocriptos
        print("📈 Processando historicocriptos...")
        historico_df = get_historico_data()
        if historico_df is not None and not historico_df.empty:
            transformed_df = transform_historicocriptos_df(historico_df)
            insert_dataframe(transformed_df, 'historicocriptos')
        else:
            print("⚠️ Sem dados para historicocriptos")
        
        # 3. Cadastro (com substituição de dados)
        print("📋 Processando cadastro...")
        cadastro_df = get_top_cryptocurrencies(5)  # Pegando top 10 criptomoedas
        if cadastro_df is not None and not cadastro_df.empty:
            transformed_df = transform_cadastro_df(cadastro_df)
            insert_dataframe(transformed_df, 'cadastro', replace_data=True)
        else:
            print("⚠️ Sem dados para cadastro")
        
        # 4. Candle
        print("🕯️ Processando candle...")
        candle_df = get_crypto_historical_data()
        if candle_df is not None and not candle_df.empty:
            transformed_df = transform_candle_df(candle_df)
            insert_dataframe(transformed_df, 'candle')
        else:
            print("⚠️ Sem dados para candle")
        
        print("🎉 ETL concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante o ETL: {e}")
        import traceback
        print(f"❌ Traceback completo: {traceback.format_exc()}")

if __name__ == "__main__":
    run_etl()