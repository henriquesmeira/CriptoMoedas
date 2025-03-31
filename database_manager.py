import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np

# Importando módulos que já geram os dataframes
from cadastro import get_top_cryptocurrencies
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
        
        conn.commit()
        print("✅ Tabelas criadas com sucesso")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao criar tabelas: {e}")
        return False
    finally:
        conn.close()

# Outras funções mantidas sem mudanças...

# Função principal do ETL
def run_etl():
    print("🚀 Iniciando ETL...")
    
    # Criar tabelas
    if not create_tables():
        print("❌ Falha ao criar tabelas, abortando ETL")
        return
    
    print("📊 Obtendo e processando dataframes...")
    
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
        cadastro_df = get_top_cryptocurrencies(5)
        if cadastro_df is not None and not cadastro_df.empty:
            transformed_df = transform_cadastro_df(cadastro_df)
            insert_dataframe(transformed_df, 'cadastro', replace_data=True)
        else:
            print("⚠️ Sem dados para cadastro")
        
        print("🎉 ETL concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante o ETL: {e}")
        import traceback
        print(f"❌ Traceback completo: {traceback.format_exc()}")

if __name__ == "__main__":
    run_etl()
