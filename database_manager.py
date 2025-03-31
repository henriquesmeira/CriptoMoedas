import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
import numpy as np
import traceback

# Importando módulos que geram os dataframes
from cadastro import get_top_cryptocurrencies
from historicocriptos import get_crypto_historical_data as get_historico_data
from volume import get_crypto_data

# Carregar variáveis de ambiente
load_dotenv()


def connect_db():
    """
    Conecta ao banco de dados PostgreSQL usando variáveis de ambiente.
    
    Returns:
        Objeto de conexão ou None se a conexão falhar
    """
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
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def create_tables():
    """
    Cria as tabelas necessárias no banco de dados, se não existirem.
    
    Returns:
        Booleano indicando sucesso ou falha
    """
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
        print("Tabelas criadas com sucesso")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Erro ao criar tabelas: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def transform_volume_df(df):
    """
    Transforma o DataFrame de volume para corresponder ao esquema do banco de dados.
    
    Args:
        df: DataFrame bruto de volume
        
    Returns:
        DataFrame transformado pronto para inserção no banco de dados
    """
    # Implementação seria colocada aqui
    # Placeholder para a função mencionada no código original
    return df


def transform_historicocriptos_df(df):
    """
    Transforma o DataFrame histórico de criptomoedas para corresponder ao esquema do banco de dados.
    
    Args:
        df: DataFrame histórico bruto
        
    Returns:
        DataFrame transformado pronto para inserção no banco de dados
    """
    # Implementação seria colocada aqui
    # Placeholder para a função mencionada no código original
    return df


def transform_cadastro_df(df):
    """
    Transforma o DataFrame de cadastro para corresponder ao esquema do banco de dados.
    
    Args:
        df: DataFrame de cadastro bruto
        
    Returns:
        DataFrame transformado pronto para inserção no banco de dados
    """
    # Implementação seria colocada aqui
    # Placeholder para a função mencionada no código original
    return df


def insert_dataframe(df, table_name, replace_data=False):
    """
    Insere DataFrame na tabela do banco de dados especificada.
    
    Args:
        df: DataFrame a inserir
        table_name: Nome da tabela de destino
        replace_data: Se deve substituir dados existentes
        
    Returns:
        Booleano indicando sucesso ou falha
    """
    # Implementação seria colocada aqui
    # Placeholder para a função mencionada no código original
    return True


def run_etl():
    """
    Processo ETL principal para extrair, transformar e carregar dados de criptomoedas.
    """
    print("Iniciando processo ETL...")
    
    # Criar tabelas
    if not create_tables():
        print("Falha ao criar tabelas, abortando ETL")
        return
    
    print("Obtendo e processando dataframes...")
    
    try:
        # 1. Volume (com substituição de dados)
        print("Processando dados de volume...")
        volume_df = get_crypto_data()
        if volume_df is not None and not volume_df.empty:
            transformed_df = transform_volume_df(volume_df)
            insert_dataframe(transformed_df, 'volume', replace_data=True)
        else:
            print("Sem dados disponíveis para volume")
        
        # 2. Dados históricos de criptomoedas
        print("Processando dados históricos de criptomoedas...")
        historico_df = get_historico_data()
        if historico_df is not None and not historico_df.empty:
            transformed_df = transform_historicocriptos_df(historico_df)
            insert_dataframe(transformed_df, 'historicocriptos')
        else:
            print("Sem dados disponíveis para histórico de criptomoedas")
        
        # 3. Cadastro (com substituição de dados)
        print("Processando dados de cadastro...")
        cadastro_df = get_top_cryptocurrencies(5)
        if cadastro_df is not None and not cadastro_df.empty:
            transformed_df = transform_cadastro_df(cadastro_df)
            insert_dataframe(transformed_df, 'cadastro', replace_data=True)
        else:
            print("Sem dados disponíveis para cadastro")
        
        print("ETL concluído com sucesso!")
        
    except Exception as e:
        print(f"Erro durante o processo ETL: {e}")
        print(f"Traceback completo: {traceback.format_exc()}")


if __name__ == "__main__":
    run_etl()