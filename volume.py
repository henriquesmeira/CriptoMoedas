import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time

# Configuração
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")
CRYPTO_IDS = ["bitcoin", "ethereum", "xrp", "binance-coin"]
MAX_RETRIES = 5
BASE_DELAY = 2


def fetch_crypto_data(crypto_id):
    """
    Recupera dados atuais de uma criptomoeda com tratamento de limite de requisições.
    
    Args:
        crypto_id: ID da criptomoeda a ser buscada
        
    Returns:
        DataFrame com dados atuais ou None se falhar
    """
    url = f"https://api.coincap.io/v2/assets/{crypto_id}"
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 429:
                wait_time = BASE_DELAY * (2 ** attempt)  # Backoff exponencial
                print(f"Limite de requisições atingido. Aguardando {wait_time} segundos...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            return process_crypto_response(response.json()["data"], crypto_id)
            
        except requests.exceptions.RequestException as e:
            wait_time = BASE_DELAY * (2 ** attempt)
            print(f"Erro na requisição para {crypto_id}: {e}")
            print(f"Aguardando {wait_time} segundos antes da próxima tentativa...")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Erro inesperado para {crypto_id}: {e}")
            return None
    
    print(f"Falha após {MAX_RETRIES} tentativas para {crypto_id}")
    return None


def process_crypto_response(data, crypto_id):
    """
    Processa os dados de resposta da API em um DataFrame.
    
    Args:
        data: Dados brutos da resposta da API
        crypto_id: ID da criptomoeda
        
    Returns:
        DataFrame com dados processados
    """
    return pd.DataFrame([{
        'crypto': crypto_id,
        'rank': data['rank'],
        'symbol': data['symbol'],
        'name': data['name'],
        'supply': data['supply'],
        'maxSupply': data['maxSupply'],
        'marketCapUsd': data['marketCapUsd'],
        'volumeUsd24Hr': data['volumeUsd24Hr'],
        'priceUsd': data['priceUsd'],
        'changePercent24Hr': data['changePercent24Hr'],
        'vwap24Hr': data['vwap24Hr'],
        'timestamp': datetime.now()
    }])


def get_crypto_data():
    """
    Consolida dados de múltiplas criptomoedas.
    
    Returns:
        DataFrame combinado com todas as criptomoedas ou None se todas falharem
    """
    all_dataframes = []
    
    for crypto in CRYPTO_IDS:
        df = fetch_crypto_data(crypto)
        if df is not None:
            all_dataframes.append(df)
            # Adiciona um pequeno atraso entre requisições para evitar limites de taxa
            time.sleep(0.5)
    
    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else None


def main():
    """Executa a funcionalidade principal do script"""
    df = get_crypto_data()
    
    if df is not None:
        print("Dados recuperados com sucesso!")
        print("\nPrevisualização dos dados:")
        print(df.head())
        
        print("\nEstatísticas do DataFrame:")
        print(df.describe())
        return df
        
    print("Falha ao recuperar os dados")
    return None


if __name__ == "__main__":
    main()