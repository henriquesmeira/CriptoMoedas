import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# Configuração
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")
CRYPTO_IDS = ["bitcoin", "ethereum", "xrp", "binance-coin"]
MAX_RETRIES = 3
BASE_DELAY = 2


def fetch_crypto_history(crypto_id):
    """
    Recupera dados históricos de criptomoedas.
    
    Args:
        crypto_id: ID da criptomoeda a ser buscada
        
    Returns:
        DataFrame com dados históricos ou None se falhar
    """
    url = f"https://api.coincap.io/v2/assets/{crypto_id}/history"
    
    params = {
        "interval": "d1",
        "start": int((datetime.now() - timedelta(days=730)).timestamp() * 1000),
        "end": int(datetime.now().timestamp() * 1000)
    }
    
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, headers=headers)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', BASE_DELAY * (2 ** attempt)))
                print(f"Limite de requisições atingido. Aguardando {retry_after} segundos...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            
            df = pd.DataFrame(response.json()["data"])
            df['date'] = pd.to_datetime(df['time'], unit='ms')
            df['crypto'] = crypto_id
            
            return df
        
        except requests.exceptions.RequestException as e:
            delay = BASE_DELAY * (2 ** attempt)
            print(f"Erro ao buscar dados para {crypto_id}: {e}")
            print(f"Aguardando {delay} segundos antes da próxima tentativa...")
            time.sleep(delay)
            
        except Exception as e:
            print(f"Erro inesperado para {crypto_id}: {e}")
            return None
    
    print(f"Falha após {MAX_RETRIES} tentativas para {crypto_id}")
    return None


def get_crypto_historical_data():
    """
    Consolida dados históricos de múltiplas criptomoedas.
    
    Returns:
        DataFrame combinado com todas as criptomoedas ou None se todas falharem
    """
    all_dataframes = []
    
    for crypto in CRYPTO_IDS:
        df = fetch_crypto_history(crypto)
        if df is not None:
            all_dataframes.append(df)
    
    return pd.concat(all_dataframes, ignore_index=True) if all_dataframes else None


def main():
    """Executa a funcionalidade principal do script"""
    df = get_crypto_historical_data()
    
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