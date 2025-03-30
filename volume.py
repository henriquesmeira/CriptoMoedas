import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time

# Configuração rápida
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")
CRYPTO_IDS = ["bitcoin", "ethereum", "xrp", "binance-coin"]

MAX_RETRIES = 5  # Número máximo de tentativas em caso de erro 429
BASE_WAIT_TIME = 2  # Tempo base de espera (segundos)

def fetch_crypto_data(crypto_id):
    """Recupera dados de uma criptomoeda a partir do novo endpoint (dados atuais), com tratamento de rate limit."""
    url = f"https://api.coincap.io/v2/assets/{crypto_id}"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 429:
                wait_time = BASE_WAIT_TIME * (2 ** retries)  # Exponential backoff
                print(f"⚠️ Rate limit atingido. Aguardando {wait_time} segundos...")
                time.sleep(wait_time)
                retries += 1
                continue
            
            response.raise_for_status()
            data = response.json()["data"]
            
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
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao buscar dados de {crypto_id}: {e}")
            return None
    
    print(f"❌ Falha após {MAX_RETRIES} tentativas ao buscar {crypto_id}")
    return None

def get_crypto_data():
    """Consolida dados de várias criptomoedas."""
    dataframes = [fetch_crypto_data(crypto) for crypto in CRYPTO_IDS if fetch_crypto_data(crypto) is not None]
    return pd.concat(dataframes, ignore_index=True) if dataframes else None

def main():
    df = get_crypto_data()
    if df is not None:
        print("✅ Dados recuperados com sucesso!")
        print("\n🔍 Prévia dos dados:")
        print(df.head())
        print("\n📊 Estatísticas do DataFrame:")
        print(df.describe())
        return df
    print("❌ Falha na recuperação dos dados")
    return None

if __name__ == "__main__":
    main()
