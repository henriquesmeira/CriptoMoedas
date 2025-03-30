import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Configuração rápida
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")
CRYPTO_IDS = ["bitcoin", "ethereum", "xrp", "binance-coin"]

def fetch_crypto_data(crypto_id):
    """Recupera dados de uma criptomoeda a partir do novo endpoint (dados atuais)."""
    url = f"https://api.coincap.io/v2/assets/{crypto_id}"
    
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()["data"]
        
        # Criando o DataFrame com as informações recebidas
        df = pd.DataFrame([{
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
        
        return df
    
    except Exception as e:
        print(f"❌ Erro ao buscar dados de {crypto_id}: {e}")
        return None

def get_crypto_data():
    """Consolida dados de várias criptomoedas (usando o novo endpoint)."""
    dataframes = [
        fetch_crypto_data(crypto) 
        for crypto in CRYPTO_IDS 
        if fetch_crypto_data(crypto) is not None
    ]
    
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