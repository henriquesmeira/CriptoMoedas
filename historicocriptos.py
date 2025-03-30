import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Configuração rápida
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")
CRYPTO_IDS = ["bitcoin", "ethereum", "xrp", "binance-coin"]

def fetch_crypto_history(crypto_id):
    """Recupera dados históricos de criptomoedas de forma eficiente."""
    url = f"https://api.coincap.io/v2/assets/{crypto_id}/history"
    
    params = {
        "interval": "d1",
        "start": int((datetime.now() - timedelta(days=730)).timestamp() * 1000),
        "end": int(datetime.now().timestamp() * 1000)
    }
    
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json()["data"])
        df['date'] = pd.to_datetime(df['time'], unit='ms')
        df['crypto'] = crypto_id
        
        return df
    
    except Exception as e:
        print(f"❌ Erro ao buscar dados de {crypto_id}: {e}")
        return None

def get_crypto_historical_data():
    """Consolida dados históricos de múltiplas criptomoedas."""
    dataframes = [
        fetch_crypto_history(crypto) 
        for crypto in CRYPTO_IDS 
        if fetch_crypto_history(crypto) is not None
    ]
    
    return pd.concat(dataframes, ignore_index=True) if dataframes else None

def main():
    df = get_crypto_historical_data()
    
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