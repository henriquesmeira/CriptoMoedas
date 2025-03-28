import os
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Carrega as variáveis do arquivo .env
load_dotenv()
API_KEY = os.getenv("COINCAP_API_KEY")

# Lista de criptomoedas a serem analisadas (somente Bitcoin e Ethereum)
CRYPTO_IDS = ["bitcoin", "ethereum"]

def fetch_crypto_history(crypto_id):
    """Recupera dados históricos de criptomoedas de forma eficiente."""
    url = f"https://api.coincap.io/v2/assets/{crypto_id}/history"
    
    params = {
        "interval": "d1",  # intervalo diário (1 dia)
        "start": int((datetime.now() - timedelta(days=730)).timestamp() * 1000),  # Início do intervalo de 2 anos
        "end": int(datetime.now().timestamp() * 1000)  # Fim do intervalo (data atual)
    }
    
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        # Faz a requisição para pegar o histórico da criptomoeda
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        # Converte os dados para DataFrame
        df = pd.DataFrame(response.json()["data"])
        df['date'] = pd.to_datetime(df['time'], unit='ms')  # Converte o timestamp para data legível
        df['crypto'] = crypto_id  # Adiciona uma coluna para identificar a criptomoeda
        
        return df
    
    except Exception as e:
        print(f"❌ Erro ao buscar dados de {crypto_id}: {e}")
        return None

def get_crypto_historical_data():
    """Consolida dados históricos de múltiplas criptomoedas (somente Bitcoin e Ethereum)."""
    dataframes = [
        fetch_crypto_history(crypto) 
        for crypto in CRYPTO_IDS 
        if fetch_crypto_history(crypto) is not None
    ]
    
    # Concatena os dataframes de todas as criptomoedas em um único DataFrame
    return pd.concat(dataframes, ignore_index=True) if dataframes else None

def main():
    df = get_crypto_historical_data()
    
    if df is not None:
        print("✅ Dados recuperados com sucesso!")
        print("\n🔍 Prévia dos dados:")
        print(df.head())  # Exibe as primeiras linhas do DataFrame
        
        print("\n📊 Estatísticas do DataFrame:")
        print(df.describe())  # Exibe as estatísticas gerais do DataFrame
        
        return df
    
    print("❌ Falha na recuperação dos dados")
    return None

if __name__ == "__main__":
    main()
