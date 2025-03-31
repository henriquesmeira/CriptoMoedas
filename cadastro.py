import requests
import pandas as pd
import time
import random


def get_top_cryptocurrencies(limit=6, max_retries=3, base_delay=2):
    """
    Recupera as principais criptomoedas com base na capitalização de mercado.
    
    Args:
        limit: Número de criptomoedas a serem retornadas
        max_retries: Número máximo de tentativas de retry
        base_delay: Tempo base de espera (em segundos) entre tentativas
    
    Returns:
        DataFrame com dados formatados de criptomoedas ou None se falhar
    """
    url = "https://api.coincap.io/v2/assets"
    params = {
        "limit": limit,
        "sort": "marketCapUsd"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                print(f"Limite de requisições atingido. Aguardando {retry_after} segundos...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            
            data = response.json().get('data', [])
            return format_cryptocurrency_data(data)
            
        except requests.exceptions.RequestException as e:
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            print(f"Erro na requisição (tentativa {attempt+1}/{max_retries}): {e}")
            print(f"Aguardando {delay:.2f} segundos antes da próxima tentativa...")
            time.sleep(delay)
        except Exception as e:
            print(f"Erro inesperado: {e}")
            return None
    
    print(f"Falha após {max_retries} tentativas")
    return None


def format_cryptocurrency_data(data):
    """
    Formata os dados da API de criptomoedas em um DataFrame legível.
    
    Args:
        data: Dados brutos de criptomoedas da API
    
    Returns:
        DataFrame formatado
    """
    df = pd.DataFrame(data)
    
    df['Rank'] = df['rank']
    df['Nome'] = df['name']
    df['Símbolo'] = df['symbol']
    df['Preço (USD)'] = df['priceUsd'].apply(lambda x: f"${float(x):,.2f}")
    df['Cap. Mercado (USD)'] = df['marketCapUsd'].apply(lambda x: f"${float(x):,.0f}")
    df['Variação 24h (%)'] = df['changePercent24Hr'].apply(lambda x: f"{float(x):,.2f}%")
    
    return df[[
        'Rank', 
        'Nome', 
        'Símbolo', 
        'Preço (USD)', 
        'Cap. Mercado (USD)', 
        'Variação 24h (%)'
    ]]


def main():
    """Executa a funcionalidade principal do script"""
    df = get_top_cryptocurrencies()
    
    if df is not None:
        print("Top Criptomoedas:\n")
        print(df)
        return df
    
    print("Não foi possível recuperar os dados")
    return None


if __name__ == "__main__":
    main()