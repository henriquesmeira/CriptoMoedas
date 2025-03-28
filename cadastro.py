import requests
import pandas as pd

def get_top_cryptocurrencies(limit=5):
    """Recupera as principais criptomoedas em termos de capitalização de mercado."""
    try:
        # Configuração da requisição
        url = "https://api.coincap.io/v2/assets"
        params = {
            "limit": limit,
            "sort": "marketCap"
        }
        
        # Realizar requisição
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # Processar dados
        data = response.json().get('data', [])
        
        # Criar DataFrame
        df = pd.DataFrame(data)
        
        # Formatar colunas
        df['Rank'] = df['rank']
        df['Nome'] = df['name']
        df['Símbolo'] = df['symbol']
        df['Preço (USD)'] = df['priceUsd'].apply(lambda x: f"${float(x):,.2f}")
        df['Cap. Mercado (USD)'] = df['marketCapUsd'].apply(lambda x: f"${float(x):,.0f}")
        df['Variação 24h (%)'] = df['changePercent24Hr'].apply(lambda x: f"{float(x):,.2f}%")
        
        # Selecionar colunas formatadas
        return df[[
            'Rank', 
            'Nome', 
            'Símbolo', 
            'Preço (USD)', 
            'Cap. Mercado (USD)', 
            'Variação 24h (%)'
        ]]
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro na requisição: {e}")
        return None
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return None

def main():
    """Função principal para execução do script"""
    df = get_top_cryptocurrencies()
    
    if df is not None:
        print("🚀 Top Criptomoedas:\n")
        print(df)
        return df
    
    print("❌ Não foi possível recuperar os dados")
    return None

if __name__ == "__main__":
    main()