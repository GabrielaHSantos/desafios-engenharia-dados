# Importação de Bibliotecas
import os
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta

def obter_tickers_maior_market_cap(tickers_candidatos, num_top):
    """
    Identifica os tickers das N empresas com maior valor de mercado.

    Args:
        tickers_candidatos (list): Lista de strings com os tickers a serem avaliados.
        num_top (int): O número de tickers a serem retornados.

    Returns:
        list: Lista contendo as strings dos N tickers com maior valor de mercado.
    """
    print(f"Buscando valor de mercado para definir os Top {num_top} tickers...")
    valores_mercado = []
    
    for ticker_str in tickers_candidatos:
        try:
            ticker_obj = yf.Ticker(ticker_str)
            valor_mercado = ticker_obj.info.get('marketCap')
            if valor_mercado:
                valores_mercado.append((ticker_str, valor_mercado))
            else:
                print(f"  - Não foi possível obter o valor de mercado para {ticker_str}")
        except Exception as e:
            print(f"  - Erro ao buscar informações de {ticker_str}: {e}")
            
    valores_mercado.sort(key=lambda x: x[1], reverse=True)
    top_tickers_lista = [ticker for ticker, cap in valores_mercado[:num_top]]
    
    print(f"\nTop {num_top} tickers selecionados: {top_tickers_lista}")
    return top_tickers_lista


def extrair_e_enriquecer_dados(lista_tickers):
    """
    Extrai dados históricos e os enriquece com metadados, processando um ticker por vez.

    Args:
        lista_tickers (list): A lista dos tickers selecionados para extração.

    Returns:
        pd.DataFrame or None: DataFrame com dados combinados ou None se a extração falhar.
    """
    print("\nIniciando ETAPA DE EXTRAÇÃO E ENRIQUECIMENTO...")
    data_final = datetime.now()
    data_inicial = data_final - relativedelta(years=1)
    
    todos_dados = []
    
    for ticker_str in lista_tickers:
        try:
            print(f"+ Processando {ticker_str}...")
            ticker_obj = yf.Ticker(ticker_str)
            
            # Baixa os dados históricos para este ticker
            dados_historicos = ticker_obj.history(start=data_inicial, end=data_final, auto_adjust=True)
            
            if dados_historicos.empty:
                print(f"  - Nenhum dado histórico encontrado para {ticker_str}.")
                continue # Pula para o próximo ticker

            # Enriquece os dados
            info = ticker_obj.info
            dados_historicos['ticker'] = ticker_str
            dados_historicos['nome_empresa'] = info.get('longName', '')
            dados_historicos['setor'] = info.get('sector', '')
            dados_historicos['industria'] = info.get('industry', '')
            
            todos_dados.append(dados_historicos)

        except Exception as e:
            print(f"  - ERRO ao processar o ticker {ticker_str}: {e}")

    if not todos_dados:
        print("\nNenhum dado foi extraído com sucesso.")
        return None

    # Concatena todos os dataframes em um só ao final do loop
    df_final = pd.concat(todos_dados).reset_index()
    print("\n+ Extração e enriquecimento concluídos.")
    return df_final


def transformar_dataframe_final(df):
    """
    Transforma e padroniza o DataFrame de dados brutos.
    """
    print("\nIniciando ETAPA DE TRANSFORMAÇÃO...")
    
    df.columns = [col.lower() for col in df.columns]

    mapa_nomes = {
        'date': 'data', 'ticker': 'codigo_acao', 'open': 'preco_abertura',
        'high': 'preco_maximo', 'low': 'preco_minimo',
        'close': 'preco_fechamento', 'volume': 'volume_negociado'
    }
    df.rename(columns=mapa_nomes, inplace=True)

    colunas_finais = [
        'data', 'codigo_acao', 'nome_empresa', 'setor', 'industria',
        'preco_abertura', 'preco_maximo', 'preco_minimo', 'preco_fechamento', 'volume_negociado'
    ]
    
    colunas_existentes = [col for col in colunas_finais if col in df.columns]
    df = df[colunas_existentes].copy()

    colunas_preco = ['preco_abertura', 'preco_maximo', 'preco_minimo', 'preco_fechamento']
    for col in colunas_preco:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    if 'data' in df.columns:
        df['data'] = pd.to_datetime(df['data']).dt.strftime('%Y-%m-%d')

    print("Dados transformados com sucesso!")
    return df


def carregar_para_gsheets(df, nome_planilha, id_spreadsheet, arq_credenciais):
    """
    Carrega o DataFrame final em uma planilha do Google Sheets.
    """
    print("\nIniciando ETAPA DE CARREGAMENTO...")
    try:
        creds = Credentials.from_service_account_file(arq_credenciais, scopes=[
            "https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"
        ])
        client = gspread.authorize(creds)
        planilha = client.open_by_key(id_spreadsheet)

        try:
            aba = planilha.worksheet(nome_planilha)
        except gspread.exceptions.WorksheetNotFound:
            aba = planilha.add_worksheet(title=nome_planilha, rows="1000", cols="20")

        aba.clear()
        df_limpo = df.fillna('').astype(str)
        
        aba.update(
            range_name='A1',
            values=[df_limpo.columns.values.tolist()] + df_limpo.values.tolist()
        )

        id_aba = aba.id
        print(f"Dados carregados! Acesse: https://docs.google.com/spreadsheets/d/{id_spreadsheet}/edit#gid={id_aba}")
        return True

    except Exception as e:
        print(f"ERRO durante o carregamento para o Google Sheets: {e}")
        return False


# --- Bloco de Execução Principal ---
def main():
    """Função principal que orquestra todo o pipeline ETL."""
    # 1. CONFIGURAÇÃO
    ID_SPREADSHEET = "157VewSYyGWUZqifKKU0Zt2cxwS7r2wtjaib2znw9_Os" 
    ARQUIVO_CREDENCIAS = "credentials.json"
    NOME_PLANILHA = "Dados de Ações - Desafio"
    TICKERS_CANDIDATOS = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
        'JPM', 'JNJ', 'V', 'UNH', 'LLY', 'XOM', 'WMT', 'PG', 'MA', 'HD',
        'CVX', 'AVGO'
    ]

    if not os.path.exists(ARQUIVO_CREDENCIAS):
        print(f"ERRO: Arquivo de credenciais '{ARQUIVO_CREDENCIAS}' não encontrado.")
        return

    # 2. EXECUÇÃO DO PIPELINE
    top_tickers = obter_tickers_maior_market_cap(TICKERS_CANDIDATOS, num_top=10)
    if not top_tickers:
        print("\nPipeline interrompido: não foi possível definir os top tickers.")
        return

    dados_acoes_enriquecidos = extrair_e_enriquecer_dados(top_tickers)
    if dados_acoes_enriquecidos is None or dados_acoes_enriquecidos.empty:
        print("\nPipeline ETL falhou na etapa de extração.")
        return

    dados_acoes_limpos = transformar_dataframe_final(dados_acoes_enriquecidos)
    if dados_acoes_limpos is None:
        print("\nPipeline ETL falhou na etapa de transformação.")
        return

    sucesso = carregar_para_gsheets(dados_acoes_limpos, NOME_PLANILHA, ID_SPREADSHEET, ARQUIVO_CREDENCIAS)
    if sucesso:
        print("\nPipeline ETL concluído com sucesso!")
    else:
        print("\nPipeline ETL falhou na etapa de carregamento.")


if __name__ == "__main__":
    main()