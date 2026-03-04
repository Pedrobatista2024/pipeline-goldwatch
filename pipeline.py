import requests
import sqlite3
from datetime import datetime

# --- CONFIGURAÇÕES ---
DB_NAME = "commodities.db"
API_URL = "https://economia.awesomeapi.com.br/last/USD-BRL,XAU-BRL"

def extrair_dados():
    """Etapa 1: EXTRACT - Busca os dados na API"""
    print("Iniciando extração...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Garante que o script para se a API der erro
        return response.json()
    except Exception as e:
        print(f"Erro na extração: {e}")
        return None

def transformar_dados(dados_brutos):
    """Etapa 2: TRANSFORM - Limpa e organiza os dados"""
    print("Iniciando transformação...")
    if not dados_brutos:
        return None
    
    # Extraindo campos específicos do JSON da API
    cotacao_ouro = float(dados_brutos['XAUBRL']['bid'])
    cotacao_dolar = float(dados_brutos['USDBRL']['bid'])
    data_consulta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Criando um dicionário limpo
    dados_limpos = {
        "data": data_consulta,
        "ouro": cotacao_ouro,
        "dolar": cotacao_dolar
    }
    return dados_limpos

def carregar_dados(dados_limpos):
    """Etapa 3: LOAD - Salva no Banco de Dados SQLite"""
    print("Iniciando carga no banco...")
    if not dados_limpos:
        return
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Cria a tabela se ela não existir
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historico_precos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            preco_ouro REAL,
            preco_dolar REAL
        )
    ''')
    
    # Insere os dados
    cursor.execute('''
        INSERT INTO historico_precos (data, preco_ouro, preco_dolar)
        VALUES (?, ?, ?)
    ''', (dados_limpos['data'], dados_limpos['ouro'], dados_limpos['dolar']))
    
    conn.commit()
    conn.close()
    print("Pipeline finalizada com sucesso!")

# --- EXECUÇÃO MANUAL ---
if __name__ == "__main__":
    dados = extrair_dados()
    processados = transformar_dados(dados)
    carregar_dados(processados)