# Bibliotecas base de conexão:
import pyodbc
import pandas as pd
from datetime import datetime
import os

# Defina as informações de conexão
server = 'DCMDWF01A.MOURA.INT'
database = 'ax'
username = 'uAuditoria'
password = '@ud!t0$!@202&22'
driver = 'SQL Server'  # Driver específico para o banco de dados que você está usando

# Construa a string de conexão
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Execute a consulta e salve em Excel
try:
    # Conecte-se ao banco de dados
    conexao = pyodbc.connect(connection_string)
    
    # Consulta SQL
    query = """
    SELECT  
        COD_ESTABELECIMENTO,
        COD_CLIENTE,
        NOME_CLIENTE,
        DATA_TRANSACAO,
        DATA_VENCIMENTO,
        DATA_LIQUIDACAO,
        DATEDIFF(DAY, DATA_VENCIMENTO, DATA_LIQUIDACAO) AS DIAS,
        'SIM' AS LIQUIDACAO_ANTES_VENCIMENTO, 
        PERFIL_LANC,
        METODO_PAGAMENTO,
        COMPROVANTE,
        NUM_NOTA_FISCAL,
        PARCELA,
        VALOR_MOEDA,
        VALOR_ORIGINAL,
        STATUS,
        RECID_TRANSACAO,
        TEXTO
    FROM    
        VW_AUDIT_RM_TRANSACOES_CLIENTES
    WHERE   
        COD_ESTABELECIMENTO = 'R121'
        AND DATA_TRANSACAO BETWEEN '2025-07-07' AND '2026-01-07'
        AND PERFIL_LANC = 'DPC'
        AND DATA_LIQUIDACAO <> '1900-01-01'
        AND DATA_LIQUIDACAO < DATA_VENCIMENTO  -- Filtra apenas antecipadas
    """
    
    # Executar a consulta diretamente com pandas para facilitar
    df = pd.read_sql_query(query, conexao)
    
    # Fechar a conexão
    conexao.close()
    
    # Verificar se há dados
    if len(df) > 0:
        # Definir o caminho para salvar o arquivo
        caminho_base = r'C:\Users\matheus.melo\OneDrive - Acumuladores Moura SA\Documentos\Drive - Matheus Melo\Auditoria\2026\04. Circularização\Validações\Fluminense - R121\Python'
        
        # Criar o diretório se não existir
        os.makedirs(caminho_base, exist_ok=True)
        
        # Nome do arquivo fixo como solicitado
        nome_arquivo = 'Pagamento_Antecipado.xlsx'
        caminho_completo = os.path.join(caminho_base, nome_arquivo)
        
        # Salvar em Excel
        df.to_excel(caminho_completo, index=False, engine='openpyxl')
        
        print(f"✅ Arquivo salvo com sucesso!")
        print(f"📊 Total de registros: {len(df)}")
        print(f"📂 Caminho: {caminho_completo}")
        
        # Mostrar prévia dos dados
        print("\n📋 Prévia dos dados:")
        print(df.head())
        
    else:
        print("⚠️  Nenhum dado encontrado com os critérios especificados.")
        
except pyodbc.Error as e:
    print(f"❌ Erro na conexão ou consulta: {e}")
except Exception as e:
    print(f"❌ Erro inesperado: {e}")
