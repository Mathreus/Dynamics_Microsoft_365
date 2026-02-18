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

    DECLARE @DATA_BASE DATE = '2026-01-31'

    SELECT
        COD_ESTABELECIMENTO,
        COD_CLIENTE,
        NOME_CLIENTE,  
        DATA_TRANSACAO,
        DATA_VENCIMENTO,
        DATA_LIQUIDACAO,
        GETDATE() AS DATA_HOJE,
        DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) AS DIAS_ATRASO,  
        CASE    
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) < 0 THEN 'A Vencer'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 0 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 30 THEN 'Vencidos 0 a 30 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 30 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 60 THEN 'Vencidos 31 a 60 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 60 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 90 THEN 'Vencidos 61 a 90 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 90 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 120 THEN 'Vencidos 91 a 120 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 120 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 180 THEN 'Vencidos 121 a 180 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 180 
                AND DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) <= 360 THEN 'Vencidos de 181 a 360 dias'
            WHEN DATEDIFF(DAY, DATA_VENCIMENTO, @DATA_BASE) > 360 THEN 'Vencidos a mais de 360 dias'
            ELSE 'No Vencimento'
        END AS AGING_BIN,
        PESO_BIN_ABERTO
    FROM    
        VW_AUDIT_RM_ABERTO_BIN
    WHERE   
        COD_ESTABELECIMENTO = 'R371'
        AND PESO_BIN_ABERTO IS NOT NULL

        """
    
    # Executar a consulta diretamente com pandas para facilitar
    df = pd.read_sql_query(query, conexao)
    
    # Fechar a conexão
    conexao.close()
    
    # Verificar se há dados
    if len(df) > 0:
        # Definir o caminho para salvar o arquivo
        caminho_base = r'C:\Users\matheus.melo\OneDrive - Acumuladores Moura SA\Documentos\Drive - Matheus Melo\Auditoria\2026\04. Circularização\Validações\Catarinense - R371'
        
        # Criar o diretório se não existir
        os.makedirs(caminho_base, exist_ok=True)
        
        # Nome do arquivo fixo como solicitado
        nome_arquivo = 'Aging_BIN.xlsx'
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
