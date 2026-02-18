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
        DATA_NOTA_FISCAL,
        DATENAME(WEEKDAY, DATA_NOTA_FISCAL) AS DIA_SEMANA,
        NUM_NOTA_FISCAL,
        QUANTIDADE,
        PESO_BIN,
        VALOR
    FROM 
        VW_AUDIT_RM_ORDENS_VENDA
    WHERE 
        COD_ESTABELECIMENTO = 'R371'
        AND DATA_NOTA_FISCAL BETWEEN '2025-08-01' AND '2026-01-31'
        AND CFOP IN ('5.102', '5.104', '5.106', '5.114', '5.403', '5.405', '5.655', '6.102', '6.108', '6.403', '6.404')
        AND NUM_NOTA_FISCAL NOT LIKE '%EST%'
        AND (
        DATEPART(WEEKDAY, DATA_NOTA_FISCAL) IN (1, 7)  -- Final de semana
        OR DATA_NOTA_FISCAL = EOMONTH(DATA_NOTA_FISCAL)  -- Último dia do mês
        )
    """
    
    # Executar a consulta diretamente com pandas para facilitar
    df = pd.read_sql_query(query, conexao)
    
    # Fechar a conexão
    conexao.close()
    
    # Verificar se há dados
    if len(df) > 0:
        # Definir o caminho para salvar o arquivo
        caminho_base = r'C:\Users\matheus.melo\OneDrive - Acumuladores Moura SA\Documentos\Drive - Matheus Melo\Auditoria\2026\04. Circularização\Validações\Catarinense - R371\Seleção'
        
        # Criar o diretório se não existir
        os.makedirs(caminho_base, exist_ok=True)
        
        # Nome do arquivo fixo como solicitado
        nome_arquivo = 'Vendas_Atípicas.xlsx'
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