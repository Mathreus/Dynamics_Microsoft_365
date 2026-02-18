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
        CASE
    	    WHEN COD_ESTABELECIMENTO IN ('R261', 'R221', 'R222', 'R541', 'R591', 'R281', 'R282', 'R282', 'R283', 'R611', 'R121', 'R831', 'R351', 'R352', 'R461', 'R521') THEN 'AVANÇAR'
		    WHEN COD_ESTABELECIMENTO IN ('R201', 'R311', 'R312', 'R313', 'R191', 'R781', 'R301') THEN 'BASE'
		    WHEN COD_ESTABELECIMENTO IN ('R031', 'R041', 'R291', 'R292', 'R641', 'R791', 'R801') THEN 'CRESCER'
		    WHEN COD_ESTABELECIMENTO IN ('R651', 'R671', 'R681', 'R021', 'R181', 'R691', 'R131', 'R721', 'R751') THEN 'FORTALEZA'
		    WHEN COD_ESTABELECIMENTO IN ('R211', 'R341', 'R451', 'R481', 'R711', 'R231', 'R234', 'R471', 'R472', 'R061', 'R531') THEN 'PLANALTO'
		    WHEN COD_ESTABELECIMENTO IN ('R071', 'R074', 'R382', 'R501', 'R502', 'R661', 'R701', 'R491', 'R492', 'R241', 'R243', 'R621', 'R761', 'R371', 'R731', 'R821') THEN 'SUL'
		    WHEN COD_ESTABELECIMENTO IN ('R011', 'R511', 'R101', 'R811', 'R051', 'R052', 'R161') THEN 'VISÃO'
    	    ELSE 'CADASTRAR'
	    END AS GRUPO,
        COD_CLIENTE,
        NOME_CLIENTE,
        DATA_TRANSACAO,
        DATA_VENCIMENTO,
        PERFIL_LANCAMENTO,
        COMPROVANTE,
        TEXTO,
        VALOR_PARCELA,
        VALOR_TITULO
    FROM 
        VW_AUDIT_RM_TRANSACOES_FECHADAS
    WHERE 
        COD_ESTABELECIMENTO = 'R121'
        AND DATA_TRANSACAO BETWEEN '2025-07-01' AND '2025-12-31'
        AND (
            -- Condição 1: Se contém ENC_CTA, perfil deve ser TCC ou DNI
            (COMPROVANTE LIKE '%ENC_CTA%' AND PERFIL_LANCAMENTO IN ('TCC', 'DNI'))
            OR
            -- Condição 2: Se contém LAN, qualquer perfil é aceito
            COMPROVANTE LIKE '%LAN%'
        );
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
        nome_arquivo = 'Lançamento_Manual.xlsx'
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
