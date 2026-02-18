import pandas as pd
import pyodbc
import os
from datetime import datetime
from warnings import filterwarnings

# Filtrar o warning do pandas
filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Configurações de conexão com o banco de dados
server = '' -- Inserir o servidor  
database = '' -- Inserir o Banco de Dados   
username = '' -- Inserir o usuário 
password = '' -- Inserir a senha 

# Definir o caminho de salvamento
caminho_salvamento = r'C:\Users\matheus.melo\OneDrive - Acumuladores Moura SA\Documentos\Drive - Matheus Melo\Auditoria\2026\04. Circularização\Validações\Fluminense - R121'
nome_arquivo = f'analise_bonificacoes_7grupos_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
caminho_completo = os.path.join(caminho_salvamento, nome_arquivo)

# Definição dos grupos
grupos = {
    'AVANÇAR': ['R261', 'R221', 'R222', 'R541', 'R591', 'R281', 'R282', 'R283', 
                'R611', 'R121', 'R831', 'R351', 'R352', 'R461', 'R521'],
    
    'BASE': ['R201', 'R311', 'R312', 'R313', 'R191', 'R781', 'R301', 'R841'],
    
    'CRESCER': ['R031', 'R041', 'R091', 'R111', 'R151', 'R291', 'R292', 'R641', 
                'R791', 'R801', 'R551', 'R561', 'R571', 'R581', 'R601', 'R631', 
                'R741', 'R771'],
    
    'FORTALEZA': ['R651', 'R671', 'R681', 'R021', 'R181', 'R691', 'R131', 'R141', 
                  'R721', 'R751'],
    
    'PLANALTO': ['R211', 'R341', 'R451', 'R481', 'R711', 'R231', 'R234', 'R471', 
                 'R472', 'R061', 'R531'],
    
    'SUL': ['R071', 'R074', 'R382', 'R501', 'R502', 'R661', 'R701', 'R491', 
            'R492', 'R241', 'R243', 'R621', 'R761', 'R371', 'R373', 'R731', 'R821'],
    
    'VISÃO': ['R011', 'R511', 'R101', 'R811', 'R051', 'R052', 'R161']
}

# Criar lista completa de todos os estabelecimentos
todos_estabelecimentos = []
for estabelecimentos in grupos.values():
    todos_estabelecimentos.extend(estabelecimentos)

# Remover duplicatas (se houver)
todos_estabelecimentos = list(set(todos_estabelecimentos))

def conectar_banco():
    """Estabelece conexão com o banco de dados"""
    try:
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        conexao = pyodbc.connect(conn_str)
        print("Conexão estabelecida com sucesso!")
        return conexao
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None

def executar_query(conn, query):
    """Executa uma query e retorna DataFrame"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        return df
    except Exception as e:
        print(f"Erro ao executar query: {e}")
        return pd.DataFrame()

def gerar_case_grupos():
    """Gera a expressão CASE para todos os grupos"""
    case_parts = []
    
    # Adicionar cada grupo
    for grupo_nome, estabelecimentos in grupos.items():
        estabelecimentos_str = ','.join([f"'{e}'" for e in estabelecimentos])
        case_parts.append(f"WHEN COD_ESTABELECIMENTO IN ({estabelecimentos_str}) THEN '{grupo_nome}'")
    
    # Adicionar ELSE para qualquer outro estabelecimento não listado
    case_parts.append("ELSE 'OUTROS'")
    
    return '\n        '.join(case_parts)

def executar_query_bonificacoes_grupo(conn):
    """Executa a query de bonificações por grupo"""
    case_expression = gerar_case_grupos()
    
    query_bonificacoes = f"""
    SELECT
        CASE 
            WHEN COD_ESTABELECIMENTO IN ('R261', 'R221', 'R222', 'R541', 'R591', 'R281', 'R282', 'R283', 'R611', 'R121', 'R831', 'R351', 'R352', 'R461', 'R521', 'R831') THEN 'AVANÇAR'
            WHEN COD_ESTABELECIMENTO IN ('R201', 'R311', 'R312', 'R313', 'R191', 'R781', 'R301', 'R841') THEN 'BASE'
            WHEN COD_ESTABELECIMENTO IN ('R031', 'R041', 'R091', 'R111', 'R151', 'R291', 'R292', 'R641', 'R791', 'R801', 'R551', 'R561', 'R571', 'R581', 'R601', 'R631', 'R741',  'R771') THEN 'CRESCER'
            WHEN COD_ESTABELECIMENTO IN ('R651', 'R671', 'R681', 'R021', 'R181', 'R691', 'R131', 'R141', 'R721', 'R751') THEN 'FORTALEZA'
            WHEN COD_ESTABELECIMENTO IN ('R211', 'R341', 'R451', 'R481', 'R711', 'R231', 'R234', 'R471', 'R472', 'R061', 'R531') THEN 'PLANALTO'
            WHEN COD_ESTABELECIMENTO IN ('R071', 'R074', 'R382', 'R501', 'R502', 'R661', 'R701', 'R491', 'R492', 'R241', 'R243', 'R621', 'R761', 'R371', 'R373', 'R731', 'R821') THEN 'SUL'
            WHEN COD_ESTABELECIMENTO IN ('R011', 'R511', 'R101', 'R811', 'R051', 'R052', 'R161') THEN 'VISÃO'
            ELSE 'CLASSIFICAR'
        END AS GRUPO_RM,
        SUM(QUANTIDADE) AS QUANTIDADE_BONIFICACAO,
        SUM(VALOR) AS VALOR_BONIFICADO
    FROM 
        VW_AUDIT_RM_ORDENS_VENDA
    WHERE
        DATA_NOTA_FISCAL BETWEEN '2025-07-01' AND '2025-12-31'
        AND PARA_FATURAMENTO = 'SIM'
        AND CFOP IN ('1.910', '2.910', '5.910', '6.910')
    GROUP BY
        CASE 
            WHEN COD_ESTABELECIMENTO IN ('R261', 'R221', 'R222', 'R541', 'R591', 'R281', 'R282', 'R283', 'R611', 'R121', 'R831', 'R351', 'R352', 'R461', 'R521', 'R831') THEN 'AVANÇAR'
            WHEN COD_ESTABELECIMENTO IN ('R201', 'R311', 'R312', 'R313', 'R191', 'R781', 'R301', 'R841') THEN 'BASE'
            WHEN COD_ESTABELECIMENTO IN ('R031', 'R041', 'R091', 'R111', 'R151', 'R291', 'R292', 'R641', 'R791', 'R801', 'R551', 'R561', 'R571', 'R581', 'R601', 'R631', 'R741',  'R771') THEN 'CRESCER'
            WHEN COD_ESTABELECIMENTO IN ('R651', 'R671', 'R681', 'R021', 'R181', 'R691', 'R131', 'R141', 'R721', 'R751') THEN 'FORTALEZA'
            WHEN COD_ESTABELECIMENTO IN ('R211', 'R341', 'R451', 'R481', 'R711', 'R231', 'R234', 'R471', 'R472', 'R061', 'R531') THEN 'PLANALTO'
            WHEN COD_ESTABELECIMENTO IN ('R071', 'R074', 'R382', 'R501', 'R502', 'R661', 'R701', 'R491', 'R492', 'R241', 'R243', 'R621', 'R761', 'R371', 'R373', 'R731', 'R821') THEN 'SUL'
            WHEN COD_ESTABELECIMENTO IN ('R011', 'R511', 'R101', 'R811', 'R051', 'R052', 'R161') THEN 'VISÃO'
            ELSE 'CLASSIFICAR'    
        END
    """
    
    try:
        df_bonificacoes = executar_query(conn, query_bonificacoes)
        print(f"Query de bonificações executada: {len(df_bonificacoes)} grupos encontrados")
        return df_bonificacoes
    except Exception as e:
        print(f"Erro ao executar query de bonificações: {e}")
        return pd.DataFrame()

def executar_query_vendas_grupo(conn):
    """Executa a query de vendas por grupo"""
    case_expression = gerar_case_grupos()
    
    query_vendas = f"""
    SELECT
        CASE
            {case_expression}
        END AS GRUPO_RM,
        SUM(QUANTIDADE) AS QUANTIDADE_VENDAS,
        SUM(VALOR) AS VALOR_VENDAS
    FROM 
        VW_AUDIT_RM_ORDENS_VENDA
    WHERE 
        COD_ESTABELECIMENTO IN ({','.join([f"'{e}'" for e in todos_estabelecimentos])})
        AND DATA_NOTA_FISCAL BETWEEN '2025-07-01' AND '2025-12-31'  
        AND PARA_FATURAMENTO = 'SIM'
        AND CFOP IN ('5.100', '5.101', '5.102', '5.103', '5.104', '5.105', '5.106', '5.109', '5.110', '5.111', 
                    '5.112', '5.113', '5.114', '5.115', '5.116', '5.117', '5.118', '5.119', '5.120', '5.122', 
                    '5.123', '5.250','5.251', '5.252', '5.253', '5.254', '5.255', '5.256', '5.257', '5.258', 
                    '5.401', '5.402', '5.403', '5.405', '5.651', '5.652', '5.653', '5.654', '5.655', '5.656',
                    '5.667', '6.101', '6.102', '6.103','6.104', '6.105', '6.106', '6.107', '6.108', '6.109',
                    '6.110', '6.111', '6.112', '6.113', '6.114', '6.115', '6.116', '6.117', '6.118', '6.119',
                    '6.120', '6.122', '6.123', '6.250', '6.251', '6.252', '6.253', '6.254', '6.255', '6.256',
                    '6.257', '6.258', '6.401', '6.402', '6.403', '6.404', '6.651', '6.652', '6.653', '6.654',
                    '6.655', '6.656', '6.667', '7.100', '7.101', '7.102', '7.105', '7.106','7.127', '7.250', 
                    '7.251', '7.651', '7.654', '7.667')
    GROUP BY
        CASE
            {case_expression}
        END
    ORDER BY
        CASE
            {case_expression}
        END
    """
    
    try:
        df_vendas = executar_query(conn, query_vendas)
        print(f"Query de vendas executada: {len(df_vendas)} grupos encontrados")
        return df_vendas
    except Exception as e:
        print(f"Erro ao executar query de vendas: {e}")
        return pd.DataFrame()

def formatar_numeros(df):
    """Formata todas as colunas numéricas com 2 casas decimais"""
    
    # Identificar colunas numéricas
    colunas_numericas = df.select_dtypes(include=['float64', 'int64']).columns
    
    for coluna in colunas_numericas:
        if 'QUANTIDADE' in coluna:
            # Para quantidades, formatar como inteiro
            df[coluna] = df[coluna].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "0")
        elif 'VALOR' in coluna:
            # Para valores monetários, formatar com 2 casas decimais
            df[coluna] = df[coluna].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else "R$ 0.00")
    
    return df

def calcular_analise_bonificacoes(df_bonificacoes, df_vendas):
    """Calcula a análise de bonificações vs vendas por grupo"""
    
    # Ordem desejada dos grupos
    ordem_grupos = ['AVANÇAR', 'BASE', 'CRESCER', 'FORTALEZA', 'PLANALTO', 'SUL', 'VISÃO', 'OUTROS']
    
    # Verificar se temos dados
    if df_bonificacoes.empty and df_vendas.empty:
        print("AVISO: Ambas as queries não retornaram dados.")
        
        # Criar DataFrame com todos os grupos
        df_resultado = pd.DataFrame({
            'GRUPO_RM': ordem_grupos,
            'QUANTIDADE_BONIFICACAO': [0.0] * len(ordem_grupos),
            'VALOR_BONIFICADO': [0.0] * len(ordem_grupos),
            'QUANTIDADE_VENDAS': [0.0] * len(ordem_grupos),
            'VALOR_VENDAS': [0.0] * len(ordem_grupos)
        })
    else:
        # Realizar merge das duas tabelas usando GRUPO_RM como chave
        df_merge = pd.merge(df_bonificacoes, df_vendas, 
                            on=['GRUPO_RM'], 
                            how='outer', 
                            suffixes=('_BON', '_VEND'))
        
        # Garantir que todos os grupos estejam presentes
        todos_grupos_df = pd.DataFrame({'GRUPO_RM': ordem_grupos})
        df_resultado = pd.merge(todos_grupos_df, df_merge, 
                               on=['GRUPO_RM'], 
                               how='left')
        
        # Preencher valores nulos com 0
        for col in ['QUANTIDADE_BONIFICACAO', 'VALOR_BONIFICADO', 'QUANTIDADE_VENDAS', 'VALOR_VENDAS']:
            df_resultado[col] = df_resultado[col].fillna(0)
    
    # Calcular taxas de bonificação
    def calcular_taxa(valor_bon, valor_vendas):
        if valor_vendas == 0:
            return 0.0
        return (valor_bon / valor_vendas)
    
    # Taxa de bonificação em valor
    df_resultado['TAXA_BONIFICACAO_VALOR'] = df_resultado.apply(
        lambda x: calcular_taxa(x['VALOR_BONIFICADO'], x['VALOR_VENDAS']), 
        axis=1
    )
    
    # Taxa de bonificação em quantidade
    df_resultado['TAXA_BONIFICACAO_QUANTIDADE'] = df_resultado.apply(
        lambda x: calcular_taxa(x['QUANTIDADE_BONIFICACAO'], x['QUANTIDADE_VENDAS']), 
        axis=1
    )
    
    # Formatar taxas como porcentagem
    df_resultado['TAXA_BONIFICACAO_VALOR_PCT'] = df_resultado['TAXA_BONIFICACAO_VALOR'].apply(
        lambda x: f"{x:.2%}"
    )
    
    df_resultado['TAXA_BONIFICACAO_QUANTIDADE_PCT'] = df_resultado['TAXA_BONIFICACAO_QUANTIDADE'].apply(
        lambda x: f"{x:.2%}"
    )
    
    # Calcular proporção bonificação/vendas
    df_resultado['PROPORCAO_BONIFICACAO'] = df_resultado.apply(
        lambda x: f"1:{1/x['TAXA_BONIFICACAO_VALOR']:.0f}" if x['TAXA_BONIFICACAO_VALOR'] > 0 else "N/A",
        axis=1
    )
    
    # Ordenar por ordem definida
    df_resultado['ORDEM'] = df_resultado['GRUPO_RM'].apply(lambda x: ordem_grupos.index(x) if x in ordem_grupos else 999)
    df_resultado = df_resultado.sort_values('ORDEM').drop('ORDEM', axis=1)
    
    # Criar cópia formatada para exibição
    df_formatado = df_resultado.copy()
    df_formatado = formatar_numeros(df_formatado)
    
    # Definir ordem das colunas para a análise detalhada
    colunas_analise_detalhada = [
        'GRUPO_RM',
        'QUANTIDADE_VENDAS', 
        'VALOR_VENDAS',
        'QUANTIDADE_BONIFICACAO', 
        'VALOR_BONIFICADO',
        'TAXA_BONIFICACAO_VALOR_PCT', 
        'TAXA_BONIFICACAO_QUANTIDADE_PCT',
        'PROPORCAO_BONIFICACAO'
    ]
    
    # Manter apenas colunas que existem
    colunas_existentes = [col for col in colunas_analise_detalhada if col in df_formatado.columns]
    df_formatado = df_formatado[colunas_existentes]
    
    return df_resultado, df_formatado

def salvar_analise_detalhada(df_formatado, caminho_completo):
    """Salva apenas a planilha Analise_Detalhada em Excel"""
    
    try:
        # Criar diretório se não existir
        diretorio = os.path.dirname(caminho_completo)
        if not os.path.exists(diretorio):
            os.makedirs(diretorio, exist_ok=True)
            print(f"Diretório criado: {diretorio}")
        
        # Salvar apenas a aba Analise_Detalhada
        with pd.ExcelWriter(caminho_completo, engine='openpyxl') as writer:
            df_formatado.to_excel(writer, sheet_name='Analise_Detalhada', index=False)
        
        print(f"✅ Arquivo Excel salvo com sucesso em: {caminho_completo}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo Excel: {e}")
        
        # Fallback: salvar como CSV
        try:
            caminho_fallback = caminho_completo.replace('.xlsx', '.csv')
            df_formatado.to_csv(caminho_fallback, index=False, encoding='utf-8-sig', sep=';', decimal=',')
            print(f"📁 Arquivo salvo como CSV (fallback): {caminho_fallback}")
            return True
        except Exception as e2:
            print(f"❌ Erro no fallback CSV: {e2}")
            
            # Último fallback: salvar no diretório atual
            try:
                caminho_simples = f'analise_bonificacoes_7grupos_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
                df_formatado.to_excel(caminho_simples, index=False)
                print(f"📁 Arquivo salvo no diretório atual: {caminho_simples}")
                print(f"   Caminho atual: {os.getcwd()}")
                return True
            except Exception as e3:
                print(f"❌ Erro no último fallback: {e3}")
                return False

def main():
    """Função principal"""
    
    print("=" * 70)
    print("ANÁLISE DE BONIFICAÇÕES vs VENDAS - 7 GRUPOS")
    print("=" * 70)
    print(f"Destino do arquivo: {caminho_completo}")
    
    # Resumo dos grupos
    print(f"\n📊 GRUPOS CONFIGURADOS:")
    for grupo_nome, estabelecimentos in grupos.items():
        print(f"   • {grupo_nome}: {len(estabelecimentos)} estabelecimentos")
    
    print(f"\n📋 RESUMO:")
    print(f"   • Total de grupos: {len(grupos)}")
    print(f"   • Total de estabelecimentos: {len(todos_estabelecimentos)}")
    print(f"   • Estabelecimento foco bonificações: R121")
    print()
    
    # Conectar ao banco de dados
    conn = conectar_banco()
    if not conn:
        print("❌ Não foi possível conectar ao banco de dados.")
        return
    
    try:
        # Executar queries
        print("📊 Coletando dados de bonificações (R121)...")
        df_bonificacoes = executar_query_bonificacoes_grupo(conn)
        
        print("📊 Coletando dados de vendas...")
        df_vendas = executar_query_vendas_grupo(conn)
        
        # Calcular análise
        print("📈 Calculando análise...")
        df_resultado, df_formatado = calcular_analise_bonificacoes(df_bonificacoes, df_vendas)
        
        # Salvar apenas a análise detalhada
        print("💾 Salvando análise detalhada...")
        sucesso = salvar_analise_detalhada(df_formatado, caminho_completo)
        
        if sucesso:
            print("\n" + "=" * 70)
            print("RESULTADOS DA ANÁLISE DE BONIFICAÇÕES - 7 GRUPOS")
            print("=" * 70)
            
            # Exibir resumo
            total_bonificacao = df_resultado['VALOR_BONIFICADO'].sum()
            total_vendas = df_resultado['VALOR_VENDAS'].sum()
            taxa_geral = (total_bonificacao / total_vendas) if total_vendas > 0 else 0
            
            print(f"\n📋 RESUMO GERAL:")
            print(f"   • Total de grupos analisados: {len(df_resultado)}")
            print(f"   • Total vendido: R$ {total_vendas:,.2f}")
            print(f"   • Total bonificado: R$ {total_bonificacao:,.2f}")
            print(f"   • Taxa geral de bonificação: {taxa_geral:.2%}")
            
            # Top grupos por taxa de bonificação
            print(f"\n🏆 TOP 3 GRUPOS POR TAXA DE BONIFICAÇÃO:")
            df_top_taxa = df_resultado[df_resultado['TAXA_BONIFICACAO_VALOR'] > 0].nlargest(3, 'TAXA_BONIFICACAO_VALOR')
            for i, (_, row) in enumerate(df_top_taxa.iterrows(), 1):
                print(f"   {i}. {row['GRUPO_RM']}: {row['TAXA_BONIFICACAO_VALOR']:.2%} "
                      f"(R$ {row['VALOR_BONIFICADO']:,.2f} / R$ {row['VALOR_VENDAS']:,.2f})")
            
            # Grupos sem bonificação
            sem_bonificacao = df_resultado[df_resultado['VALOR_BONIFICADO'] == 0]
            grupos_sem = [g for g in sem_bonificacao['GRUPO_RM'] if g != 'OUTROS']
            if grupos_sem:
                print(f"\n⚠️  GRUPOS SEM BONIFICAÇÃO: {', '.join(grupos_sem)}")
            
            # Mostrar prévia dos dados formatados
            print(f"\n📄 ANÁLISE DETALHADA (formato Excel):")
            print(df_formatado.to_string(index=False))
            
            # Informações do arquivo
            print(f"\n📁 INFORMAÇÕES DO ARQUIVO:")
            print(f"   • Nome: {os.path.basename(caminho_completo)}")
            print(f"   • Local: {caminho_completo}")
            print(f"   • Grupos incluídos: {len(df_formatado)}")
            print(f"   • Colunas: {', '.join(df_formatado.columns.tolist())}")
            
        else:
            print("\n❌ Não foi possível salvar o arquivo.")
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n🔒 Conexão com o banco de dados fechada.")
    
    print("\n" + "=" * 70)
    print("ANÁLISE CONCLUÍDA")
    print("=" * 70)

if __name__ == "__main__":
    main()

