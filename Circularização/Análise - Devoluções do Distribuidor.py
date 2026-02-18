import pandas as pd
import pyodbc
import os
from datetime import datetime
from warnings import filterwarnings

# Filtrar o warning do pandas
filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

# Configurações de conexão com o banco de dados
server = 'DCMDWF01A.MOURA.INT'  
database = 'ax'   
username = 'uAuditoria' 
password = '@ud!t0$!@202&22'  

# Definir o caminho de salvamento
caminho_salvamento = r'C:\Users\matheus.melo\OneDrive - Acumuladores Moura SA\Documentos\Drive - Matheus Melo\Auditoria\2026\04. Circularização\Validações\Fluminense - R121'
nome_arquivo = f'analise_distribuidores_grupos_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
caminho_completo = os.path.join(caminho_salvamento, nome_arquivo)

# Definição dos 7 grupos
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

# Criar lista completa de todos os estabelecimentos para análise
todos_estabelecimentos = []
for estabelecimentos in grupos.values():
    todos_estabelecimentos.extend(estabelecimentos)

# Remover duplicatas
todos_estabelecimentos = list(set(todos_estabelecimentos))

# Lista específica de distribuidores do grupo AVANÇAR (para manter compatibilidade)
distribuidores_avancar = grupos['AVANÇAR']

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
    """Gera a expressão CASE para todos os 7 grupos"""
    case_parts = []
    
    # Adicionar cada grupo
    for grupo_nome, estabelecimentos in grupos.items():
        estabelecimentos_str = ','.join([f"'{e}'" for e in estabelecimentos])
        case_parts.append(f"WHEN COD_ESTABELECIMENTO IN ({estabelecimentos_str}) THEN '{grupo_nome}'")
    
    # Adicionar ELSE para qualquer outro estabelecimento não listado
    case_parts.append("ELSE 'OUTROS'")
    
    return '\n        '.join(case_parts)

def executar_query_devolucoes_distribuidor(conn):
    """Executa a query de devoluções por distribuidor incluindo grupo"""
    case_expression = gerar_case_grupos()
    
    query_devolucoes = f"""
    SELECT
        COD_ESTABELECIMENTO,
        CASE
            {case_expression}
        END AS GRUPO_RM,
        SUM(QUANTIDADE) AS QUANTIDADE_DEVOLVIDO,
        SUM(VALOR) AS VALOR_DEVOLVIDO
    FROM 
        VW_AUDIT_RM_ORDENS_VENDA
    WHERE
        COD_ESTABELECIMENTO IN ({','.join([f"'{e}'" for e in todos_estabelecimentos])})
        AND DATA_NOTA_FISCAL BETWEEN '2025-07-01' AND '2025-12-31' 
        AND PARA_FATURAMENTO = 'SIM'
        AND CFOP IN ('1.201', '1.202', '1.203', '1.204', '1.410', '1.411', '1.553', '1.660', '1.661', '1.662', 
                    '2.201', '2.202', '2.203', '2.204', '2.410', '2.411', '2.553', '2.660', '2.661', '2.662',
                    '3.201', '3.202', '3.211', '3.553')
    GROUP BY
        COD_ESTABELECIMENTO,
        CASE
            {case_expression}
        END
    ORDER BY
        GRUPO_RM,
        COD_ESTABELECIMENTO
    """
    
    try:
        df_devolucoes = executar_query(conn, query_devolucoes)
        print(f"Query de devoluções executada: {len(df_devolucoes)} distribuidores encontrados")
        return df_devolucoes
    except Exception as e:
        print(f"Erro ao executar query de devoluções: {e}")
        return pd.DataFrame()

def executar_query_faturamento_distribuidor(conn):
    """Executa a query de faturamento por distribuidor incluindo grupo"""
    case_expression = gerar_case_grupos()
    
    query_faturamento = f"""
    SELECT
        COD_ESTABELECIMENTO,
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
        AND PARA_FATURAMENTO = 'Sim'
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
        COD_ESTABELECIMENTO,
        CASE
            {case_expression}
        END
    ORDER BY
        GRUPO_RM,
        COD_ESTABELECIMENTO
    """
    
    try:
        df_faturamento = executar_query(conn, query_faturamento)
        print(f"Query de faturamento executada: {len(df_faturamento)} distribuidores encontrados")
        return df_faturamento
    except Exception as e:
        print(f"Erro ao executar query de faturamento: {e}")
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

def calcular_analise_distribuidores(df_devolucoes, df_faturamento):
    """Calcula a análise de devoluções vs faturamento por distribuidor"""
    
    # Verificar se temos dados
    if df_devolucoes.empty and df_faturamento.empty:
        print("AVISO: Ambas as queries não retornaram dados.")
        
        # Criar DataFrame com todos os distribuidores de todos os grupos
        dados_iniciais = []
        for grupo_nome, estabelecimentos in grupos.items():
            for estabelecimento in estabelecimentos:
                dados_iniciais.append({
                    'COD_ESTABELECIMENTO': estabelecimento,
                    'GRUPO_RM': grupo_nome,
                    'QUANTIDADE_DEVOLVIDO': 0.0,
                    'VALOR_DEVOLVIDO': 0.0,
                    'QUANTIDADE_VENDAS': 0.0,
                    'VALOR_VENDAS': 0.0
                })
        
        df_resultado = pd.DataFrame(dados_iniciais)
    else:
        # Realizar merge das duas tabelas usando COD_ESTABELECIMENTO como chave
        df_merge = pd.merge(df_devolucoes, df_faturamento, 
                            on=['COD_ESTABELECIMENTO', 'GRUPO_RM'], 
                            how='outer', 
                            suffixes=('_DEV', '_FAT'))
        
        # Preencher valores nulos com 0
        for col in ['QUANTIDADE_DEVOLVIDO', 'VALOR_DEVOLVIDO', 'QUANTIDADE_VENDAS', 'VALOR_VENDAS']:
            df_merge[col] = df_merge[col].fillna(0)
        
        df_resultado = df_merge
    
    # Calcular taxas de devolução
    def calcular_taxa(valor_dev, valor_fat):
        if valor_fat == 0:
            return 0.0
        return (valor_dev / valor_fat)
    
    df_resultado['TAXA_DEVOLUCAO_VALOR'] = df_resultado.apply(
        lambda x: calcular_taxa(x['VALOR_DEVOLVIDO'], x['VALOR_VENDAS']), 
        axis=1
    )
    
    df_resultado['TAXA_DEVOLUCAO_QUANTIDADE'] = df_resultado.apply(
        lambda x: calcular_taxa(x['QUANTIDADE_DEVOLVIDO'], x['QUANTIDADE_VENDAS']), 
        axis=1
    )
    
    # Formatar taxas como porcentagem
    df_resultado['TAXA_DEVOLUCAO_VALOR_PCT'] = df_resultado['TAXA_DEVOLUCAO_VALOR'].apply(
        lambda x: f"{x:.2%}"
    )
    
    df_resultado['TAXA_DEVOLUCAO_QUANTIDADE_PCT'] = df_resultado['TAXA_DEVOLUCAO_QUANTIDADE'].apply(
        lambda x: f"{x:.2%}"
    )
    
    # Ordenar por grupo e depois por código do distribuidor
    ordem_grupos = ['AVANÇAR', 'BASE', 'CRESCER', 'FORTALEZA', 'PLANALTO', 'SUL', 'VISÃO', 'OUTROS']
    df_resultado['ORDEM_GRUPO'] = df_resultado['GRUPO_RM'].apply(lambda x: ordem_grupos.index(x) if x in ordem_grupos else 999)
    df_resultado = df_resultado.sort_values(['ORDEM_GRUPO', 'COD_ESTABELECIMENTO']).drop('ORDEM_GRUPO', axis=1)
    
    # Criar cópia formatada para exibição
    df_formatado = df_resultado.copy()
    df_formatado = formatar_numeros(df_formatado)
    
    # Definir ordem das colunas para a análise detalhada
    colunas_analise_detalhada = [
        'GRUPO_RM',
        'COD_ESTABELECIMENTO',
        'QUANTIDADE_VENDAS', 
        'VALOR_VENDAS',
        'QUANTIDADE_DEVOLVIDO', 
        'VALOR_DEVOLVIDO',
        'TAXA_DEVOLUCAO_VALOR_PCT', 
        'TAXA_DEVOLUCAO_QUANTIDADE_PCT'
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
                caminho_simples = f'analise_distribuidores_grupos_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
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
    print("ANÁLISE DE DEVOLUÇÕES vs FATURAMENTO POR DISTRIBUIDOR E GRUPO")
    print("=" * 70)
    print(f"Destino do arquivo: {caminho_completo}")
    
    # Resumo dos grupos
    print(f"\n📊 GRUPOS CONFIGURADOS:")
    total_distribuidores = 0
    for grupo_nome, estabelecimentos in grupos.items():
        print(f"   • {grupo_nome}: {len(estabelecimentos)} estabelecimentos")
        total_distribuidores += len(estabelecimentos)
    
    print(f"\n📋 RESUMO:")
    print(f"   • Total de grupos: {len(grupos)}")
    print(f"   • Total de distribuidores: {total_distribuidores}")
    print(f"   • Período analisado: 2025-07-01 a 2025-12-31")
    print()
    
    # Conectar ao banco de dados
    conn = conectar_banco()
    if not conn:
        print("❌ Não foi possível conectar ao banco de dados.")
        return
    
    try:
        # Executar queries
        print("📊 Coletando dados de devoluções...")
        df_devolucoes = executar_query_devolucoes_distribuidor(conn)
        
        print("📊 Coletando dados de faturamento...")
        df_faturamento = executar_query_faturamento_distribuidor(conn)
        
        # Calcular análise
        print("📈 Calculando análise...")
        df_resultado, df_formatado = calcular_analise_distribuidores(df_devolucoes, df_faturamento)
        
        # Salvar apenas a análise detalhada
        print("💾 Salvando análise detalhada...")
        sucesso = salvar_analise_detalhada(df_formatado, caminho_completo)
        
        if sucesso:
            print("\n" + "=" * 70)
            print("RESULTADOS DA ANÁLISE POR DISTRIBUIDOR E GRUPO")
            print("=" * 70)
            
            # Exibir resumo
            total_devolucao = df_resultado['VALOR_DEVOLVIDO'].sum()
            total_faturamento = df_resultado['VALOR_VENDAS'].sum()
            taxa_geral = (total_devolucao / total_faturamento) if total_faturamento > 0 else 0
            
            print(f"\n📋 RESUMO GERAL:")
            print(f"   • Total de distribuidores analisados: {len(df_resultado)}")
            print(f"   • Total faturado: R$ {total_faturamento:,.2f}")
            print(f"   • Total devolvido: R$ {total_devolucao:,.2f}")
            print(f"   • Taxa geral de devolução: {taxa_geral:.2%}")
            
            # Resumo por grupo
            print(f"\n📊 RESUMO POR GRUPO:")
            resumo_grupos = df_resultado.groupby('GRUPO_RM').agg({
                'COD_ESTABELECIMENTO': 'count',
                'VALOR_VENDAS': 'sum',
                'VALOR_DEVOLVIDO': 'sum'
            }).reset_index()
            
            resumo_grupos['TAXA'] = resumo_grupos.apply(
                lambda x: x['VALOR_DEVOLVIDO'] / x['VALOR_VENDAS'] if x['VALOR_VENDAS'] > 0 else 0,
                axis=1
            )
            
            for _, row in resumo_grupos.iterrows():
                print(f"   • {row['GRUPO_RM']}: {row['COD_ESTABELECIMENTO']} distribuidores, "
                      f"Taxa: {row['TAXA']:.2%}")
            
            # Top distribuidores com maior taxa de devolução
            print(f"\n⚠️  TOP 5 DISTRIBUIDORES COM MAIOR TAXA DE DEVOLUÇÃO:")
            df_top_problemas = df_resultado.nlargest(5, 'TAXA_DEVOLUCAO_VALOR')
            for i, (_, row) in enumerate(df_top_problemas.iterrows(), 1):
                if row['TAXA_DEVOLUCAO_VALOR'] > 0:
                    print(f"   {i}. {row['COD_ESTABELECIMENTO']} ({row['GRUPO_RM']}): "
                          f"{row['TAXA_DEVOLUCAO_VALOR']:.2%} "
                          f"(R$ {row['VALOR_DEVOLVIDO']:,.2f} / R$ {row['VALOR_VENDAS']:,.2f})")
            
            # Distribuidores sem devolução
            sem_devolucao = df_resultado[df_resultado['VALOR_DEVOLVIDO'] == 0]
            if len(sem_devolucao) > 0:
                print(f"\n✅ DISTRIBUIDORES SEM DEVOLUÇÃO: {len(sem_devolucao)} distribuidores")
                # Mostrar alguns exemplos
                exemplos = sem_devolucao.head(5)['COD_ESTABELECIMENTO'].tolist()
                print(f"   Exemplos: {', '.join(exemplos)}")
            
            # Mostrar prévia dos dados
            print(f"\n📄 PRÉVIA DA ANÁLISE DETALHADA (primeiras 8 linhas):")
            print(df_formatado.head(8).to_string(index=False))
            
            # Informações do arquivo
            print(f"\n📁 INFORMAÇÕES DO ARQUIVO:")
            print(f"   • Nome: {os.path.basename(caminho_completo)}")
            print(f"   • Local: {caminho_completo}")
            print(f"   • Distribuidores analisados: {len(df_formatado)}")
            print(f"   • Colunas incluídas: {', '.join(df_formatado.columns.tolist())}")
            
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
