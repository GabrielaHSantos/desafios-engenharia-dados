# Importação de Bibliotecas 

import pandas as pd
import numpy as np 
from rapidfuzz import process, fuzz

def carregar_dados_excel(caminho_arquivo):
    """Carrega os dados das abas 'Base' e 'SKUS' de um arquivo Excel."""
    print(f"Carregando dados do arquivo: {caminho_arquivo}...")
    try:
        df_base = pd.read_excel(caminho_arquivo, sheet_name='Base')
        df_skus = pd.read_excel(caminho_arquivo, sheet_name='SKUS')
        print("-> Dados carregados com sucesso.")
        return df_base, df_skus
    except Exception as e:
        print(f"ERRO ao carregar o arquivo: {e}")
        return None, None

def limpar_e_unificar_dados(df_base, df_skus):
    """
    Orquestra todo o pipeline de limpeza e transformação dos dados.
    
    Recebe os DataFrames brutos e retorna três itens: o DataFrame limpo, 
    um DataFrame com os dados rejeitados para auditoria, e a contagem de
    linhas que eram válidas antes da etapa final de validação.
    """
    print("\nIniciando o pipeline de limpeza e transformação...")
    
    # Ponto de partida: armazena a contagem de linhas do arquivo bruto
    contagem_inicial_bruta = len(df_base)
    
    lista_rejeitados = []
    
    # 1. LIMPEZA INICIAL: Remove linhas que não contêm um objeto de venda válido e celulas com apenas espaços são primeiramente convertidas para NaN
    df_base['Objeto'] = df_base['Objeto'].astype(str).replace(r'^\s*$', np.nan, regex=True)
    df_base.dropna(subset=['Objeto'], inplace=True)
    
    # Contagem de referencia após a primeira etapa de limpeza
    contagem_apos_limpeza_inicial = len(df_base)

    # 2. PADRONIZAÇÃO: Prepara as colunas de texto para comparação, converte para maiúsculas e remove espaços para garantir consistência
    df_base['objeto_norm'] = df_base['Objeto'].astype(str).str.upper().str.strip()
    df_skus.dropna(subset=['Nome', 'SKU'], inplace=True)
    df_skus['nome_norm'] = df_skus['Nome'].astype(str).str.upper().str.strip()

    # Garante uma fonte da verdade única para os produtos, evitando duplicação no join
    df_skus.drop_duplicates(subset=['nome_norm'], keep='first', inplace=True)

    # 3. ENRIQUECIMENTO SEGURO: Associa os dados de SKU aos produtos da base e cria dicionários de mapeamento para uma busca rápida e segura
    mapa_norm_para_nome_final = pd.Series(df_skus.Nome.values, index=df_skus.nome_norm).to_dict()
    mapa_norm_para_sku = pd.Series(df_skus.SKU.values, index=df_skus.nome_norm).to_dict()
    
    # Constroi um dicionário de correção para os nomes de produtos com erros
    mapa_sujo_para_limpo_norm = {}
    nomes_corretos_norm = list(mapa_norm_para_nome_final.keys())

    for nome_sujo_norm in df_base['objeto_norm'].unique():
        resultado = process.extractOne(nome_sujo_norm, nomes_corretos_norm, scorer=fuzz.WRatio)
        if resultado:
            melhor_match, score, _ = resultado
            # O threshold é dinâmico para ser mais rigoroso com nomes curtos
            threshold = 75 if len(nome_sujo_norm) <= 4 else 80
            if score >= threshold:
                mapa_sujo_para_limpo_norm[nome_sujo_norm] = melhor_match

    # Usa o dicionário de correção via .map() para adicionar NomeProduto e SKU.(essa abordagem é mais segura que um 'merge', pois não cria linhas duplicadas)

    df_base['chave_norm_limpa'] = df_base['objeto_norm'].map(mapa_sujo_para_limpo_norm)
    df_base['NomeProduto'] = df_base['chave_norm_limpa'].map(mapa_norm_para_nome_final)
    df_base['SKU'] = df_base['chave_norm_limpa'].map(mapa_norm_para_sku)
    
    # Registra e remove produtos que não puderam ser mapeados com confiança.
    rejeitados2 = df_base[df_base['NomeProduto'].isna()].copy()
    if not rejeitados2.empty:
        rejeitados2['Motivo_Remocao'] = 'SKU não mapeado'
        lista_rejeitados.append(rejeitados2)
    df_base.dropna(subset=['NomeProduto'], inplace=True)
    
    df = df_base.copy()

    # 4. TRATAMENTO DE DATAS: Aplica uma função de correção robusta linha a linha
    def corrigir_data_linha(linha):
        """Define a hierarquia de regras para extrair um ano e mês válidos de uma linha."""
        # Prioridade 1: Tenta usar a coluna 'Data' se for um formato reconhecivel
        data_dt = pd.to_datetime(linha['Data'], errors='coerce', dayfirst=True)
        if pd.notna(data_dt):
            return data_dt.year, data_dt.month

        # Prioridade 2: Se a coluna 'Data' falhar, usa as colunas 'Ano' e 'Mês'
        ano_str = str(linha['Ano']).replace('.0', '')
        mes_str = str(linha['Mês']).replace('.0', '')
        
        # Sub-regras de correção para o ano
        if ano_str == 'YY':
            ano_final = 2022
        elif ano_str == '9999':
            ano_final = None # Inválido se a coluna 'Data' também não ajudou
        else:
            ano_final = pd.to_numeric(ano_str, errors='coerce')

        if pd.notna(ano_final) and ano_final < 100:
            ano_final += 2000

        # Sub-regras de correção para o mês
        mapa_meses = {'JANEIRO': 1, 'FEVEREIRO': 2, 'MARÇO': 3, 'ABRIL': 4, 'MAIO': 5, 'JUNHO': 6, 'JULHO': 7, 'AGOSTO': 8, 'SETEMBRO': 9, 'OUTUBRO': 10, 'NOVEMBRO': 11, 'DEZEMBRO': 12, 'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6, 'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12}
        mes_final = pd.to_numeric(mes_str, errors='coerce')
        if pd.isna(mes_final):
            mes_final = mapa_meses.get(mes_str.upper(), None)
            
        return ano_final, mes_final
    
    # Aplica a função de correção para criar as colunas de data finais
    resultados_data = df.apply(corrigir_data_linha, axis=1, result_type='expand')
    df['Ano_Final'] = resultados_data[0]
    df['Mes_Final'] = resultados_data[1]
    
    # 5. VALIDAÇÃO FINAL: Descarta registros que, mesmo após as correções, permanecem invalidos
    df['Ano_Final'] = pd.to_numeric(df['Ano_Final'], errors='coerce')
    df['Mes_Final'] = pd.to_numeric(df['Mes_Final'], errors='coerce')

    rejeitados_data_invalida = df[df['Ano_Final'].isna() | df['Mes_Final'].isna()].copy()
    if not rejeitados_data_invalida.empty:
        rejeitados_data_invalida['Motivo_Remocao'] = 'Mês ou Ano inválido'
        lista_rejeitados.append(rejeitados_data_invalida)
    df.dropna(subset=['Ano_Final', 'Mes_Final'], inplace=True)
    
    df['Ano_Final'] = df['Ano_Final'].astype(int)
    df['Mes_Final'] = df['Mes_Final'].astype(int)

    # Valida o intervalo de negocio para o ano
    mask_ano_invalido = ~df['Ano_Final'].between(1990, 2030)
    rejeitados_ano_intervalo = df[mask_ano_invalido].copy()
    if not rejeitados_ano_intervalo.empty:
        rejeitados_ano_intervalo['Motivo_Remocao'] = 'Ano fora do intervalo (1990-2030)'
        lista_rejeitados.append(rejeitados_ano_intervalo)
    df = df[~mask_ano_invalido]

    # Valida o intervalo para o mês
    mask_mes_invalido = ~df['Mes_Final'].between(1, 12)
    rejeitados_mes_intervalo = df[mask_mes_invalido].copy()
    if not rejeitados_mes_intervalo.empty:
        rejeitados_mes_intervalo['Motivo_Remocao'] = 'Mês fora do intervalo (1-12)'
        lista_rejeitados.append(rejeitados_mes_intervalo)
    df = df[~mask_mes_invalido]

    df_rejeitados = pd.concat(lista_rejeitados, ignore_index=True) if lista_rejeitados else pd.DataFrame()

    # 6. FINALIZAÇÃO: Prepara o DataFrame para a saída
    colunas_numericas = ['Investido', 'Cliques', 'Receita', 'Conversões']
    for col in colunas_numericas:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Constroi o DataFrame limpo de forma explícita para evitar colunas indesejadas
    df['Data_Final'] = pd.to_datetime(df['Ano_Final'].astype(str) + '-' + df['Mes_Final'].astype(str) + '-01', format='%Y-%m-%d')
    mapa_colunas_finais = {'Data_Final': 'Data','Mes_Final': 'Mes','Ano_Final': 'Ano','SKU': 'SKU','NomeProduto': 'NomeProduto','Investido': 'Investido','Cliques': 'Cliques','Receita': 'Receita','Conversões': 'Conversões'}
    colunas_de_origem = [col for col in mapa_colunas_finais.keys() if col in df.columns]
    df_limpo = df[colunas_de_origem]
    df_limpo = df_limpo.rename(columns=mapa_colunas_finais)

    print("\nLimpeza de dados concluída!")
    return df_limpo, df_rejeitados, contagem_apos_limpeza_inicial

def analisar_dados(df_limpo, df_rejeitados, contagem_inicial_valida):
    """Apresenta os relatórios analíticos e os insights sobre a qualidade dos dados."""
    print("\n--- RELATÓRIO DE ANÁLISE ---")
    
    # Seção 1: Análise de performance de produtos
    print("\n1. Top 5 Produtos por Faturamento (Receita) Mensal:")
    top_5_receita_mes = df_limpo.groupby(['Mes', 'NomeProduto'])['Receita'].sum().reset_index()
    for mes in sorted(df_limpo['Mes'].unique()):
        print(f"\nMês {mes}:")
        top_produtos = top_5_receita_mes[top_5_receita_mes['Mes'] == mes].nlargest(5, 'Receita')
        print(top_produtos[['NomeProduto', 'Receita']].to_string(index=False, float_format='R$ {:,.2f}'.format))

    print("\n\n2. Top 5 Produtos com Menos Cliques (que tiveram receita e cliques > 0):")
    df_ativos = df_limpo[(df_limpo['Receita'] > 0) & (df_limpo['Cliques'] > 0)]
    bottom_5_cliques_mes = df_ativos.groupby(['Mes', 'NomeProduto'])['Cliques'].sum().reset_index()
    for mes in sorted(df_limpo['Mes'].unique()):
        print(f"\nMês {mes}:")
        bottom_produtos = bottom_5_cliques_mes[bottom_5_cliques_mes['Mes'] == mes].nsmallest(5, 'Cliques')
        print(bottom_produtos[['NomeProduto', 'Cliques']].to_string(index=False))

    print("\n\n3. Top 5 Produtos por Receita Média por Registro (Anual):")
    receita_media = df_limpo.groupby('NomeProduto')['Receita'].mean().nlargest(5)
    print(receita_media.to_string(float_format='R$ {:,.2f}'.format))

    # Seção 2: Análise sobre o processo de limpeza e qualidade dos dados
    print("\n\n4. Insights Estratégicos sobre a Qualidade dos Dados:")
    num_rejeitados = len(df_rejeitados)
    num_limpos = len(df_limpo)
    
    if contagem_inicial_valida > 0:
        perc_rejeitado = (num_rejeitados / contagem_inicial_valida) * 100
        print(f"\n>> Diagnóstico de Entrada: De {contagem_inicial_valida} registros válidos (sem linhas fantasma), {num_rejeitados} não puderam ser recuperados.")
        print(f"   Isso indica que aproximadamente {perc_rejeitado:.1f}% dos dados de entrada possuem falhas críticas que impedem a análise.")
    else:
        print("\n>> Nenhum dado válido encontrado para análise.")

    print("\n>> Padrões de Erro Sistêmicos: A análise dos dados rejeitados e as correções aplicadas revelaram múltiplos problemas na fonte:")
    print("   - Inconsistência nos nomes de produtos (corrigidos com 'fuzzy matching').")
    print("   - Múltiplos formatos e entradas de data (corrigidos com lógica hierárquica).")
    print("   - Uso de valores inválidos como placeholders (ex: 'YY', '9999').")
    
    print("\n>> Valor da Engenharia de Dados: O pipeline de limpeza foi essencial para garantir a confiabilidade dos relatórios.")
    print(f"   Sem este tratamento, apenas uma fração dos {num_limpos} registros válidos seria aproveitável, levando a conclusões de negócio imprecisas.")


def salvar_dados_limpos(df_limpo, df_rejeitados, caminho_saida):
    """
    Salva os DataFrames de dados limpos e rejeitados em um único arquivo Excel, 
    com abas distintas para facilitar a auditoria.
    """
    print(f"\nSalvando arquivo de dados limpos e removidos em: {caminho_saida}...")
    try:
        with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
            df_limpo_para_salvar = df_limpo.copy()
            if 'Data' in df_limpo_para_salvar.columns:
                df_limpo_para_salvar['Data'] = df_limpo_para_salvar['Data'].dt.strftime('%d/%m/%Y')
            df_limpo_para_salvar.to_excel(writer, index=False, sheet_name='Dados_Limpos')
            
            if not df_rejeitados.empty:
                df_rejeitados.to_excel(writer, index=False, sheet_name='Dados_Removidos')

            # Auto-ajuste da largura das colunas para melhor visualização.
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    worksheet.column_dimensions[column_letter].width = (max_length + 2)

        print("-> Arquivo salvo com sucesso!")
    except Exception as e:
        print(f"ERRO ao salvar o arquivo: {e}")

def main():
    """Função principal que orquestra o fluxo: carregar, limpar e analisar os dados."""
    ARQUIVO_ENTRADA = 'ObjetosTeca.xlsx'
    ARQUIVO_SAIDA = 'ObjetosTeca_Limpo.xlsx'
    
    df_base, df_skus = carregar_dados_excel(ARQUIVO_ENTRADA)
    
    if df_base is not None and df_skus is not None:
        df_limpo, df_rejeitados, contagem_inicial = limpar_e_unificar_dados(df_base, df_skus)
        
        if not df_rejeitados.empty:
            print("\n--- RELATÓRIO DE DADOS REMOVIDOS ---")
            colunas_relevantes = ['Data', 'Mês', 'Ano', 'Objeto', 'Motivo_Remocao']
            print(df_rejeitados[[col for col in colunas_relevantes if col in df_rejeitados.columns]])
            print("------------------------------------")
        else:
            print("\n--- NENHUM DADO REMOVIDO DURANTE A LIMPEZA ---")

        if df_limpo is not None and not df_limpo.empty:
            analisar_dados(df_limpo, df_rejeitados, contagem_inicial)
            salvar_dados_limpos(df_limpo, df_rejeitados, ARQUIVO_SAIDA)
        else:
            print("\nProcesso interrompido, pois não restaram dados após a limpeza.")

if __name__ == "__main__":
    main()