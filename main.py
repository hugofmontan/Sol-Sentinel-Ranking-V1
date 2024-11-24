import pandas as pd
import os

# Nome do arquivo consolidado
consolidated_file = "consolidated_data.csv"

# Verificar se o arquivo consolidado já existe
if os.path.exists(consolidated_file):
    # Carregar o arquivo consolidado existente
    data_consolidated = pd.read_csv(consolidated_file)
    print(f"Arquivo consolidado '{consolidated_file}' carregado com {len(data_consolidated)} registros.")
else:
    # Criar um DataFrame vazio se o arquivo não existir
    data_consolidated = pd.DataFrame()
    print(f"Arquivo consolidado '{consolidated_file}' ainda não existe. Será criado após o processamento.")

# 1. Solicitar o novo arquivo CSV
file_name = input("Digite o caminho ou nome do novo arquivo CSV a ser adicionado: ")

try:
    # 2. Ler o novo arquivo CSV
    new_data = pd.read_csv(file_name)
    print(f"Novo arquivo '{file_name}' carregado com {len(new_data)} registros.")

    # Verificar se os registros do novo arquivo já existem no consolidado
    if not data_consolidated.empty:
        # Verificar duplicados com base na coluna "address"
        existing_addresses = set(data_consolidated["address"])
        new_data["is_duplicate"] = new_data["address"].apply(lambda x: x in existing_addresses)
        new_data_filtered = new_data[~new_data["is_duplicate"]].drop(columns=["is_duplicate"])
    else:
        # Se o consolidado está vazio, usar o novo arquivo completo
        new_data_filtered = new_data

    # Exibir o número de registros já processados
    num_duplicates = len(new_data) - len(new_data_filtered)
    print(f"{num_duplicates} registros já existentes foram ignorados.")

    # 3. Eliminar endereços duplicados no novo arquivo
    new_data_cleaned = new_data_filtered.drop_duplicates(subset="address", keep="first")

    # 4. Remover linhas com 'sells' igual a zero
    new_data_filtered = new_data_cleaned[new_data_cleaned["sells"] != 0]

    # Concatenar o novo arquivo filtrado com o arquivo consolidado existente
    data_consolidated = pd.concat([data_consolidated, new_data_filtered], ignore_index=True)

    # Remover duplicados do consolidado final
    data_consolidated = data_consolidated.drop_duplicates(subset="address", keep="first")

    # Salvar o arquivo consolidado atualizado
    data_consolidated.to_csv(consolidated_file, index=False)
    print(f"Arquivo consolidado atualizado e salvo como '{consolidated_file}' com {len(data_consolidated)} registros.")

    # Criar a coluna `Total Plays` como a soma de todas as faixas
    faixa_columns = ["1500%", "500% to 1500%", "200% to 500%",
                     "0% to 200%", "-50% to 0%", "-100% to 50%"]
    data_consolidated["Total Plays"] = data_consolidated[faixa_columns].sum(axis=1)

    # Criar um novo DataFrame para métricas
    df_metrics = pd.DataFrame()

    # Fatores de multiplicação (medianas das faixas)
    multiplication_factors = {
        "1500%": 1500,
        "500% to 1500%": 1000,
        "200% to 500%": 350,
        "0% to 200%": 100,
        "-50% to 0%": -25,
        "-100% to 50%": -50
    }

    # Inicializar o retorno acumulado total
    total_return_accumulated = 0

    # Calcular somas simples e valores ajustados
    for column, factor in multiplication_factors.items():
        # Soma total de ocorrências
        total_count = data_consolidated[column].sum()
        
        # Adicionar ao DataFrame original (somas simples)
        df_metrics[column] = [total_count]
        
        # Calcular o valor ajustado (ocorrências * fator)
        adjusted_value = total_count * factor
        df_metrics[f"{column}_adjusted"] = [adjusted_value]
        
        # Somar ao retorno acumulado total
        total_return_accumulated += adjusted_value

    # Adicionar o retorno acumulado total ao DataFrame
    df_metrics["total_return_accumulated"] = [total_return_accumulated]

    # Criar um novo DataFrame para pesos
    df_pesos = pd.DataFrame()

    # Calcular os pesos (retorno acumulado de cada faixa / retorno acumulado total)
    weights = {}
    for column, factor in multiplication_factors.items():
        adjusted_value = df_metrics[f"{column}_adjusted"].iloc[0]  # Retorno acumulado da faixa
        peso = adjusted_value / total_return_accumulated if total_return_accumulated != 0 else 0  # Peso proporcional
        weights[column] = peso
        df_pesos[column] = [peso]

    # Multiplicar os pesos das faixas `1500%` e `500% to 1500%` por 2
    weights["1500%"] *= 2
    weights["500% to 1500%"] *= 2

    # Atualizar o DataFrame `df_pesos` com os novos valores ajustados
    for column in weights:
        df_pesos[column] = [weights[column]]

    # Exibir os DataFrames resultantes
    print("\nResumo das métricas de trade por faixa de porcentagem (somas, ajustadas e total):")
    print(df_metrics)

    print("\nPesos calculados para cada faixa (com ajuste):")
    print(df_pesos)

    # Criar a coluna `ranking`
    data_consolidated["ranking"] = 0  # Inicializa a coluna de ranking

    # Loop para calcular o ranking baseado nas ocorrências e pesos
    for index, row in data_consolidated.iterrows():
        ranking_value = 0
        for column, weight in weights.items():
            ranking_value += row[column] * weight  # Multiplica ocorrência pelo peso
        # Divide pelo total de ocorrências (Total Plays) para calcular a pontuação proporcional
        if row["Total Plays"] > 0:
            ranking_value /= row["Total Plays"]
        data_consolidated.at[index, "ranking"] = ranking_value

    # Ordenar o DataFrame pelo ranking
    data_consolidated = data_consolidated.sort_values(by="ranking", ascending=False)

    # Salvar apenas os endereços e rankings no arquivo consolidado final
    consolidated_ranking_output_file = "consolidated_ranking.csv"
    data_consolidated[["address", "ranking"]].to_csv(consolidated_ranking_output_file, index=False)
    print(f"Arquivo consolidado com rankings salvo em: {consolidated_ranking_output_file}")

except FileNotFoundError:
    print("Arquivo não encontrado. Verifique o caminho ou nome do arquivo.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
