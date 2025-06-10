import pandas as pd
from math import floor

enriched_df = pd.read_csv("csv/final/resultados_scraping_mx.csv", dtype=str)

print("Total de linhas:", len(enriched_df))


# df_org = pd.read_csv("csv/tmp/df_high_mx_with_turno_nivel_telefone_mexico_org.csv", dtype=str)

# merged_df = pd.merge(
#     enriched_df,
#     df_org[['clavecct', 'city', 'n_entidad', 'address', 'telefone_escuelaMexicoOrg', 'id_escola_fuzzy', 'telefone_scraping']],
#     on=['clavecct', 'city', 'n_entidad', 'address'],
#     how='inner')

# merged_df.drop(columns=['column_to_remove'], inplace=True)
# merged_df.to_csv("csv/tmp/merged_df_high_mx_1.csv", index=False)


def drop_value_if_equal(row, BASE_COLS):
    seen = set()
    for col in BASE_COLS:
        val = row[col]
        if pd.isna(val):
            continue
        if val in seen:
            row[col] = None
        else:
            seen.add(val)
    return row

validated_df = pd.read_csv("csv/definitivo.csv", dtype=str)

validated_df = validated_df.drop(columns=['telefone_scraping','telefone_escuelaMexicoOrg','telefone_scraping_1', 'telefone_scraping_2', 'telefone_scraping_3', 'telefone_scraping_4', 'telefone_scraping_5', 'telefone_scraping_6'], errors='ignore')

validated_df = validated_df.apply(drop_value_if_equal, axis=1, BASE_COLS=['telefone_scraping_verificado', 'telefone_scraping_2_verificado', 'telefone_scraping_3_verificado', 'telefone_scraping_4_verificado', 'telefone_scraping_5_verificado','telefone_scraping_6_verificado', 'telefone_escuelaMexicoOrg_verificado', 'telefone_scraping_1_verificado'])
validated_df.to_csv("csv/tmp/merged_df_high_mx_deduplicated.csv", index=False)


tel_cols = [{i: None} for i in validated_df.columns if i.endswith('verificado')]

for col in range(len(tel_cols)):
    col_name = list(tel_cols[col].keys())[0]
    tel_cols[col]= validated_df[col_name].notna().sum()

total = sum(tel_cols)
porcentagem = floor((total / len(validated_df)) * 100)
print("Total de telefones preenchidos:", total)
print("Porcentagem de telefones preenchidos:", porcentagem,"%")
print("Total de linhas após remoção de duplicatas:", validated_df.notna().sum())
print("Total de websites preenchidos:", validated_df['website'].notna().sum())




