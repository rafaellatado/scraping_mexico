import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

original_mx = pd.read_csv('data/ESTANDAR_BASICA_I2324.csv')

original_mx['id_escola_fuzzy'] = (
    original_mx['n_cct'].str.upper().str.strip() + ' - ' +
    original_mx['n_municipi'].str.upper().str.strip() + ' - ' +
    original_mx['n_entidad'].str.upper().str.strip()
)

original_mx = original_mx[original_mx['control'].str.upper().str.strip() == 'PRIVADO']

info_localizacao = original_mx[['id_escola_fuzzy', 'n_municipi', 'n_entidad', 'n_cct', 'domicilio']].drop_duplicates()

docentes_por_escola = original_mx.groupby('id_escola_fuzzy')['doc_tot'].sum().reset_index()

segmentos_binarios = pd.crosstab(original_mx['id_escola_fuzzy'], original_mx['nivel'])
segmentos_binarios = (segmentos_binarios > 0).astype(int)
segmentos_binarios.columns = ['tem_' + col.lower() for col in segmentos_binarios.columns]
segmentos_binarios = segmentos_binarios.reset_index()

matriculas_por_nivel = original_mx.pivot_table(index='id_escola_fuzzy',
                                               columns='nivel',
                                               values='ins_t',
                                               aggfunc='sum',
                                               fill_value=0)
matriculas_por_nivel.columns = ['matricula_' + col.lower() for col in matriculas_por_nivel.columns]
matriculas_por_nivel = matriculas_por_nivel.reset_index()

escolas_segmentadas = pd.merge(segmentos_binarios, matriculas_por_nivel, on='id_escola_fuzzy')

escolas_segmentadas['matricula_inf_fund'] = (
    escolas_segmentadas.get('matricula_preescolar', 0)+
    escolas_segmentadas.get('matricula_primaria', 0)
)

escolas_segmentadas = escolas_segmentadas[escolas_segmentadas['matricula_inf_fund'] > 40]

escolas_segmentadas['matricula_total'] = (
    escolas_segmentadas.get('matricula_preescolar', 0)+
    escolas_segmentadas.get('matricula_primaria', 0)+
    escolas_segmentadas.get('matricula_inicial', 0)+
    escolas_segmentadas.get('matricula_secundaria', 0)

)

# cria df só com id_escola_fuzzy e clavecct, garantindo 1 valor por escola
clavecct_por_escola = original_mx[['id_escola_fuzzy', 'clavecct']].drop_duplicates(subset=['id_escola_fuzzy'])

# cria df só com id_escola_fuzzy e n_entidad, garantindo 1 valor por escola
n_entidad_por_escola = original_mx[['id_escola_fuzzy', 'n_entidad']].drop_duplicates(subset=['id_escola_fuzzy'])

# mantém merges que já tínhamos
escolas_segmentadas = pd.merge(escolas_segmentadas, info_localizacao, on='id_escola_fuzzy')
escolas_segmentadas = pd.merge(escolas_segmentadas, docentes_por_escola, on='id_escola_fuzzy', how='left')

# adiciona as colunas clavecct e n_entidad sem impactar o tamanho
escolas_segmentadas = pd.merge(escolas_segmentadas, clavecct_por_escola, on='id_escola_fuzzy', how='left')
escolas_segmentadas = pd.merge(escolas_segmentadas, n_entidad_por_escola, on='id_escola_fuzzy', how='left')

escolas_segmentadas['docentes_por_aluno'] = escolas_segmentadas['matricula_total'] / escolas_segmentadas['doc_tot']

print(len(escolas_segmentadas))
print(escolas_segmentadas.head())

escolas_segmentadas.to_csv('dist/base_nova.csv', index=False)
