import pandas as pd
import phonenumbers
import re
import csv
import unicodedata

def verify_phonenumber(numero, codigo_pais='MX'):
    if pd.isna(numero) or numero is None or numero == 'nan':
        return None
    clean_number = re.sub(r'\D', '', str(numero))
    if not clean_number:
        return None
    try:
        parsed = phonenumbers.parse(clean_number, codigo_pais)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return None

def remover_acentos(texto):
    if pd.isna(texto):
        return ''
    nfkd = unicodedata.normalize('NFKD', texto)
    return ''.join([c for c in nfkd if not unicodedata.combining(c)])


def load_ladas_mx(path):
    df = pd.read_csv(path, dtype=str)

    df['ciudad_normalizado'] = df['ciudad'].apply(lambda x: remover_acentos(x).strip().lower())
    df['estado_normalizado'] = df['estado'].apply(lambda x: remover_acentos(x).strip().lower())

    lada_df = (
        df.groupby(['ciudad_normalizado', 'estado_normalizado'])
        ['lada'].apply(list)
        .reset_index(name='claves_lada')
    )

    lada_dict = {}
    for _, row in lada_df.iterrows():
        key = (row['ciudad_normalizado'], row['estado_normalizado'])
        lada_dict[key] = row['claves_lada']
    return lada_dict, lada_df


def get_lada_mx(lada_dict, city, state):
    if pd.isna(city) or pd.isna(state):
        return None

    city = remover_acentos(str(city)).strip().lower()
    state = remover_acentos(str(state)).strip().lower()

    if state == 'ciudad de mexico':
        state = 'distrito federal'

    key = (city, state)
    return lada_dict.get(key)


def processar_linha(row, colns, lada_dict):
    telefone = row.get(colns)
    estado = row.get("n_entidad")
    cidade = row.get("city")

    resultados = {f'{colns}_verificado': telefone}

    if pd.isna(telefone) or not str(telefone).strip():
        return pd.Series(resultados)

    clean_number = re.sub(r'\D', '', str(telefone))

    if len(clean_number) in (7, 8):
        ladas = get_lada_mx(lada_dict, cidade, estado)
        if ladas:
            for lada_opcao in ladas:
                numero_com_ddd = f"+52{lada_opcao}{clean_number}"
                validado = verify_phonenumber(numero_com_ddd)
                if validado:
                    resultados[f'{colns}_verificado'] = validado
                    break
        return pd.Series(resultados)

    if len(clean_number) == 10 and not clean_number.startswith('+52'):
        clean_number = f"+52{clean_number}"

    validado = verify_phonenumber(clean_number)
    if validado:
        resultados[f'{colns}_verificado'] = validado

    return pd.Series(resultados)


if __name__ == "__main__":
    lada_dict, lada_df = load_ladas_mx('./csv/ladas_mexico.csv')
    df_principal = pd.read_csv('./csv/merged_df_high_mx_1.csv', dtype=str)

    phone_colunas = [
        'telefone_scraping',
        'telefone_scraping_1', 
        'telefone_scraping_2', 
        'telefone_scraping_3', 
        'telefone_scraping_4', 
        'telefone_scraping_5',
        'telefone_scraping_6',
        'telefone_escuelaMexicoOrg'
    ]

    dataframes_resultado = []
    for col in phone_colunas:
        resultado = df_principal.apply(lambda row: processar_linha(row, col, lada_dict), axis=1)
        resultado.columns = [f"{col}_verificado"]
        dataframes_resultado.append(resultado)

    df_final = pd.concat([df_principal] + dataframes_resultado, axis=1)
    df_final.to_csv('./csv/definitivo.csv', index=False)
    print("CSV final salvo como 'df_final_teste.csv'")
