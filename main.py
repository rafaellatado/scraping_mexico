import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os

# Configurações
caminho_base = './csv/ESTANDAR_BASICA_I2324.csv'
caminho_progresso = './csv/df_mx_com_telefone.csv'

checkpoint_interval = 5
delay_entre_reqs = 0.5

def extrair_telefone(clavecct):
    url = f"https://nte.mx/escuela/{clavecct}/a/"
    print(f"🔗 Acessando: {url}")
    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        strong_tag = soup.find('strong', string=lambda text: text and 'Teléfono' in text)
        if strong_tag and strong_tag.next_sibling:
            telefone = strong_tag.next_sibling.strip()
            if telefone == 'No disponible':
                return None
            return telefone
        print("❌ Tag 'Teléfono' não encontrada.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erro na requisição: {e}")
        raise e  # relança para ser tratado no script principal

def processar_linhas(df, indices, delay=delay_entre_reqs, checkpoint_interval=checkpoint_interval, caminho_progresso=caminho_progresso):
    """
    Processa as linhas indicadas no dataframe, atualizando telefone e status.
    Salva progresso a cada checkpoint_interval linhas, incluindo os erros.
    """
    for count, i in enumerate(indices, start=1):
        print(f"🔄 Processando linha {i + 1}")
        clave = df.at[i, 'clavecct']
        try:
            telefone = extrair_telefone(clave)
            df.at[i, 'telefone_scraping'] = telefone
            df.at[i, 'status_requisicao'] = 'sucesso'
            print(f"📞 Telefone da linha {i + 1}: {telefone}")
            print(f"✔️ Requisição bem-sucedida na linha {i + 1}")
        except Exception:
            df.at[i, 'status_requisicao'] = 'erro'
            print(f"❌ Erro na linha {i + 1}")

        if count % checkpoint_interval == 0:
            df.to_csv(caminho_progresso, index=False)
            print(f"💾 Progresso salvo até linha {i + 1}")

        time.sleep(delay)

    # Salva tudo no final (com erros também)
    df.to_csv(caminho_progresso, index=False)
    print("✅ Execução finalizada e progresso salvo.")
    return df, None  # mantendo compatibilidade

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)

    df = pd.read_csv(caminho_base)
    colunas_desejadas = ['clavecct']
    df_mexico = df[colunas_desejadas]
    df_mx = df_mexico.copy()
    df_mx = df_mx.head(20)  # Ajuste para teste, pode remover

    if 'telefone_scraping' not in df_mx.columns:
        df_mx['telefone_scraping'] = None
    if 'status_requisicao' not in df_mx.columns:
        df_mx['status_requisicao'] = 'pendente'

    if os.path.exists(caminho_progresso):
        df_salvo = pd.read_csv(caminho_progresso)
        if 'telefone_scraping' not in df_salvo.columns:
            df_salvo['telefone_scraping'] = None
        if 'status_requisicao' not in df_salvo.columns:
            df_salvo['status_requisicao'] = 'pendente'
        df_mx.update(df_salvo)

    pendentes = df_mx.index[df_mx['status_requisicao'] == 'pendente'].tolist()
    if not pendentes:
        print("✅ Essa base já está completa.")
        return
    print(f"🚀 Iniciando processamento nas {len(pendentes)} linhas pendentes.")
    processar_linhas(df_mx, pendentes)

if __name__ == '__main__':
    main()
