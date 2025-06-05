import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os

# Configurações
caminho_base = './csv/df_mx_com_telefone_parte_1.csv'
caminho_progresso = './csv/df_mx_com_telefone_parte_1.csv'

checkpoint_interval = 50
delay_entre_reqs = 0.5

def extrair_dados_escola(clavecct):
    url = f"https://nte.mx/escuela/{clavecct}/a/"
    print(f"🔗 Acessando: {url}")
    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Telefone
        strong_tel = soup.find('strong', string=lambda text: text and 'Teléfono' in text)
        telefone = None
        if strong_tel and strong_tel.next_sibling:
            telefone = strong_tel.next_sibling.strip()
            if telefone == 'No disponible':
                telefone = None

        # Nome da escola
        meta_tag = soup.find('meta', property='url')
        nome_escola = None
        if meta_tag and 'content' in meta_tag.attrs:
            partes = meta_tag['content'].strip('/').split('/')
            if len(partes) >= 3:
                nome_escola = partes[-1].replace('%20', ' ')

        # Endereço da escola
        strong_endereco = soup.find('strong', string=lambda text: text and 'Dirección' in text)
        endereco = None
        if strong_endereco and strong_endereco.next_sibling:
            endereco = strong_endereco.next_sibling.strip()
            if endereco == 'No disponible':
                endereco = None

        return telefone, nome_escola, endereco
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erro na requisição: {e}")
        raise e  

def processar_linhas(df, indices, delay=delay_entre_reqs, checkpoint_interval=checkpoint_interval, caminho_progresso=caminho_progresso):
    for count, i in enumerate(indices, start=1):
        print(f"🔄 Processando linha {i + 1}")
        clave = df.at[i, 'clavecct']
        try:
            telefone, nome_escola, endereco = extrair_dados_escola(clave)
            df.at[i, 'telefone_scraping'] = telefone
            df.at[i, 'nome_escola'] = nome_escola
            df.at[i, 'endereco_escola_scraping'] = endereco
            df.at[i, 'status_requisicao'] = 'sucesso'
            print(f"📞 {telefone} | 🏫 {nome_escola} | 📍 {endereco}")
            print(f"✔️ Requisição bem-sucedida na linha {i + 1}")
        except Exception:
            df.at[i, 'status_requisicao'] = 'erro'
            print(f"❌ Erro na linha {i + 1}")

        if count % checkpoint_interval == 0:
            df.to_csv(caminho_progresso, index=False)
            print(f"💾 Progresso salvo até linha {i + 1}")

        time.sleep(delay)

    df.to_csv(caminho_progresso, index=False)
    print("✅ Execução finalizada e progresso salvo.")
    return df, None  

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)

    df = pd.read_csv(caminho_base, dtype={'clavecct': str})
    colunas_desejadas = ['clavecct']
    df_mexico = df[colunas_desejadas]
    df_mx = df_mexico.copy()
    #df_mx = df_mx.head(20)  # Ajuste para teste

    if 'telefone_scraping' not in df_mx.columns:
        df_mx['telefone_scraping'] = None
    if 'nome_escola' not in df_mx.columns:
        df_mx['nome_escola'] = None
    if 'endereco_escola_scraping' not in df_mx.columns:
        df_mx['endereco_escola_scraping'] = None
    if 'status_requisicao' not in df_mx.columns:
        df_mx['status_requisicao'] = 'pendente'

    if os.path.exists(caminho_progresso):
        df_salvo = pd.read_csv(
            caminho_progresso, 
            dtype={'clavecct': str, 'telefone_scraping': object, 'nome_escola': object, 'endereco_escola_scraping': object}
        )
        for col in ['telefone_scraping', 'nome_escola', 'endereco_escola_scraping', 'status_requisicao']:
            if col not in df_salvo.columns:
                df_salvo[col] = None if col != 'status_requisicao' else 'pendente'
        df_mx.update(df_salvo)

    pendentes = df_mx.index[df_mx['status_requisicao'] == 'pendente'].tolist()
    if not pendentes:
        print("✅ Essa base já está completa.")
        return
    print(f"🚀 Iniciando processamento nas {len(pendentes)} linhas pendentes.")
    processar_linhas(df_mx, pendentes)

if __name__ == '__main__':
    main()
