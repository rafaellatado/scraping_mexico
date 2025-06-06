import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
from pathlib import Path
import re

# Configurações
caminho_base = "csv/df_high_mx.csv"
caminho_progresso = "./csv/tmp/df_mx_com_telefone_parte_2222.csv"
from typing import List, Union, Optional

checkpoint_interval = 50
delay_entre_reqs = 0.5


def make_dirs(
    base_path: Union[str, Path],
    sub_dirs: Optional[List[str]] = None,
    create_base: bool = True,
) -> bool:
    try:
        path = Path(base_path) if isinstance(base_path, str) else base_path
        if not path.is_dir() and path.exists():
            raise ValueError(f"⚠️ O caminho {path} já existe e não é um diretório.")
        created_sub_dirs = False
        created_base = False
        if create_base:
            path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Diretório base criado: {path}")
            created_base = True
            if sub_dirs:
                for sub_dir in sub_dirs:
                    sub_path = path / sub_dir
                    sub_path.mkdir(parents=True, exist_ok=True)
                    print(f"✅ Subdiretório criado: {sub_path}")
                created_sub_dirs = True

        return True if (created_sub_dirs and created_base) else False
    except Exception as e:
        print(f"⚠️ Erro ao criar diretórios: {e}")
        return False


def extract_from_nte_mx(clavecct):
    url = f"https://nte.mx/escuela/{clavecct}/a/"
    print(f"🔗 Acessando: {url}")
    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Telefone
        strong_tel = soup.find(
            "strong", string=lambda text: text and "Teléfono" in text
        )
        telefone = None
        if strong_tel and strong_tel.next_sibling:
            telefone = strong_tel.next_sibling.strip()
            if telefone == "No disponible":
                telefone = None

        # Nome da escola
        meta_tag = soup.find("meta", property="url")
        nome_escola = None
        if meta_tag and "content" in meta_tag.attrs:
            partes = meta_tag["content"].strip("/").split("/")
            if len(partes) >= 3:
                nome_escola = partes[-1].replace("%20", " ")

        # Endereço da escola
        strong_endereco = soup.find(
            "strong", string=lambda text: text and "Dirección" in text
        )
        endereco = None
        if strong_endereco and strong_endereco.next_sibling:
            endereco = strong_endereco.next_sibling.strip()
            if endereco == "No disponible":
                endereco = None

        return telefone, nome_escola, endereco
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Erro na requisição: {e}")
        raise e
    
    
def name_normalization(school_name: str) -> str:
    """
    Normaliza o nome da escola para ser usado em URLs.
    
    Args:
        school_name (str): Nome da escola.
    
    Returns:
        str: Nome normalizado.
    """
    return school_name.lower().replace(" ", "-").replace("ñ", "n").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace('"', "").replace("'", "") if school_name else ""


def extract_from_escuelas_mex(clavecct: str, school_name: str) -> tuple:
    """
    Extrai informações de telefone, nome da escola e endereço a partir de uma chave CCT.
    
    Args:
        clavecct (str): Chave CCT da escola.
        school_name (str): Nome da escola.
    
    Returns:
        tuple: Tupla com (telefone, nome_escola, endereco)
    """
    url = f"https://www.escuelasmex.com/directorio/{clavecct}/{school_name}"
    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extrair telefone
        telephone = None
        ul = soup.find("ul", class_="ul-list contacto")
        if ul:
            for li in ul.find_all("li"):
                text = li.text.strip()
                if "Telefono" in text:
                    # Extrai apenas os números do texto
                    numbers = re.findall(r'\d+', text)
                    telephone = " - ".join(numbers) if numbers else None
                if "Web" in text:
                    website = text.split(" ")[1:3]
                    website = " ".join(website).strip()
                    print(f"🌐 Website: {website}")
                    
        
        nome_escola = school_name.replace("-", " ").title()
        
        return telephone, nome_escola, website if not 'Sin registro' in website else None
    
    except Exception as e:
        print(f"⚠️ Erro ao acessar {url}: {e}")
        return None, None, None
    

def evaluate_rows(
    df: pd.DataFrame,
    indices,
    delay=delay_entre_reqs,
    checkpoint_interval=checkpoint_interval,
    caminho_progresso=caminho_progresso,
):
    for count, row in enumerate(indices, start=1):
        print(f"🔄 Processando linha {row + 1}")
        clave = df.at[row, "clavecct"]
        city = name_normalization(df.at[row, "city"])
        school_name = name_normalization(df.at[row, "name"])
        try:
            telefone, nome_escola, website = extract_from_escuelas_mex(clave, school_name)
            telefones = telefone.split("-") if telefone else []
            for i in range(len(telefones)):
                df.at[row, f"telefone_scraping_{i + 1}"] = telefones[i].strip()
            df.at[row, "nome_escola"] = nome_escola
            df.at[row, 'website'] = website
            
            if telefone:
                df.at[row, "status_requisicao"] = "sucesso"
                print(f"📞 {telefone} | 🏫 {nome_escola} | 🌐 {website}")
                print(f"✔️ Requisição bem-sucedida na linha {row + 1}")
            else:
                df.at[row, "status_requisicao"] = "sem_telefone"
                print(f"ℹ️ Telefone não encontrado para a linha {row + 1}")
            
        except Exception as e:
            df.at[row, "status_requisicao"] = "erro"
            print(f"❌ Erro na linha {row + 1}:", e)

        if count % checkpoint_interval == 0:
            df.to_csv(caminho_progresso, index=False, encoding='utf-8')
            print(f"💾 Progresso salvo até linha {row + 1}")

        time.sleep(delay)

    df.to_csv(caminho_progresso, index=False)
    print("✅ Execução finalizada e progresso salvo.")
    return df, None


def main():
    COLUMNS_TO_CREATE = [
        "telefone_scraping",
        "telefone_scraping_2",
        "telefone_scraping_3",
        "telefone_scraping_4",
        "telefone_scraping_5",
        "nome_escola",
        "endereco_escola_scraping",
        "website",
        "status_requisicao",
    ]
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.width", None)

    df = pd.read_csv(caminho_base, dtype={"clavecct": str})
    colunas_desejadas = ["clavecct", "name", 'city','n_entidad']
    df_mexico = df[colunas_desejadas]
    df_mx = df_mexico.copy()
    # df_mx = df_mx.head(20)  # Ajuste para teste

    for column in COLUMNS_TO_CREATE:
        if "status_requisicao" not in df_mx.columns:
            df_mx["status_requisicao"] = "pendente"
            
        if column not in df_mx.columns:
            df_mx[column] = None

    if os.path.exists(caminho_progresso):
        df_salvo = pd.read_csv(
            caminho_progresso,
            dtype={
                "clavecct": str,
                "telefone_scraping": object,
                "nome_escola": object,
                "endereco_escola_scraping": object,
            },
        )
        for col in [
            "telefone_scraping",
            "nome_escola",
            "endereco_escola_scraping",
            "status_requisicao",
        ]:
            if col not in df_salvo.columns:
                df_salvo[col] = None if col != "status_requisicao" else "pendente"
        df_mx.update(df_salvo)

    pendentes = df_mx.index[df_mx["status_requisicao"] == "pendente"].tolist()
    if not pendentes:
        print("✅ Essa base já está completa.")
        return
    print(f"🚀 Iniciando processamento nas {len(pendentes)} linhas pendentes.")
    evaluate_rows(df_mx, pendentes, checkpoint_interval=10)


if __name__ == "__main__":
    make_dirs(base_path="./csv", sub_dirs=["tmp", "final"], create_base=True)
    main()

    