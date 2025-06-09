import requests
from bs4 import BeautifulSoup
import re

def findInEscuelasmex(clavecct):
    url = f"https://escuelasmex.com/directorio/{clavecct}"

    try:
        response = requests.get(url, timeout=(5, 10))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for li in soup.find_all("li"):
            icon = li.find("i", class_="fi-telephone")
            if icon:
                texto = li.get_text(strip=True)
                numero = re.sub(r"\D", "", texto)
                print("Telefone encontrado:", numero)
                return numero

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")



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
        
        return telefone
    
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Request failed: {e}")


findInEscuelasmex("01DJN0454F")
extrair_dados_escola("01DJN0454F")