import requests
from bs4 import BeautifulSoup
from googlesearch import search

def buscar_noticias(query, num=10):
    resultados = []
    for url in search(query, num_results=num, lang="pt"):
        try:
            response = requests.get(url, timeout=5)
            soup = BeautifulSoup(response.text, 'html.parser')
            titulo = soup.title.string if soup.title else 'Sem título'
            resultados.append({'titulo': titulo.strip(), 'link': url})
        except Exception as e:
            print(f"Erro ao acessar {url}: {e}")
    return resultados
