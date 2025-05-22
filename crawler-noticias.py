from googlesearch import search
import requests
from bs4 import BeautifulSoup

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

query = "Metrô São Paulo site:g1.globo.com"

noticias = buscar_noticias(query)

for noticia in noticias:
    print(f"📰 {noticia['titulo']}")
    print(f"🔗 {noticia['link']}\n")
