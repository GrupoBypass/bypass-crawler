import os
from crawlers.crawler_headway import baixar_oferta_csv
from crawlers.crawler_noticias import buscar_noticias
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

URL = os.environ.get("METRO_OFERTA_URL")
LOCAL_PATH = os.environ.get("METRO_LOCAL_PATH")
S3_BUCKET = os.environ.get("METRO_S3_BUCKET")
S3_KEY_PREFIX = os.environ.get("METRO_S3_KEY_PREFIX")

if not S3_BUCKET:
    raise RuntimeError("A variável METRO_S3_BUCKET deve ser definida no ambiente.")

baixar_oferta_csv(URL, LOCAL_PATH, S3_BUCKET, S3_KEY_PREFIX)

# === CRAWLER DE NOTÍCIAS ===
query = "Metrô São Paulo site:g1.globo.com"
noticias = buscar_noticias(query)

if not noticias:
    print("Nenhuma notícia encontrada.")
else:
    for noticia in noticias:
        print(f"📰 {noticia['titulo']}")
        print(f"🔗 {noticia['link']}\n")
