import os
import sys
from dotenv import load_dotenv
from crawlers.crawler_headway import baixar_oferta_csv
from crawlers.crawler_noticias import buscar_noticias
from process.trusted_headway import process_headway_from_s3

def run_headway():
    """Executa o fluxo completo do crawler de Headway."""
    print("🚄 Iniciando crawler de Headway...")

    # Variáveis do .env
    URL = os.environ.get("URL")
    LOCAL_PATH = os.environ.get("LOCAL_PATH")
    S3_BUCKET = os.environ.get("S3_BUCKET")
    S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX")

    if not all([URL, LOCAL_PATH, S3_BUCKET, S3_KEY_PREFIX]):
        raise RuntimeError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

    # 1️⃣ Baixar CSV e enviar ao S3
    baixar_oferta_csv(URL, LOCAL_PATH, S3_BUCKET, S3_KEY_PREFIX)

    # ⚙️ (opcional) Determinar dinamicamente o nome do arquivo salvo
    # Exemplo fixo para pipeline:
    input_s3_path = f"s3://{S3_BUCKET}/raw/headway2025.csv"
    output_s3_path = f"s3://{S3_BUCKET}/trusted/headway_trusted.csv"

    # 2️⃣ Processar o CSV direto do S3
    print("🚀 Iniciando processamento do arquivo Headway...")
    process_headway_from_s3(input_s3_path, output_s3_path)
    print(f"✅ Processamento concluído. Arquivo final disponível em: {output_s3_path}\n")


def run_noticias():
    """Executa o crawler de notícias."""
    print("🗞️ Iniciando crawler de notícias do Metrô...")

    query = "Metrô São Paulo site:g1.globo.com"
    noticias = buscar_noticias(query)

    if not noticias:
        print("Nenhuma notícia encontrada.")
    else:
        print("\n🗞️ Últimas notícias sobre o Metrô de São Paulo:\n")
        for noticia in noticias:
            print(f"📰 {noticia['titulo']}")
            print(f"🔗 {noticia['link']}\n")

def main():
    load_dotenv()

    run_headway()
    # run_noticias()

if __name__ == "__main__":
    main()
