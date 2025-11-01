import requests
import os
from datetime import datetime
from urllib.parse import unquote, urlparse
from utils.s3_uploader import upload_to_s3


def baixar_oferta_csv(url, local_path, bucket, s3_key_prefix):
    """
    Faz o download do CSV de oferta do Metrô e envia para o S3
    com um nome único baseado em timestamp e nome original.
    """

    # Criar diretório local se não existir
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Extrair nome do arquivo original da URL (ex: Oferta - 2025_6.csv)
    filename = os.path.basename(unquote(urlparse(url).path))

    # Adicionar timestamp para evitar sobrescrita
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_filename = f"{timestamp}_{filename}"

    # Caminho local completo
    unique_local_path = os.path.join(os.path.dirname(local_path), unique_filename)

    # Fazer download
    print(f"Baixando CSV de {url}...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    # Salvar localmente
    with open(unique_local_path, "wb") as f:
        f.write(response.content)

    # Montar a key completa no S3
    s3_key = os.path.join(s3_key_prefix, unique_filename)

    # Upload para S3
    upload_to_s3(unique_local_path, bucket, s3_key)

    print(f"CSV enviado com sucesso para s3://{bucket}/{s3_key}")

    # (opcional) apagar o arquivo local após upload
    os.remove(unique_local_path)
