import requests
import os
import re
from urllib.parse import unquote, urlparse
from utils.s3_uploader import upload_to_s3

def baixar_oferta_csv(url, local_path, bucket, s3_key_prefix):
    # Criar diretório local se não existir
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Baixando CSV de {url}...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    content_bytes = response.content

    # Quebrar o conteúdo em linhas de bytes
    byte_lines = content_bytes.splitlines()

    # Tentar encontrar o ano em alguma linha útil
    ano = None
    decodings_to_try = ["utf-8", "iso-8859-1", "cp1252"]

    for line_bytes in byte_lines[:10]:  # olhar só as 10 primeiras linhas
        if not line_bytes.strip():
            continue  # pula linhas vazias

        # tenta decodificar
        line = None
        for enc in decodings_to_try:
            try:
                line = line_bytes.decode(enc)
                break
            except (UnicodeDecodeError, LookupError):
                continue

        if not line:
            continue

        # ignora linhas só com separadores
        if re.fullmatch(r"[;, ]+", line):
            continue

        # procura ano (4 dígitos consecutivos)
        match = re.search(r"\b(20\d{2})\b", line)
        if match:
            ano = match.group(1)
            break

    if not ano:
        raise ValueError("Não foi possível extrair o ano de nenhuma linha do CSV.")

    # Nome final e caminho local (salvamos os bytes originais)
    final_filename = f"headway{ano}.csv"
    final_local_path = os.path.join(os.path.dirname(local_path), final_filename)

    # Gravar bytes originais no arquivo final
    with open(final_local_path, "wb") as f:
        f.write(content_bytes)

    # Montar a key completa no S3
    s3_key = os.path.join(s3_key_prefix, final_filename)

    # Upload para S3
    upload_to_s3(final_local_path, bucket, s3_key)

    print(f"✅ CSV enviado com sucesso para s3://{bucket}/{s3_key}")

    # (opcional) apagar o arquivo local após upload
    os.remove(final_local_path)
