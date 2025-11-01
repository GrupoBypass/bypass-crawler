import requests
import os
from utils.s3_uploader import upload_to_s3

def baixar_oferta_csv(url, local_path, bucket, s3_key):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    upload_to_s3(local_path, bucket, s3_key)
    print(f"✅ CSV enviado com sucesso para s3://{bucket}/{s3_key}")
