import boto3
from botocore.exceptions import ClientError, NoCredentialsError

def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
    except FileNotFoundError:
        print("Arquivo local não encontrado.")
    except NoCredentialsError:
        print("Credenciais da AWS não configuradas.")
    except ClientError as e:
        print(f"Erro no upload: {e}")
