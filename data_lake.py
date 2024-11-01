from minio import Minio
import tempfile
import glob
import os
from dotenv import load_dotenv


load_dotenv()

minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
minio_use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"


def load_to_minio(host = minio_host, port = minio_port, access_key = minio_access_key, secret_key = minio_secret_key,
                  bucket_name = minio_bucket_name, use_ssl = minio_use_ssl, df = None, file = "data"):
    if df is not None:
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                df.coalesce(1).write.mode("overwrite").json(temp_dir)
                json_files = glob.glob(os.path.join(temp_dir, "part-*.json"))
                if not json_files:
                    print("No JSON files found in temporary directory.")
                    return
                json_file = json_files[0]

                endpoint = f"{host}:{port}"
                minio_client = Minio(
                    endpoint=endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=use_ssl
                )
                if not minio_client.bucket_exists(minio_bucket_name):
                    minio_client.make_bucket(minio_bucket_name)
                object_name = f"data/output/{file}.json"
                minio_client.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=json_file,
                    content_type="application/json"
                )
                print(f"Data successfully uploaded to MinIO at {minio_bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error writing data to MinIO: {e}")
    else:
        print("DataFrame is None. Skipping write to MinIO.")
