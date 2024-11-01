from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from minio import Minio
import tempfile
import glob


load_dotenv()

mongo_username = os.getenv("MONGO_USERNAME")
mongo_password = os.getenv("MONGO_PASSWORD")
mongo_database = os.getenv("MONGO_DATABASE")
mongo_collection = os.getenv("MONGO_COLLECTION")
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")
mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/"

minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
minio_use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"
minio_endpoint = f"{minio_host}:{minio_port}"

ms_url = os.getenv("MSSQL_URL")
ms_database = os.getenv("MSSQL_DATABASE")
ms_user, ms_password = os.getenv("MSSQL_USERNAME"), os.getenv("MSSQL_PASSWORD")
ms_table = os.getenv("MSSQL_TABLE")

spark_app_name = os.getenv("SPARK_APP_NAME", "ETL-V4-Spark")

spark = SparkSession.builder \
    .appName(spark_app_name) \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11") \
    .getOrCreate()


def extract_mssql(database = ms_database, table = ms_table):
    try:
        jdbc_url = f"jdbc:sqlserver://{ms_url};databaseName={database};encrypt=false;trustServerCertificate=true"
        connection_properties = {
            "user": ms_user,
            "password": ms_password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        query = f"(SELECT * FROM {table}) AS temp"
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
        return df
    except Exception as e:
        print(f"Error extracting data from SQL Server: {e}")
        return None


def extract_mongo(database = mongo_database, collection = mongo_collection):
    try:
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("spark.mongodb.input.uri", mongo_uri) \
            .option("spark.mongodb.input.database", database) \
            .option("collection", collection) \
            .load()
        print("Successfully extracted data")
        return df
    except Exception as e:
        print(f"Error extracting data from MongoDB: {e}")
        return None


def load_to_minio(df, file = "data"):
    if df is not None:
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                df.coalesce(1).write.mode("overwrite").json(temp_dir)
                json_files = glob.glob(os.path.join(temp_dir, "part-*.json"))
                if not json_files:
                    print("No JSON files found in temporary directory.")
                    return
                json_file = json_files[0]
                minio_client = Minio(
                    endpoint=minio_endpoint,
                    access_key=minio_access_key,
                    secret_key=minio_secret_key,
                    secure=minio_use_ssl
                )
                if not minio_client.bucket_exists(minio_bucket_name):
                    minio_client.make_bucket(minio_bucket_name)
                object_name = f"data/output/{file}.json"
                minio_client.fput_object(
                    bucket_name=minio_bucket_name,
                    object_name=object_name,
                    file_path=json_file,
                    content_type="application/json"
                )
                print(f"Data successfully uploaded to MinIO at {minio_bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error writing data to MinIO: {e}")
    else:
        print("DataFrame is None. Skipping write to MinIO.")
