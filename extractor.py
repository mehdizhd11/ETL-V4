from dotenv import load_dotenv
import os
from main import spark


load_dotenv()

mongo_username = os.getenv("MONGO_USERNAME")
mongo_password = os.getenv("MONGO_PASSWORD")
mongo_database = os.getenv("MONGO_DATABASE")
mongo_collection = os.getenv("MONGO_COLLECTION")
mongo_host = os.getenv("MONGO_HOST")
mongo_port = os.getenv("MONGO_PORT")

ms_host = os.getenv("MSSQL_HOST")
ms_port = os.getenv("MSSQL_PORT")
ms_database = os.getenv("MSSQL_DATABASE")
ms_username, ms_password = os.getenv("MSSQL_USERNAME"), os.getenv("MSSQL_PASSWORD")
ms_table = os.getenv("MSSQL_TABLE")


def extract_mssql(host = ms_host, port = ms_port, username = ms_username, password = ms_password,
                  database = ms_database, table = ms_table):
    try:
        url = f"{host}:{port}"
        jdbc_url = f"jdbc:sqlserver://{url};databaseName={database};encrypt=false;trustServerCertificate=true"
        connection_properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        query = f"(SELECT * FROM {table}) AS temp"
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
        return df
    except Exception as e:
        print(f"Error extracting data from SQL Server: {e}")
        return None


def extract_mongo(host = mongo_host, port = mongo_port, username = mongo_username, password = ms_password,
                  database = mongo_database, collection = mongo_collection):
    try:
        mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/"
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("spark.mongodb.input.uri", mongo_uri) \
            .option("spark.mongodb.input.database", database) \
            .option("collection", collection) \
            .load()
        print("Successfully extracted data")
        return df
    except Exception as e:
        print(f"Error extracting data from MongoDB: {e}")
