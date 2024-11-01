from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv


load_dotenv()

spark_app_name = os.getenv("SPARK_APP_NAME", "ETL-V4-Spark")

spark = SparkSession.builder \
    .appName(spark_app_name) \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,"
            "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11") \
    .getOrCreate()
