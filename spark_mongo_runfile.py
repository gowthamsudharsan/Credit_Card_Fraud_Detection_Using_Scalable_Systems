from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

# STEP 1: Connect to Azure Blob Storage
account_name = "" #account name
account_key = "" #copy from your Azure account
container_name = "" #container name from Azure account
blob_name = "dataset"

blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_data = blob_client.download_blob().readall().decode("utf-8")

# STEP 2: Load CSV to pandas
df_pd = pd.read_csv(StringIO(blob_data))
df_pd['trans_date_trans_time'] = pd.to_datetime(df_pd['trans_date_trans_time'])
df_pd[['year', 'month', 'day']] = df_pd['trans_date_trans_time'].apply(lambda x: x.timetuple()[:3]).tolist()

# STEP 3: Start SparkSession with MongoDB Connector
spark = SparkSession.builder \
    .appName("AzureToMongoDB") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mongoDBname") \
    .getOrCreate()

# STEP 4: Convert pandas → Spark
df_spark = spark.createDataFrame(df_pd)

# STEP 5: Upload to MongoDB
df_spark.write \
    .format("mongo") \
    .mode("overwrite") \
    .save()

print("Credit card transactions uploaded to MongoDB successfully.")
