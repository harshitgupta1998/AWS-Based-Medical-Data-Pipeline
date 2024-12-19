from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Initialize Spark session
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

# Load data from S3
df = spark.read.csv("/home/ubuntu/calihealth/raw_data/Independent_Medical_Reviews.csv", header=True, inferSchema=True)
# Drop rows with any null values
df = df.dropna()

df.show(2)