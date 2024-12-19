from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, count, split

# Initialize SparkSession
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

# Load data from local file
df = spark.read.csv("/home/ubuntu/calihealth/raw_data/Independent_Medical_Reviews.csv", header=True, inferSchema=True)

# Drop rows with any null values
df = df.dropna()

# Data Transformation
df = df.withColumn("Diagnosis and Treatment Summary", 
                   concat_ws(" - ", col("Diagnosis Category"), col("Treatment Category")))
df = df.withColumn("Reference Code", split(col("Reference ID"), "-").getItem(0))
df = df.withColumn("Reference Number", split(col("Reference ID"), "-").getItem(1))

# Dropping unnecessary columns
df = df.drop("Diagnosis Category", "Treatment Category", "Reference ID", "Findings")

# Save the transformed DataFrame
processed_data_path = "/home/ubuntu/calihealth/processed_data/transformed_data.csv"
df.write.csv(processed_data_path, header=True, mode="overwrite")

# Data Aggregation
total_cases_by_year = df.groupBy("Report Year").agg(count("*").alias("Total Cases"))
determination_distribution = df.groupBy("Determination").agg(count("*").alias("Total Cases"))
gender_distribution = df.groupBy("Patient Gender").agg(count("*").alias("Total Cases"))
age_range_distribution = df.groupBy("Age Range").agg(count("*").alias("Total Cases"))
cases_by_diagnosis = df.groupBy("Diagnosis Category").agg(count("*").alias("Total Cases"))

print("Total Cases by Year")
total_cases_by_year.show()
print("Determination Distribution")
determination_distribution.show()

print("Cases by Diagnosis Category")
cases_by_diagnosis.show()

print("Gender-based Distribution")
gender_distribution.show()

print("Age Range Distribution")
age_range_distribution.show()

# Save the results
total_cases_by_year_path = "/home/ubuntu/calihealth/processed_data/total_cases_by_year.csv"
determination_distribution_path = "/home/ubuntu/calihealth/processed_data/determination_distribution.csv"
gender_distribution_path = "/home/ubuntu/calihealth/processed_data/gender_distribution.csv"
age_range_distribution_path = "/home/ubuntu/calihealth/processed_data/age_range_distribution.csv"
cases_by_diagnosis_path = "/home/ubuntu/calihealth/processed_data/cases_by_diagnosis.csv"

total_cases_by_year.write.csv(total_cases_by_year_path, header=True, mode="overwrite")
determination_distribution.write.csv(determination_distribution_path, header=True, mode="overwrite")
gender_distribution.write.csv(gender_distribution_path, header=True, mode="overwrite")
age_range_distribution.write.csv(age_range_distribution_path, header=True, mode="overwrite")
cases_by_diagnosis.write.csv(cases_by_diagnosis_path, header=True, mode="overwrite")
# Stop the Spark session
spark.stop()
