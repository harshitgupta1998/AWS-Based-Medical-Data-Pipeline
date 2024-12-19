from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, month, year
from pyspark.sql.functions import concat_ws, col, count, split

# Initialize SparkSession
spark = SparkSession.builder.appName("DataAnalysisUsingSQL").getOrCreate()

# Load data from CSV (modify path as necessary)
df = spark.read.csv("/home/ubuntu/calihealth/raw_data/Independent_Medical_Reviews.csv", header=True, inferSchema=True)

# Drop rows with any null values
df = df.dropna()

# Data Transformation
df = df.withColumn("Diagnosis and Treatment Summary", 
                   concat_ws(" - ", col("Diagnosis Category"), col("Treatment Category")))

df = df.withColumn("Reference Code", split(col("Reference ID"), "-").getItem(0))
df = df.withColumn("Reference Number", split(col("Reference ID"), "-").getItem(1))

# Drop unnecessary columns
df = df.drop("Diagnosis Category", "Treatment Category", "Reference ID", "Findings")

# Register DataFrame as a SQL temporary table
df.createOrReplaceTempView("medical_reviews")

# SQL Queries

# 1. Identify top-performing regions (e.g., regions with the highest case counts)
top_regions_query = """
SELECT `Age Range`, COUNT(*) AS TotalCases
FROM medical_reviews
GROUP BY `Age Range`
ORDER BY TotalCases DESC
LIMIT 5
"""
top_regions = spark.sql(top_regions_query)
print("Top Performing Regions:")
top_regions.show()

# 2. Determine the most popular Diagnosis and Treatment combinations
popular_combinations_query = """
SELECT `Diagnosis and Treatment Summary`, COUNT(*) AS CaseCount
FROM medical_reviews
GROUP BY `Diagnosis and Treatment Summary`
ORDER BY CaseCount DESC
LIMIT 5
"""
popular_combinations = spark.sql(popular_combinations_query)
print("Most Popular Diagnosis and Treatment Combinations:")
popular_combinations.show()

# 3. Gender-based distribution of cases
gender_distribution_query = """
SELECT `Patient Gender`, COUNT(*) AS TotalCases
FROM medical_reviews
GROUP BY `Patient Gender`
ORDER BY TotalCases DESC
"""
gender_distribution = spark.sql(gender_distribution_query)
print("Gender-based Distribution of Cases:")
gender_distribution.show()

# 4. Count of Overturned vs. Upheld decisions
decision_distribution_query = """
SELECT Determination, COUNT(*) AS TotalCases
FROM medical_reviews
GROUP BY Determination
ORDER BY TotalCases DESC
"""
decision_distribution = spark.sql(decision_distribution_query)
print("Count of Overturned vs. Upheld Decisions:")
decision_distribution.show()

# 5. Age Range with Most Upheld Decisions
age_range_upheld_query = """
SELECT `Age Range`, COUNT(*) AS UpheldCases
FROM medical_reviews
WHERE Determination = 'Upheld Decision of Health Plan'
GROUP BY `Age Range`
ORDER BY UpheldCases DESC
LIMIT 5
"""
age_range_upheld = spark.sql(age_range_upheld_query)
print("Age Range with Most Upheld Decisions:")
age_range_upheld.show()
# Stop the Spark session
spark.stop()
