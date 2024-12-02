from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Example") \
    .getOrCreate()

# Create a simple DataFrame from a list of tuples
data = [("Alice", 30, 3500),
        ("Bob", 25, 2800),
        ("Charlie", 35, 4000),
        ("David", 40, 5000)]

columns = ["name", "age", "salary"]

# Create a DataFrame using the data and columns
df = spark.createDataFrame(data, columns)

# Show the original DataFrame
print("Original DataFrame:")
df.show()

# Perform some transformations
# 1. Add a new column "salary_in_1000s" which divides the salary by 1000
df_transformed = df.withColumn("salary_in_1000s", col("salary") / 1000)

# 2. Filter out people whose age is greater than 30
df_filtered = df_transformed.filter(col("age") > 30)

# Show the transformed DataFrame
print("Transformed DataFrame:")
df_filtered.show()

# Stop the Spark session
spark.stop()
