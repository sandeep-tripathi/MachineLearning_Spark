from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Specify column names and types
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("text", StringType(), True),
    StructField("label", IntegerType(), True)
])

# Load data from a delimited file
sms = spark.read.csv("sms.csv", sep=";", header=False, schema=schema)

# Print schema of DataFrame
sms.printSchema()


"""
Output:
 root
     |-- id: integer (nullable = true)
     |-- text: string (nullable = true)
     |-- label: integer (nullable = true)
"""
