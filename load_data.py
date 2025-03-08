# Read data from CSV file
flights = spark.read.csv("flights.csv",
                         sep=",",
                         header=True,
                         inferSchema=True,
                         nullValue="NA")

# Get number of records
print("The data contain %d records." % flights.count())

# View the first five records
flights.show(5)

# Check column data types
print(flights.dtypes)
