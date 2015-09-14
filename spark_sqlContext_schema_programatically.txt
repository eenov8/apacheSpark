# The purpose of the program is to illustrate how we can create DataFrames such that it takes the schema at run time from a SchemaString.
# Using Spark 1.5

# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
lines = sc.textFile("/home/anurag/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))
# This has created an RDD with tuples like (Anurag, 30) and so on 

# The schema is encoded in a string.
schemaString = "name age"

# Now we will split the schemaString and associate that with StructField type array
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(people, schema)

# Register the DataFrame as a table.
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT name FROM people")

# The results of SQL queries are RDDs and support all the normal RDD operations.
names = results.map(lambda p: "Name: " + p.name)
for name in names.collect():
  print(name)