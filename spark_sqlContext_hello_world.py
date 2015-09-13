from pyspark.sql import SQLContext, Row
from pyspark.sql.dataframe import DataFrame

#we would need to create a SQLContext from the SparkContext object
sqlContext = SQLContext(sc)

# Load a text file from the local file system and convert each line to a Row.
lines = sc.textFile("file:///home/people.txt")

#now split each line in the RDD on the basis of ","
parts = lines.map(lambda l: l.split(","))

#now convert the RDD by mapping it to a row with column names and corresponding values
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
teenNames = teenagers.map(lambda p: "Name: " + p.name)
for teenName in teenNames.collect():
	print(teenName)