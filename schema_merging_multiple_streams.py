# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Create a simple DataFrame, stored into a partition directory
# partition is going to be on the basis of key which is 1 for first DF and 2 for second
df1 = sqlContext.createDataFrame(sc.parallelize(range(1, 6))\
                                   .map(lambda i: Row(single=i, double=i * 2)))
df1.write.parquet("data/test_table/key=1")

# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
df2 = sqlContext.createDataFrame(sc.parallelize(range(6, 11))
                                   .map(lambda i: Row(single=i, triple=i * 3)))
df2.write.parquet("data/test_table/key=2")

# Read the partitioned table
df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
df3.printSchema()

# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
# |-- single: int (nullable = true)
# |-- double: int (nullable = true)
# |-- triple: int (nullable = true)
# |-- key : int (nullable = true)

#Parquet form:
#message root {
#  optional int64 single;
#  optional int64 triple;
#  optional int64 double;
#}
