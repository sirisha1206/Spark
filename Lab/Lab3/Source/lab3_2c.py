from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
os.environ["SPARK_HOME"] = "C:\\Spark\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\Sirisha Sunkara\\Desktop\\Summer18\\Spark\\Module2\\winutils"
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *
sc = spark.sparkContext


# Load a text file and convert each line to a Row.
lines = sc.textFile("ks-projects-201612_rdd.txt")
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
projects = parts.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12] .strip()))

# The schema is encoded in a string.
schemafields = "ID project_name category main_category currency deadline goal launched pledged state backers country usd_pledged"

columns = [StructField(field_name, StringType(), True) for field_name in schemafields.split()]
schema = StructType(columns)
# Apply the schema to the RDD
projectsschema = spark.createDataFrame(projects,schema)
projectsschema.createOrReplaceTempView('ksprojects')

df1 = spark.sql("select * from ksprojects")
df1.show()

#query using not operator
print("NOT operator")
dfq1 = spark.sql("select project_name,category from ksprojects where currency != 'USD'");
print(dfq1.show())

#query using group by and order by
print("Query using group by and order by")
dfq2 = spark.sql("select count(project_name) as c,main_category  from ksprojects group by main_category order by c desc")
print(dfq2.show())

#subquery
print("Subquery implementation")
dfq3 = spark.sql("select project_name,country from ksprojects where goal > (select avg(goal) from ksprojects)")
print(dfq3.show())

#pattern recognition query
print("pattern recognition query")
dfq6 = spark.sql("select category from ksprojects where category like '%ar%' or category like '%ra%'")
print(dfq6.show())

#range selection query
print("Range query")
dfq7 = spark.sql("select project_name,goal from ksprojects where goal between 1000 and 10000")
print(dfq7.show())

