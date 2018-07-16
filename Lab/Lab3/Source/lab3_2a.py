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


schema1 = StructType([
    StructField("ID", IntegerType(), True),
    StructField("project_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("main_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("deadline", TimestampType(), True),
    StructField("goal", IntegerType(), True),
    StructField("launched", TimestampType(), True),
    StructField("pledged", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("backers", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("usd pledged", FloatType(), True)
    ])
df = spark.read.format("csv").option("header","true").option("delimiter", ",").schema(schema1).load("ks-projects-201612.csv")

schema2 = StructType([
    StructField("ID", IntegerType(), True),
    StructField("project_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("main_category", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("deadline", TimestampType(), True),
    StructField("goal", IntegerType(), True),
    StructField("launched", TimestampType(), True),
    StructField("pledged", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("backers", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("usd pledged", FloatType(), True),
    StructField("usd_goal_real", FloatType(), True)
    ])

df1 = spark.read.format("csv").option("header","true").option("delimiter", ",").schema(schema2).load("ks-projects-201801.csv")

#df = spark.read.format("csv").option("header","true").load("ks-projects-201612.csv")
#df.show()

df.createOrReplaceTempView("ksprojects");
df1.createOrReplaceTempView("ksprojects1");


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

#aggregate functions
print("query using aggregate functions")
dfq4 = spark.sql("select max(backers) as max_backers,min(backers) as min_backers from ksprojects ")
print(dfq4.show())

#join query
print("join query implementation")
dfq5 = spark.sql("select count(*) as no_of_projects from ksprojects ks join ksprojects1 ks1 on ks.project_name = ks1.project_name ")
print(dfq5.show())

#pattern recognition query
print("pattern recognition query")
dfq6 = spark.sql("select category from ksprojects where category like '%ar%' or category like '%ra%'")
print(dfq6.show())

#range selection query
print("Range query")
dfq7 = spark.sql("select project_name,goal from ksprojects where goal between 1000 and 10000")
print(dfq7.show())

#multiple conditions using in operator
print("IN operator")
dfq8 = spark.sql("select project_name,category,country from ksprojects where country in ('CA','AU')")
print(dfq8.show())

#union
print("Union implementation")
dfq9 = spark.sql("select count(*) from (select * from ksprojects union all select * from ksprojects)q1")
print(dfq9.show())

#right join query
print("right join query")
dfq10 = spark.sql("select ks.project_name,ks1.usd_goal_real from ksprojects ks right join ksprojects1 ks1 on ks.project_name = ks1.project_name ")
print(dfq10.show())