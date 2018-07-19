import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

os.environ["SPARK_HOME"] = "C:\\Spark\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\Sirisha Sunkara\\Desktop\\Summer18\\Spark\\Module2\\winutils"

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 1234)
# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
# Count each word in each batch
pairs = words.map(lambda word: (len(word), word))
wordCounts = pairs.reduceByKey(lambda x, y: x +" "+ y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate