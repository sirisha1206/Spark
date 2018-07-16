import os
os.environ["SPARK_HOME"] = "C:\\Spark\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\Users\\Sirisha Sunkara\\Desktop\\Summer18\\Spark\\Module2\\winutils"
from pyspark import SparkContext

def mapper(line):
    line = line.split("->")
    user = line[0]
    friends = line[1]
    friends1= friends.split(',')
    keys = []

    for friend in friends1:
        if(user < friend):
            keys.append((''.join(user+' '+friend), friends))
        else:
            keys.append((''.join(friend + ' ' + user), friends))
    return keys


def reducer(key, value):
    routput = ''
    for friend in key:
        if friend in value:
            routput += friend
    return routput

if __name__ == "__main__":

    sc = SparkContext.getOrCreate()
    Lines = sc.textFile("C:\\Users\Sirisha Sunkara\Desktop\Summer18\Spark\\7-11\Pyspark\Pyspark\\sample2.txt", 1)
    Line = Lines.flatMap(mapper)
    Line.saveAsTextFile("Mapper")
    mutualFriends = Line.reduceByKey(reducer)
    mutualFriends.coalesce(1).saveAsTextFile("mutualFriends")
    sc.stop()