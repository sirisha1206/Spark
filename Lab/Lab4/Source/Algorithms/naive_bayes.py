import os
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jre1.8.0_161"
os.environ["SPARK_HOME"] = "C:\\Spark\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils"

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
from pyspark.ml.feature import VectorAssembler

from pyspark.python.pyspark.shell import spark

data = spark.read.load("C:\\test_data.csv", format="csv", header=True, delimiter=",")
data = data.withColumn("AGE_FACTOR", data['age'] - 0).withColumn("Area", data['Area'] - 0).withColumn("ID", data["induration_diameter"] - 0).withColumn("label", data['sex'] - 0)
data.show(100)
assem = VectorAssembler(inputCols=["AGE_FACTOR", "Area", "ID"], outputCol='features')
data = assem.transform(data)
# Split the data into train and test
splits = data.randomSplit([0.7, 0.3], 1234)
train = splits[0]
test = splits[1]
# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
# train the model
model = nb.fit(train)
# select example rows to display.
predictions = model.transform(test)
predictions.show(100)
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",                                           metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))