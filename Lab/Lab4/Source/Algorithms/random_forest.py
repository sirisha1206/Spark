from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.sql import SparkSession
import os
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jre1.8.0_161"
os.environ["SPARK_HOME"] = "C:\\Spark\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils"
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

data = spark.read.load("C:\\test_data.csv", format="csv", header=True, delimiter=",")
data = data.withColumn("AGE_FACTOR", data['age'] - 0).withColumn("Area", data['Area'] - 0).withColumn("ID", data["induration_diameter"] - 0).withColumn("label", data['sex'] - 0)
# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
assem = VectorAssembler(inputCols=["AGE_FACTOR", "Area", "ID"], outputCol='features')
data = assem.transform(data)
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# Set maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])
# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)
# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])
# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
# Make predictions.
predictions = model.transform(testData)
# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy:")
print(accuracy)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[2]
print(rfModel)  # summary only