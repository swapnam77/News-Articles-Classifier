from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, udf, trim, concat
import pyspark.ml.feature
from pyspark.ml.feature import Tokenizer,StopWordsRemover,CountVectorizer,IDF
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StringType
from pyspark import SparkContext
import pickle

classes = {0: "Finance", 1: "Sports", 2: "Politics",3: "Religion",4: "Education",5: "Entertainment",6: "Health",7: "Business",8: "cryptocurrency",9: "environment"}
r_classes = {y: x for x, y in classes.items()}

def load_model():
    global my_spark1
    my_spark1 = SparkSession \
        .builder \
        .appName("myApp1") \
        .config("spark.mongodb.input.uri",
                "mongodb+srv://Capstone:Capstone@capstone.itrtq.mongodb.net/Capstone.news") \
        .config("spark.mongodb.output.uri",
                "mongodb+srv://Capstone:Capstone@capstone.itrtq.mongodb.net/Capstone.news") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()

    # print(my_spark1)
    df = my_spark1.read.format("mongo").load()
    df = df.drop('_id')
    df = df.drop('date')
    df = df.drop('source')
    df = df.select((concat(col("summary"), col("title")).alias("description")), col("category").alias("category"))
    df = df.na.drop()
    # df.groupBy("category").count().show()

    # Stages For the Pipeline
    tokenizer = Tokenizer(inputCol='description', outputCol='mytokens')
    stopwords_remover = StopWordsRemover(inputCol='mytokens', outputCol='filtered_tokens')
    vectorizer = CountVectorizer(inputCol='filtered_tokens', outputCol='rawFeatures')
    idf = IDF(inputCol='rawFeatures', outputCol='vectorizedFeatures')

    # LabelEncoding/LabelIndexing
    labelEncoder = StringIndexer(inputCol='category', outputCol='label').fit(df)
    # labelEncoder.transform(df).show(5)
    df = labelEncoder.transform(df)

    ### Split Dataset
    (trainDF, testDF) = df.randomSplit((0.7, 0.3), seed=42)
    # trainDF.show()

    ### Estimator
    lr = LogisticRegression(featuresCol='vectorizedFeatures', labelCol='label')
    rf = RandomForestClassifier(numTrees=3, maxDepth=2, featuresCol='vectorizedFeatures', labelCol="label", seed=42)

    # Building the Pipeline
    global pipeline
    global pipeline1
    pipeline = Pipeline(stages=[tokenizer, stopwords_remover, vectorizer, idf, lr])
    pipeline1 = Pipeline(stages=[tokenizer, stopwords_remover, vectorizer, idf, rf])

    # Building Model
    global lr_model
    lr_model = pipeline.fit(trainDF)
    predictions = lr_model.transform(testDF)
    global rf_model
    rf_model = pipeline1.fit(trainDF)
    predictions1 = rf_model.transform(testDF)

    #Save Model
    #lr_model.save("./models")
    pickle.dump(lr_model, open("models/lr_model.pkl", "wb"))
    ### Model Evaluation
    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions1)
    print("Model accuracy", accuracy)


# function to predict the flower using the model
def predict(query_data):
    classes = {0: 'Business', 1: 'Entertainment', 2: 'Health', 3: 'Sports', 4: 'environment', 5: 'Finance',
              6: 'Religion', 7: 'cryptocurrency', 8: 'Politics', 9: 'Education'}
    x = list(query_data.dict().values())
    var1 = x[0]
    var2 = x[1]
    news = " ".join([var1, var2])
    ex1 = my_spark1.createDataFrame([(news,StringType())],["description"])
    category = lr_model.transform(ex1)
    result=category.select("prediction").collect()[0][0]
    return classes[result]

# function to retrain the model as part of the feedback loop
def retrain(data):
    x = list(data.dict().values())
    var1=x[0]
    var2=x[1]
    var3=x[2]
    description=" ".join([var1, var2])
    ex1=my_spark1.createDataFrame([(description,StringType())],["description"])
    ex2=my_spark1.createDataFrame([(var3,StringType())],["category"])
    retrain_data=ex1.join(ex2)
    retrain_data = retrain_data.drop('_2')
    retrain_data.show()
    lr_model = pipeline.fit(retrain_data)


#load_model()
