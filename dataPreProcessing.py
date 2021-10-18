import json
from pyspark.sql.functions import col, lower, regexp_replace, split, udf, trim
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from nltk.stem import WordNetLemmatizer
import numpy as np
# nltk.download('wordnet')
from pyspark.sql.readwriter import DataFrameWriter


def preprocessData():
    my_spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb+srv://Capstone:Capstone@capstone.itrtq.mongodb.net/Capstone.newsCollection") \
        .config("spark.mongodb.output.uri", "mongodb+srv://Capstone:Capstone@capstone.itrtq.mongodb.net/Capstone.newsCollection") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .getOrCreate()

    df = my_spark.read.format("mongo").load()
    df.show()

    # na.drop(),  drops the entire row, where there is a null value present
    df = df.na.drop()

    def clean_text(c):
        c = lower(c)
        c = regexp_replace(c, "^rt ", "")
        c = regexp_replace(c, "(https?\://)\S+", "")
        c = regexp_replace(c, "[^a-zA-Z0-9\\s]", "")
        c = regexp_replace(c, "[0-9]", "")
        # c = split(c, "\\s+") tokenization...
        return c

    # Clear text Punctuation
    df = df.select(clean_text(col("title")).alias("title"), clean_text(col("summary")).alias("summary"),
                   clean_text(col("category")).alias("category"))


    # Tokenize title and Summary
    tokenizer = Tokenizer(inputCol='title', outputCol='title_token')
    df = tokenizer.transform(df).select('title_token', 'summary', 'category')

    tokenizer = Tokenizer(inputCol='summary', outputCol='summary_token')
    df = tokenizer.transform(df).select('title_token', 'summary_token', 'category')


    # Remove stop words
    remover = StopWordsRemover(inputCol='title_token', outputCol='title')
    df = remover.transform(df).select('title', 'summary_token', 'category')

    remover = StopWordsRemover(inputCol='summary_token', outputCol='summary')
    df = remover.transform(df).select('title', 'summary', 'category')
    # df.show(10, truncate=False)


    # lemmatization on text
    lemmatizer = WordNetLemmatizer()
    df = df.select(lemmatizer.lemmatize('title'), lemmatizer.lemmatize('summary'), 'category')


    #Save DataFrame into a MongoDB
    df.write.format("mongo").mode("append").option("database","Capstone").option("collection", "newsDataFrame").save()
    df.show(10, truncate=False)





preprocessData()