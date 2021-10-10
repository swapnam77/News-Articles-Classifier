from typing import Any

from kafka import KafkaConsumer
from json import loads
import json
import time
from pymongo import MongoClient

consumer = KafkaConsumer('news', bootstrap_servers=['localhost:9092'])
client = MongoClient("mongodb://localhost:27017/")
mydb = client["Capstone"]
mycoll = mydb["newsCollection"]

for message in consumer:
    record = json.loads(message.value)
    news_record=json.loads(record)
    mycoll.insert_one(news_record)
    print (news_record)

