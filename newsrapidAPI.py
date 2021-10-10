import requests
from requests import Response
from kafka import KafkaProducer
from time import sleep
import time
import sys
import json

def getNews(title):
    url = "https://free-news.p.rapidapi.com/v1/search"
    querystring = {"q": title, "lang": "en", "page": "1", "page_size": "25"}
    headers = {
        'x-rapidapi-key': "3b1d7a3252mshc8b5142455609abp1c8c1ejsn164eebb8369c",
        'x-rapidapi-host': "free-news.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)
    response = response.json()

    def json_serializer(newsDist):
        return json.dumps(newsDist).encode("utf-8")
    newsDist = {}
    i=0
    for i in range(len(response['articles'])):
        newsDist.update(title=response['articles'][i]['title'], date=response['articles'][i]['published_date'],
                    summary=response['articles'][i]['summary'], category=response['articles'][i]['topic'],
                    source=response['articles'][i]['link'])
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
        producer.send('news', json.dumps(newsDist))
        i=i+1
        time.sleep(5)
        print (json.dumps(newsDist))

