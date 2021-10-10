from newsapi import NewsApiClient
from kafka import KafkaProducer
import json
import time


def getNews(title):
    newsapi = NewsApiClient(api_key='934545bbdbc14506a2624791a03d6fb9')

    top_headlines = newsapi.get_top_headlines(q='business',
                                              category='business',
                                              language='en',
                                              country='us')

    def json_serializer(newsDist):
        return json.dumps(newsDist).encode("utf-8")

    newsDist = {}
    i=0
    for i in range(len(top_headlines['articles'])):
        newsDist.update(title=top_headlines['articles'][i]['title'],date=top_headlines['articles'][i]['publishedAt'],summary=top_headlines['articles'][i]['content'],category=title,source=top_headlines['articles'][i]['url'])
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
        producer.send('news', json.dumps(newsDist))
        time.sleep(5)
        print (json.dumps(newsDist))

