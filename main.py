import newsrapidAPI
import newsAPIService

if __name__ == '__main__':
    title=["Finance","Sports","Politics","Religion","Education","Entertainment","Health","Business","cryptocurrency"]
    for i in title:
        print(i)
        newsrapidAPI.getNews(i)
        newsAPIService.getNews(i)


