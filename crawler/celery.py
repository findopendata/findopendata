import os

from celery import Celery

app = Celery("crawler",
             broker=os.getenv("CELERY_BROKER", "amqp://"),
            include=[
                 "crawler.ckan_crawler",
                 "crawler.socrata_crawler",
                 "crawler.curation",
                 "crawler.sketch",
                 "crawler.metadata",
                 ])


if __name__ == "__main__":
    app.start()
