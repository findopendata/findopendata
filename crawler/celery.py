import os

from celery import Celery

from .settings import celery_configs

app = Celery("crawler",
        broker=celery_configs.get("broker", "amqp://"),
        include=[
                "crawler.ckan_crawler",
                "crawler.socrata_crawler",
                "crawler.curation",
                "crawler.indexing",
                "crawler.metadata",
                ])


if __name__ == "__main__":
    app.start()
