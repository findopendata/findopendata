import os

from celery import Celery

from .settings import celery_configs


app = Celery("findopendata",
        broker=celery_configs.get("broker", "amqp://"),
        include=[
                "findopendata.ckan_crawler",
                "findopendata.socrata_crawler",
                "findopendata.indexing",
                "findopendata.metadata",
                ])
app.conf.task_default_queue = celery_configs.get("queue")


if __name__ == "__main__":
    app.start()
