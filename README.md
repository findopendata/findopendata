# Find Open Data

[![Build Status](https://travis-ci.org/findopendata/findopendata.svg?branch=master)](https://travis-ci.org/findopendata/findopendata)

![Screenshot](screencapture.gif)

Table of Content:
1. [Introduction](#introduction)
2. [System Overview](#system-overview)
3. [Development Guide](#development-guide)
4. [Cloud Storage Systems](#cloud-storage-systems)
5. [Crawler Guide](#crawler-guide)

## Introduction

This is the source code repository for [findopendata.com](https://findopendata.com).
The project goal is to make a search engine for Open Data with rich 
features beyond simple keyword search. The current search methods are:

* Keyword search based on metadata
* Similar dataset search based on metadata similarity
* Joinable table search based on content (i.e., data values) similarity using LSH index

Next steps:

 * Unionable/similar table search based on content similarity
 * Time and location-based serach based on extracted timestamps and Geo tags
 * Dataset versioning
 * API for external data science tools (e.g., Jupyter Notebook, Plot.ly)

**This is a work in progress.**


## System Overview

The Find Open Data system has the following components:

1. **Frontend**: a React app, located in `frontend`.
2. **API Server**: a Flask web server, located in `apiserver`.
3. **LSH Server**: a Go web server, located in `lshserver`.
4. **Crawler**: a set of [Celery](https://docs.celeryproject.org/en/latest/userguide/tasks.html) tasks, located in `crawler`. 

The Frontend, the API Server, and the LSH Server can be 
deployed to 
[Google App Engine](https://cloud.google.com/appengine/docs/).

We also use two external storage systems for persistence:

1. A PostgreSQL database for storing dataset registry, metadata, and sketches for content-based search.
2. A cloud-based storage system for storing dataset files, currently supporting Google Cloud Storage and Azure Blob Storage. A local storage using file system is also available.

![System Overview](system_overview.png)

## Development Guide

To develop locally, you need the following:

* PostgreSQL 9.6 or above
* RabbitMQ

#### 1. Install PostgreSQL

[PostgreSQL](https://www.postgresql.org/download/) 
(version 9.6 or above) is used by the crawler to register and save the
summaries of crawled datasets. It is also used by the API Server as the 
database backend.
If you are using Cloud SQL Postgres, you need to download 
[Cloud SQL Proxy](https://cloud.google.com/sql/docs/postgres/connect-admin-proxy#install)
and make it executable.

Once the PostgreSQL database is running, create a database, and
use the SQL scripts in `sql` to create tables:
```
psql -f sql/create_crawler_tables.sql
psql -f sql/create_metadata_tables.sql
psql -f sql/create_sketch_tables.sql
```

#### 2. Install RabbitMQ

[RabbitMQ](https://www.rabbitmq.com/download.html) 
is required to manage and queue crawl tasks.
On Mac OS X you can [install it using Homebrew](https://www.rabbitmq.com/install-homebrew.html).

Run the RabbitMQ server after finishing install.

#### 3. Python Environment

We use virtualenv for Python development and dependencies:
```
virtualenv -p python3 pyenv
pip install -r requirements.txt
```

`python-snappy` requires `libsnappy`. On Ubuntu you can 
simply install it by `sudo apt-get install libsnappy-dev`.
On Mac OS X use `brew install snappy`.
On Windows, instead of the `python-snappy` binary on Pypi, use the 
unofficial binary maintained by UC Irvine 
([download here](https://www.lfd.uci.edu/~gohlke/pythonlibs/)),
and install directly, for example (Python 3.7, amd64):
```
pip install python_snappy‑0.5.4‑cp37‑cp37m‑win_amd64.whl
```

#### 4. Configuration File

Create a `configs.yaml` by copying `configs-example.yaml`, complete fields
related to PostgreSQL and storage.

If you plan to store all datasets on your local file system,
you can skip the `gcp` and `azure` sections and only complete 
the `local` section, and make sure the `storage.provider` is 
set to `local`.

For cloud-based storage systems, see 
[Cloud Storage Systems](#cloud-storage-systems).

## Cloud Storage Systems

Currently we support using 
[Google Cloud Storage](https://cloud.google.com/storage/) and 
[Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) 
as the dataset storage system.

To use Google Cloud Storage, you need:
* A Google Cloud project with Cloud Storage enabled, and a bucket created.
* A Google Cloud service account key file (JSON formatted) with read and write access to the Cloud Storage bucket.
* Set `storage.provider` to `gcp` in `configs.yaml`.

To use Azure Blob Storage, you need:
* An Azure storage account enabled, and a blob storage container created.
* A connection string to access the storage account.
* Set `storage.provider` to `azure` in `configs.yaml`.

## Crawler Guide

The crawler has a set of [Celery](http://www.celeryproject.org/) tasks that 
runs in parallel.
It uses the RabbitMQ server to manage and queue the tasks.

### Setup Crawler

#### Data Sources (CKAN and Socrata APIs)

The crawler uses PostgreSQL to maintain all data sources.
CKAN sources are maintained in the table `findopendata.ckan_apis`.
Socrata Discovery APIs are maintained in the table 
`findopendata.socrata_discovery_apis`.
The SQL script `sql/create_crawler_tables.sql` has already created some 
initial sources for you.

To show the CKAN APIs currently available to the crawler and whether they
are enabled:
```sql
SELECT * FROM findopendata.ckan_apis;
```

To add a new CKAN API and enable it:
```sql
INSERT INTO findopendata.ckan_apis (endpoint, name, region, enabled) VALUES
('catalog.data.gov', 'US Open Data', 'United States', true);
```

#### Socrata App Tokens

Add your [Socrata app tokens](https://dev.socrata.com/docs/app-tokens.html) 
to the table `findopendata.socrata_app_tokens`.
The app tokens are required for harvesting datasets from Socrata APIs.

For example:
```sql
INSERT INTO findopendata.socrata_app_tokens (token) VALUES ('<your app token>');
```

### Run Crawler

[Celery workers](https://docs.celeryproject.org/en/latest/userguide/workers.html) 
are processes that fetch crawler tasks from RabbitMQ and execute them.
The worker processes must be started before starting any tasks.

For example:
```
celery -A crawler worker -l info -Ofair
```

On Windows there are some issues with using prefork process pool.
Use `gevent` instead:
```
celery -A crawler worker -l info -Ofair -P gevent
```

#### Harvest Datasets

Run `harvest_datasets.py` to start data harvesting tasks that download 
datasets from various data sources. Downloaded datasets will be stored on
a Google Cloud Storage bucket (set in `configs.yaml`), and registed in 
Postgres tables 
`findopendata.ckan_packages` and `findopendata.socrata_resources`.

#### Generate Metadata

Run `generate_metadata.py` to start metadata generation tasks for 
downloaded and registed datasets in 
`findopendata.ckan_packages` and `findopendata.socrata_resources`
tables.

It generates metadata by extracting titles, description etc. and 
annotates them with entities for enrichment.
The metadata is stored in table `findopendata.packages`, which is 
also used by the API server to serve the frontend.

#### Sketch Dataset Content

Run `sketch_dataset_content.py` to start tasks for creating 
sketches (e.g., 
[MinHash](https://github.com/ekzhu/datasketch),
samples, data types, etc.) of dataset
content (i.e., data values, columns, and records).
The sketches will be used for content-based search such as
finding joinable tables.

