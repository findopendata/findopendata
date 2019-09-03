import os

import psycopg2
from psycopg2.extras import Json, RealDictCursor
from celery.utils.log import get_task_logger
import spacy
from bs4 import BeautifulSoup

from .celery import app
from .storage import CloudStorageBucket


logger = get_task_logger(__name__)

# Load Spacy NLP model
# TODO: handle different languages other than English.
nlp = spacy.load("en_core_web_sm")


@app.task(ignore_result=True)
def index_ckan_package(
        crawler_package_key, 
        package_blob_name, 
        endpoint, 
        endpoint_name, 
        endpoint_region,
        bucket_name):
    """Register the crawled CKAN package into the centralized packages table for
    all types of packages to make it searchable. The following processes are
    performed:
        1. Extract metadata such as title an description from the raw package
            JSON file.
        2. Create the fulltext search doc for keyword search.
        3. Extract named entities from the title and description for
            metadata enrichment.
        4. Submit separate jobs for indexing ckan package files and
            sketching.
    """
    # Get package metadata
    storage = CloudStorageBucket(bucket_name)
    package = storage.get_object(package_blob_name)

    # Get connection.
    conn = psycopg2.connect("")
    # Get CKAN resources
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""SELECT key, filename, resource_blob, file_size, raw_metadata
            FROM findopendata.ckan_resources where package_key = %s""",
            (crawler_package_key,))
    resources = [row for row in cur]

    # Parse package metadata
    created = package.get("metadata_created", None)
    modified = package.get("metadata_modified", None)
    title = BeautifulSoup(package.get("title", "")).get_text()
    title_spacy = nlp(title).to_json()
    name = package.get("name", None)
    description = BeautifulSoup(package.get("notes", "")).get_text()
    description_spacy = nlp(description).to_json()
    tags = [tag["name"] for tag in package.get("tags", []) if "name" in tag]
    license_title = package.get("license_title", None)
    license_url = package.get("license_url", None)
    organization_display_name = package.get("organization", {})\
            .get("title", None)
    organization_name = package.get("organization", {}).get("name", None)
    if organization_name is not None:
        organization_name = "{}:{}".format(endpoint, organization_name)
    organization_image_url = package.get("organization", {})\
            .get("image_url", None)
    organization_description = package.get("organization", {})\
            .get("description", None)
    raw_metadata = package

    crawler_table = "ckan_packages"
    crawler_key = crawler_package_key
    original_host = endpoint
    original_host_display_name = endpoint_name
    original_host_region = endpoint_region
    num_files = len(resources)
    fts_doc = " ".join(s for s in [title, description] + tags if s is not None)

    # Save package
    cur.execute("""INSERT INTO findopendata.packages
            (
                crawler_table, 
                crawler_key, 
                id, 
                original_host, 
                original_host_display_name,
                original_host_region,
                num_files, 
                fts_doc,
                created, 
                modified, 
                title, 
                title_spacy,
                name,
                description, 
                description_spacy,
                tags, 
                license_title,
                license_url, 
                organization_display_name,
                organization_name,
                organization_image_url,
                organization_description,
                raw_metadata
            )
            VALUES (
                %s, %s, uuid_generate_v1mc(), %s, %s, %s, %s, to_tsvector(%s),
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (crawler_table, crawler_key) DO UPDATE SET
            original_host = EXCLUDED.original_host,
            original_host_display_name = EXCLUDED.original_host_display_name,
            original_host_region = EXCLUDED.original_host_region,
            num_files = EXCLUDED.num_files,
            fts_doc = EXCLUDED.fts_doc,
            created = EXCLUDED.created,
            modified = EXCLUDED.modified,
            title = EXCLUDED.title,
            title_spacy = EXCLUDED.title_spacy,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            description_spacy = EXCLUDED.description_spacy,
            tags = EXCLUDED.tags,
            license_title = EXCLUDED.license_title,
            license_url = EXCLUDED.license_url,
            organization_display_name = EXCLUDED.organization_display_name,
            organization_name = EXCLUDED.organization_name,
            organization_image_url = EXCLUDED.organization_image_url,
            organization_description = EXCLUDED.organization_description,
            raw_metadata = EXCLUDED.raw_metadata
            RETURNING key;""",
            (
                crawler_table, 
                crawler_key, 
                original_host, 
                original_host_display_name,
                original_host_region,
                num_files, 
                fts_doc,
                created, 
                modified, 
                title, 
                Json(title_spacy),
                name,
                description, 
                Json(description_spacy),
                tags, 
                license_title,
                license_url, 
                organization_display_name,
                organization_name,
                organization_image_url,
                organization_description,
                Json(raw_metadata),
            ))
    package_key = cur.fetchone()["key"]
    # Commit all changes.
    conn.commit()
    conn.close()
    logger.info("Indexed CKAN package {} and {} package files".format(
        crawler_package_key, len(resources)))

    # Submit jobs for indexing package files.
    for resource in resources:
        index_ckan_package_file(crawler_resource_key=resource["key"],
                package_key=package_key,
                bucket_name=bucket_name,
                blob_name=resource["resource_blob"],
                filename=resource["filename"],
                file_size=resource["file_size"],
                raw_metadata=resource["raw_metadata"])


@app.task(ignore_result=True)
def index_ckan_package_file(
        crawler_resource_key, 
        package_key, 
        bucket_name,
        blob_name, 
        filename, 
        file_size, 
        raw_metadata):
    """Register the CKAN resource into the package_files table by doing the
    following:
        1. Extract metadata such as name and description from the source JSON.
        2. [TODO] Detect if this package file is well-formated.
        3. [TODO] Extract column headers and data types of columns.
        4. [TODO] Extract entities and word embeddings from column headers
            for metadata enrichment.

    Args:
        crawler_resource_key: the primary key of the ckan_resources table.
        package_key: the primary key of the packages table.
        bucket_name: the Cloud Storage bucket storing the blob.
        blob_name: the relative path to the blob of this package file.
        filename: the UNIX-friendly filename.
        file_size: the file size in bytes.
        raw_metadata: the 'resource' extracted from the CKAN's package JSON
            corresponding to this package file.
    """
    # Extract metadata from raw_metadata
    crawler_table = "ckan_resources"
    created = raw_metadata.get("created", None)
    modified = raw_metadata.get("last_modified", None)
    name = raw_metadata.get("name", None)
    description = BeautifulSoup(raw_metadata.get("description", "")).get_text()
    description_spacy = nlp(description).to_json()
    original_url = raw_metadata.get("url", None)
    file_format = raw_metadata.get("format", None)

    # Get connection.
    conn = psycopg2.connect("")
    # Get CKAN resources
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""INSERT INTO findopendata.package_files
            (
                package_key, 
                crawler_table, 
                crawler_key, 
                id, 
                created, 
                modified,
                filename, 
                name, 
                description, 
                description_spacy,
                original_url,
                format, 
                file_size, 
                blob_name,
                raw_metadata
            ) VALUES
            (%s, %s, %s, uuid_generate_v1mc(), %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s) ON CONFLICT (crawler_table, crawler_key) DO UPDATE SET
            created = EXCLUDED.created,
            modified = EXCLUDED.modified,
            filename = EXCLUDED.filename,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            description_spacy = EXCLUDED.description_spacy,
            original_url = EXCLUDED.original_url,
            format = EXCLUDED.format,
            file_size = EXCLUDED.file_size,
            blob_name = EXCLUDED.blob_name,
            raw_metadata = EXCLUDED.raw_metadata
            RETURNING key;""",
            (
                package_key, 
                crawler_table, 
                crawler_resource_key, 
                created, 
                modified,
                filename, 
                name, 
                description, 
                Json(description_spacy),
                original_url,
                file_format, 
                file_size, 
                blob_name,
                Json(raw_metadata)
            ))
    # Commit all changes.
    conn.commit()
    conn.close()
    logger.info("Indexed CKAN resource {} into package_files".format(
        crawler_resource_key))
