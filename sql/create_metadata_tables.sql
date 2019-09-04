CREATE SCHEMA IF NOT EXISTS findopendata;

/* Create extensions used.
 */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


/* ============ FINDOPENDATA.COM TABLES ===============
 */
/* The metadata table for all data packages used by findopendata.com
 */
CREATE TABLE IF NOT EXISTS findopendata.packages (
    key serial PRIMARY KEY,

    ----- Fields assigned by ingestion process -----
    -- This set of attributes are used by the indexer to uniquely 
    -- identify this package.
    -- The name of the crawler table. e.g., ckan_packages, socrata_resources.
    crawler_table text NOT NULL,
    -- The primary key to the crawler table.
    crawler_key integer NOT NULL,
    -- The unique ID of this package used publicly.
    id uuid NOT NULL,
    -- The host name of the original publishing platform.
    original_host text,
    -- The display name of the original publishing platform.
    original_host_display_name text,
    -- The geographic region of the original host.
    original_host_region text,
    -- Number of associated package files
    num_files integer NOT NULL,
    -- The document for supporting full text search.
    fts_doc tsvector NOT NULL,

    ----- Fields extracted from raw metadata -----
    -- The creation time of this data package by original publisher
    created timestamp,
    -- The last modified time of this data package original publisher.
    modified timestamp,
    -- The title of this data package for display.
    title text,
    -- The JSON-serialized Spacy doc of title.
    title_spacy jsonb,
    -- The slug name of this data package.
    name text,
    -- The text description of this data package.
    description text,
    -- The JSON-serialized Spacy doc of the description.
    description_spacy jsonb,
    -- Tags associated with this package.
    tags text[],
    -- License name
    license_title text,
    -- License URL
    license_url text,
    -- Publishing organization display name
    organization_display_name text,
    -- Publishing organization slug name
    organization_name text,
    -- Publishing organization's image URL
    organization_image_url text,
    -- Publishing organization's description
    organization_description text,
    -- The original JSON metadata
    raw_metadata jsonb NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS package_crawler_idx ON findopendata.packages(crawler_table, crawler_key);
CREATE UNIQUE INDEX IF NOT EXISTS packages_idx ON findopendata.packages (id);
CREATE INDEX IF NOT EXISTS packages_fts_idx ON findopendata.packages USING gin (fts_doc);
CREATE INDEX IF NOT EXISTS packages_title_trgm_idx ON findopendata.packages USING GIN (title gin_trgm_ops);

/* The metadata table for all package files (datasets) used by findopendata.com.
 */
CREATE TABLE IF NOT EXISTS findopendata.package_files (
    key serial PRIMARY KEY,
    package_key serial NOT NULL REFERENCES findopendata.packages(key),

    -- This set of attributes are used by the indexer to uniquely 
    -- identify this package file.
    -- The name of the crawler table. e.g., ckan_resources, socrata_resources.
    crawler_table text NOT NULL,
    -- The primary key to the crawler table.
    crawler_key integer NOT NULL,

    -- The unique ID of this package file; used publicly.
    id uuid NOT NULL,

    -- The created time of this package file.
    created timestamp,
    -- The updated time of this package file.
    modified timestamp,

    -- The filename of this package file.
    filename text,
    -- The display name of this package file.
    name text,
    -- The description of this package file.
    description text,
    -- The JSON-serialized Spacy doc of description.
    description_spacy jsonb,
    -- The original URL where this package file can be downloaded.
    original_url text,
    -- The format of this package file. e.g. csv, jsonl
    format text,
    -- The file size of this package file.
    file_size bigint,
    
    -- The storage blob name of this package file.
    blob_name text,

    -- The raw metadata of this package file.
    raw_metadata jsonb NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS package_files_crawler_idx ON findopendata.package_files(crawler_table, crawler_key);
CREATE UNIQUE INDEX IF NOT EXISTS package_files_idx ON findopendata.package_files(id);
CREATE INDEX IF NOT EXISTS package_files_package_key_idx ON findopendata.package_files(package_key);
