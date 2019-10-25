CREATE SCHEMA IF NOT EXISTS findopendata;

/* Create extensions used.
 */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";


/* ============ FINDOPENDATA.COM TABLES ===============
 */

/* The allowed original hosts to be indexed on findopendata.com
 */
CREATE TABLE IF NOT EXISTS findopendata.original_hosts (
    original_host text PRIMARY KEY,
    display_name text,
    region text,
    enabled boolean default false not null
);
INSERT INTO findopendata.original_hosts (original_host, display_name, region, enabled) VALUES
('open.canada.ca/data/en', 'Canadian Open Data', 'Canada', true),
('data.gov.uk', 'UK Open Data', 'United Kingdoms', true)
ON CONFLICT DO NOTHING;

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
    -- Number of associated package files
    num_files integer NOT NULL,
    -- The added time of this data package to the metadata table.
    added timestamp default current_timestamp,
    -- The last updated time of this data pacakge in the metadata table.
    updated timestamp default current_timestamp,

    ----- Fields extracted from raw metadata -----
    -- The creation time of this data package by original publisher
    created timestamp,
    -- The last modified time of this data package by the original publisher.
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
CREATE INDEX IF NOT EXISTS packages_fts_idx ON findopendata.packages USING gin (to_tsvector('english', title || organization_display_name || description));
CREATE INDEX IF NOT EXISTS packages_fts_title_idx ON findopendata.packages USING gin (to_tsvector('english', title));
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
    
    -- The time when this package file is added to this table.
    added timestamp default current_timestamp,
    -- The last updated time when this package is updated in this table.
    updated timestamp default current_timestamp,

    -- The created time of this package file by the original publisher.
    created timestamp,
    -- The updated time of this package file by the original publisher.
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
    raw_metadata jsonb NOT NULL,

    -- The column names of this package file.
    column_names text[],
    -- The column sketch IDs of this package file, in the same order as the column_names.
    column_sketch_ids uuid[],
    -- The sample of records of this package file in JSON.
    sample jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS package_files_crawler_idx ON findopendata.package_files(crawler_table, crawler_key);
CREATE UNIQUE INDEX IF NOT EXISTS package_files_idx ON findopendata.package_files(id);
CREATE INDEX IF NOT EXISTS package_files_package_key_idx ON findopendata.package_files(package_key);

