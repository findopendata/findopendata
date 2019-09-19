CREATE SCHEMA IF NOT EXISTS findopendata;


/* Create extensions used.
 */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


/* ============ COLUMN SKETCH TABLES ===============
 */
/* The metadata table for all column sketches for finding joinable tables.
 */
CREATE TABLE IF NOT EXISTS findopendata.column_sketches (
    key serial PRIMARY KEY,
    package_file_key serial NOT NULL REFERENCES findopendata.package_files(key),

    -- The unique ID of this column; used publicly.
    id uuid NOT NULL,

    -- The created time of this column sketch.
    added timestamp default current_timestamp,
    -- The updated time of this column sketch.
    modified timestamp default current_timestamp,

    -- The name of this column.
    column_name text NOT NULL,
    -- A sample of data values.
    sample text[],
    -- The number of rows.
    count int,
    -- The number of empty rows.
    empty_count int,
    -- The number of non-empty and out-of-vocabulary rows.
    out_of_vocabulary_count int,
    -- The number of non-empty and numerical rows.
    numeric_count int,
    -- The approximate distinct count of non-empty rows.
    distinct_count int,
    -- The word embedding vector of the column name.
    word_vector_column_name real[],
    -- The mean word embedding vector of the data values.
    word_vector_data real[],
    -- The MinHash sketch of this column.
    minhash bigint[],
    -- The random seed used to generate the MinHash sketch
    seed bigint,
    -- The HyperLogLog registers of this column.
    hyperloglog int[]
);
CREATE UNIQUE INDEX IF NOT EXISTS column_sketches_column_name_idx ON findopendata.column_sketches(package_file_key, column_name);
CREATE UNIQUE INDEX IF NOT EXISTS column_sketches_idx ON findopendata.column_sketches(id);

