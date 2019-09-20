CREATE TEMP VIEW v AS (
    SELECT row_to_json(sketches) FROM (
        SELECT 
            s.id as column_id, 
            s.column_name as column_name,
            s.sample as sample,
            s.count as count,
            s.empty_count as empty_count,
            s.numeric_count as numeric_count,
            s.distinct_count as approx_distinct_count,
            s.minhash as minhash,
            s.seed as seed,
            f.id as table_id,
            f.original_url as table_original_url,
            f.format as table_format,
            f.file_size as table_file_size,
            f.blob_name as table_blob_name,
            p.id as package_id,
            p.title as package_title
        FROM
            findopendata.column_sketches as s,
            findopendata.package_files as f,
            findopendata.packages as p
        WHERE
            s.package_file_key = f.key
            AND f.package_key = p.key
        LIMIT 10
    ) sketches
);
\COPY (SELECT * FROM v) TO PROGRAM 'gzip > column_sketches.jsonl.gz';
DROP VIEW v;