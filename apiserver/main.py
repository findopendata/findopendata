import os

from flask import Flask, render_template, jsonify, request, abort, Response
import flask.json as json
from flask_cors import CORS
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from gcsfs.core import GCSFileSystem
import requests
from datasketch import LeanMinHash

import settings
import storage


# Flask app.
app = Flask(__name__)


# When deployed to App Engine, 
# the `GAE_ENV` environment variable will be set.
if os.environ.get('GAE_SERVICE') == 'apiserver':
    configs = settings.from_datastore("Settings")
    # If deployed, use the local socket interface for accessing Cloud SQL
    db_host = '/cloudsql/{}'.format(configs.get("CLOUD_SQL_CONNECTION_NAME"))
    db_user = configs.get("CLOUD_SQL_USERNAME")
    db_password = configs.get('CLOUD_SQL_PASSWORD')
    db_name = configs.get('CLOUD_SQL_DATABASE_NAME')
    bucket_name = configs.get('BUCKET_NAME')
    db_config = {
        'user': db_user,
        'password': db_password,
        'dbname': db_name,
        'host': db_host
    }
    gcsfs_token = "cloud"
    lshserver_endpoint = "https://findopendata.com/lsh"
else:
    # If running locally, use the TCP connections instead
    # Set up Cloud SQL Proxy (cloud.google.com/sql/docs/mysql/sql-proxy)
    # so that your application can use 127.0.0.1:3306 to connect to your
    # Cloud SQL instance
    configs = settings.from_yaml(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        os.path.pardir,
        "configs.yaml"))
    db_config = configs.get("postgres")
    gcp_config = configs.get("gcp")
    bucket_name = gcp_config.get("bucket_name")
    gcsfs_token = gcp_config.get("service_account_file")
    lshserver_dev_configs = configs.get("lshserver_local")
    lshserver_endpoint = "http://{}:{}/lsh".format(
        lshserver_dev_configs.get("host"),
        lshserver_dev_configs.get("port"))
    # All all domains for local server. 
    CORS(app, resources={r"/api/*": {"origins": "*"}})


# Postgres connection pool.
cnxpool = ThreadedConnectionPool(minconn=1, maxconn=3, **db_config)


# Cloud Storage filesystem interface. 
fs = GCSFileSystem(access="read_only", token=gcsfs_token, check_connection=True)


def _execute_get_package_brief(cur, package_id=None, package_key=None):
    sql = r"""SELECT 
                key, 
                id, 
                original_host, 
                original_host_display_name, 
                original_host_region,
                title
            FROM findopendata.packages """
    if package_key is not None:
        sql += r" WHERE key = %s "
        args = (str(package_key),)
    elif package_id is not None:
        sql += r" WHERE id = %s "
        args = (str(package_id),)
    else:
        raise ValueError("Missing package_id and package_key")
    cur.execute(sql, args)


def _execute_get_package_detailed(cur, package_id=None, package_key=None):
    sql = r"""SELECT 
                key, 
                id, 
                original_host, 
                original_host_display_name, 
                original_host_region,
                num_files,
                created, 
                modified, 
                title, 
                name,
                description,
                tags,
                license_title,
                license_url,
                organization_name, 
                organization_display_name, 
                organization_image_url,
                organization_description
            FROM findopendata.packages """
    if package_key is not None:
        sql += r" WHERE key = %s "
        args = (str(package_key),)
    elif package_id is not None:
        sql += r" WHERE id = %s "
        args = (str(package_id),)
    else:
        raise ValueError("Missing package_id and package_key")
    cur.execute(sql, args)


def _execute_keyword_search(cur, query, original_hosts=[], limit=50):
    sql = r"""SELECT 
                id, 
                original_host, 
                original_host_display_name, 
                original_host_region,
                num_files,
                created, 
                modified, 
                title, 
                description,
                tags,
                organization_name, 
                organization_display_name, 
                organization_image_url
            FROM findopendata.packages, plainto_tsquery(%s) query
            WHERE query @@ fts_doc"""
    if original_hosts:
        sql += r" AND original_host in %s "
    sql += r" ORDER BY ts_rank_cd(fts_doc, query) DESC LIMIT %s;"
    if original_hosts:
        cur.execute(sql, (query, original_hosts, limit))
    else:
        cur.execute(sql, (query, limit))


def _execute_similar_packages(cur, ids, original_hosts=[], limit=50):
    sql = r"""SELECT 
                r.id, 
                r.original_host, 
                r.original_host_display_name, 
                r.original_host_region,
                r.num_files,
                r.created, 
                r.modified, 
                r.title, 
                r.description,
                r.tags,
                r.organization_name, 
                r.organization_display_name, 
                r.organization_image_url,
                similarity(q.title, r.title) as title_similarity, 
                similarity(q.description, r.description) as description_similarity
            FROM findopendata.packages as r,
            (
                SELECT key, title, description 
                FROM findopendata.packages 
                WHERE id IN %s
            ) as q
            WHERE 
            q.title %% r.title 
    """
    if original_hosts:
        sql += r" AND original_host in %s "
    sql += r" ORDER BY title_similarity DESC, description_similarity DESC LIMIT %s;"
    if original_hosts:
        cur.execute(sql, (ids, original_hosts, limit))
    else:
        cur.execute(sql, (ids, limit))


def _execute_get_column_sketches(cur, ids, original_hosts=[]):
    sql = r"""
            SELECT 
                c.id as id,
                c.seed,
                c.minhash,
                c.column_name,
                c.sample,
                f.id as package_file_id,
                f.created, 
                f.modified, 
                f.filename,
                f.name,
                f.description,
                f.original_url,
                f.format,
                p.id as package_id, 
                p.original_host, 
                p.original_host_display_name, 
                p.original_host_region,
                p.created as package_created, 
                p.modified as package_modified, 
                p.title as package_title, 
                p.description as package_description,
                p.tags,
                p.organization_name, 
                p.organization_display_name, 
                p.organization_image_url
            FROM
                findopendata.column_sketches as c,
                findopendata.package_files as f,
                findopendata.packages as p
            WHERE 
                c.package_file_key = f.key
                AND f.package_key = p.key
                AND c.id IN %s
    """
    if original_hosts:
        sql += r" AND original_host in %s "
        cur.execute(sql, (ids, original_hosts))
    else:
        cur.execute(sql, (ids,))


_original_hosts = []
cnx = cnxpool.getconn()
with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
    cursor.execute(r"""SELECT DISTINCT 
                    original_host, 
                    original_host_display_name, 
                    original_host_region
                FROM findopendata.packages 
                ORDER BY original_host""")
    _original_hosts = [row for row in cursor.fetchall()]
cnxpool.putconn(cnx)


@app.route('/api/original-hosts', methods=['GET'])
def original_hosts():
    return jsonify(_original_hosts)


@app.route('/api/keyword-search', methods=['GET'])
def keyword_search():
    query = request.args.get('query', '')
    if query == '':
        return jsonify([])
    original_host_filter = tuple(request.args.getlist('original_host'))
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_keyword_search(cursor, query, original_host_filter)
        results = cursor.fetchall()
    cnxpool.putconn(cnx)
    return jsonify(results)


@app.route('/api/similar-packages', methods=['GET'])
def similar_packages():
    package_ids = tuple(request.args.getlist('id'))
    if len(package_ids) == 0:
        return jsonify([])
    original_host_filter = tuple(request.args.getlist('original_host'))
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_similar_packages(cursor, package_ids, original_host_filter)
        results = cursor.fetchall()
    cnxpool.putconn(cnx)
    return jsonify(results)


@app.route('/api/package/<uuid:package_id>', methods=['GET'])
def package(package_id):
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_get_package_detailed(cursor, package_id=package_id)
        package = cursor.fetchone()
    if package is None:
        cnxpool.putconn(cnx)
        abort(404)
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(r"""SELECT 
                    id, 
                    created, 
                    modified, 
                    filename, 
                    name,
                    description,
                    original_url,
                    format,
                    blob_name IS NOT NULL as available
                FROM findopendata.package_files
                WHERE package_key = %s""", (package["key"],))
        package_files = cursor.fetchall()
    cnxpool.putconn(cnx)
    # Remove key from output
    package.pop("key")
    return jsonify(package=package, package_files=package_files)


@app.route('/api/package-file/<uuid:file_id>', methods=['GET'])
def package_file(file_id):
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(r"""SELECT 
                    package_key, 
                    created, 
                    modified, 
                    filename, 
                    name,
                    description,
                    original_url,
                    format 
                FROM findopendata.package_files
                WHERE id = %s""", (str(file_id),))
        package_file = cursor.fetchone()
    if package_file is None:
        cnxpool.putconn(cnx)
        abort(404)
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_get_package_brief(cursor, package_key=package_file["package_key"])
        package = cursor.fetchone()
    cnxpool.putconn(cnx)
    if package is None:
        abort(404)
    # remove key from output
    package_file.pop("package_key")
    package.pop("key")
    return jsonify(package_file=package_file, package=package)


@app.route('/api/package-file-data/<uuid:file_id>', methods=['GET'])
def package_file_data(file_id):
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(r"""select format, blob_name
                from findopendata.package_files
                where id = %s""", (str(file_id),))
        package_file = cursor.fetchone()
        if package_file is None:
            abort(404)
    cnxpool.putconn(cnx)
    # load data file from blob
    nrows = request.args.get('nrows', default=20, type=int)
    try:
        headers, records = storage.read_package_file(fs, package_file["format"],
                bucket_name, package_file["blob_name"], nrows)
        return app.response_class(
                response=json.dumps({
                        "headers" : headers,
                        "records" : records,
                    }, ignore_nan=True),
                status=200,
                mimetype='application/json')
    except Exception as e:
        app.logger.error("Error in reading {}: {}".format(
            package_file["blob_name"], e))
        abort(500)


@app.route('/api/joinable-column-search', methods=['GET'])
def joinable_column_search():
    query_id = request.args.get('id', '')
    if query_id == '':
        return jsonify([])
    original_host_filter = tuple(request.args.getlist('original_host'))
    cnx = cnxpool.getconn()
    # Obtain the MinHash of the query.
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_get_column_sketches(cursor, (query_id,))
        query = cursor.fetchone()
    if query is None:
        # The query does not exist.
        cnxpool.putconn(cnx)
        abort(404)
    # Query the LSH Server.
    resp = requests.post(lshserver_endpoint+"/query",
            json={"seed": query["seed"], "hashvalues": query["minhash"]})
    if not resp.ok:
        app.logger.error("Error in querying the LSH server")
        cnxpool.putconn(cnx)
        abort(500)
    column_ids = resp.json()
    if len(column_ids) == 0:
        # Return empty result.
        cnxpool.putconn(cnx)
        return jsonify([])
    # Obtain the column sketches of the results.
    with cnx.cursor(cursor_factory=RealDictCursor) as cursor:
        _execute_get_column_sketches(cursor, tuple(column_ids),
                original_hosts=original_host_filter)
        column_sketches = cursor.fetchall()
    # Done with SQL.
    cnxpool.putconn(cnx)
    # Create the final query results.
    results = []
    query_minhash = LeanMinHash(seed=query["seed"], hashvalues=query["minhash"])
    for column in column_sketches:
        # Compute the similarities for each column in the result.
        jaccard = query_minhash.jaccard(LeanMinHash(
                seed=column["seed"], hashvalues=column["minhash"])) 
        column.pop("seed")
        column.pop("minhash")
        column["jaccard"] = jaccard
        results.append(column)
    results = sorted(results, key=lambda r : r["jaccard"], reverse=True)
    return jsonify(results)


if __name__ == '__main__':
    dev_configs = configs.get("apiserver_local")
    host = dev_configs.get("host")
    port = dev_configs.get("port")
    app.run(host=host, port=port, debug=True)
