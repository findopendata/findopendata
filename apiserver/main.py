import os

from flask import Flask, render_template, jsonify, request, abort, Response
import flask.json as json
from flask_cors import CORS
import psycopg2.extras
import psycopg2.pool

import settings
import storage

db_user = settings.get('CLOUD_SQL_USERNAME')
db_password = settings.get('CLOUD_SQL_PASSWORD')
db_name = settings.get('CLOUD_SQL_DATABASE_NAME')
db_connection_name = settings.get('CLOUD_SQL_CONNECTION_NAME')
bucket_name = settings.get('BUCKET_NAME')

# When deployed to App Engine, the `GAE_ENV` environment variable will be
# set to `standard`
if os.environ.get('GAE_ENV') == 'standard':
    # If deployed, use the local socket interface for accessing Cloud SQL
    host = '/cloudsql/{}'.format(db_connection_name)
    allowed_origins = ["https://findopendata.com"]
else:
    # If running locally, use the TCP connections instead
    # Set up Cloud SQL Proxy (cloud.google.com/sql/docs/mysql/sql-proxy)
    # so that your application can use 127.0.0.1:3306 to connect to your
    # Cloud SQL instance
    host = '127.0.0.1'
    allowed_origins = ["http://localhost:3000"]

db_config = {
    'user': db_user,
    'password': db_password,
    'database': db_name,
    'host': host
}

cnxpool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=3,
                                               **db_config)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": allowed_origins}})


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


_original_hosts = []
cnx = cnxpool.getconn()
with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        _execute_similar_packages(cursor, package_ids, original_host_filter)
        results = cursor.fetchall()
    cnxpool.putconn(cnx)
    return jsonify(results)


@app.route('/api/package/<uuid:package_id>', methods=['GET'])
def package(package_id):
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        _execute_get_package_detailed(cursor, package_id=package_id)
        package = cursor.fetchone()
        if package is None:
            abort(404)
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
            abort(404)
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        _execute_get_package_brief(cursor, package_key=package_file["package_key"])
        package = cursor.fetchone()
        if package is None:
            abort(404)
    # remove key from output
    package_file.pop("package_key")
    package.pop("key")
    cnxpool.putconn(cnx)
    return jsonify(package_file=package_file, package=package)


@app.route('/api/package-file-data/<uuid:file_id>', methods=['GET'])
def package_file_data(file_id):
    cnx = cnxpool.getconn()
    with cnx.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
        headers, records = storage.read_package_file(package_file["format"],
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


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
