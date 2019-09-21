import os

from flask import Flask, render_template, jsonify, request, abort, Response
import flask.json as json
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datasketch import LeanMinHash, MinHashLSH

import settings


# Flask app.
app = Flask(__name__)


# When deployed to App Engine, 
# the `GAE_SERVICE` environment variable will be set.
if os.environ.get('GAE_SERVICE') == 'lshserver':
    configs = settings.from_datastore("Settings")
    # If deployed, use the local socket interface for accessing Cloud SQL
    db_host = '/cloudsql/{}'.format(configs.get("CLOUD_SQL_CONNECTION_NAME"))
    db_user = configs.get("CLOUD_SQL_USERNAME")
    db_password = configs.get('CLOUD_SQL_PASSWORD')
    db_name = configs.get('CLOUD_SQL_DATABASE_NAME')
    bucket_name = configs.get('BUCKET_NAME')
    minhash_lsh_threshold = configs.get("MINHASH_LSH_THRESHOLD")
    minhash_size = configs.get("MINHASH_SIZE")
    db_config = {
        'user': db_user,
        'password': db_password,
        'dbname': db_name,
        'host': db_host
    }
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
    index_config = configs.get("index")
    minhash_lsh_threshold = index_config.get("minhash_lsh_threshold")
    minhash_size = index_config.get("minhash_size")
    # All all domains for local server. 
    CORS(app, resources={r"/lsh/*": {"origins": "*"}})


# Build MinHash LSH index.
print("Start building MinHash LSH index")
lsh = MinHashLSH(threshold=minhash_lsh_threshold, num_perm=minhash_size)
conn = psycopg2.connect(**db_config)
cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute(r"""SELECT id, minhash, seed
                FROM findopendata.column_sketches
                """)
for row in cur:
    minhash = LeanMinHash(seed=row["seed"], hashvalues=row["minhash"])
    lsh.insert(row["id"], minhash)
cur.close()
conn.close()
print("Finished building MinHash LSH index")


@app.route('/lsh/query', methods=['POST'])
def query():
    query_body = request.json
    seed = query_body.get("seed")
    hashvalues = query_body.get("hashvalues")
    try:
        minhash = LeanMinHash(seed=seed, hashvalues=hashvalues)
    except ValueError as e:
        app.logger.error("Error in query body: {}".format(e))
        abort(400)
    column_ids = lsh.query(minhash)
    return jsonify(column_ids)


if __name__ == '__main__':
    dev_configs = configs.get("lshserver_local")
    host = dev_configs.get("host")
    port = dev_configs.get("port")
    app.run(host=host, port=port, debug=True)

