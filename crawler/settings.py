import psycopg2

def get_settings():
    conn = psycopg2.connect("")
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM findopendata.crawler_settings")
    settings = dict()
    for key, value in cur:
        if value is None:
            raise ValueError("{} is not set".format(key))
        settings[key] = value
    cur.close()
    conn.close()
    return settings

