import requests


def socrata_records(resource_url, app_token, limit=25000):
    """Reading records from given resource URL.

    Args:
        resource_url: the Socrata dataset API.
        app_token: the Socrata dataset access credential.
        limit (defautl 25000): the pagination limit used for reading the
            Socrata dataset API.
    """
    def _call_api(resource_url, app_token, limit, offset):
        if app_token is not None:
            resp = requests.get(resource_url,
                    params={
                        r"$order" : r":id",
                        r"$offset" : offset,
                        r"$limit" : limit},
                    headers={
                        "X-App-Token" : app_token})
            if resp.status_code == 403:
                # Re-try without app token
                return _call_api(resource_url, None, limit, offset)
        else:
            resp = requests.get(resource_url,
                    params={
                        r"$order" : r":id",
                        r"$offset" : offset,
                        r"$limit" : limit})
        if not resp.ok:
            resp.raise_for_status()
        records = resp.json()
        return records

    # Main loop
    offset = 0
    while True:
        records = _call_api(resource_url, app_token, limit, offset)
        offset += limit
        if len(records) == 0:
            return
        for record in records:
            yield record
