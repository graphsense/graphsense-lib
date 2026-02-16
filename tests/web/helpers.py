def get_json(client, path, **kwargs):
    """GET path (with {kwarg} substitution), assert 200, return JSON."""
    auth = kwargs.pop("auth", "x")
    headers = {"Accept": "application/json", "Authorization": auth}
    response = client.get(path.format(**kwargs), headers=headers)
    assert response.status_code == 200, f"Expected 200: {response.text}"
    return response.json()


def raw_request(client, path, body=None, **kwargs):
    """Request without status assertion. Returns (status_code, body_text)."""
    auth = kwargs.pop("auth", "x")
    headers = {"Accept": "application/json", "Authorization": auth}
    method = "GET" if body is None else "POST"
    response = client.request(
        method, path.format(**kwargs), json=body, headers=headers
    )
    return response.status_code, response.text


def request_with_status(client, path, expected_status, body=None, **kwargs):
    """Request, assert status code, return JSON or None for non-200."""
    auth = kwargs.pop("auth", "x")
    headers = {"Accept": "application/json", "Authorization": auth}
    method = "GET" if body is None else "POST"
    response = client.request(
        method, path.format(**kwargs), json=body, headers=headers
    )
    assert response.status_code == expected_status, (
        f"Expected {expected_status}: {response.text}"
    )
    if expected_status != 200:
        return None
    return response.json()


def assert_equal_sorted(a, b, *keys):
    """Compare dicts after sorting nested list found by traversing keys."""
    keys = iter(keys)
    key = next(keys)
    pa = a
    pb = b
    aa = a[key]
    bb = b[key]
    while not isinstance(aa, list):
        key = next(keys)
        pa = aa
        pb = bb
        aa = aa[key]
        bb = bb[key]
    listkey = next(keys)

    def fun(x):
        return x[listkey]

    pa[key] = sorted(pa[key], key=fun)
    pb[key] = sorted(pb[key], key=fun)

    assert a == b
