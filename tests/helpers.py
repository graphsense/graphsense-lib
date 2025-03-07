import copy


def vcr_strip_headers(response):
    resp = copy.deepcopy(response)
    # resp["headers"] = {}
    return resp


vcr_default_params = {
    "record_mode": "once",
    "serializer": "yaml",
    "match_on": ["method", "path", "query", "raw_body", "headers"],
    "filter_headers": ["authorization", "User-Agent"],
    "before_record_response": vcr_strip_headers,
}
