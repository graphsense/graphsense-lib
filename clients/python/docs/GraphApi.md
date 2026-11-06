# graphsense.GraphApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**convert_graph**](GraphApi.md#convert_graph) | **POST** /graph/convert | Convert a Pathfinder graph to another format, or lay it out (beta)
[**graph_compare**](GraphApi.md#graph_compare) | **POST** /graph/compare | Compare multiple transactions (beta)
[**graph_summary**](GraphApi.md#graph_summary) | **POST** /graph/summary | Summarize a set of transactions and/or addresses (beta)


# **convert_graph**
> bytearray convert_graph(request_body, to=to, tx_mode=tx_mode, layout=layout, include=include)

Convert a Pathfinder graph to another format, or lay it out (beta)

**BETA**: this endpoint is new and its contract may still change without a deprecation cycle. Takes a Pathfinder graph, either a .gs file (application/octet-stream) or the JSON payload the dashboard encodes (application/json), and returns it in the format `to`. `to=graphml` writes a GraphML document: every transaction is a node, every input or output touching an address of the graph an edge with its value in base units; the attribute keys (`gs:v0:*`) are experimental. `tx_mode=aggregate` writes one edge per sender, receiver and asset instead (total value, tx count, first/last timestamp, the txs); its value is exact or absent, never estimated: a UTXO tx with several sender addresses does not say who paid whom. `include=tags` adds the address-level attribution (no cluster tags), one lookup per address. `to=gs` returns a .gs file. `layout=auto` places every node afresh first, with senders left of their transaction. Each transaction is looked up once; a lookup that fails leaves that transaction without flow edges or direction and is reported in the X-Convert-Warnings header (count) and, for graph formats, in the document. The response is the file itself, named after the graph. At most 500 nodes and 4194304 bytes.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.convert_format import ConvertFormat
from graphsense.models.convert_layout import ConvertLayout
from graphsense.models.convert_tx_mode import ConvertTxMode
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.iknaio.com
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.iknaio.com"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.GraphApi(api_client)
    request_body = ['pathfinder', '1', 'first bitcoin transfer', [[['btc', '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'], 0, 0, True, 0], [['btc', '1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3'], 8, 0, False, 0]], [[['btc', 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16'], 4, 0, False, 0]], [], [[['btc', '12cbQLTFMXRnSzktFkuoG3eHoMeFtpTu3S'], ['btc', '1Q2TWHE3GMdB6BZKafqwxXtWAWgFt5Jvm3'], [['btc', 'f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16']], 0]]] # List[object] | 
    to = 'graphml' # ConvertFormat | Target format. (optional)
    tx_mode = 'node' # ConvertTxMode | How transactions are written in graph formats: `node`, one node per transaction (lossless), or `aggregate`, one edge per sender, receiver and asset. (optional)
    layout = 'keep' # ConvertLayout | `keep` the positions of the input, or `auto`: lay the graph out afresh. (optional)
    include = 'tags' # str | Comma-separated enrichment for graph formats: `tags` (address-level attribution, one lookup per address). (optional)

    try:
        # Convert a Pathfinder graph to another format, or lay it out (beta)
        api_response = api_instance.convert_graph(request_body, to=to, tx_mode=tx_mode, layout=layout, include=include)
        print("The response of GraphApi->convert_graph:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->convert_graph: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | [**List[object]**](object.md)|  | 
 **to** | [**ConvertFormat**](.md)| Target format. | [optional] 
 **tx_mode** | [**ConvertTxMode**](.md)| How transactions are written in graph formats: &#x60;node&#x60;, one node per transaction (lossless), or &#x60;aggregate&#x60;, one edge per sender, receiver and asset. | [optional] 
 **layout** | [**ConvertLayout**](.md)| &#x60;keep&#x60; the positions of the input, or &#x60;auto&#x60;: lay the graph out afresh. | [optional] 
 **include** | **str**| Comma-separated enrichment for graph formats: &#x60;tags&#x60; (address-level attribution, one lookup per address). | [optional] 

### Return type

**bytearray**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json, application/octet-stream
 - **Accept**: application/graphml+xml, application/octet-stream, application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The converted graph, as a file download. |  -  |
**400** | The body is not a Pathfinder graph (undecodable .gs file, invalid JSON, or an old Graph file), an unknown include, tx_mode or include given with to&#x3D;gs, or the graph has more than 500 nodes. |  -  |
**413** | The body is larger than 4194304 bytes. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **graph_compare**
> GraphComparison graph_compare(graph_compare_request)

Compare multiple transactions (beta)

**BETA**: this endpoint is new and its contract may still change without a deprecation cycle. Returns per-tx characteristics, pairwise similarity signals, and a rollup verdict on whether the supplied transactions are likely linked to the same actor. The fingerprinting analysis is BTC-only; every ref's network must be btc. For chain-agnostic aggregate stats over a node set use POST /graph/summary instead. Tx refs are canonicalized (hashes lowercased, 0x stripped) and duplicates collapsed; the response echoes the canonical hashes, and all positional references — signal per_tx entries and lineage from_idx/to_idx — index into the response's txs list, which may be shorter than the request's.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.graph_compare_request import GraphCompareRequest
from graphsense.models.graph_comparison import GraphComparison
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.iknaio.com
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.iknaio.com"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.GraphApi(api_client)
    graph_compare_request = graphsense.GraphCompareRequest.from_dict({'include': ['characteristics', 'signals', 'lineage', 'verdict'], 'txs': [{'network': 'btc', 'tx_hash': '756a95ba337d5dab4ee32fb46071e6cdcd78a6dd3970f025b25daae7c67298e5'}, {'network': 'btc', 'tx_hash': 'e67a0550848b7932d7796aeea16ab0e48a5cfe81c4e8cca2c5b03e0416850114'}]}) # GraphCompareRequest | 

    try:
        # Compare multiple transactions (beta)
        api_response = api_instance.graph_compare(graph_compare_request)
        print("The response of GraphApi->graph_compare:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->graph_compare: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_compare_request** | [**GraphCompareRequest**](GraphCompareRequest.md)|  | 

### Return type

[**GraphComparison**](GraphComparison.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**400** | Invalid request. Causes: fewer than 2 distinct tx refs; a non-BTC network; a sub-transaction identifier instead of a base tx hash; combined inputs/outputs above the comparison work limit. |  -  |
**404** | One or more transactions were not found; the message names every missing hash. The analysis is all-or-nothing — there is no partial comparison. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **graph_summary**
> GraphSummary graph_summary(graph_summary_request)

Summarize a set of transactions and/or addresses (beta)

**BETA**: this endpoint is new and its contract may still change without a deprecation cycle. Returns aggregate stats over the transactions and/or addresses in the request body. Every item carries its own network, so the set may span chains. Each node-type block holds a network-agnostic overall part (fiat totals per code, timestamp span) and one full per-network block (native base-unit values via the Values pattern) per network in the request. Each block is present iff the request carried that node type. Each non-empty list must hold at least 2 distinct entries; together they may hold at most 100. References are canonicalized before processing (tx hashes lowercased, 0x stripped; addresses network-canonicalized), and duplicates — including spelling variants of one node — are collapsed and counted once. Unknown references are dropped and reported per network in a nodes_not_found note (its items list carries the refs); the request only fails when fewer than 2 of a list's references exist. Value totals are gross: UTXO txs contribute their full output sum (change included), so sets containing linked txs (e.g. a peel chain) count the same coins once per hop.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.graph_summary import GraphSummary
from graphsense.models.graph_summary_request import GraphSummaryRequest
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.iknaio.com
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.iknaio.com"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure API key authorization: api_key
configuration.api_key['api_key'] = os.environ["API_KEY"]

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.GraphApi(api_client)
    graph_summary_request = graphsense.GraphSummaryRequest.from_dict({'addresses': [{'address': '14QK3yVfakMHD2W5oect54AtCez77wJgGf', 'network': 'btc'}, {'address': '15Lv7zkEtTfTeBvUB9Py7BZQoKKBpFQLsn', 'network': 'btc'}], 'txs': [{'network': 'btc', 'tx_hash': '756a95ba337d5dab4ee32fb46071e6cdcd78a6dd3970f025b25daae7c67298e5'}, {'network': 'btc', 'tx_hash': 'e67a0550848b7932d7796aeea16ab0e48a5cfe81c4e8cca2c5b03e0416850114'}]}) # GraphSummaryRequest | 

    try:
        # Summarize a set of transactions and/or addresses (beta)
        api_response = api_instance.graph_summary(graph_summary_request)
        print("The response of GraphApi->graph_summary:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->graph_summary: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **graph_summary_request** | [**GraphSummaryRequest**](GraphSummaryRequest.md)|  | 

### Return type

[**GraphSummary**](GraphSummary.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**400** | Invalid request. Causes: both lists empty; a non-empty list with fewer than 2 distinct entries; more than 100 entries combined; an unsupported network; a sub-transaction identifier instead of a base tx hash. |  -  |
**404** | Fewer than 2 of a list&#39;s references exist (the message names the missing ones). Unknown references in an otherwise viable request do not 404 — they are dropped and reported in a nodes_not_found note. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

