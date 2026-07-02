# graphsense.GraphApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**graph_compare**](GraphApi.md#graph_compare) | **POST** /graph/compare | Compare multiple transactions
[**graph_summary**](GraphApi.md#graph_summary) | **POST** /graph/summary | Summarize a set of transactions and/or addresses


# **graph_compare**
> TransactionComparison graph_compare(graph_compare_request)

Compare multiple transactions

Returns per-tx characteristics, pairwise similarity signals, and a rollup verdict on whether the supplied transactions are likely linked to the same actor. The fingerprinting analysis is BTC-only; every ref's network must be btc. For chain-agnostic aggregate stats over a node set use POST /graph/summary instead.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.graph_compare_request import GraphCompareRequest
from graphsense.models.transaction_comparison import TransactionComparison
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
    graph_compare_request = graphsense.GraphCompareRequest() # GraphCompareRequest | 

    try:
        # Compare multiple transactions
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

[**TransactionComparison**](TransactionComparison.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**400** | Invalid request (need 2+ distinct tx refs, or a non-BTC network). |  -  |
**404** | One of the transactions was not found. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **graph_summary**
> GraphSummary graph_summary(graph_summary_request)

Summarize a set of transactions and/or addresses

Returns aggregate stats over the transactions and/or addresses in the request body. Every item carries its own network, so the set may span chains. Each node-type block holds a network-agnostic overall part (fiat totals per code, timestamp span) and one full per-network block (native base-unit values via the Values pattern) per network in the request. Each block is present iff the request carried that node type. Each non-empty list must hold at least 2 distinct entries; together they may hold at most 100.

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
    graph_summary_request = graphsense.GraphSummaryRequest() # GraphSummaryRequest | 

    try:
        # Summarize a set of transactions and/or addresses
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
**400** | Invalid request (each non-empty list needs at least 2 distinct entries, at most 100 nodes combined, networks must be supported). |  -  |
**404** | One of the transactions or addresses was not found. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

