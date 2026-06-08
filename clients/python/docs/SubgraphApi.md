# graphsense.SubgraphApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**subgraph_summary**](SubgraphApi.md#subgraph_summary) | **POST** /{currency}/subgraph/summary | Summarize a set of transactions


# **subgraph_summary**
> SubgraphSummary subgraph_summary(currency, subgraph_summary_request)

Summarize a set of transactions

Returns aggregate stats (value, fee, input/output counts, block and timestamp ranges) over the transactions in the request body. The summary is derived from tx headers only, so it works for every supported chain. The node set (txs + addresses) must hold at least 2 and at most 100 distinct nodes; addresses are reserved for a future extension and must be empty for now.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.subgraph_summary import SubgraphSummary
from graphsense.models.subgraph_summary_request import SubgraphSummaryRequest
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
    api_instance = graphsense.SubgraphApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    subgraph_summary_request = graphsense.SubgraphSummaryRequest() # SubgraphSummaryRequest | 

    try:
        # Summarize a set of transactions
        api_response = api_instance.subgraph_summary(currency, subgraph_summary_request)
        print("The response of SubgraphApi->subgraph_summary:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SubgraphApi->subgraph_summary: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **subgraph_summary_request** | [**SubgraphSummaryRequest**](SubgraphSummaryRequest.md)|  | 

### Return type

[**SubgraphSummary**](SubgraphSummary.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**400** | Invalid request (need 2-100 nodes, or addresses supplied which are not yet supported). |  -  |
**404** | One of the transactions was not found. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

