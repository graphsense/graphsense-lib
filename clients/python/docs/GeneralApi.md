# graphsense.GeneralApi

All URIs are relative to *https://api.ikna.io*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_statistics**](GeneralApi.md#get_statistics) | **GET** /stats | Get statistics of supported currencies
[**search**](GeneralApi.md#search) | **GET** /search | Returns matching addresses, transactions and labels


# **get_statistics**
> Stats get_statistics()

Get statistics of supported currencies

Get statistics of supported currencies

### Example


```python
import graphsense
from graphsense.models.stats import Stats
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.ikna.io
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.ikna.io"
)


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.GeneralApi(api_client)

    try:
        # Get statistics of supported currencies
        api_response = api_instance.get_statistics()
        print("The response of GeneralApi->get_statistics:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GeneralApi->get_statistics: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**Stats**](Stats.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **search**
> SearchResult search(q, currency=currency, limit=limit, include_sub_tx_identifiers=include_sub_tx_identifiers, include_labels=include_labels, include_actors=include_actors, include_txs=include_txs, include_addresses=include_addresses)

Returns matching addresses, transactions and labels

Returns matching addresses, transactions and labels

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.search_result import SearchResult
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.ikna.io
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.ikna.io"
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
    api_instance = graphsense.GeneralApi(api_client)
    q = 'q_example' # str | Search query (address, transaction, or label)
    currency = 'currency_example' # str | The cryptocurrency (e.g., btc) (optional)
    limit = 10 # int | Maximum number of search results (optional) (default to 10)
    include_sub_tx_identifiers = False # bool | Whether to include sub-transaction identifiers (optional) (default to False)
    include_labels = True # bool | Whether to include labels (optional) (default to True)
    include_actors = True # bool | Whether to include actors (optional) (default to True)
    include_txs = True # bool | Whether to include transactions (optional) (default to True)
    include_addresses = True # bool | Whether to include addresses (optional) (default to True)

    try:
        # Returns matching addresses, transactions and labels
        api_response = api_instance.search(q, currency=currency, limit=limit, include_sub_tx_identifiers=include_sub_tx_identifiers, include_labels=include_labels, include_actors=include_actors, include_txs=include_txs, include_addresses=include_addresses)
        print("The response of GeneralApi->search:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GeneralApi->search: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **q** | **str**| Search query (address, transaction, or label) |
 **currency** | **str**| The cryptocurrency (e.g., btc) | [optional]
 **limit** | **int**| Maximum number of search results | [optional] [default to 10]
 **include_sub_tx_identifiers** | **bool**| Whether to include sub-transaction identifiers | [optional] [default to False]
 **include_labels** | **bool**| Whether to include labels | [optional] [default to True]
 **include_actors** | **bool**| Whether to include actors | [optional] [default to True]
 **include_txs** | **bool**| Whether to include transactions | [optional] [default to True]
 **include_addresses** | **bool**| Whether to include addresses | [optional] [default to True]

### Return type

[**SearchResult**](SearchResult.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)
