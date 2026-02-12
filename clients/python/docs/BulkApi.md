# graphsense.BulkApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**bulk_csv**](BulkApi.md#bulk_csv) | **POST** /{currency}/bulk.csv/{operation} | Get data as CSV in bulk
[**bulk_json**](BulkApi.md#bulk_json) | **POST** /{currency}/bulk.json/{operation} | Get data as JSON in bulk


# **bulk_csv**
> str bulk_csv(currency, api, operation, body)

Get data as CSV in bulk

### Example

* Api Key Authentication (api_key):
```python
import graphsense
from graphsense.api import bulk_api
from pprint import pprint
import pandas
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
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = bulk_api.BulkApi(api_client)
    currency = "btc" # str | The cryptocurrency (e.g., btc)
    operation = "get_block" # str | The operation to execute in bulk
    body = {"height": [1, 2, 3]}

    # example passing only required values which don't have defaults set
    try:
        # Get data as CSV in bulk
        api_response = api_instance.bulk_csv(currency, operation, body=body, num_pages=1)
        pprint(api_response)
    except graphsense.ApiException as e:
        print("Exception when calling BulkApi->bulk_csv: %s\n" % e)

    # It is also possible to read CSV data directly into a dataframe:

    dataframe = \
        pandas.read_csv(
            api_instance.bulk_csv(currency,
                                  operation,
                                  body=body,
                                  num_pages=1,
                                  _preload_content=False # set to False to omit decoding
                                  ))
    pprint(dataframe)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency (e.g., btc) |
 **operation** | **str**| The operation to execute in bulk |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**| Map of the operation&#39;s parameter names to (arrays of) values |
 **num_pages** | **int**| Number of pages to be retrieved per bulked request |
**_preload_content** | **bool** | If False, the urllib3.HTTPResponse object will be returned without reading/decoding response data. | [optional] default is True.
**async_req** | **bool** | Execute request asynchronously | [optional] default is False.

### Return type

**str**

**Notes:**

* If `async_req` parameter is True, the request will be called asynchronously.  The method will return the request thread.  If parameter `async_req` is False or missing, then the method will return the response directly.

* If the HTTP response code is `429 Too Many Requests` due to rate limit policies, the underlying `urllib3` HTTP client will automatically stall the request as long as advised by the `Retry-After` header.

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: text/csv


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **bulk_json**
> [{str: (bool, date, datetime, dict, float, int, list, str, none_type)}] bulk_json(currency, api, operation, body)

Get data as JSON in bulk

### Example

* Api Key Authentication (api_key):
```python
import graphsense
from graphsense.api import bulk_api
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
configuration.api_key['api_key'] = 'YOUR_API_KEY'

# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
# configuration.api_key_prefix['api_key'] = 'Bearer'

# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = bulk_api.BulkApi(api_client)
    currency = "btc" # str | The cryptocurrency (e.g., btc)
    operation = "get_block" # str | The operation to execute in bulk
    body = {"height": [1, 2, 3]}

    # example passing only required values which don't have defaults set
    try:
        # Get data as JSON in bulk
        api_response = api_instance.bulk_json(currency, operation, 1, body)
        pprint(api_response)
    except graphsense.ApiException as e:
        print("Exception when calling BulkApi->bulk_json: %s\n" % e)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency (e.g., btc) |
 **operation** | **str**| The operation to execute in bulk |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**| Map of the operation&#39;s parameter names to (arrays of) values |
 **num_pages** | **int**| Number of pages to be retrieved per bulked request |
**_preload_content** | **bool** | If False, the urllib3.HTTPResponse object will be returned without reading/decoding response data. | [optional] default is True.
**async_req** | **bool** | Execute request asynchronously | [optional] default is False.

### Return type

**[{str: (bool, date, datetime, dict, float, int, list, str, none_type)}]**

**Notes:**

* If `async_req` parameter is True, the request will be called asynchronously.  The method will return the request thread.  If parameter `async_req` is False or missing, then the method will return the response directly.

* If the HTTP response code is `429 Too Many Requests` due to rate limit policies, the underlying `urllib3` HTTP client will automatically stall the request as long as advised by the `Retry-After` header.

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)
