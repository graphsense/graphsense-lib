# graphsense.TokensApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**list_supported_tokens**](TokensApi.md#list_supported_tokens) | **GET** /{currency}/supported_tokens | Get supported tokens for a currency


# **list_supported_tokens**
> TokenConfigs list_supported_tokens(currency)

Get supported tokens for a currency

Get supported tokens for a currency

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.token_configs import TokenConfigs
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
    api_instance = graphsense.TokensApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)

    try:
        # Get supported tokens for a currency
        api_response = api_instance.list_supported_tokens(currency)
        print("The response of TokensApi->list_supported_tokens:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TokensApi->list_supported_tokens: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 

### Return type

[**TokenConfigs**](TokenConfigs.md)

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

