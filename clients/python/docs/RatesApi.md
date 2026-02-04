# graphsense.RatesApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_exchange_rates**](RatesApi.md#get_exchange_rates) | **GET** /{currency}/rates/{height} | Get exchange rates for a given block height


# **get_exchange_rates**
> Rates get_exchange_rates(currency, height)

Get exchange rates for a given block height

Get exchange rates for a given block height

### Example


```python
import graphsense
from graphsense.models.rates import Rates
from graphsense.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to https://api.iknaio.com
# See configuration.py for a list of all supported configuration parameters.
configuration = graphsense.Configuration(
    host = "https://api.iknaio.com"
)


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.RatesApi(api_client)
    currency = 'currency_example' # str | The cryptocurrency code (e.g., btc)
    height = 56 # int | The block height

    try:
        # Get exchange rates for a given block height
        api_response = api_instance.get_exchange_rates(currency, height)
        print("The response of RatesApi->get_exchange_rates:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling RatesApi->get_exchange_rates: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **height** | **int**| The block height | 

### Return type

[**Rates**](Rates.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

