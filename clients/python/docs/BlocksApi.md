# graphsense.BlocksApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_block**](BlocksApi.md#get_block) | **GET** /{currency}/blocks/{height} | Get a block by its height
[**get_block_by_date**](BlocksApi.md#get_block_by_date) | **GET** /{currency}/block_by_date/{date} | Get block by date
[**list_block_txs**](BlocksApi.md#list_block_txs) | **GET** /{currency}/blocks/{height}/txs | Get block transactions


# **get_block**
> Block get_block(currency, height)

Get a block by its height

Get a block by its height

### Example


```python
import graphsense
from graphsense.models.block import Block
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
    api_instance = graphsense.BlocksApi(api_client)
    currency = 'currency_example' # str | The cryptocurrency code (e.g., btc)
    height = 56 # int | The block height

    try:
        # Get a block by its height
        api_response = api_instance.get_block(currency, height)
        print("The response of BlocksApi->get_block:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BlocksApi->get_block: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **height** | **int**| The block height | 

### Return type

[**Block**](Block.md)

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

# **get_block_by_date**
> BlockAtDate get_block_by_date(currency, var_date)

Get block by date

Get block by date

### Example


```python
import graphsense
from graphsense.models.block_at_date import BlockAtDate
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
    api_instance = graphsense.BlocksApi(api_client)
    currency = 'currency_example' # str | The cryptocurrency code (e.g., btc)
    var_date = 'var_date_example' # str | The date (YYYY-MM-DD)

    try:
        # Get block by date
        api_response = api_instance.get_block_by_date(currency, var_date)
        print("The response of BlocksApi->get_block_by_date:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BlocksApi->get_block_by_date: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **var_date** | **str**| The date (YYYY-MM-DD) | 

### Return type

[**BlockAtDate**](BlockAtDate.md)

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

# **list_block_txs**
> List[Tx] list_block_txs(currency, height)

Get block transactions

Get block transactions

### Example


```python
import graphsense
from graphsense.models.tx import Tx
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
    api_instance = graphsense.BlocksApi(api_client)
    currency = 'currency_example' # str | The cryptocurrency code (e.g., btc)
    height = 56 # int | The block height

    try:
        # Get block transactions
        api_response = api_instance.list_block_txs(currency, height)
        print("The response of BlocksApi->list_block_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BlocksApi->list_block_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **height** | **int**| The block height | 

### Return type

[**List[Tx]**](Tx.md)

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

