# graphsense.TxsApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_spending_txs**](TxsApi.md#get_spending_txs) | **GET** /{currency}/txs/{tx_hash}/spending | List source transactions
[**get_spent_in_txs**](TxsApi.md#get_spent_in_txs) | **GET** /{currency}/txs/{tx_hash}/spent_in | List spending transactions
[**get_tx**](TxsApi.md#get_tx) | **GET** /{currency}/txs/{tx_hash} | Get transaction details by hash
[**get_tx_conversions**](TxsApi.md#get_tx_conversions) | **GET** /{currency}/txs/{tx_hash}/conversions | List DeFi conversions in a transaction
[**get_tx_io**](TxsApi.md#get_tx_io) | **GET** /{currency}/txs/{tx_hash}/{io} | List transaction inputs or outputs
[**list_token_txs**](TxsApi.md#list_token_txs) | **GET** /{currency}/token_txs/{tx_hash} | List token transfers in a transaction
[**list_tx_flows**](TxsApi.md#list_tx_flows) | **GET** /{currency}/txs/{tx_hash}/flows | List transaction asset flows


# **get_spending_txs**
> List[TxRef] get_spending_txs(currency, tx_hash, io_index=io_index)

List source transactions

Returns references to transactions whose outputs are consumed by this transaction.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.tx_ref import TxRef
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash
    io_index = 0 # int | Input index to check (optional)

    try:
        # List source transactions
        api_response = api_instance.get_spending_txs(currency, tx_hash, io_index=io_index)
        print("The response of TxsApi->get_spending_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->get_spending_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 
 **io_index** | **int**| Input index to check | [optional] 

### Return type

[**List[TxRef]**](TxRef.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_spent_in_txs**
> List[TxRef] get_spent_in_txs(currency, tx_hash, io_index=io_index)

List spending transactions

Returns references to transactions that spend outputs created by this transaction.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.tx_ref import TxRef
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash
    io_index = 0 # int | Output index to check (optional)

    try:
        # List spending transactions
        api_response = api_instance.get_spent_in_txs(currency, tx_hash, io_index=io_index)
        print("The response of TxsApi->get_spent_in_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->get_spent_in_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 
 **io_index** | **int**| Output index to check | [optional] 

### Return type

[**List[TxRef]**](TxRef.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_tx**
> Tx get_tx(currency, tx_hash, token_tx_id=token_tx_id, include_io=include_io, include_nonstandard_io=include_nonstandard_io, include_io_index=include_io_index, include_heuristics=include_heuristics)

Get transaction details by hash

Returns a transaction, including optional input/output details for UTXO-like currencies and token transaction selection for account-like currencies.

### Example

* Api Key Authentication (api_key):

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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash
    token_tx_id = None # int | Token transaction ID for account-model currencies (optional)
    include_io = None # bool | Include transaction inputs/outputs (optional)
    include_nonstandard_io = None # bool | Include non-standard inputs/outputs (optional)
    include_io_index = None # bool | Include input/output indices (optional)
    include_heuristics = None # List[str] | Heuristics to compute (e.g. one_time_change) as list, or simply all (optional) (default to [])

    try:
        # Get transaction details by hash
        api_response = api_instance.get_tx(currency, tx_hash, token_tx_id=token_tx_id, include_io=include_io, include_nonstandard_io=include_nonstandard_io, include_io_index=include_io_index, include_heuristics=include_heuristics)
        print("The response of TxsApi->get_tx:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->get_tx: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 
 **token_tx_id** | **int**| Token transaction ID for account-model currencies | [optional] 
 **include_io** | **bool**| Include transaction inputs/outputs | [optional] 
 **include_nonstandard_io** | **bool**| Include non-standard inputs/outputs | [optional] 
 **include_io_index** | **bool**| Include input/output indices | [optional] 
 **include_heuristics** | [**List[str]**](str.md)| Heuristics to compute (e.g. one_time_change) as list, or simply all | [optional] [default to []]

### Return type

[**Tx**](Tx.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_tx_conversions**
> List[ExternalConversion] get_tx_conversions(currency, tx_hash)

List DeFi conversions in a transaction

Returns detected DeFi conversion events contained in the transaction.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.external_conversion import ExternalConversion
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash

    try:
        # List DeFi conversions in a transaction
        api_response = api_instance.get_tx_conversions(currency, tx_hash)
        print("The response of TxsApi->get_tx_conversions:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->get_tx_conversions: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 

### Return type

[**List[ExternalConversion]**](ExternalConversion.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_tx_io**
> List[TxValue] get_tx_io(currency, tx_hash, io, include_nonstandard_io=include_nonstandard_io, include_io_index=include_io_index)

List transaction inputs or outputs

Returns transaction input or output values, including optional index and non-standard entries.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.tx_value import TxValue
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash
    io = 'outputs' # str | Input or output values of a transaction (inputs or outputs)
    include_nonstandard_io = None # bool | Include non-standard inputs/outputs (optional)
    include_io_index = None # bool | Include input/output indices (optional)

    try:
        # List transaction inputs or outputs
        api_response = api_instance.get_tx_io(currency, tx_hash, io, include_nonstandard_io=include_nonstandard_io, include_io_index=include_io_index)
        print("The response of TxsApi->get_tx_io:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->get_tx_io: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 
 **io** | **str**| Input or output values of a transaction (inputs or outputs) | 
 **include_nonstandard_io** | **bool**| Include non-standard inputs/outputs | [optional] 
 **include_io_index** | **bool**| Include input/output indices | [optional] 

### Return type

[**List[TxValue]**](TxValue.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_token_txs**
> List[TxAccount] list_token_txs(currency, tx_hash)

List token transfers in a transaction

Returns token transfer records associated with the given transaction hash.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.tx_account import TxAccount
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash

    try:
        # List token transfers in a transaction
        api_response = api_instance.list_token_txs(currency, tx_hash)
        print("The response of TxsApi->list_token_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->list_token_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 

### Return type

[**List[TxAccount]**](TxAccount.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_tx_flows**
> object list_tx_flows(currency, tx_hash, strip_zero_value_txs=strip_zero_value_txs, only_token_txs=only_token_txs, token_currency=token_currency, page=page, pagesize=pagesize)

List transaction asset flows

Returns paginated asset flow events within the transaction, optionally filtered to token transfers.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
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
    api_instance = graphsense.TxsApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    tx_hash = '04d92601677d62a985310b61a301e74870fa942c8be0648e16b1db23b996a8cd' # str | The transaction hash
    strip_zero_value_txs = None # bool | Strip zero value transactions (optional)
    only_token_txs = False # bool | Only return token transactions (optional)
    token_currency = 'WETH' # str | Return transactions of given token or base currency e.g. 'WETH' (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List transaction asset flows
        api_response = api_instance.list_tx_flows(currency, tx_hash, strip_zero_value_txs=strip_zero_value_txs, only_token_txs=only_token_txs, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of TxsApi->list_tx_flows:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TxsApi->list_tx_flows: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **tx_hash** | **str**| The transaction hash | 
 **strip_zero_value_txs** | **bool**| Strip zero value transactions | [optional] 
 **only_token_txs** | **bool**| Only return token transactions | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency e.g. &#39;WETH&#39; | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

**object**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Transaction not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

