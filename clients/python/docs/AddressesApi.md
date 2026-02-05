# graphsense.AddressesApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_address**](AddressesApi.md#get_address) | **GET** /{currency}/addresses/{address} | Get an address
[**get_address_entity**](AddressesApi.md#get_address_entity) | **GET** /{currency}/addresses/{address}/entity | Get the entity of an address
[**get_tag_summary_by_address**](AddressesApi.md#get_tag_summary_by_address) | **GET** /{currency}/addresses/{address}/tag_summary | Get attribution tag summary for a given address
[**list_address_links**](AddressesApi.md#list_address_links) | **GET** /{currency}/addresses/{address}/links | Get outgoing transactions between two addresses
[**list_address_neighbors**](AddressesApi.md#list_address_neighbors) | **GET** /{currency}/addresses/{address}/neighbors | Get an address&#39;s neighbors in the address graph
[**list_address_txs**](AddressesApi.md#list_address_txs) | **GET** /{currency}/addresses/{address}/txs | Get all transactions an address has been involved in
[**list_related_addresses**](AddressesApi.md#list_related_addresses) | **GET** /{currency}/addresses/{address}/related_addresses | Get related addresses to the input address
[**list_tags_by_address**](AddressesApi.md#list_tags_by_address) | **GET** /{currency}/addresses/{address}/tags | Get attribution tags for a given address


# **get_address**
> Address get_address(currency, address, include_actors=include_actors)

Get an address

Get an address

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.address import Address
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    include_actors = True # bool | Whether to include actor information (optional) (default to True)

    try:
        # Get an address
        api_response = api_instance.get_address(currency, address, include_actors=include_actors)
        print("The response of AddressesApi->get_address:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->get_address: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to True]

### Return type

[**Address**](Address.md)

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

# **get_address_entity**
> Entity get_address_entity(currency, address, include_actors=include_actors)

Get the entity of an address

Get the entity of an address

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.entity import Entity
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    include_actors = True # bool | Whether to include actor information (optional) (default to True)

    try:
        # Get the entity of an address
        api_response = api_instance.get_address_entity(currency, address, include_actors=include_actors)
        print("The response of AddressesApi->get_address_entity:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->get_address_entity: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to True]

### Return type

[**Entity**](Entity.md)

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

# **get_tag_summary_by_address**
> TagSummary get_tag_summary_by_address(currency, address, include_best_cluster_tag=include_best_cluster_tag)

Get attribution tag summary for a given address

Get attribution tag summary for a given address

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.tag_summary import TagSummary
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    include_best_cluster_tag = True # bool | If the best cluster tag should be inherited to the address level (optional)

    try:
        # Get attribution tag summary for a given address
        api_response = api_instance.get_tag_summary_by_address(currency, address, include_best_cluster_tag=include_best_cluster_tag)
        print("The response of AddressesApi->get_tag_summary_by_address:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->get_tag_summary_by_address: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **include_best_cluster_tag** | **bool**| If the best cluster tag should be inherited to the address level | [optional] 

### Return type

[**TagSummary**](TagSummary.md)

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

# **list_address_links**
> Links list_address_links(currency, address, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

Get outgoing transactions between two addresses

Get outgoing transactions between two addresses

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.links import Links
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    neighbor = '1FKCzy3BEtiZDhRDtivp7Y7RVb9edg5BH7' # str | Neighbor address
    min_height = 56 # int | Return transactions starting from given height (optional)
    max_height = 56 # int | Return transactions up to (including) given height (optional)
    min_date = 'min_date_example' # str | Min date of txs (optional)
    max_date = 'max_date_example' # str | Max date of txs (optional)
    order = 'order_example' # str | Sorting order (optional)
    token_currency = 'token_currency_example' # str | Return transactions of given token or base currency (optional)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get outgoing transactions between two addresses
        api_response = api_instance.list_address_links(currency, address, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of AddressesApi->list_address_links:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->list_address_links: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **neighbor** | **str**| Neighbor address | 
 **min_height** | **int**| Return transactions starting from given height | [optional] 
 **max_height** | **int**| Return transactions up to (including) given height | [optional] 
 **min_date** | **str**| Min date of txs | [optional] 
 **max_date** | **str**| Max date of txs | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**Links**](Links.md)

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

# **list_address_neighbors**
> NeighborAddresses list_address_neighbors(currency, address, direction, only_ids=only_ids, include_labels=include_labels, include_actors=include_actors, page=page, pagesize=pagesize)

Get an address's neighbors in the address graph

Get an address's neighbors in the address graph

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.neighbor_addresses import NeighborAddresses
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    direction = 'out' # str | Incoming or outgoing neighbors
    only_ids = 'only_ids_example' # str | Restrict result to given set of comma separated addresses (optional)
    include_labels = True # bool | Whether to include labels of first page of address tags (optional)
    include_actors = True # bool | Whether to include actor information (optional) (default to True)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get an address's neighbors in the address graph
        api_response = api_instance.list_address_neighbors(currency, address, direction, only_ids=only_ids, include_labels=include_labels, include_actors=include_actors, page=page, pagesize=pagesize)
        print("The response of AddressesApi->list_address_neighbors:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->list_address_neighbors: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **direction** | **str**| Incoming or outgoing neighbors | 
 **only_ids** | **str**| Restrict result to given set of comma separated addresses | [optional] 
 **include_labels** | **bool**| Whether to include labels of first page of address tags | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to True]
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**NeighborAddresses**](NeighborAddresses.md)

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

# **list_address_txs**
> AddressTxs list_address_txs(currency, address, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

Get all transactions an address has been involved in

Get all transactions an address has been involved in

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.address_txs import AddressTxs
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    direction = 'direction_example' # str | Incoming or outgoing transactions (optional)
    min_height = 56 # int | Return transactions starting from given height (optional)
    max_height = 56 # int | Return transactions up to (including) given height (optional)
    min_date = 'min_date_example' # str | Min date of txs (optional)
    max_date = 'max_date_example' # str | Max date of txs (optional)
    order = 'order_example' # str | Sorting order (optional)
    token_currency = 'token_currency_example' # str | Return transactions of given token or base currency (optional)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get all transactions an address has been involved in
        api_response = api_instance.list_address_txs(currency, address, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of AddressesApi->list_address_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->list_address_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **direction** | **str**| Incoming or outgoing transactions | [optional] 
 **min_height** | **int**| Return transactions starting from given height | [optional] 
 **max_height** | **int**| Return transactions up to (including) given height | [optional] 
 **min_date** | **str**| Min date of txs | [optional] 
 **max_date** | **str**| Max date of txs | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**AddressTxs**](AddressTxs.md)

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

# **list_related_addresses**
> RelatedAddresses list_related_addresses(currency, address, address_relation_type=address_relation_type, page=page, pagesize=pagesize)

Get related addresses to the input address

Get related addresses to the input address

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.related_addresses import RelatedAddresses
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    address_relation_type = pubkey # str | What type of related addresses to return (optional) (default to pubkey)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get related addresses to the input address
        api_response = api_instance.list_related_addresses(currency, address, address_relation_type=address_relation_type, page=page, pagesize=pagesize)
        print("The response of AddressesApi->list_related_addresses:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->list_related_addresses: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **address_relation_type** | **str**| What type of related addresses to return | [optional] [default to pubkey]
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**RelatedAddresses**](RelatedAddresses.md)

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

# **list_tags_by_address**
> AddressTags list_tags_by_address(currency, address, page=page, pagesize=pagesize, include_best_cluster_tag=include_best_cluster_tag)

Get attribution tags for a given address

Get attribution tags for a given address

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.address_tags import AddressTags
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
    api_instance = graphsense.AddressesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    address = '1Archive1n2C579dMsAu3iC6tWzuQJz8dN' # str | The cryptocurrency address
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)
    include_best_cluster_tag = True # bool | If the best cluster tag should be inherited to the address level (optional)

    try:
        # Get attribution tags for a given address
        api_response = api_instance.list_tags_by_address(currency, address, page=page, pagesize=pagesize, include_best_cluster_tag=include_best_cluster_tag)
        print("The response of AddressesApi->list_tags_by_address:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AddressesApi->list_tags_by_address: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **address** | **str**| The cryptocurrency address | 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 
 **include_best_cluster_tag** | **bool**| If the best cluster tag should be inherited to the address level | [optional] 

### Return type

[**AddressTags**](AddressTags.md)

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

