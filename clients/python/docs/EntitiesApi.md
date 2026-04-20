# graphsense.EntitiesApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_entity**](EntitiesApi.md#get_entity) | **GET** /{currency}/entities/{entity} | Get entity details
[**list_address_tags_by_entity**](EntitiesApi.md#list_address_tags_by_entity) | **GET** /{currency}/entities/{entity}/tags | List entity address tags
[**list_entity_addresses**](EntitiesApi.md#list_entity_addresses) | **GET** /{currency}/entities/{entity}/addresses | List entity addresses
[**list_entity_links**](EntitiesApi.md#list_entity_links) | **GET** /{currency}/entities/{entity}/links | List transactions between entities
[**list_entity_neighbors**](EntitiesApi.md#list_entity_neighbors) | **GET** /{currency}/entities/{entity}/neighbors | List neighboring entities
[**list_entity_txs**](EntitiesApi.md#list_entity_txs) | **GET** /{currency}/entities/{entity}/txs | List entity transactions
[**search_entity_neighbors**](EntitiesApi.md#search_entity_neighbors) | **GET** /{currency}/entities/{entity}/search | Search entity neighborhood


# **get_entity**
> Entity get_entity(currency, entity, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

Get entity details

Deprecated alias for `GET /{currency}/clusters/{cluster}`. Returns details for a single address cluster.

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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    exclude_best_address_tag = None # bool | Whether to exclude best address tag (optional)
    include_actors = True # bool | Whether to include actor information (optional) (default to False)

    try:
        # Get entity details
        api_response = api_instance.get_entity(currency, entity, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)
        print("The response of EntitiesApi->get_entity:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->get_entity: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **exclude_best_address_tag** | **bool**| Whether to exclude best address tag | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to False]

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
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_address_tags_by_entity**
> AddressTags list_address_tags_by_entity(currency, entity, page=page, pagesize=pagesize)

List entity address tags

Returns paginated attribution tags observed on addresses in the entity.

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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List entity address tags
        api_response = api_instance.list_address_tags_by_entity(currency, entity, page=page, pagesize=pagesize)
        print("The response of EntitiesApi->list_address_tags_by_entity:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->list_address_tags_by_entity: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

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
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_entity_addresses**
> EntityAddresses list_entity_addresses(currency, entity, page=page, pagesize=pagesize)

List entity addresses

Deprecated alias for `GET /{currency}/clusters/{cluster}/addresses`. Returns paginated addresses that belong to the cluster.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.entity_addresses import EntityAddresses
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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List entity addresses
        api_response = api_instance.list_entity_addresses(currency, entity, page=page, pagesize=pagesize)
        print("The response of EntitiesApi->list_entity_addresses:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->list_entity_addresses: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**EntityAddresses**](EntityAddresses.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_entity_links**
> Links list_entity_links(currency, entity, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

List transactions between entities

Returns paginated transaction links between the entity and a neighbor entity.

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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    neighbor = 123456 # int | Neighbor entity ID
    min_height = 1 # int | Return transactions starting from given height (optional)
    max_height = 2 # int | Return transactions up to (including) given height (optional)
    min_date = None # str | Min date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    max_date = None # str | Max date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    order = 'desc' # str | Sorting order (optional)
    token_currency = 'WETH' # str | Return transactions of given token or base currency e.g. 'WETH' (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List transactions between entities
        api_response = api_instance.list_entity_links(currency, entity, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of EntitiesApi->list_entity_links:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->list_entity_links: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **neighbor** | **int**| Neighbor entity ID | 
 **min_height** | **int**| Return transactions starting from given height | [optional] 
 **max_height** | **int**| Return transactions up to (including) given height | [optional] 
 **min_date** | **str**| Min date of txs in the format YYYY-MM-DDTHH:MM:SSZ | [optional] 
 **max_date** | **str**| Max date of txs in the format YYYY-MM-DDTHH:MM:SSZ | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency e.g. &#39;WETH&#39; | [optional] 
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
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_entity_neighbors**
> NeighborEntities list_entity_neighbors(currency, entity, direction, only_ids=only_ids, include_labels=include_labels, page=page, pagesize=pagesize, relations_only=relations_only, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

List neighboring entities

Returns neighboring entities connected to the given entity in the entity graph.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.neighbor_entities import NeighborEntities
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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    direction = 'out' # str | Incoming or outgoing neighbors
    only_ids = None # str | Restrict result to given set of comma separated IDs (optional)
    include_labels = True # bool | Whether to include labels of first page of address tags (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)
    relations_only = None # bool | Return only relations without entity details (optional)
    exclude_best_address_tag = None # bool | Whether to exclude best address tag (optional)
    include_actors = True # bool | Whether to include actor information (optional) (default to False)

    try:
        # List neighboring entities
        api_response = api_instance.list_entity_neighbors(currency, entity, direction, only_ids=only_ids, include_labels=include_labels, page=page, pagesize=pagesize, relations_only=relations_only, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)
        print("The response of EntitiesApi->list_entity_neighbors:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->list_entity_neighbors: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **direction** | **str**| Incoming or outgoing neighbors | 
 **only_ids** | **str**| Restrict result to given set of comma separated IDs | [optional] 
 **include_labels** | **bool**| Whether to include labels of first page of address tags | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 
 **relations_only** | **bool**| Return only relations without entity details | [optional] 
 **exclude_best_address_tag** | **bool**| Whether to exclude best address tag | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to False]

### Return type

[**NeighborEntities**](NeighborEntities.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_entity_txs**
> AddressTxs list_entity_txs(currency, entity, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

List entity transactions

Returns paginated transactions involving the entity, with optional height, date, direction, and token filters.

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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    direction = 'out' # str | Incoming or outgoing transactions (optional)
    min_height = 1 # int | Return transactions starting from given height (optional)
    max_height = 2 # int | Return transactions up to (including) given height (optional)
    min_date = None # str | Min date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    max_date = None # str | Max date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    order = 'desc' # str | Sorting order (optional)
    token_currency = 'WETH' # str | Return transactions of given token or base currency e.g. 'WETH' (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List entity transactions
        api_response = api_instance.list_entity_txs(currency, entity, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of EntitiesApi->list_entity_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->list_entity_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **direction** | **str**| Incoming or outgoing transactions | [optional] 
 **min_height** | **int**| Return transactions starting from given height | [optional] 
 **max_height** | **int**| Return transactions up to (including) given height | [optional] 
 **min_date** | **str**| Min date of txs in the format YYYY-MM-DDTHH:MM:SSZ | [optional] 
 **max_date** | **str**| Max date of txs in the format YYYY-MM-DDTHH:MM:SSZ | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency e.g. &#39;WETH&#39; | [optional] 
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
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **search_entity_neighbors**
> List[SearchResultLevel1] search_entity_neighbors(currency, entity, direction, key, value, depth, breadth, skip_num_addresses=skip_num_addresses)

Search entity neighborhood

Returns matching neighboring entities found by key/value criteria within the specified search depth and breadth.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.search_result_level1 import SearchResultLevel1
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
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    direction = 'out' # str | Incoming or outgoing neighbors
    key = 'category' # str | Search key
    value = 'Miner' # str | Comma separated search values
    depth = 2 # int | Search depth
    breadth = 16 # int | Search breadth
    skip_num_addresses = None # int | Skip entities with more than N addresses (optional)

    try:
        # Search entity neighborhood
        api_response = api_instance.search_entity_neighbors(currency, entity, direction, key, value, depth, breadth, skip_num_addresses=skip_num_addresses)
        print("The response of EntitiesApi->search_entity_neighbors:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling EntitiesApi->search_entity_neighbors: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **entity** | **int**| The entity ID | 
 **direction** | **str**| Incoming or outgoing neighbors | 
 **key** | **str**| Search key | 
 **value** | **str**| Comma separated search values | 
 **depth** | **int**| Search depth | 
 **breadth** | **int**| Search breadth | 
 **skip_num_addresses** | **int**| Skip entities with more than N addresses | [optional] 

### Return type

[**List[SearchResultLevel1]**](SearchResultLevel1.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Entity not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

