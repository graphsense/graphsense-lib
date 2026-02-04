# graphsense.EntitiesApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_entity**](EntitiesApi.md#get_entity) | **GET** /{currency}/entities/{entity} | Get an entity
[**list_address_tags_by_entity**](EntitiesApi.md#list_address_tags_by_entity) | **GET** /{currency}/entities/{entity}/tags | Get address tags for a given entity
[**list_entity_addresses**](EntitiesApi.md#list_entity_addresses) | **GET** /{currency}/entities/{entity}/addresses | Get an entity&#39;s addresses
[**list_entity_links**](EntitiesApi.md#list_entity_links) | **GET** /{currency}/entities/{entity}/links | Get transactions between two entities
[**list_entity_neighbors**](EntitiesApi.md#list_entity_neighbors) | **GET** /{currency}/entities/{entity}/neighbors | Get an entity&#39;s neighbors in the entity graph
[**list_entity_txs**](EntitiesApi.md#list_entity_txs) | **GET** /{currency}/entities/{entity}/txs | Get all transactions an entity has been involved in
[**search_entity_neighbors**](EntitiesApi.md#search_entity_neighbors) | **GET** /{currency}/entities/{entity}/search | Search neighbors of an entity


# **get_entity**
> Entity get_entity(currency, entity, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

Get an entity

Get an entity

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    exclude_best_address_tag = True # bool | Whether to exclude best address tag (optional)
    include_actors = False # bool | Whether to include actor information (optional) (default to False)

    try:
        # Get an entity
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

# **list_address_tags_by_entity**
> AddressTags list_address_tags_by_entity(currency, entity, page=page, pagesize=pagesize)

Get address tags for a given entity

Get address tags for a given entity

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get address tags for a given entity
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

# **list_entity_addresses**
> EntityAddresses list_entity_addresses(currency, entity, page=page, pagesize=pagesize)

Get an entity's addresses

Get an entity's addresses

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get an entity's addresses
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

# **list_entity_links**
> Links list_entity_links(currency, entity, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

Get transactions between two entities

Get transactions between two entities

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    neighbor = 123456 # int | Neighbor entity ID
    min_height = 56 # int | Return transactions starting from given height (optional)
    max_height = 56 # int | Return transactions up to (including) given height (optional)
    min_date = 'min_date_example' # str | Min date of txs (optional)
    max_date = 'max_date_example' # str | Max date of txs (optional)
    order = 'order_example' # str | Sorting order (optional)
    token_currency = 'token_currency_example' # str | Return transactions of given token or base currency (optional)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)

    try:
        # Get transactions between two entities
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
 **min_date** | **str**| Min date of txs | [optional] 
 **max_date** | **str**| Max date of txs | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**Links**](Links.md)

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

# **list_entity_neighbors**
> NeighborEntities list_entity_neighbors(currency, entity, direction, only_ids=only_ids, include_labels=include_labels, page=page, pagesize=pagesize, relations_only=relations_only, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

Get an entity's neighbors in the entity graph

Get an entity's neighbors in the entity graph

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
    direction = 'out' # str | Incoming or outgoing neighbors
    only_ids = 'only_ids_example' # str | Restrict result to given set of comma separated IDs (optional)
    include_labels = True # bool | Whether to include labels (optional)
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 56 # int | Number of items returned in a single page (optional)
    relations_only = True # bool | Return only relations without entity details (optional)
    exclude_best_address_tag = True # bool | Whether to exclude best address tag (optional)
    include_actors = False # bool | Whether to include actor information (optional) (default to False)

    try:
        # Get an entity's neighbors in the entity graph
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
 **include_labels** | **bool**| Whether to include labels | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 
 **relations_only** | **bool**| Return only relations without entity details | [optional] 
 **exclude_best_address_tag** | **bool**| Whether to exclude best address tag | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to False]

### Return type

[**NeighborEntities**](NeighborEntities.md)

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

# **list_entity_txs**
> AddressTxs list_entity_txs(currency, entity, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

Get all transactions an entity has been involved in

Get all transactions an entity has been involved in

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.EntitiesApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    entity = 67065 # int | The entity ID
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
        # Get all transactions an entity has been involved in
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
 **min_date** | **str**| Min date of txs | [optional] 
 **max_date** | **str**| Max date of txs | [optional] 
 **order** | **str**| Sorting order | [optional] 
 **token_currency** | **str**| Return transactions of given token or base currency | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**AddressTxs**](AddressTxs.md)

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

# **search_entity_neighbors**
> List[SearchResultLevel1] search_entity_neighbors(currency, entity, direction, key, value, depth, breadth, skip_num_addresses=skip_num_addresses)

Search neighbors of an entity

Search neighbors of an entity

### Example


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
    skip_num_addresses = 56 # int | Skip entities with more than N addresses (optional)

    try:
        # Search neighbors of an entity
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

