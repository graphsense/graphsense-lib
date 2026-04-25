# graphsense.ClustersApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_cluster**](ClustersApi.md#get_cluster) | **GET** /{currency}/clusters/{cluster} | Get cluster details
[**list_address_tags_by_cluster**](ClustersApi.md#list_address_tags_by_cluster) | **GET** /{currency}/clusters/{cluster}/tags | List cluster address tags
[**list_cluster_addresses**](ClustersApi.md#list_cluster_addresses) | **GET** /{currency}/clusters/{cluster}/addresses | List cluster addresses
[**list_cluster_links**](ClustersApi.md#list_cluster_links) | **GET** /{currency}/clusters/{cluster}/links | List transactions between clusters
[**list_cluster_neighbors**](ClustersApi.md#list_cluster_neighbors) | **GET** /{currency}/clusters/{cluster}/neighbors | List neighboring clusters
[**list_cluster_txs**](ClustersApi.md#list_cluster_txs) | **GET** /{currency}/clusters/{cluster}/txs | List cluster transactions
[**search_cluster_neighbors**](ClustersApi.md#search_cluster_neighbors) | **GET** /{currency}/clusters/{cluster}/search | Search cluster neighborhood


# **get_cluster**
> Cluster get_cluster(currency, cluster, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

Get cluster details

Returns details for a single address cluster.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.cluster import Cluster
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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    exclude_best_address_tag = None # bool | Whether to exclude best address tag (optional)
    include_actors = True # bool | Whether to include actor information (optional) (default to False)

    try:
        # Get cluster details
        api_response = api_instance.get_cluster(currency, cluster, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)
        print("The response of ClustersApi->get_cluster:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->get_cluster: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
 **exclude_best_address_tag** | **bool**| Whether to exclude best address tag | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to False]

### Return type

[**Cluster**](Cluster.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_address_tags_by_cluster**
> AddressTags list_address_tags_by_cluster(currency, cluster, page=page, pagesize=pagesize)

List cluster address tags

Returns paginated attribution tags observed on addresses in the cluster.

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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List cluster address tags
        api_response = api_instance.list_address_tags_by_cluster(currency, cluster, page=page, pagesize=pagesize)
        print("The response of ClustersApi->list_address_tags_by_cluster:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->list_address_tags_by_cluster: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
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
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_cluster_addresses**
> ClusterAddresses list_cluster_addresses(currency, cluster, page=page, pagesize=pagesize)

List cluster addresses

Returns paginated addresses that belong to the cluster.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.cluster_addresses import ClusterAddresses
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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List cluster addresses
        api_response = api_instance.list_cluster_addresses(currency, cluster, page=page, pagesize=pagesize)
        print("The response of ClustersApi->list_cluster_addresses:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->list_cluster_addresses: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 

### Return type

[**ClusterAddresses**](ClusterAddresses.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_cluster_links**
> Links list_cluster_links(currency, cluster, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

List transactions between clusters

Returns paginated transaction links between the cluster and a neighbor cluster.

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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    neighbor = 123456 # int | Neighbor cluster ID
    min_height = 1 # int | Return transactions starting from given height (optional)
    max_height = 2 # int | Return transactions up to (including) given height (optional)
    min_date = None # str | Min date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    max_date = None # str | Max date of txs in the format YYYY-MM-DDTHH:MM:SSZ (optional)
    order = 'desc' # str | Sorting order (optional)
    token_currency = 'WETH' # str | Return transactions of given token or base currency e.g. 'WETH' (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List transactions between clusters
        api_response = api_instance.list_cluster_links(currency, cluster, neighbor, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of ClustersApi->list_cluster_links:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->list_cluster_links: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
 **neighbor** | **int**| Neighbor cluster ID | 
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
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_cluster_neighbors**
> NeighborClusters list_cluster_neighbors(currency, cluster, direction, only_ids=only_ids, include_labels=include_labels, page=page, pagesize=pagesize, relations_only=relations_only, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)

List neighboring clusters

Returns neighboring clusters connected to the given cluster in the cluster graph.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.neighbor_clusters import NeighborClusters
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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    direction = 'out' # str | Incoming or outgoing neighbors
    only_ids = None # str | Restrict result to given set of comma separated IDs (optional)
    include_labels = True # bool | Whether to include labels of first page of address tags (optional)
    page = None # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)
    relations_only = None # bool | Return only relations without cluster details (optional)
    exclude_best_address_tag = None # bool | Whether to exclude best address tag (optional)
    include_actors = True # bool | Whether to include actor information (optional) (default to False)

    try:
        # List neighboring clusters
        api_response = api_instance.list_cluster_neighbors(currency, cluster, direction, only_ids=only_ids, include_labels=include_labels, page=page, pagesize=pagesize, relations_only=relations_only, exclude_best_address_tag=exclude_best_address_tag, include_actors=include_actors)
        print("The response of ClustersApi->list_cluster_neighbors:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->list_cluster_neighbors: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
 **direction** | **str**| Incoming or outgoing neighbors | 
 **only_ids** | **str**| Restrict result to given set of comma separated IDs | [optional] 
 **include_labels** | **bool**| Whether to include labels of first page of address tags | [optional] 
 **page** | **str**| Resumption token for retrieving the next page | [optional] 
 **pagesize** | **int**| Number of items returned in a single page | [optional] 
 **relations_only** | **bool**| Return only relations without cluster details | [optional] 
 **exclude_best_address_tag** | **bool**| Whether to exclude best address tag | [optional] 
 **include_actors** | **bool**| Whether to include actor information | [optional] [default to False]

### Return type

[**NeighborClusters**](NeighborClusters.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_cluster_txs**
> AddressTxs list_cluster_txs(currency, cluster, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)

List cluster transactions

Returns paginated transactions involving the cluster, with optional height, date, direction, and token filters.

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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
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
        # List cluster transactions
        api_response = api_instance.list_cluster_txs(currency, cluster, direction=direction, min_height=min_height, max_height=max_height, min_date=min_date, max_date=max_date, order=order, token_currency=token_currency, page=page, pagesize=pagesize)
        print("The response of ClustersApi->list_cluster_txs:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->list_cluster_txs: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
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
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **search_cluster_neighbors**
> List[SearchResultLevel1] search_cluster_neighbors(currency, cluster, direction, key, value, depth, breadth, skip_num_addresses=skip_num_addresses)

Search cluster neighborhood

Returns matching neighboring clusters found by key/value criteria within the specified search depth and breadth.

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
    api_instance = graphsense.ClustersApi(api_client)
    currency = 'btc' # str | The cryptocurrency code (e.g., btc)
    cluster = 67065 # int | The cluster ID
    direction = 'out' # str | Incoming or outgoing neighbors
    key = 'category' # str | Search key
    value = 'Miner' # str | Comma separated search values
    depth = 2 # int | Search depth
    breadth = 16 # int | Search breadth
    skip_num_addresses = None # int | Skip clusters with more than N addresses (optional)

    try:
        # Search cluster neighborhood
        api_response = api_instance.search_cluster_neighbors(currency, cluster, direction, key, value, depth, breadth, skip_num_addresses=skip_num_addresses)
        print("The response of ClustersApi->search_cluster_neighbors:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ClustersApi->search_cluster_neighbors: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **currency** | **str**| The cryptocurrency code (e.g., btc) | 
 **cluster** | **int**| The cluster ID | 
 **direction** | **str**| Incoming or outgoing neighbors | 
 **key** | **str**| Search key | 
 **value** | **str**| Comma separated search values | 
 **depth** | **int**| Search depth | 
 **breadth** | **int**| Search breadth | 
 **skip_num_addresses** | **int**| Skip clusters with more than N addresses | [optional] 

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
**404** | Cluster not found for the selected currency. |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

