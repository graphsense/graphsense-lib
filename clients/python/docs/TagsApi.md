# graphsense.TagsApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_actor**](TagsApi.md#get_actor) | **GET** /tags/actors/{actor} | Get actor details by ID
[**get_actor_tags**](TagsApi.md#get_actor_tags) | **GET** /tags/actors/{actor}/tags | List tags associated with an actor
[**list_address_tags**](TagsApi.md#list_address_tags) | **GET** /tags | List address tags by label
[**list_concepts**](TagsApi.md#list_concepts) | **GET** /tags/taxonomies/{taxonomy}/concepts | List concepts for a taxonomy
[**list_taxonomies**](TagsApi.md#list_taxonomies) | **GET** /tags/taxonomies | List all taxonomies
[**report_tag**](TagsApi.md#report_tag) | **POST** /tags/report-tag | Submit a user-reported tag


# **get_actor**
> Actor get_actor(actor)

Get actor details by ID

Returns metadata for the actor identified by the actor ID.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.actor import Actor
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
    api_instance = graphsense.TagsApi(api_client)
    actor = 'binance' # str | The actor ID

    try:
        # Get actor details by ID
        api_response = api_instance.get_actor(actor)
        print("The response of TagsApi->get_actor:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->get_actor: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **actor** | **str**| The actor ID | 

### Return type

[**Actor**](Actor.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Actor metadata for the requested actor ID. |  -  |
**422** | Validation error in path parameters. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_actor_tags**
> AddressTags get_actor_tags(actor, page=page, pagesize=pagesize)

List tags associated with an actor

Returns paginated address tags associated with the specified actor.

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
    api_instance = graphsense.TagsApi(api_client)
    actor = 'binance' # str | The actor ID
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List tags associated with an actor
        api_response = api_instance.get_actor_tags(actor, page=page, pagesize=pagesize)
        print("The response of TagsApi->get_actor_tags:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->get_actor_tags: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **actor** | **str**| The actor ID | 
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
**200** | Paginated address tags linked to the actor. |  -  |
**422** | Validation error in path/query parameters. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_address_tags**
> AddressTags list_address_tags(label, page=page, pagesize=pagesize)

List address tags by label

Returns paginated address tags matching the provided label.

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
    api_instance = graphsense.TagsApi(api_client)
    label = 'cimedy' # str | The label to search for
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # List address tags by label
        api_response = api_instance.list_address_tags(label, page=page, pagesize=pagesize)
        print("The response of TagsApi->list_address_tags:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->list_address_tags: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **label** | **str**| The label to search for | 
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
**200** | Paginated address tags for the requested label. |  -  |
**422** | Validation error in query parameters. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_concepts**
> List[Concept] list_concepts(taxonomy)

List concepts for a taxonomy

Returns all concepts defined for the specified taxonomy.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.concept import Concept
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
    api_instance = graphsense.TagsApi(api_client)
    taxonomy = 'concept' # str | The taxonomy name

    try:
        # List concepts for a taxonomy
        api_response = api_instance.list_concepts(taxonomy)
        print("The response of TagsApi->list_concepts:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->list_concepts: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **taxonomy** | **str**| The taxonomy name | 

### Return type

[**List[Concept]**](Concept.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of concepts for the requested taxonomy. |  -  |
**422** | Validation error in path parameters. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_taxonomies**
> List[Taxonomy] list_taxonomies()

List all taxonomies

Returns all available tag taxonomies.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.taxonomy import Taxonomy
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
    api_instance = graphsense.TagsApi(api_client)

    try:
        # List all taxonomies
        api_response = api_instance.list_taxonomies()
        print("The response of TagsApi->list_taxonomies:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->list_taxonomies: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

[**List[Taxonomy]**](Taxonomy.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of available tag taxonomies. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **report_tag**
> UserTagReportResponse report_tag(user_reported_tag, x_consumer_username=x_consumer_username)

Submit a user-reported tag

Stores a user-reported tag submission for review.

### Example

* Api Key Authentication (api_key):

```python
import graphsense
from graphsense.models.user_reported_tag import UserReportedTag
from graphsense.models.user_tag_report_response import UserTagReportResponse
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
    api_instance = graphsense.TagsApi(api_client)
    user_reported_tag = graphsense.UserReportedTag() # UserReportedTag | 
    x_consumer_username = 'x_consumer_username_example' # str |  (optional)

    try:
        # Submit a user-reported tag
        api_response = api_instance.report_tag(user_reported_tag, x_consumer_username=x_consumer_username)
        print("The response of TagsApi->report_tag:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling TagsApi->report_tag: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **user_reported_tag** | [**UserReportedTag**](UserReportedTag.md)|  | 
 **x_consumer_username** | **str**|  | [optional] 

### Return type

[**UserTagReportResponse**](UserTagReportResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Confirmation that the tag report was accepted. |  -  |
**422** | Validation error in request body. |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

