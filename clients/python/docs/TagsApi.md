# graphsense.TagsApi

All URIs are relative to *https://api.iknaio.com*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_actor**](TagsApi.md#get_actor) | **GET** /tags/actors/{actor} | Get an actor by ID
[**get_actor_tags**](TagsApi.md#get_actor_tags) | **GET** /tags/actors/{actor}/tags | Get tags associated with an actor
[**list_address_tags**](TagsApi.md#list_address_tags) | **GET** /tags | Get address tags by label
[**list_concepts**](TagsApi.md#list_concepts) | **GET** /tags/taxonomies/{taxonomy}/concepts | List concepts for a taxonomy
[**list_taxonomies**](TagsApi.md#list_taxonomies) | **GET** /tags/taxonomies | List all taxonomies
[**report_tag**](TagsApi.md#report_tag) | **POST** /tags/report-tag | Report a new tag


# **get_actor**
> Actor get_actor(actor)

Get an actor by ID

Get an actor by ID

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.TagsApi(api_client)
    actor = 'binance' # str | The actor ID

    try:
        # Get an actor by ID
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

# **get_actor_tags**
> AddressTags get_actor_tags(actor, page=page, pagesize=pagesize)

Get tags associated with an actor

Get tags associated with an actor

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
    api_instance = graphsense.TagsApi(api_client)
    actor = 'binance' # str | The actor ID
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # Get tags associated with an actor
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

# **list_address_tags**
> AddressTags list_address_tags(label, page=page, pagesize=pagesize)

Get address tags by label

Get address tags by label

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
    api_instance = graphsense.TagsApi(api_client)
    label = 'cimedy' # str | The label to search for
    page = 'page_example' # str | Resumption token for retrieving the next page (optional)
    pagesize = 10 # int | Number of items returned in a single page (optional)

    try:
        # Get address tags by label
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

# **list_concepts**
> List[Concept] list_concepts(taxonomy)

List concepts for a taxonomy

List concepts for a taxonomy

### Example


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

# **list_taxonomies**
> List[Taxonomy] list_taxonomies()

List all taxonomies

List all taxonomies

### Example


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

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **report_tag**
> UserTagReportResponse report_tag(user_reported_tag, x_consumer_username=x_consumer_username)

Report a new tag

Report a new tag

### Example


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


# Enter a context with an instance of the API client
with graphsense.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = graphsense.TagsApi(api_client)
    user_reported_tag = graphsense.UserReportedTag() # UserReportedTag | 
    x_consumer_username = 'x_consumer_username_example' # str |  (optional)

    try:
        # Report a new tag
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

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

