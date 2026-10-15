# ServiceInfo

An optional service of this deployment behind this API.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | The feature it serves, e.g. \&quot;notifications\&quot; for the /notifications/* routes. | 
**available** | **bool** | Whether the service answered just now. False: it is enabled but unreachable; its routes answer 502 until it is back. | 
**version** | **str** | The service&#39;s release, when it answered. | [optional] 
**api** | **int** | Contract version of its routes; rises only on a breaking change. | [optional] 
**beta** | **bool** | Its routes may still change. | [optional] [default to False]
**limits** | **Dict[str, int]** | Configured limits a client can show, by name, as the service reports them, e.g. instant_mail_interval_minutes (at most one instant email per user in that time) or subscriptions_per_user. Unknown names must be ignored. | [optional] 

## Example

```python
from graphsense.models.service_info import ServiceInfo

# TODO update the JSON string below
json = "{}"
# create an instance of ServiceInfo from a JSON string
service_info_instance = ServiceInfo.from_json(json)
# print the JSON string representation of the object
print(ServiceInfo.to_json())

# convert the object into a dict
service_info_dict = service_info_instance.to_dict()
# create an instance of ServiceInfo from a dict
service_info_from_dict = ServiceInfo.from_dict(service_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


