# ActorContext

Actor context model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**uris** | **List[str]** |  | 
**images** | **List[str]** |  | 
**refs** | **List[str]** |  | 
**coingecko_ids** | **List[str]** |  | 
**defilama_ids** | **List[str]** |  | 
**twitter_handle** | **str** |  | [optional] 
**github_organisation** | **str** |  | [optional] 
**legal_name** | **str** |  | [optional] 

## Example

```python
from graphsense.models.actor_context import ActorContext

# TODO update the JSON string below
json = "{}"
# create an instance of ActorContext from a JSON string
actor_context_instance = ActorContext.from_json(json)
# print the JSON string representation of the object
print(ActorContext.to_json())

# convert the object into a dict
actor_context_dict = actor_context_instance.to_dict()
# create an instance of ActorContext from a dict
actor_context_from_dict = ActorContext.from_dict(actor_context_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


