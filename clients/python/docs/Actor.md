# Actor

Actor model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  |
**label** | **str** |  |
**uri** | **str** |  |
**categories** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  |
**jurisdictions** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  |
**nr_tags** | **int** |  | [optional]
**context** | [**ActorContext**](ActorContext.md) |  | [optional]

## Example

```python
from graphsense.models.actor import Actor

# TODO update the JSON string below
json = "{}"
# create an instance of Actor from a JSON string
actor_instance = Actor.from_json(json)
# print the JSON string representation of the object
print(Actor.to_json())

# convert the object into a dict
actor_dict = actor_instance.to_dict()
# create an instance of Actor from a dict
actor_from_dict = Actor.from_dict(actor_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
