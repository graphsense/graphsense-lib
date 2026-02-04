# Concept

Concept model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  |
**label** | **str** |  |
**taxonomy** | **str** |  |
**uri** | **str** |  | [optional]
**description** | **str** |  | [optional]

## Example

```python
from graphsense.models.concept import Concept

# TODO update the JSON string below
json = "{}"
# create an instance of Concept from a JSON string
concept_instance = Concept.from_json(json)
# print the JSON string representation of the object
print(Concept.to_json())

# convert the object into a dict
concept_dict = concept_instance.to_dict()
# create an instance of Concept from a dict
concept_from_dict = Concept.from_dict(concept_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
