# LabeledItemRef

Reference to a labeled item.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **str** |  |
**label** | **str** |  |

## Example

```python
from graphsense.models.labeled_item_ref import LabeledItemRef

# TODO update the JSON string below
json = "{}"
# create an instance of LabeledItemRef from a JSON string
labeled_item_ref_instance = LabeledItemRef.from_json(json)
# print the JSON string representation of the object
print(LabeledItemRef.to_json())

# convert the object into a dict
labeled_item_ref_dict = labeled_item_ref_instance.to_dict()
# create an instance of LabeledItemRef from a dict
labeled_item_ref_from_dict = LabeledItemRef.from_dict(labeled_item_ref_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
