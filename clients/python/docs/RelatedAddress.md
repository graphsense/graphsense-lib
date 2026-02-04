# RelatedAddress

Related address model (cross-chain).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address** | **str** |  |
**currency** | **str** |  |
**relation_type** | **str** |  |

## Example

```python
from graphsense.models.related_address import RelatedAddress

# TODO update the JSON string below
json = "{}"
# create an instance of RelatedAddress from a JSON string
related_address_instance = RelatedAddress.from_json(json)
# print the JSON string representation of the object
print(RelatedAddress.to_json())

# convert the object into a dict
related_address_dict = related_address_instance.to_dict()
# create an instance of RelatedAddress from a dict
related_address_from_dict = RelatedAddress.from_dict(related_address_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
