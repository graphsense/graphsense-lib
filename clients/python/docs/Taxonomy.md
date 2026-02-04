# Taxonomy

Taxonomy model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**taxonomy** | **str** |  |
**uri** | **str** |  |

## Example

```python
from graphsense.models.taxonomy import Taxonomy

# TODO update the JSON string below
json = "{}"
# create an instance of Taxonomy from a JSON string
taxonomy_instance = Taxonomy.from_json(json)
# print the JSON string representation of the object
print(Taxonomy.to_json())

# convert the object into a dict
taxonomy_dict = taxonomy_instance.to_dict()
# create an instance of Taxonomy from a dict
taxonomy_from_dict = Taxonomy.from_dict(taxonomy_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
