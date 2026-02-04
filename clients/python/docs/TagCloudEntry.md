# TagCloudEntry

Tag cloud entry model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**cnt** | **int** |  |
**weighted** | **float** |  |

## Example

```python
from graphsense.models.tag_cloud_entry import TagCloudEntry

# TODO update the JSON string below
json = "{}"
# create an instance of TagCloudEntry from a JSON string
tag_cloud_entry_instance = TagCloudEntry.from_json(json)
# print the JSON string representation of the object
print(TagCloudEntry.to_json())

# convert the object into a dict
tag_cloud_entry_dict = tag_cloud_entry_instance.to_dict()
# create an instance of TagCloudEntry from a dict
tag_cloud_entry_from_dict = TagCloudEntry.from_dict(tag_cloud_entry_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
