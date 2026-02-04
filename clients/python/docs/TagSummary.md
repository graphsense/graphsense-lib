# TagSummary

Tag summary model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**broad_category** | **str** |  |
**tag_count** | **int** |  |
**label_summary** | [**Dict[str, LabelSummary]**](LabelSummary.md) |  |
**concept_tag_cloud** | [**Dict[str, TagCloudEntry]**](TagCloudEntry.md) |  |
**tag_count_indirect** | **int** |  | [optional]
**best_actor** | **str** |  | [optional]
**best_label** | **str** |  | [optional]

## Example

```python
from graphsense.models.tag_summary import TagSummary

# TODO update the JSON string below
json = "{}"
# create an instance of TagSummary from a JSON string
tag_summary_instance = TagSummary.from_json(json)
# print the JSON string representation of the object
print(TagSummary.to_json())

# convert the object into a dict
tag_summary_dict = tag_summary_instance.to_dict()
# create an instance of TagSummary from a dict
tag_summary_from_dict = TagSummary.from_dict(tag_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
