# GraphCompareNote

A machine-readable annotation on the verdict. ``code`` is the stable contract (closed vocabulary, shared with the internal model); ``message`` is display text and may be reworded without notice.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**code** | **str** |  | 
**message** | **str** |  | 

## Example

```python
from graphsense.models.graph_compare_note import GraphCompareNote

# TODO update the JSON string below
json = "{}"
# create an instance of GraphCompareNote from a JSON string
graph_compare_note_instance = GraphCompareNote.from_json(json)
# print the JSON string representation of the object
print(GraphCompareNote.to_json())

# convert the object into a dict
graph_compare_note_dict = graph_compare_note_instance.to_dict()
# create an instance of GraphCompareNote from a dict
graph_compare_note_from_dict = GraphCompareNote.from_dict(graph_compare_note_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


