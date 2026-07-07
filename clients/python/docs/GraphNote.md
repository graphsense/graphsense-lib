# GraphNote

A caveat attached to a summary block. ``code`` is the stable machine-readable contract (closed vocabulary, shared with the internal model so new codes surface in the OpenAPI schema); ``message`` is display text and may be reworded without notice. ``network`` attributes overall-rollup notes to their source network. ``items`` carries the references a note applies to (e.g. the not-found tx hashes / addresses of a ``nodes_not_found`` note), so clients never have to parse ``message``.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**code** | **str** |  | 
**message** | **str** |  | 
**network** | **str** |  | [optional] 
**items** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.graph_note import GraphNote

# TODO update the JSON string below
json = "{}"
# create an instance of GraphNote from a JSON string
graph_note_instance = GraphNote.from_json(json)
# print the JSON string representation of the object
print(GraphNote.to_json())

# convert the object into a dict
graph_note_dict = graph_note_instance.to_dict()
# create an instance of GraphNote from a dict
graph_note_from_dict = GraphNote.from_dict(graph_note_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


