# GraphLineageEdge

Direct on-chain relationship between two compared transactions.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**from_idx** | **int** |  | 
**to_idx** | **int** |  | 
**kind** | **str** |  | 
**out_index** | **int** |  | [optional] 
**in_index** | **int** |  | [optional] 

## Example

```python
from graphsense.models.graph_lineage_edge import GraphLineageEdge

# TODO update the JSON string below
json = "{}"
# create an instance of GraphLineageEdge from a JSON string
graph_lineage_edge_instance = GraphLineageEdge.from_json(json)
# print the JSON string representation of the object
print(GraphLineageEdge.to_json())

# convert the object into a dict
graph_lineage_edge_dict = graph_lineage_edge_instance.to_dict()
# create an instance of GraphLineageEdge from a dict
graph_lineage_edge_from_dict = GraphLineageEdge.from_dict(graph_lineage_edge_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


