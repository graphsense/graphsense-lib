# GraphAddressOverall

Network-agnostic rollup over all addresses in the set (fiat totals per code, usage span, tag overview). ``actors`` are distinct across networks.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address_count** | **int** |  | 
**total_received_fiat** | [**List[Rate]**](Rate.md) |  | [optional] 
**total_spent_fiat** | [**List[Rate]**](Rate.md) |  | [optional] 
**balance_fiat** | [**List[Rate]**](Rate.md) |  | [optional] 
**first_usage** | **int** |  | [optional] 
**last_usage** | **int** |  | [optional] 
**tagged_address_count** | **int** |  | [optional] [default to 0]
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional] 
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.graph_address_overall import GraphAddressOverall

# TODO update the JSON string below
json = "{}"
# create an instance of GraphAddressOverall from a JSON string
graph_address_overall_instance = GraphAddressOverall.from_json(json)
# print the JSON string representation of the object
print(GraphAddressOverall.to_json())

# convert the object into a dict
graph_address_overall_dict = graph_address_overall_instance.to_dict()
# create an instance of GraphAddressOverall from a dict
graph_address_overall_from_dict = GraphAddressOverall.from_dict(graph_address_overall_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


