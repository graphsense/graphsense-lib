# SubgraphAddressSummary

Aggregate stats over the addresses in a subgraph.  Value totals are in the chain's base unit; the ``*_fiat`` fields sum the ``fiat_currency`` value across the set. ``first_usage`` / ``last_usage`` span the set's on-chain activity and are omitted when no selected address has any. ``tagged_address_count`` counts addresses with at least one visible tag; ``actors`` lists the distinct actors across all tags on the set. ``notes`` flags caveats (partial fiat totals, token holdings excluded from native totals).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**address_count** | **int** |  | 
**total_received** | **int** |  | 
**total_received_fiat** | **float** |  | [optional] 
**total_spent** | **int** |  | 
**total_spent_fiat** | **float** |  | [optional] 
**balance** | **int** |  | 
**balance_fiat** | **float** |  | [optional] 
**fiat_currency** | **str** |  | [optional] [default to 'usd']
**first_usage** | **int** |  | [optional] 
**last_usage** | **int** |  | [optional] 
**tagged_address_count** | **int** |  | [optional] [default to 0]
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional] 
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.subgraph_address_summary import SubgraphAddressSummary

# TODO update the JSON string below
json = "{}"
# create an instance of SubgraphAddressSummary from a JSON string
subgraph_address_summary_instance = SubgraphAddressSummary.from_json(json)
# print the JSON string representation of the object
print(SubgraphAddressSummary.to_json())

# convert the object into a dict
subgraph_address_summary_dict = subgraph_address_summary_instance.to_dict()
# create an instance of SubgraphAddressSummary from a dict
subgraph_address_summary_from_dict = SubgraphAddressSummary.from_dict(subgraph_address_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


