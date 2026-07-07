# GraphTxNetworkSummary

Aggregate stats over one network's transactions.  ``total_value.value`` is the network's native base unit (satoshi for UTXO, wei/sun for account chains) and sums native transfers only; ``total_value.fiat_values`` sum per fiat code across all transfers, including tokens. ``total_fee`` stays in the native unit. ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for account-model summaries. ``notes`` flags caveats. ``assets`` lists the distinct assets involved on this network (lowercase, native first then tokens sorted).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**network** | **str** |  | 
**tx_count** | **int** |  | 
**total_value** | [**Values**](Values.md) |  | 
**total_fee** | **int** |  | [optional] 
**total_inputs** | **int** |  | [optional] 
**total_outputs** | **int** |  | [optional] 
**block_min** | **int** |  | 
**block_max** | **int** |  | 
**timestamp_min** | **int** |  | 
**timestamp_max** | **int** |  | 
**notes** | [**List[GraphNote]**](GraphNote.md) |  | [optional] 
**assets** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.graph_tx_network_summary import GraphTxNetworkSummary

# TODO update the JSON string below
json = "{}"
# create an instance of GraphTxNetworkSummary from a JSON string
graph_tx_network_summary_instance = GraphTxNetworkSummary.from_json(json)
# print the JSON string representation of the object
print(GraphTxNetworkSummary.to_json())

# convert the object into a dict
graph_tx_network_summary_dict = graph_tx_network_summary_instance.to_dict()
# create an instance of GraphTxNetworkSummary from a dict
graph_tx_network_summary_from_dict = GraphTxNetworkSummary.from_dict(graph_tx_network_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


