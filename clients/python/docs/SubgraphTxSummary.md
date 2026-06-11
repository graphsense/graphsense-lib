# SubgraphTxSummary

Aggregate stats over the transactions in a subgraph.  ``total_value`` and ``total_fee`` are in the chain's base unit (satoshi for UTXO, wei/sun for account chains); ``total_value`` sums native transfers only (token transfers carry no native-unit amount). ``total_value_fiat`` sums the fiat value (in ``fiat_currency``) across all transfers, including tokens, so it is comparable across assets. ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for account-model (ETH/TRX) summaries. ``notes`` flags caveats (e.g. a partial fiat total when some txs had no rate, or token transfers excluded from ``total_value``).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_count** | **int** |  | 
**total_value** | **int** |  | 
**total_value_fiat** | **float** |  | [optional] 
**fiat_currency** | **str** |  | [optional] [default to 'usd']
**total_fee** | **int** |  | [optional] 
**total_inputs** | **int** |  | [optional] 
**total_outputs** | **int** |  | [optional] 
**block_min** | **int** |  | 
**block_max** | **int** |  | 
**timestamp_min** | **int** |  | 
**timestamp_max** | **int** |  | 
**notes** | **List[str]** |  | [optional] 

## Example

```python
from graphsense.models.subgraph_tx_summary import SubgraphTxSummary

# TODO update the JSON string below
json = "{}"
# create an instance of SubgraphTxSummary from a JSON string
subgraph_tx_summary_instance = SubgraphTxSummary.from_json(json)
# print the JSON string representation of the object
print(SubgraphTxSummary.to_json())

# convert the object into a dict
subgraph_tx_summary_dict = subgraph_tx_summary_instance.to_dict()
# create an instance of SubgraphTxSummary from a dict
subgraph_tx_summary_from_dict = SubgraphTxSummary.from_dict(subgraph_tx_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


