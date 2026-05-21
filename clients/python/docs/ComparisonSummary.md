# ComparisonSummary

Aggregate stats over all compared transactions.  ``total_value`` and ``total_fee`` are in the chain's base unit (satoshi for UTXO, wei/sun for account chains); ``total_value`` sums native transfers only (token transfers carry no native-unit amount). ``total_value_usd`` sums the USD fiat value across all transfers, including tokens, so it is comparable across assets. ``total_inputs`` / ``total_outputs`` are UTXO-only and omitted for account-model (ETH/TRX) summaries. ``notes`` flags caveats (e.g. a partial USD total when some txs had no rate, or token transfers excluded from ``total_value``).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tx_count** | **int** |  | 
**currency** | **str** |  | 
**total_value** | **int** |  | 
**total_value_usd** | **float** |  | [optional] 
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
from graphsense.models.comparison_summary import ComparisonSummary

# TODO update the JSON string below
json = "{}"
# create an instance of ComparisonSummary from a JSON string
comparison_summary_instance = ComparisonSummary.from_json(json)
# print the JSON string representation of the object
print(ComparisonSummary.to_json())

# convert the object into a dict
comparison_summary_dict = comparison_summary_instance.to_dict()
# create an instance of ComparisonSummary from a dict
comparison_summary_from_dict = ComparisonSummary.from_dict(comparison_summary_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


