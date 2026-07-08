# TxCharacteristics

Extracted characteristics for a single transaction.  ``input_script_types`` / ``output_script_types`` hold the distinct script types observed across the inputs/outputs, sorted for stable output. Empty list means none could be derived from address strings.  Several internal fields are intentionally omitted from the API surface because the same information is exposed via the corresponding signals (``rbf``, ``witness_present``, ``bip69_outputs_sorted``, ``exchange_input_overlap``) and on-chain edge collections (``input_addresses_canon``, ``change_addresses_canon``, ``parent_tx_hashes``, ``utxo_parent_indexes``). Surface them here if a consumer needs the per-tx booleans alongside the comparison verdict.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**input_script_types** | **List[str]** |  | [optional] 
**output_script_types** | **List[str]** |  | [optional] 
**n_inputs** | **int** |  | 
**n_outputs** | **int** |  | 
**total_input_sat** | **int** |  | 
**total_output_sat** | **int** |  | 
**fee_sat** | **int** |  | [optional] 
**tx_version** | **int** |  | [optional] 
**locktime** | **int** |  | [optional] 
**input_cluster_ids** | **List[int]** |  | [optional] 
**coinjoin_detected** | **bool** |  | [optional] [default to False]
**coinjoin_protocol** | **str** |  | [optional] 

## Example

```python
from graphsense.models.tx_characteristics import TxCharacteristics

# TODO update the JSON string below
json = "{}"
# create an instance of TxCharacteristics from a JSON string
tx_characteristics_instance = TxCharacteristics.from_json(json)
# print the JSON string representation of the object
print(TxCharacteristics.to_json())

# convert the object into a dict
tx_characteristics_dict = tx_characteristics_instance.to_dict()
# create an instance of TxCharacteristics from a dict
tx_characteristics_from_dict = TxCharacteristics.from_dict(tx_characteristics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


