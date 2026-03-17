# UtxoHeuristics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**change_heuristics** | [**ChangeHeuristics**](ChangeHeuristics.md) |  | [optional] 

## Example

```python
from graphsense.models.utxo_heuristics import UtxoHeuristics

# TODO update the JSON string below
json = "{}"
# create an instance of UtxoHeuristics from a JSON string
utxo_heuristics_instance = UtxoHeuristics.from_json(json)
# print the JSON string representation of the object
print(UtxoHeuristics.to_json())

# convert the object into a dict
utxo_heuristics_dict = utxo_heuristics_instance.to_dict()
# create an instance of UtxoHeuristics from a dict
utxo_heuristics_from_dict = UtxoHeuristics.from_dict(utxo_heuristics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


