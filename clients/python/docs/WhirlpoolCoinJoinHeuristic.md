# WhirlpoolCoinJoinHeuristic


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**detected** | **bool** |  | 
**confidence** | **int** |  | 
**pool_denomination_sat** | **int** |  | 
**n_remixers** | **int** |  | 
**n_new_entrants** | **int** |  | 

## Example

```python
from graphsense.models.whirlpool_coin_join_heuristic import WhirlpoolCoinJoinHeuristic

# TODO update the JSON string below
json = "{}"
# create an instance of WhirlpoolCoinJoinHeuristic from a JSON string
whirlpool_coin_join_heuristic_instance = WhirlpoolCoinJoinHeuristic.from_json(json)
# print the JSON string representation of the object
print(WhirlpoolCoinJoinHeuristic.to_json())

# convert the object into a dict
whirlpool_coin_join_heuristic_dict = whirlpool_coin_join_heuristic_instance.to_dict()
# create an instance of WhirlpoolCoinJoinHeuristic from a dict
whirlpool_coin_join_heuristic_from_dict = WhirlpoolCoinJoinHeuristic.from_dict(whirlpool_coin_join_heuristic_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


