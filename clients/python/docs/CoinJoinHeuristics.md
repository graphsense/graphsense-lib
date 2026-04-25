# CoinJoinHeuristics


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**consensus** | [**CoinJoinConsensus**](CoinJoinConsensus.md) |  | [optional] 
**joinmarket** | [**JoinMarketHeuristic**](JoinMarketHeuristic.md) |  | [optional] 
**wasabi** | [**WasabiHeuristic**](WasabiHeuristic.md) |  | [optional] 
**whirlpool_tx0** | [**WhirlpoolTx0Heuristic**](WhirlpoolTx0Heuristic.md) |  | [optional] 
**whirlpool_coinjoin** | [**WhirlpoolCoinJoinHeuristic**](WhirlpoolCoinJoinHeuristic.md) |  | [optional] 

## Example

```python
from graphsense.models.coin_join_heuristics import CoinJoinHeuristics

# TODO update the JSON string below
json = "{}"
# create an instance of CoinJoinHeuristics from a JSON string
coin_join_heuristics_instance = CoinJoinHeuristics.from_json(json)
# print the JSON string representation of the object
print(CoinJoinHeuristics.to_json())

# convert the object into a dict
coin_join_heuristics_dict = coin_join_heuristics_instance.to_dict()
# create an instance of CoinJoinHeuristics from a dict
coin_join_heuristics_from_dict = CoinJoinHeuristics.from_dict(coin_join_heuristics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


