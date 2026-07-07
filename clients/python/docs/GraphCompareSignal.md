# GraphCompareSignal

One row of the pairwise comparison table.  ``per_tx`` holds one typed observation per compared tx, aligned with the response's ``txs`` order. The value type depends on the signal: booleans for flag signals (``witness_present``, ``rbf`` — true means BIP-125 signaled, ``bip69_outputs_sorted``, ``exchange_input_overlap`` — true means the tx has an exchange-tagged input); an integer for ``tx_version``; categorical snake_case strings for ``locktime_pattern`` (``zero``/``anti_sniping``/``other``) and ``output_count_shape`` (``single``/``pay_plus_change``/``many``); sorted string lists for ``script_type`` (the tx's distinct input script types), ``direct_input_overlap`` (input addresses shared with peer txs), ``change_chain`` (own change addresses spent by peer txs) and ``common_ancestor`` (parent tx hashes shared with peers); sorted integer lists for ``utxo_linkage`` (indexes of peer txs with a direct spend edge) and ``shared_cluster`` (the tx's own input cluster ids). ``null`` means the value was not derivable for that tx; an empty list means computed, but no items.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** |  | 
**kind** | **str** |  | 
**per_tx** | [**List[PerTxInner]**](PerTxInner.md) |  | 
**verdict** | **str** |  | 
**weight** | **int** |  | [optional] [default to 0]

## Example

```python
from graphsense.models.graph_compare_signal import GraphCompareSignal

# TODO update the JSON string below
json = "{}"
# create an instance of GraphCompareSignal from a JSON string
graph_compare_signal_instance = GraphCompareSignal.from_json(json)
# print the JSON string representation of the object
print(GraphCompareSignal.to_json())

# convert the object into a dict
graph_compare_signal_dict = graph_compare_signal_instance.to_dict()
# create an instance of GraphCompareSignal from a dict
graph_compare_signal_from_dict = GraphCompareSignal.from_dict(graph_compare_signal_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


