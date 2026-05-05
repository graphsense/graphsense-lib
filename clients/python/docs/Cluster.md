# Cluster

Neighbor cluster reference (preferred alias for `entity`).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  | 
**entity** | **int** | Deprecated alias of &#x60;cluster&#x60;. Use &#x60;cluster&#x60; instead; this field is retained for backwards compatibility and will be removed in a future release. | 
**root_address** | **str** |  | 
**balance** | [**Values**](Values.md) |  | 
**total_received** | [**Values**](Values.md) |  | 
**total_spent** | [**Values**](Values.md) |  | 
**first_tx** | [**TxSummary**](TxSummary.md) |  | 
**last_tx** | [**TxSummary**](TxSummary.md) |  | 
**in_degree** | **int** |  | 
**out_degree** | **int** |  | 
**no_addresses** | **int** |  | 
**no_incoming_txs** | **int** |  | 
**no_outgoing_txs** | **int** |  | 
**no_address_tags** | **int** |  | 
**token_balances** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**total_tokens_received** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**total_tokens_spent** | [**Dict[str, Values]**](Values.md) |  | [optional] 
**actors** | [**List[LabeledItemRef]**](LabeledItemRef.md) |  | [optional] 
**best_address_tag** | [**AddressTag**](AddressTag.md) |  | [optional] 
**cluster** | **int** | Cluster ID (preferred alias for the deprecated &#x60;entity&#x60; field). | [readonly] 

## Example

```python
from graphsense.models.cluster import Cluster

# TODO update the JSON string below
json = "{}"
# create an instance of Cluster from a JSON string
cluster_instance = Cluster.from_json(json)
# print the JSON string representation of the object
print(Cluster.to_json())

# convert the object into a dict
cluster_dict = cluster_instance.to_dict()
# create an instance of Cluster from a dict
cluster_from_dict = Cluster.from_dict(cluster_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


