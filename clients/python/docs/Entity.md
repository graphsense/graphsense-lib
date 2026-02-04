# Entity


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**currency** | **str** |  |
**entity** | **int** |  |
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

## Example

```python
from graphsense.models.entity import Entity

# TODO update the JSON string below
json = "{}"
# create an instance of Entity from a JSON string
entity_instance = Entity.from_json(json)
# print the JSON string representation of the object
print(Entity.to_json())

# convert the object into a dict
entity_dict = entity_instance.to_dict()
# create an instance of Entity from a dict
entity_from_dict = Entity.from_dict(entity_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
