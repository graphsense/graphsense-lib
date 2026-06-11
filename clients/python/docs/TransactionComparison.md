# TransactionComparison

Top-level response for /txs/compare.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**txs** | [**List[TxComparedItem]**](TxComparedItem.md) |  | 
**signals** | [**List[ComparisonSignal]**](ComparisonSignal.md) |  | 
**lineage** | [**List[LineageEdge]**](LineageEdge.md) |  | [optional] 
**verdict** | [**ComparisonVerdict**](ComparisonVerdict.md) |  | [optional] 

## Example

```python
from graphsense.models.transaction_comparison import TransactionComparison

# TODO update the JSON string below
json = "{}"
# create an instance of TransactionComparison from a JSON string
transaction_comparison_instance = TransactionComparison.from_json(json)
# print the JSON string representation of the object
print(TransactionComparison.to_json())

# convert the object into a dict
transaction_comparison_dict = transaction_comparison_instance.to_dict()
# create an instance of TransactionComparison from a dict
transaction_comparison_from_dict = TransactionComparison.from_dict(transaction_comparison_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


