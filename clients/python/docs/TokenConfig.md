# TokenConfig

Token configuration model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ticker** | **str** |  |
**decimals** | **int** |  |
**peg_currency** | **str** |  | [optional]
**contract_address** | **str** |  | [optional]

## Example

```python
from graphsense.models.token_config import TokenConfig

# TODO update the JSON string below
json = "{}"
# create an instance of TokenConfig from a JSON string
token_config_instance = TokenConfig.from_json(json)
# print the JSON string representation of the object
print(TokenConfig.to_json())

# convert the object into a dict
token_config_dict = token_config_instance.to_dict()
# create an instance of TokenConfig from a dict
token_config_from_dict = TokenConfig.from_dict(token_config_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
