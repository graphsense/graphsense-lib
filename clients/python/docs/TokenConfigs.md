# TokenConfigs

List of token configurations.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**token_configs** | [**List[TokenConfig]**](TokenConfig.md) |  |

## Example

```python
from graphsense.models.token_configs import TokenConfigs

# TODO update the JSON string below
json = "{}"
# create an instance of TokenConfigs from a JSON string
token_configs_instance = TokenConfigs.from_json(json)
# print the JSON string representation of the object
print(TokenConfigs.to_json())

# convert the object into a dict
token_configs_dict = token_configs_instance.to_dict()
# create an instance of TokenConfigs from a dict
token_configs_from_dict = TokenConfigs.from_dict(token_configs_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
