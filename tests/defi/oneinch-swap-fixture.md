# 1inch swap regression fixture

The regression tests use transaction
`0x3e6538270a0b7dbb9bc00a80ee9c4ac5487b9a4dd94c73e753e45538219f0cf2` instead of the original transaction.

It is a successful Ethereum mainnet transaction from
`0xedcea136f0f7e5d51e1834bd96937847089fcdd4` to the same 1inch router,
`0x111111125421ca6dc452d289314280a0f8842a65`. Its calldata uses selector
`0x07ed2379`, the 1inch `swap` function that was missing from the function
signature registry. The call swaps native ETH for GOOGLX
(`0xe92f673ca36c5e2efd2de7628f815f84807e803f`).

The receipt contains WETH `Deposit` and `Approval` events and token transfers,
including the final GOOGLX transfer to the sender at log index 1864. This makes
the fixture exercise call-level swap decoding and the resulting input/output
payment flow with a different sender and a real token output.

Sources:

- [Transaction](https://eth.blockscout.com/api/v2/transactions/0x3e6538270a0b7dbb9bc00a80ee9c4ac5487b9a4dd94c73e753e45538219f0cf2)
- [Transaction logs](https://eth.blockscout.com/api/v2/transactions/0x3e6538270a0b7dbb9bc00a80ee9c4ac5487b9a4dd94c73e753e45538219f0cf2/logs)
- [Internal transactions](https://eth.blockscout.com/api/v2/transactions/0x3e6538270a0b7dbb9bc00a80ee9c4ac5487b9a4dd94c73e753e45538219f0cf2/internal-transactions?items_count=50)
