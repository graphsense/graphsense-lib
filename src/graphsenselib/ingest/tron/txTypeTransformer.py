import logging

logger = logging.getLogger(__name__)


class TxTypeTransformer:
    def __init__(self):
        self.type_to_transform_fkt = {
            0: self.AccountCreateContract,
            1: self.TransferContract,
            2: self.TransferAssetContract,
            3: self.no_handling,
            4: self.VoteWitnessContract,
            5: self.no_handling,
            6: self.no_handling,
            8: self.no_handling,
            9: self.no_handling,
            10: self.no_handling,
            11: self.FreezeBalanceContract,
            12: self.UnfreezeBalanceContract,
            13: self.WithdrawBalanceContract,
            14: self.no_handling,
            15: self.no_handling,
            16: self.no_handling,
            17: self.no_handling,
            18: self.no_handling,
            19: self.no_handling,
            20: self.no_handling,
            30: self.CreateSmartContract,
            31: self.TriggerSmartContract,
            32: self.no_handling,
            33: self.no_handling,
            41: self.no_handling,
            42: self.no_handling,
            43: self.no_handling,
            44: self.ExchangeTransactionContract,
            45: self.no_handling,
            46: self.AccountPermissionUpdateContract,
            48: self.no_handling,
            49: self.no_handling,
            51: self.no_handling,
            52: self.no_handling,
            53: self.no_handling,
            54: self.FreezeBalanceV2Contract,
            55: self.UnfreezeBalanceV2Contract,
            56: self.WithdrawExpireUnfreezeContract,
            57: self.DelegateResourceContract,
            58: self.UnDelegateResourceContract,
            59: self.CancelAllUnfreezeV2Contract,
        }

    def type_not_supported(self, type_):
        logger.warning(
            f"transaction_type {type_} not considered. Probably new. Check "
            "https://github.com/tronprotocol/java-tron/"
            "blob/develop/Tron%20protobuf%20protocol%20document.md"
            "and update the function mapping accordingly if necessary "
            "with a new transformation fkt."
        )
        return None

    def transform(self, x):
        type_ = x["transaction_type"]
        f = self.type_to_transform_fkt.get(type_)
        if f is None:
            self.type_not_supported(type_)
            return self.no_handling(x)
        return f(x)

    def no_handling(self, x):
        return x

    def WithdrawBalanceContract(self, x):
        """
        Example: e4f4ab696d4f3e00cfc41a7b89a93bd4b95b82d5faeaa70b9813c8aa558081da
        Swap to and from because tron shows that the recipient of the rewards is
        the sender of the transaction
        and sends those rewards to the null address. This does not represent the
        flow of funds, therefore we reverse it.
        """
        x["to_address"], x["from_address"] = x["from_address"], x["to_address"]
        return x

    def AccountCreateContract(self, x):
        """
        Example: a15559a627a9691097c6809be8d2815b768e41e045afeceaedaeda15c168c39c
        Value = 0 in test sample of 100 tx
        grpc_GetTransactionInfoById.fee = 1000000
        Fine
        """
        return x

    def TransferContract(self, x):
        """
        Example: dcdfb509b33d493e9553b279675a8e9053701cfc3088519f205feca302a3ff02
        Fine
        """
        return x

    def TransferAssetContract(self, x):
        """
        Example: 99cd1c4b193d2dd0ae703c457726c0dfe82ba2c785d8cad66f3b5c45fef25ec6
        Value = Number (In smallest denomination) of TRC10 Token transferred!
        This would break our pipeline because value is TRX for us, not some token
        """
        x["value"] = 0
        return x

    def VoteWitnessContract(self, x):
        """
        Example: 7f973ec59417fb12375b3475e16a4a4bb150cf0a8f64cb6e19093183525b215c
        Value = Number of Votes; not TRX -> set value to 0
        """
        x["value"] = 0
        return x

    def FreezeBalanceContract(self, x):
        """
        Example: Not found in the sample but to be safe:
        Set value to 0 like in UnfreezeBalanceContract
        """
        x["value"] = 0
        return x

    def UnfreezeBalanceContract(self, x):
        """
        Example: d2fcbf5cdeb3efc06d90012838c6e559d12dde1cf4711afe1e798e694c70595c
        from: unstaker
        to: None
        value: Quantity unstaked != 0
        """
        x["value"] = 0
        return x

    def CreateSmartContract(self, x):
        """
        Example: 140e3404392a690e31c7009b7cee2b8dc84ecf58c391cebbd531adf7772efae7
        from: Creator
        to: None
        Value: 0

        todo: In the small simulation, those tx appear duplicated very often.
            Might be worth it to check that in productive system aswell.
        fine
        """

        return x

    def TriggerSmartContract(self, x):
        """
        Example: a53187fb4b99c53e1447637285bc336679a5ce3d11857793391875ccbae214ca
        Example2: 8c9656fcc6bd588d5a02f5aaeb795ebcf6422919a353c0df36378927fe611027
        from: EOA
        to: CA
        value: Mostly 0, sometimes TRX value if TRX is paid

        Fine
        """
        return x

    def ExchangeTransactionContract(self, x):
        """
        Example: 49b2b69dc736323b99177b79c3c0e4945747cd5900ef7770bc0480b49dc73ae0
        from: owner
        to: None
        value: Quantity of asset consumed to buy TRX TODO: how much TRX gotten?
        todo: In the small simulation, those tx appear duplicated very often.
            Might be worth it to check that in productive system aswell.
        """
        return x

    def AccountPermissionUpdateContract(self, x):
        """
        Example: b820ae2b84983aa2a11bd4b4927b612b64f9a6064026275dcc75850398927044
        Update account permissions
        from: account owner
        to: None
        value: 100.000.000 (100 TRX)
        value == grpc_GetTransactionInfoById.fee
        """
        x["value"] = 0
        return x

    def FreezeBalanceV2Contract(self, x):
        """
        Example: b1509e72d6ac99e0e1efccfb91ddc0d1c0a6d4408c1810ce73fc9407692502b4
        sender account staked xxx TRX and obtained Energy & TRON Power via Stake 2.0
        from: staker
        to: None
        value: 0
        """

        return x

    def UnfreezeBalanceV2Contract(self, x):
        """
        Example: bb3e61ac76603e5a0043a6c3cfb3b702213f3a24da8f5ac6f6e6892bf317022c
        Unstake xxx asset in Stake2.0, Energy & Tron power are deducted from the account
        from: unstaker
        to: None
        value = 0 != Quantity of assets unstaked
        """
        return x

    def WithdrawExpireUnfreezeContract(self, x):
        """
        Example: a4e6b5521a5d7c68865a8d5e5eb7f181442adc4fbc6979f34711924c70f55346
        Withdraw unstaked asset
        Seems to be general (asset; not just TRX) but in the few contracts
        I checked it was only TRX
        from = Unstaker
        to = None
        Value = Quantity (smallest denomination) of that asset.
        """
        x["value"] = 0
        return x

    def DelegateResourceContract(self, x):
        """
        Example: edba09c24eabd3ddbcd0df53732c780be3f2a8e396babdf6b2e2c46c69d0a07e
        Value = 0 in test sample of 100 tx
        """
        return x

    def UnDelegateResourceContract(self, x):
        """
        Example: 0b51f06e4677125dedb753d1aee6be8942b4689377b95fb9ec12e0020b38a790
        Staked assets are released
        Value = 0;
        Dont yet understand "from" and "to" relationship, both seem to be
        regular accounts but with tons of tx
        """
        return x

    def CancelAllUnfreezeV2Contract(self, x):
        """
        Example: 45d69c16dcb2e18f59e9d1a71a6644456aef74c1460bae29dc7506b153f1ae3d
        Cancel Unstake
        from: unstaker canceller
        to: None
        Value: Tron value to cancel from unstaking != 0
        todo: In the small simulation, those tx appear duplicated very often.
            Might be worth it to check that in productive system aswell.
        """
        x["value"] = 0
        return x
