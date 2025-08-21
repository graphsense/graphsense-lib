from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from graphsenselib.utils.accountmodel import ensure_0x_prefix


class Trace(BaseModel):
    from_address: str
    to_address: str
    value: int
    is_call: bool
    trace_index: Optional[int]
    trace_address: Optional[str]
    value: int

    def dicts_to_normalized(
        network: str, trace_dicts: List[Dict[str, Any]], tx: Dict[str, Any]
    ) -> List[Optional["Trace"]]:
        traces = []
        for trace in trace_dicts:
            if network == "eth":
                is_call = trace["call_type"] == "call"
                trace_index = trace["trace_index"]
                from_address = ensure_0x_prefix(trace["from_address"].hex()).lower()
                to_address = ensure_0x_prefix(trace["to_address"].hex()).lower()
                value = trace["value"]
                trace_address = trace["trace_address"]

            if network == "trx":
                is_call = trace["note"] == "call"
                trace_index = trace["trace_index"]
                from_address = ensure_0x_prefix(trace["caller_address"].hex()).lower()
                to_address = ensure_0x_prefix(trace["transferto_address"].hex()).lower()
                value = trace["call_value"]
                trace_address = None
                if trace["call_token_id"] is None:
                    continue

            traces.append(
                Trace(
                    is_call=is_call,
                    trace_index=trace_index,
                    from_address=from_address,
                    to_address=to_address,
                    value=value,
                    trace_address=trace_address,
                )
            )

        if network == "trx":
            # for trx the first trace is the tx itself
            is_call = True
            trace_index = None
            from_address = ensure_0x_prefix(tx["from_address"].hex())
            to_address = ensure_0x_prefix(tx["to_address"].hex())
            value = tx["value"]
            trace_address = ""
            traces = [
                Trace(
                    is_call=is_call,
                    trace_index=trace_index,
                    from_address=from_address,
                    to_address=to_address,
                    value=value,
                    trace_address=trace_address,
                )
            ] + traces

        return traces
