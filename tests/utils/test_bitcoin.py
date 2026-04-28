from graphsenselib.utils.bitcoin import is_rbf_signaled


def test_all_final_sequences_are_not_rbf():
    assert is_rbf_signaled([0xFFFFFFFF, 0xFFFFFFFF]) is False


def test_pre_final_sentinel_is_not_rbf():
    # 0xfffffffe disables nLocktime weirdness but is NOT RBF-signaling
    assert is_rbf_signaled([0xFFFFFFFE]) is False


def test_any_input_below_sentinel_signals_rbf():
    assert is_rbf_signaled([0xFFFFFFFF, 0xFFFFFFFD]) is True


def test_zero_signals_rbf():
    assert is_rbf_signaled([0]) is True


def test_empty_inputs_is_not_rbf():
    assert is_rbf_signaled([]) is False


def test_none_in_list_is_treated_as_final():
    # Coinbase or shielded inputs may have null sequence; they are not RBF
    assert is_rbf_signaled([None, 0xFFFFFFFF]) is False
