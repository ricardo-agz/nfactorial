import pytest

from factorial._internal.lua.core import LuaScriptContract


def test_materialize_rejects_unexpected_bindings() -> None:
    contract = LuaScriptContract(
        script_name="example",
        key_fields=("queue_key",),
        arg_fields=("batch_size",),
    )

    with pytest.raises(KeyError, match="unexpected binding"):
        contract.materialize(
            {
                "queue_key": "queue:main",
                "batch_size": 5,
                "unexpected": "value",
            }
        )


def test_materialize_allows_declared_execute_extras() -> None:
    contract = LuaScriptContract(
        script_name="example",
        key_fields=("queue_key",),
        arg_fields=("batch_size",),
    )

    keys, args = contract.materialize(
        {
            "self": object(),
            "queue_key": "queue:main",
            "batch_size": 5,
        },
        allowed_extras=frozenset({"self"}),
    )

    assert keys == ["queue:main"]
    assert args == [5]


def test_contract_rejects_overlap_between_keys_and_args() -> None:
    with pytest.raises(ValueError, match="both KEYS and ARGV"):
        LuaScriptContract(
            script_name="example",
            key_fields=("task_id",),
            arg_fields=("task_id",),
        )


def test_contract_rejects_unknown_optional_fields() -> None:
    with pytest.raises(ValueError, match="optional_key_fields"):
        LuaScriptContract(
            script_name="example",
            key_fields=("task_key",),
            arg_fields=("task_id",),
            optional_key_fields=frozenset({"missing_key"}),
        )
