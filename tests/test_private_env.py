from __future__ import annotations

import pytest

from dt import private_env


def test_private_launch_envelope_round_trips_with_magic_and_separates_runtime():
    values = {
        "DT_LAUNCH_TOKEN": "a" * 32,
        "DT_PROXY": "http://operator:secret@proxy.invalid:8080",
        "DT_WEBHOOK": "https://hooks.invalid/private",
        "HF_TOKEN": "hf-private",
    }

    encoded = private_env.encode(values)

    assert encoded.startswith(private_env.MAGIC)
    assert private_env.decode(encoded) == values
    assert private_env.runtime_values(values) == {
        "DT_PROXY": values["DT_PROXY"],
        "DT_WEBHOOK": values["DT_WEBHOOK"],
        "HF_TOKEN": values["HF_TOKEN"],
    }


@pytest.mark.parametrize(
    "values",
    [
        {"DT_UNKNOWN": "secret"},
        {"DT_LAUNCH_TOKEN": "not-a-token"},
        {"PATH": "/tmp/secret"},
        {"9BAD": "secret"},
    ],
)
def test_private_launch_envelope_rejects_unknown_or_unsafe_names(values):
    with pytest.raises(private_env.PrivateEnvironmentError):
        private_env.encode(values)


def test_private_launch_envelope_rejects_missing_magic_and_duplicate_fields():
    with pytest.raises(private_env.PrivateEnvironmentError, match="magic"):
        private_env.decode(b"TOKEN\0value\0")

    duplicate = private_env.MAGIC + b"HF_TOKEN\0a\0HF_TOKEN\0b\0"
    with pytest.raises(private_env.PrivateEnvironmentError, match="duplicate"):
        private_env.decode(duplicate)
