import pytest

from dt.forwarding import HeadCommand


def test_head_command_builds_repeatable_options_without_mutating_prior_value():
    base = HeadCommand.start("head", "run").option("-g", 2).option("-p", None)
    command = (
        base.flag("--json", True)
        .repeat("--artifact", ["a.pt", "b.yaml"])
        .passthrough(["python", "train.py"])
    )

    assert base.argv() == ["run", "-g", "2"]
    assert command.argv() == [
        "run",
        "-g",
        "2",
        "--json",
        "--artifact",
        "a.pt",
        "--artifact",
        "b.yaml",
        "--",
        "python",
        "train.py",
    ]


def test_head_command_invoke_passes_a_fresh_mutable_argv():
    seen = []
    command = HeadCommand.start("head", "info", "job").flag("--json", True)

    def call(head, argv):
        seen.append((head, argv))
        argv.append("--mutated")
        return 5

    assert command.invoke(call) == 5
    assert seen == [("head", ["info", "job", "--json", "--mutated"])]
    assert command.argv() == ["info", "job", "--json"]


# -- laptop forwarding must mirror every head-side option ---------------------------


def _forwarded_flags(command_name: str, *, source_name: str | None = None) -> set[str]:
    """Long/short flags the laptop branch of a command forwards to the head.

    Forwarding is written by hand (`_head_command(...).option("--x", ...)`),
    so a new typer option that is not mirrored here silently vanishes when the
    command runs from a laptop. This reads the actual chain from the source of
    ``source_name`` (the command itself unless its laptop route lives in a
    helper).
    """
    import inspect
    import re

    from dt import cli

    source = inspect.getsource(getattr(cli, source_name or command_name))
    starts = [
        index
        for index in (source.find("_head_command("), source.find("HeadCommand.start("))
        if index >= 0
    ]
    assert starts, f"{source_name or command_name} has no laptop forwarding chain"
    chain = source[min(starts) :]
    chain = chain[: chain.index(".passthrough(") if ".passthrough(" in chain else None]
    return set(
        re.findall(r'\.(?:option|repeat|flag)\(\s*"(-{1,2}[a-z][a-z0-9-]*)"', chain)
    )


# Every command with a laptop route: (command, helper holding the chain,
# options the laptop consumes itself, options that travel by another channel).
_FORWARDED_COMMANDS = [
    (
        "run",
        "_forward_run_to_head",
        {"center", "follow", "poll", "lines"},
        {"environment"},
    ),
    ("task", None, {"center", "follow", "poll", "lines"}, set()),
    # --file/-F is read on the laptop; its lines travel as positional items.
    ("batch", "_inventory_command", {"center"}, {"file"}),
    ("chain", "_inventory_command", {"center"}, {"file"}),
    ("logs", None, set(), set()),
    ("info", None, set(), set()),
    ("diagnose", None, set(), set()),
    ("compare", None, set(), {"file"}),
    ("watch", None, set(), {"file"}),
    ("metrics", None, set(), set()),
    ("rerun", None, set(), set()),
    ("exec_job", None, set(), set()),
    ("fork", "_forward_fork_to_head", set(), set()),
    ("storage", None, {"center"}, set()),
]


@pytest.mark.parametrize(
    ("command_name", "source_name", "laptop_local", "other_channel"),
    _FORWARDED_COMMANDS,
    ids=[case[0] for case in _FORWARDED_COMMANDS],
)
def test_laptop_route_forwards_every_head_option(
    command_name, source_name, laptop_local, other_channel
):
    import inspect

    import typer

    from dt import cli

    forwarded = _forwarded_flags(command_name, source_name=source_name)
    missing = []
    for name, param in inspect.signature(getattr(cli, command_name)).parameters.items():
        if not isinstance(param.default, typer.models.OptionInfo):
            continue
        if name in laptop_local or name in other_channel:
            continue
        # A "--x/--no-x" declaration is one option with two spellings.
        decls = {
            spelling
            for decl in (param.default.param_decls or ())
            for spelling in decl.split("/")
        }
        if not decls & forwarded:
            missing.append(f"{name} {sorted(decls)}")
    assert not missing, (
        f"declared on `dt {command_name}` but never forwarded to the head: {missing}"
    )
