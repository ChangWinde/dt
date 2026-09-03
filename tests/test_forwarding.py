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


# -- laptop forwarding must mirror every submission-shaping option ----------------


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
    chain = source[source.index("_head_command(") :]
    chain = chain[: chain.index(".passthrough(") if ".passthrough(" in chain else None]
    return set(
        re.findall(r'\.(?:option|repeat|flag)\(\s*"(-{1,2}[a-z][a-z0-9-]*)"', chain)
    )


def test_run_forwards_every_submission_shaping_option():
    import inspect

    import typer

    from dt import cli

    # Options consumed on the laptop itself, never meant for the head.
    laptop_local = {"center", "follow", "poll", "lines"}
    # Options whose value travels by another channel than a flag.
    other_channel = {"environment"}  # names go inside the private stdin envelope
    forwarded = _forwarded_flags("run", source_name="_forward_run_to_head")
    missing = []
    for name, param in inspect.signature(cli.run).parameters.items():
        if not isinstance(param.default, typer.models.OptionInfo):
            continue
        if name in laptop_local or name in other_channel:
            continue
        decls = set(param.default.param_decls or ())
        if not decls & forwarded:
            missing.append(f"{name} {sorted(decls)}")
    assert not missing, (
        f"declared on `dt run` but never forwarded to the head: {missing}"
    )
