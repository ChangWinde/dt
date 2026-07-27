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
