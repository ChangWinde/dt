import pytest

from dt.config import ConfigError, HeadConfig, LaptopConfig, load, parse


def test_head_config():
    cfg = parse(
        {
            "center": "psibot",
            "nodes": [{"name": "psibot-hm", "local": True}, "psibot-ds"],
            "projects": {"vla": "~/proj/vla"},
            "default_project": "vla",
        }
    )
    assert isinstance(cfg, HeadConfig)
    assert cfg.nodes[0].local and not cfg.nodes[1].local
    assert cfg.envs == "~/dt/envs"
    assert "vla" in cfg.projects
    assert cfg.projects["vla"].setup is None
    assert str(cfg.projects["vla"].path).endswith("proj/vla")


def test_head_config_supports_dedicated_results_root(tmp_path):
    results = tmp_path / "experiment-results"
    cfg = parse(
        {
            "center": "psibot",
            "nodes": ["psibot-hm"],
            "paths": {
                "root": str(tmp_path / "dt"),
                "results": str(results),
            },
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.results_dir() == results


def test_project_with_setup_hook():
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "projects": {
                "omni": {
                    "path": "~/proj/omni",
                    "setup": "uv pip install libs/CleanDiffuser",
                    "setup_inputs": ["libs/CleanDiffuser", "libs/CleanDiffuser"],
                    "extras": ["sim", "data"],
                },
            },
        }
    )
    assert cfg.projects["omni"].setup == "uv pip install libs/CleanDiffuser"
    assert cfg.projects["omni"].setup_inputs == ["libs/CleanDiffuser"]
    assert cfg.projects["omni"].extras == ["sim", "data"]
    with pytest.raises(ConfigError):
        parse({"center": "c", "nodes": ["n1"], "projects": {"bad": {"setup": "x"}}})


@pytest.mark.parametrize(
    "project",
    [
        {"path": "/tmp/project", "setup_inputs": ["libs/Foo"]},
        {"path": "/tmp/project", "setup": "true", "setup_inputs": "libs/Foo"},
        {"path": "/tmp/project", "setup": "true", "setup_inputs": ["../Foo"]},
        {"path": "/tmp/project", "setup": "true", "setup_inputs": ["/tmp/Foo"]},
        {"path": "/tmp/project", "setup": "true", "setup_inputs": [""]},
    ],
)
def test_invalid_setup_inputs_rejected(project):
    with pytest.raises(ConfigError):
        parse({"center": "c", "nodes": ["n1"], "projects": {"bad": project}})


def test_laptop_config():
    cfg = parse(
        {
            "default_center": "zgca",
            "centers": {"psibot": {"head": "psibot-hm"}, "zgca": "zgca-r0"},
        }
    )
    assert isinstance(cfg, LaptopConfig)
    assert cfg.head("psibot") == "psibot-hm"
    assert cfg.head("zgca") == "zgca-r0"
    with pytest.raises(ConfigError):
        cfg.head("nope")


def test_both_roles_rejected():
    with pytest.raises(ConfigError):
        parse({"center": "a", "centers": {"a": "b"}, "nodes": ["x"]})


def test_empty_rejected():
    with pytest.raises(ConfigError):
        parse({})


@pytest.mark.parametrize(
    "days",
    [0, -1, float("inf"), float("-inf"), float("nan")],
)
def test_auto_clean_days_must_be_a_finite_positive_number(days):
    with pytest.raises(ConfigError, match="auto_clean_days"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "queue": {"auto_clean_days": days},
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"center": "c", "nodes": [{"local": True}]},
        {"centers": ["head"]},
        {"center": "c", "nodes": ["n1"], "projects": ["project"]},
    ],
)
def test_malformed_config_shapes_raise_config_error(payload):
    with pytest.raises(ConfigError):
        parse(payload)


@pytest.mark.parametrize(
    "field",
    [
        {"paths": []},
        {"paths": {"root": {}}},
        {"paths": {"envs": ""}},
        {"paths": {"results": []}},
        {"projects": []},
        {"projects": {"p": {"path": ".", "setup": 7}}},
        {"projects": {"p": {"path": ".", "extras": "gpu"}}},
        {"queue": []},
        {"webhook": []},
        {"proxy": {}},
        {"snapshot_excludes": [""]},
    ],
)
def test_head_config_rejects_wrong_nested_types(field):
    with pytest.raises(ConfigError):
        parse({"center": "c", "nodes": ["n1"], **field})


@pytest.mark.parametrize(
    "payload",
    [
        {"center": "c", "nodes": ["n1"], "poll_seconds": 1},
        {"center": "c", "nodes": [{"name": "n1", "locla": True}]},
        {"center": "c", "nodes": ["n1"], "paths": {"job_root": "/tmp"}},
        {"center": "c", "nodes": ["n1"], "queue": {"poll_seconds": 1}},
        {
            "center": "c",
            "nodes": ["n1"],
            "projects": {"p": {"path": ".", "extra": ["gpu"]}},
        },
        {"centers": {"c": {"head": "h", "host": "other"}}},
    ],
)
def test_unknown_config_keys_are_rejected_as_likely_typos(payload):
    with pytest.raises(ConfigError, match="unknown"):
        parse(payload)


def test_only_one_node_can_be_marked_local():
    with pytest.raises(ConfigError, match="one node"):
        parse(
            {
                "center": "c",
                "nodes": [
                    {"name": "n1", "local": True},
                    {"name": "n2", "local": True},
                ],
            }
        )


def test_load_wraps_invalid_yaml_as_config_error(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("center: [unterminated\n")
    monkeypatch.setenv("DT_CONFIG", str(path))

    with pytest.raises(ConfigError, match="cannot parse"):
        load()
