import pytest

from dt.config import ConfigError, HeadConfig, LaptopConfig, parse


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
