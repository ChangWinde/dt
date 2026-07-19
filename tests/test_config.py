import pytest

from dt.config import ConfigError, HeadConfig, LaptopConfig, parse


def test_head_config():
    cfg = parse({
        "center": "psibot",
        "nodes": [{"name": "psibot-hm", "local": True}, "psibot-ds"],
        "projects": {"vla": "~/proj/vla"},
        "default_project": "vla",
    })
    assert isinstance(cfg, HeadConfig)
    assert cfg.nodes[0].local and not cfg.nodes[1].local
    assert cfg.envs == "~/dt/envs"
    assert "vla" in cfg.projects


def test_laptop_config():
    cfg = parse({
        "default_center": "zgca",
        "centers": {"psibot": {"head": "psibot-hm"}, "zgca": "zgca-r0"},
    })
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
