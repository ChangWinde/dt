import os
import subprocess
import sys

import pytest

import dt.config as config_module
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
    assert cfg.envs == "~/dt/worker/envs"
    assert "vla" in cfg.projects
    assert cfg.projects["vla"].setup is None
    assert str(cfg.projects["vla"].path).endswith("proj/vla")


def test_node_probe_timeout_is_bounded_and_configurable():
    cfg = parse(
        {
            "center": "c",
            "nodes": [
                "default",
                {"name": "slow", "probe_timeout_s": 23.5},
            ],
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.nodes[0].probe_timeout_s == 15.0
    assert cfg.nodes[1].probe_timeout_s == 23.5


@pytest.mark.parametrize(
    "value",
    [0, -1, 121, float("inf"), float("-inf"), float("nan"), True],
)
def test_node_probe_timeout_rejects_unbounded_values(value):
    with pytest.raises(ConfigError, match="probe_timeout_s"):
        parse(
            {
                "center": "c",
                "nodes": [{"name": "n1", "probe_timeout_s": value}],
            }
        )


def test_explicit_site_topology_binds_nodes_and_lan_routes():
    cfg = parse(
        {
            "center": "headstar",
            "nodes": [
                {"name": "star-0", "local": True},
                {"name": "psibot-hm"},
                {
                    "name": "psibot-ds",
                    "lan_address": "lyf@172.16.6.91",
                    "lan_port": 2202,
                    "transfer_cost": 0.1,
                },
            ],
            "sites": {
                "star": {
                    "gateway": "star-0",
                    "nodes": ["star-0"],
                },
                "psibot": {
                    "gateway": "psibot-hm",
                    "nodes": ["psibot-hm", "psibot-ds"],
                    "artifact_policy": "site-cache-first",
                    "cache_root": "~/dt-site-cache",
                    "fallback_direct": True,
                },
            },
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.sites["psibot"].cache_node == "psibot-hm"
    assert cfg.sites["psibot"].artifact_policy == "site-cache-first"
    assert cfg.sites["psibot"].cache_root == "~/dt-site-cache"
    assert cfg.sites["psibot"].fallback_direct is True
    assert cfg.sites["psibot"].route_circuit_failures == 2
    assert cfg.sites["psibot"].route_circuit_cooldown_s == 60
    assert cfg.sites["psibot"].route_circuit_max_cooldown_s == 900
    assert cfg.nodes[2].site == "psibot"
    assert cfg.nodes[2].lan_address == "lyf@172.16.6.91"
    assert cfg.nodes[2].lan_port == 2202
    assert cfg.nodes[2].transfer_cost == 0.1


def test_topology_aware_site_can_discover_lan_endpoints_at_runtime():
    cfg = parse(
        {
            "center": "headstar",
            "nodes": ["psibot-hm", "psibot-ds"],
            "sites": {
                "psibot": {
                    "gateway": "psibot-hm",
                    "nodes": ["psibot-hm", "psibot-ds"],
                    "artifact_policy": "topology-aware",
                }
            },
        }
    )

    assert isinstance(cfg, HeadConfig)
    assert cfg.sites["psibot"].artifact_policy == "topology-aware"
    assert cfg.nodes[1].lan_address is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("route_circuit_failures", 0),
        ("route_circuit_failures", 11),
        ("route_circuit_failures", True),
        ("route_circuit_cooldown_s", 0),
        ("route_circuit_cooldown_s", float("inf")),
        ("route_circuit_max_cooldown_s", 30),
        ("route_circuit_max_cooldown_s", 86401),
    ],
)
def test_route_circuit_policy_is_bounded(field, value):
    site = {
        "gateway": "psibot-hm",
        "nodes": ["psibot-hm", "psibot-ds"],
        "artifact_policy": "topology-aware",
        field: value,
    }
    if field == "route_circuit_max_cooldown_s" and value == 30:
        site["route_circuit_cooldown_s"] = 60

    with pytest.raises(ConfigError, match=field):
        parse(
            {
                "center": "headstar",
                "nodes": ["psibot-hm", "psibot-ds"],
                "sites": {"psibot": site},
            }
        )


@pytest.mark.parametrize(
    ("sites", "message"),
    [
        (
            {
                "psibot": {
                    "gateway": "psibot-hm",
                    "nodes": ["psibot-hm", "missing"],
                }
            },
            "unknown node",
        ),
        (
            {
                "psibot": {
                    "gateway": "psibot-hm",
                    "nodes": ["psibot-hm", "psibot-ds"],
                    "artifact_policy": "site-cache-first",
                }
            },
            "lan_address",
        ),
        (
            {
                "psibot": {
                    "gateway": "psibot-hm",
                    "nodes": ["psibot-hm"],
                }
            },
            "topology is incomplete",
        ),
    ],
)
def test_invalid_or_incomplete_topology_fails_closed(sites, message):
    with pytest.raises(ConfigError, match=message):
        parse(
            {
                "center": "headstar",
                "nodes": ["psibot-hm", "psibot-ds"],
                "sites": sites,
            }
        )


def test_node_site_without_topology_is_rejected():
    with pytest.raises(ConfigError, match="requires a top-level"):
        parse(
            {
                "center": "headstar",
                "nodes": [{"name": "psibot-hm", "site": "psibot"}],
            }
        )


def test_site_name_rejects_path_or_shell_syntax():
    with pytest.raises(ConfigError, match="site names"):
        parse(
            {
                "center": "headstar",
                "nodes": ["psibot-hm"],
                "sites": {
                    "../psibot": {
                        "gateway": "psibot-hm",
                        "nodes": ["psibot-hm"],
                    }
                },
            }
        )


@pytest.mark.parametrize("port", [0, 65536, True, "not-a-port"])
def test_lan_port_is_bounded(port):
    with pytest.raises(ConfigError, match="lan_port"):
        parse(
            {
                "center": "headstar",
                "nodes": [
                    {
                        "name": "psibot-hm",
                        "lan_address": "psibot@172.16.17.100",
                        "lan_port": port,
                    }
                ],
            }
        )


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


@pytest.mark.parametrize("extra", ["sim data", "--offline", "*", "x" * 65])
def test_project_extras_are_safe_bounded_uv_identities(extra):
    with pytest.raises(ConfigError, match=r"extras\[\]"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"p": {"path": "~/p", "extras": [extra]}},
            }
        )


def test_project_extras_are_deduplicated_without_reordering():
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "projects": {"p": {"path": "~/p", "extras": ["sim", "data", "sim"]}},
        }
    )

    assert cfg.projects["p"].extras == ["sim", "data"]


@pytest.mark.parametrize(
    "name",
    ["foo/bar", "foo?bar", ".hidden", "p" * 65],
)
def test_project_names_are_safe_bounded_path_identities(name):
    with pytest.raises(ConfigError, match="projects name"):
        parse({"center": "c", "nodes": ["n1"], "projects": {name: "~/p"}})


@pytest.mark.parametrize(
    "payload",
    [
        {"center": "bad center", "nodes": ["n1"]},
        {"centers": {"../center": "head"}},
    ],
)
def test_center_names_are_safe_bounded_identities(payload):
    with pytest.raises(ConfigError, match="center"):
        parse(payload)


@pytest.mark.parametrize(
    ("field", "limit_name", "payload"),
    [
        ("MAX_CENTERS", "centers", {"centers": {"a": "h1", "b": "h2"}}),
        ("MAX_NODES", "nodes", {"center": "c", "nodes": ["n1", "n2"]}),
        (
            "MAX_PROJECTS",
            "projects",
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"a": "~/p", "b": "~/p"},
            },
        ),
        (
            "MAX_SITES",
            "sites",
            {
                "center": "c",
                "nodes": ["n1"],
                "sites": {
                    "a": {"gateway": "n1", "nodes": ["n1"]},
                    "b": {"gateway": "n1", "nodes": ["n1"]},
                },
            },
        ),
    ],
)
def test_config_collections_have_explicit_resource_bounds(
    monkeypatch, field, limit_name, payload
):
    monkeypatch.setattr(config_module, field, 1)

    with pytest.raises(ConfigError, match=rf"{limit_name}.*maximum is 1"):
        parse(payload)


@pytest.mark.parametrize(
    ("field", "label", "payload"),
    [
        (
            "MAX_PROJECT_EXTRAS",
            "extras",
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"p": {"path": "~/p", "extras": ["a", "b"]}},
            },
        ),
        (
            "MAX_SETUP_INPUTS",
            "setup_inputs",
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {
                    "p": {
                        "path": "~/p",
                        "setup": "true",
                        "setup_inputs": ["a", "b"],
                    }
                },
            },
        ),
        (
            "MAX_SNAPSHOT_EXCLUDES",
            "snapshot_excludes",
            {
                "center": "c",
                "nodes": ["n1"],
                "snapshot_excludes": ["a", "b"],
            },
        ),
    ],
)
def test_config_nested_lists_have_explicit_resource_bounds(
    monkeypatch, field, label, payload
):
    monkeypatch.setattr(config_module, field, 1)

    with pytest.raises(ConfigError, match=rf"{label}.*maximum is 1"):
        parse(payload)


def test_site_member_list_has_the_global_node_bound(monkeypatch):
    monkeypatch.setattr(config_module, "MAX_NODES", 1)

    with pytest.raises(ConfigError, match=r"sites\.s\.nodes.*maximum is 1"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "sites": {
                    "s": {"gateway": "n1", "nodes": ["n1", "n1"]},
                },
            }
        )


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


@pytest.mark.parametrize(
    "destination",
    [
        "-oProxyCommand=sh",
        "host name",
        "host\nProxyCommand sh",
        "host;touch-pwned",
    ],
)
def test_ssh_destinations_reject_option_and_shell_syntax(destination):
    with pytest.raises(ConfigError, match="safe SSH"):
        parse({"center": "c", "nodes": [destination]})
    with pytest.raises(ConfigError, match="safe SSH"):
        parse({"centers": {"c": destination}})


def test_ssh_destinations_reject_unusable_argument_length():
    with pytest.raises(ConfigError, match="safe SSH"):
        parse({"center": "c", "nodes": ["n" * 513]})


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
        {"projects": {"p": {"path": "~/p", "setup": 7}}},
        {"projects": {"p": {"path": "~/p", "extras": "gpu"}}},
        {"queue": []},
        {"webhook": []},
        {"proxy": {}},
        {"snapshot_excludes": [""]},
        {"snapshot_excludes": ["cache\x00escape"]},
        {"snapshot_excludes": ["cache\nother"]},
        {"snapshot_excludes": ["x" * 4097]},
    ],
)
def test_head_config_rejects_wrong_nested_types(field):
    with pytest.raises(ConfigError):
        parse({"center": "c", "nodes": ["n1"], **field})


@pytest.mark.parametrize(
    "webhook",
    ["file:///tmp/event", "ftp://example.com/event", "https:///missing-host"],
)
def test_head_config_rejects_unsafe_webhook_protocols(webhook):
    with pytest.raises(ConfigError, match=r"HTTP\(S\)"):
        parse({"center": "c", "nodes": ["n1"], "webhook": webhook})


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
            "projects": {"p": {"path": "~/p", "extra": ["gpu"]}},
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


def test_mem_threshold_zero_is_rejected():
    with pytest.raises(ConfigError, match="mem_threshold_mib"):
        parse({"center": "headstar", "nodes": ["n1"], "mem_threshold_mib": 0})


def test_project_path_must_be_absolute_or_home_rooted():
    with pytest.raises(ConfigError, match=r"projects\.p\.path"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"p": {"path": "rel/ative"}},
            }
        )
    with pytest.raises(ConfigError, match=r"projects\.p"):
        parse({"center": "c", "nodes": ["n1"], "projects": {"p": "rel/ative"}})
    ok = parse({"center": "c", "nodes": ["n1"], "projects": {"p": {"path": "~/proj"}}})
    assert "p" in ok.projects


@pytest.mark.parametrize(
    "project_path",
    ["/", "~/..", "~/../.ssh", "/srv/./project", "/srv/a/../project"],
)
def test_project_path_rejects_overbroad_or_noncanonical_roots(project_path):
    with pytest.raises(ConfigError, match="filesystem root|components|home directory"):
        parse({"center": "c", "nodes": ["n1"], "projects": {"p": project_path}})


def test_project_path_rejects_symlink_drift(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)

    with pytest.raises(ConfigError, match="canonical.*symlink"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"p": str(alias)},
            }
        )


def test_revalidate_project_root_rejects_post_load_symlink_swap(tmp_path):
    configured = tmp_path / "project"
    configured.mkdir()
    project = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "projects": {"p": str(configured)},
        }
    ).projects["p"]
    configured.rmdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    configured.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ConfigError, match="canonical.*symlink"):
        config_module.revalidate_project_root(project.path, "projects.p.path")


def test_revalidate_project_root_requires_an_existing_directory(tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(ConfigError, match="unavailable"):
        config_module.revalidate_project_root(missing)
    regular = tmp_path / "regular"
    regular.write_text("not a project\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="existing directory"):
        config_module.revalidate_project_root(regular)


@pytest.mark.parametrize(
    "section", ["paths", "projects", "queue", "operations", "job_logs", "sites"]
)
def test_explicit_null_mapping_sections_are_rejected(section):
    with pytest.raises(ConfigError, match=rf"`{section}` must be a mapping"):
        parse({"center": "c", "nodes": ["n1"], section: None})


def test_job_log_retention_is_bounded_and_head_only():
    cfg = parse(
        {
            "center": "c",
            "nodes": ["n1"],
            "job_logs": {"max_file_mib": 32, "keep_files": 5},
        }
    )
    assert cfg.job_logs.max_file_mib == 32
    assert cfg.job_logs.keep_files == 5

    for settings in (
        {"max_file_mib": 0},
        {"max_file_mib": 257},
        {"keep_files": 0},
        {"keep_files": 17},
        {"unknown": 1},
    ):
        with pytest.raises(ConfigError, match="job_logs"):
            parse({"center": "c", "nodes": ["n1"], "job_logs": settings})

    with pytest.raises(ConfigError, match="laptop config"):
        parse({"centers": {"c": "head"}, "job_logs": {"keep_files": 2}})


def test_proxy_requires_a_scheme():
    with pytest.raises(ConfigError, match="proxy"):
        parse({"center": "c", "nodes": ["n1"], "proxy": "host:3128"})
    ok = parse({"center": "c", "nodes": ["n1"], "proxy": "http://host:3128"})
    assert ok.proxy == "http://host:3128"


def test_proxy_requires_an_http_scheme_and_hostname():
    # The value is exported verbatim as HTTP_PROXY/HTTPS_PROXY into every
    # job; a non-HTTP scheme or a hostless URL breaks all egress silently.
    for bad in ("ftp://mirror", "socks5://host:1080", "http://"):
        with pytest.raises(ConfigError, match=r"HTTP\(S\) proxy"):
            parse({"center": "c", "nodes": ["n1"], "proxy": bad})
    ok = parse({"center": "c", "nodes": ["n1"], "proxy": "https://host:3128"})
    assert ok.proxy == "https://host:3128"


@pytest.mark.parametrize(
    "field,value",
    [
        ("webhook", "http://[broken"),
        ("webhook", "https://host:not-a-port/path"),
        ("webhook", "https://host:65536/path"),
        ("webhook", "https://host:/path"),
        ("webhook", "https://host:0/path"),
        ("proxy", "http://[broken"),
        ("proxy", "http://host:not-a-port"),
        ("proxy", "http://host:65536"),
        ("proxy", "http://[::1]:"),
        ("proxy", "http://host:0"),
    ],
)
def test_http_endpoints_reject_malformed_hosts_and_ports(field, value):
    with pytest.raises(ConfigError, match=r"HTTP\(S\).*(hostname|port)"):
        parse({"center": "c", "nodes": ["n1"], field: value})


def test_lan_address_rejects_ports_brackets_and_bare_ipv6():
    # lan_address is spliced into `address:path` rsync/ssh targets: the
    # first colon reads as the path separator, so `host:port` silently
    # dropped its port (lan_port stayed 22) and bare IPv6 broke the target.
    for bad in ("node1:2222", "2001:db8::1", "[2001:db8::1]:22"):
        with pytest.raises(ConfigError, match="lan_port"):
            parse(
                {
                    "center": "c",
                    "nodes": [{"name": "n1", "lan_address": bad}],
                }
            )
    ok = parse(
        {
            "center": "c",
            "nodes": [{"name": "n1", "lan_address": "lyf@172.16.6.91"}],
        }
    )
    assert ok.nodes[0].lan_address == "lyf@172.16.6.91"


def test_load_rejects_duplicate_yaml_keys(tmp_path, monkeypatch):
    # safe_load keeps only the final occurrence, so a stricter guard placed
    # earlier in the file would be silently overridden by a later typo.
    path = tmp_path / "config.yaml"
    path.write_text("center: c\nnodes: [n1]\ndisk_min_gib: 99\ndisk_min_gib: 0\n")
    monkeypatch.setenv("DT_CONFIG", str(path))

    with pytest.raises(ConfigError, match="duplicate key 'disk_min_gib'"):
        load()


def test_load_rejects_duplicate_keys_in_nested_mappings(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        "center: c\nnodes:\n  - name: n1\n    gpus: 1\n    gpus: 8\n",
    )
    monkeypatch.setenv("DT_CONFIG", str(path))

    with pytest.raises(ConfigError, match="duplicate key 'gpus'"):
        load()


def test_load_rejects_duplicate_keys_hidden_inside_a_yaml_merge(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        "center: c\nnodes: [n1]\npaths:\n  <<: {root: /srv/dt-a, root: /srv/dt-b}\n",
    )
    monkeypatch.setenv("DT_CONFIG", str(path))

    with pytest.raises(ConfigError, match="duplicate key 'root'"):
        load()


@pytest.mark.parametrize("project_path", ["~", "~definitely-no-such-user/project"])
def test_project_path_rejects_ambiguous_tilde_forms(project_path):
    with pytest.raises(ConfigError, match="absolute or start with ~/"):
        parse(
            {
                "center": "c",
                "nodes": ["n1"],
                "projects": {"p": project_path},
            }
        )


@pytest.mark.parametrize("section", ["centers", "projects", "sites"])
def test_identifiers_cannot_collide_after_whitespace_normalization(section):
    if section == "centers":
        payload = {"centers": {" lab": "head-a", "lab": "head-b"}}
    elif section == "projects":
        payload = {
            "center": "c",
            "nodes": ["n1"],
            "projects": {" p": "/srv/p1", "p": "/srv/p2"},
        }
    else:
        site = {"gateway": "n1", "nodes": ["n1"]}
        payload = {
            "center": "c",
            "nodes": ["n1"],
            "sites": {" lab": site, "lab": site},
        }

    with pytest.raises(ConfigError, match="duplicate.*after normalization"):
        parse(payload)


@pytest.mark.parametrize("name", ["host:2222", "2001:db8::1", "[::1]"])
def test_node_names_reject_rsync_path_separators(name):
    with pytest.raises(ConfigError, match="node name.*colon|nodes.*colon"):
        parse({"center": "c", "nodes": [name]})


def test_load_wraps_deeply_nested_yaml_as_config_error(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("center: test\n")
    monkeypatch.setenv("DT_CONFIG", str(path))

    def deep(_payload):
        raise RecursionError("maximum recursion depth exceeded")

    monkeypatch.setattr(config_module, "_parse_yaml_strict", deep)

    with pytest.raises(ConfigError, match="too deep"):
        load()


def test_load_refuses_fifo_and_oversized_config_files(tmp_path, monkeypatch):
    fifo = tmp_path / "config.fifo"
    os.mkfifo(fifo)
    monkeypatch.setenv("DT_CONFIG", str(fifo))
    with pytest.raises(ConfigError, match="not a regular file"):
        load()

    oversized = tmp_path / "oversized.yaml"
    oversized.write_bytes(b"x" * 129)
    monkeypatch.setattr(config_module, "MAX_CONFIG_BYTES", 128)
    monkeypatch.setenv("DT_CONFIG", str(oversized))
    with pytest.raises(ConfigError, match="exceeds"):
        load()


def test_load_reuses_unchanged_parse_and_invalidates_after_atomic_replace(
    tmp_path, monkeypatch
):
    path = tmp_path / "config.yaml"
    path.write_text("center: first\nnodes: [n1]\n")
    monkeypatch.setenv("DT_CONFIG", str(path))
    calls = 0
    original = config_module._parse_yaml_strict

    def counted_load(stream):
        nonlocal calls
        calls += 1
        return original(stream)

    monkeypatch.setattr(config_module, "_parse_yaml_strict", counted_load)

    first = load()
    repeated = load()
    replacement = tmp_path / "replacement.yaml"
    replacement.write_text("center: second\nnodes: [n2]\n")
    replacement.replace(path)
    changed = load()

    assert first is repeated
    assert isinstance(changed, HeadConfig)
    assert changed.center == "second"
    assert calls == 2


def test_active_command_contract_prefers_valid_persisted_custom_bin(
    tmp_path, monkeypatch
):
    command = tmp_path / "custom-bin" / "dt"
    command.parent.mkdir()
    command.write_text("#!/bin/sh\n", encoding="utf-8")
    command.chmod(0o700)
    data_home = tmp_path / "data"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    record.write_text(f"{command}\n", encoding="utf-8")
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))

    assert config_module.active_dt_command() == command


def test_active_command_contract_ignores_nonabsolute_or_stale_records(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    legacy = home / ".local" / "bin" / "dt"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("#!/bin/sh\n", encoding="utf-8")
    legacy.chmod(0o700)
    data_home = tmp_path / "data"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    record.write_text("relative/dt\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))

    assert config_module.active_dt_command() == legacy


def test_active_command_contract_ignores_a_non_executable_record(tmp_path, monkeypatch):
    home = tmp_path / "home"
    legacy = home / ".local" / "bin" / "dt"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("#!/bin/sh\n", encoding="utf-8")
    legacy.chmod(0o700)
    inactive = tmp_path / "custom" / "dt"
    inactive.parent.mkdir()
    inactive.write_text("#!/bin/sh\n", encoding="utf-8")
    inactive.chmod(0o600)
    data_home = tmp_path / "data"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    record.write_text(f"{inactive}\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))

    assert config_module.active_dt_command() == legacy


def test_active_command_contract_does_not_follow_a_record_symlink(
    tmp_path, monkeypatch
):
    home = tmp_path / "home"
    legacy = home / ".local" / "bin" / "dt"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("#!/bin/sh\n", encoding="utf-8")
    legacy.chmod(0o700)
    custom = tmp_path / "custom" / "dt"
    custom.parent.mkdir()
    custom.write_text("#!/bin/sh\n", encoding="utf-8")
    custom.chmod(0o700)
    outside = tmp_path / "outside-record"
    outside.write_text(f"{custom}\n", encoding="utf-8")
    data_home = tmp_path / "data"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    record.symlink_to(outside)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_DATA_HOME", str(data_home))

    assert config_module.active_dt_command() == legacy


def test_active_command_fifo_fails_fast_to_the_legacy_command(tmp_path):
    home = tmp_path / "home"
    legacy = home / ".local" / "bin" / "dt"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("#!/bin/sh\n", encoding="utf-8")
    legacy.chmod(0o700)
    data_home = tmp_path / "data"
    record = data_home / "disttrainer" / "active-command"
    record.parent.mkdir(parents=True)
    os.mkfifo(record)

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "from dt.config import active_dt_command; print(active_dt_command())",
        ],
        env={**os.environ, "HOME": str(home), "XDG_DATA_HOME": str(data_home)},
        capture_output=True,
        text=True,
        timeout=1,
        check=True,
    )

    assert proc.stdout.strip() == str(legacy)
