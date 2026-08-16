# DistTrainer documentation

This directory separates operational guidance from the evidence accumulated
while developing and validating DistTrainer.

## Start here

| Goal | Document |
|---|---|
| Install and submit a first job | [Getting started](getting-started.md) |
| Configure heads, laptops, projects, and storage | [Configuration](configuration.md) |
| Design queues, chains, forks, and comparisons | [Experiment workflows](workflows.md) |
| Operate the queue agent and recover failures | [Operations](operations.md) |
| Choose commands and handle exit codes | [Command reference](command-reference.md) |
| Understand control and data flow | [Architecture](architecture.md) |
| Build and promote a release | [Release procedure](releasing.md) |

The root [README](../README.md) is the product overview. The
[support contract](../.github/SUPPORT.md),
[security policy](../.github/SECURITY.md), and
[contribution guide](../.github/CONTRIBUTING.md) define the supported
boundary.

## Evidence library

DistTrainer retains the records used to justify behavior and performance
claims. These files are development evidence, not required reading for normal
operation and not included in release artifacts.

| Collection | Contents |
|---|---|
| [Architecture decisions](adr/README.md) | Accepted design decisions and their alternatives |
| [Validation audits](audits/README.md) | Requirement, regression, release, and live-system evidence |
| [Experiment records](experiments/README.md) | Preregistered or bounded research runs and outcomes |
| [Performance reports](performance/README.md) | Throughput, latency, utilization, and scaling measurements |
| [Implementation plans](plans/README.md) | Historical plans retained for decision context |
| [Project history](project/README.md) | Completed development goals and milestone history |

Indexes in evidence directories are generated from each document's first
heading:

```bash
uv run --no-sync python scripts/docs.py --write
uv run --no-sync python scripts/docs.py
```

The second command fails on stale indexes, broken relative links, or unbalanced
Markdown code fences.

## Documentation rules

User-facing behavior belongs in the root README or one of the core guides.
Design rationale belongs in an ADR. A live validation or release claim belongs
in an audit. Research runs must record hardware, software, configuration,
runtime, metrics, and acceptance thresholds.

This tracked documentation tree is the single maintained documentation source.
The GitHub Wiki is intentionally disabled so operational guidance cannot drift
outside versioned review and release gates.

Do not store credentials, private keys, access tokens, datasets, model weights,
raw job logs, or machine-specific configuration in this directory.
