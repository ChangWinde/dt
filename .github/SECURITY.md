# Security policy

## Supported versions

Security fixes are provided for the latest released minor version. Older
versions should be upgraded before requesting a backport.

## Trust model

DistTrainer is an operator tool for a trusted Unix account across trusted SSH
hosts. It deliberately executes the commands and project setup hooks submitted
by that account. It is not an authentication service, tenant-isolation
boundary, container sandbox, or defense against a malicious project,
configuration file, SSH host, or user sharing the same account.

Required assumptions:

- SSH host keys, account access, and configured node aliases are administered
  outside DistTrainer.
- The local configuration and project trees are writable only by trusted
  operators.
- Compute nodes enforce the organization's account, filesystem, network, and
  GPU access policy.
- Webhook and proxy values are trusted operator configuration.

`topology-aware` transfer treats configured site membership as its finite
discovery boundary. Nodes advertise only their own interfaces and public SSH
host keys through an already-authenticated control session; DT does not scan
address ranges or disable host-key checking. A direct peer connection pins
those keys under a DT-private alias and explicitly disables ProxyJump. Every
DT-managed SSH pool disables agent forwarding. A gateway or peer source must
authenticate to the destination with credentials already available on that
source; DT never copies a head-side private key to a worker. Do not place an
untrusted host in a topology-enabled site.

DistTrainer provides collision-safe GPU leases, bounded remote operations,
content identities, path validation, previewable destructive maintenance, and
process-tree cleanup within that trust model.

The private operation journal records allowlisted command categories, timing,
build identity, exit state, and problem fingerprints derived only from
exception type and code location. It never records or fingerprints argument
values and does not store exception text, environment variables, working
directories, hostnames, or usernames. Journal directories and files are
private to the Unix account and symlink targets are refused. The journal is
same-user operational evidence, not a tamper-proof audit or authorization
boundary; operation IDs passed over SSH are correlation identifiers only.

Destructive cleanup validates a registry job directory against the exact
`dt/jobs/JOB_ID` slot before invoking `rm`. A failed remote or related local
deletion retains the registry record so the operation remains visible and
retryable. Laptop cleanup is single-center by default; cross-center cleanup
requires `--all-centers`.

## Reporting a vulnerability

Report vulnerabilities privately to the maintainer or operations channel that
granted access to the repository or release artifact. Include the affected
version, command, trust-boundary assumptions, reproduction steps, and impact.
Do not include credentials, private keys, access tokens, proprietary model
weights, datasets, or unredacted job logs.

Do not open a public issue until the maintainer confirms that disclosure is
safe. Acknowledgement, severity, remediation, and disclosure timing are
coordinated through the same private channel.
