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

DistTrainer provides collision-safe GPU leases, bounded remote operations,
content identities, path validation, previewable destructive maintenance, and
process-tree cleanup within that trust model.

## Reporting a vulnerability

Report vulnerabilities privately to the maintainer or operations channel that
granted access to the repository or release artifact. Include the affected
version, command, trust-boundary assumptions, reproduction steps, and impact.
Do not include credentials, private keys, access tokens, proprietary model
weights, datasets, or unredacted job logs.

Do not open a public issue until the maintainer confirms that disclosure is
safe. Acknowledgement, severity, remediation, and disclosure timing are
coordinated through the same private channel.
