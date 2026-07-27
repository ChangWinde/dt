# DistTrainer release procedure

This procedure promotes reviewed source to an immutable `disttrainer` Python
distribution. It does not publish the repository's internal experiment
records.

## Release contract

- Distribution: `disttrainer`
- Executable and import package: `dt`
- Supported Python: 3.10–3.11
- Supported runtime: trusted-account Linux/SSH GPU centers described in
  `SUPPORT.md` and `SECURITY.md`
- License: `LicenseRef-Proprietary`; publishing or distributing outside the
  authorized organization requires copyright-holder approval

The package name `dt` must not be used: it belongs to another project on the
public Python index.

## Prepare

1. Reconcile `CHANGELOG.md`, `README.release.md`, `SECURITY.md`, `SUPPORT.md`,
   version metadata, and the intended source diff.
2. Confirm the Git worktree is clean and the current commit is the reviewed
   release source.
3. Run the CI matrix on Python 3.10 and 3.11.
4. Run the local terminal gate:

   ```bash
   scripts/release-check.sh dist
   ```

The gate refuses a dirty worktree. It runs the full tests and static checks,
builds wheel and sdist twice, compares their SHA-256 identities, audits package
paths and disclosure markers, installs the wheel in a clean Python
environment, and produces:

- `disttrainer-VERSION-py3-none-any.whl`
- `disttrainer-VERSION.tar.gz`
- `runtime-constraints.txt`
- `sbom.cdx.json`
- `bootstrap.sh`
- `release-audit.json`
- `release-manifest.json`
- `SHA256SUMS`

## Inspect and tag

```bash
(cd dist && sha256sum -c SHA256SUMS)
python3 -m json.tool dist/release-audit.json
python3 -m json.tool dist/release-manifest.json
git tag -a v0.6.0 -m "DistTrainer 0.6.0"
```

The manifest must report `git_dirty: false` and the intended release commit.
Do not move or recreate a published tag.

## Publish

Publishing is a promotion action and requires an explicitly configured target
and credentials. For a private package index, upload only the wheel and sdist
from the verified bundle. For PyPI, first confirm the `disttrainer` name,
copyright-holder authorization, and trusted-publisher configuration, then use:

```bash
uv publish dist/disttrainer-0.6.0-py3-none-any.whl \
  dist/disttrainer-0.6.0.tar.gz
```

Never pass a token on the command line or store it in this repository.

## Deploy and roll back

Preview and deploy only to explicit heads:

```bash
./deploy.sh --plan dist HEAD_A HEAD_B
./deploy.sh dist HEAD_A HEAD_B
```

Each host retains the complete verified bundle below
`~/.local/share/disttrainer/releases/VERSION/`. To restore a retained version:

```bash
./deploy.sh --plan --rollback 0.6.0 HEAD_A
./deploy.sh --rollback 0.6.0 HEAD_A
```

After deployment, run `dt --version`, `dt doctor --json`, inspect agent status,
and execute one bounded CPU-only or authorized GPU canary before broad
promotion.
