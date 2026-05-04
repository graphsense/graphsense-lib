# Versioning & Release Workflow

How `graphsense-lib` is versioned, when to mint a tag, and what each tag does in CI.

## TL;DR

- **Dev work:** just push your branch (`master`, `develop`, or `feature/**`). CI publishes a Docker image tagged `<branch-slug>` (rolling) and `<short-sha>` (immutable). No git tag required.
- **Stable release:** mint `vX.Y.Z`, push the tag. Triggers GitHub Release + PyPI library publish + Docker `vX.Y.Z` and rolling `latest`.
- **Release candidate:** mint `vX.Y.Z-rc.N`, push the tag. Triggers Docker `vX.Y.Z-rc.N` and rolling `rc`. No PyPI, no GitHub Release.
- **Web API + Python client:** managed on a separate track via `webapi-vA.B.C` tags. Triggers PyPI client publish only.
- **Python wheel version** is derived automatically by `setuptools_scm`, never hand-edited.

## Two version tracks

| Track | Tag shape | Where the version lives | What it covers |
|---|---|---|---|
| Library | `vX.Y.Z`, `vX.Y.Z-rc.N`, `vX.Y.Z-dev.N` | `setuptools_scm` (dynamic) → reads from git tag | The `graphsense-lib` package on PyPI, Docker images on ghcr.io |
| Web API + Client | `webapi-vA.B.C` | `clients/python/pyproject.toml` `version = "..."` and `src/graphsenselib/web/version.py` `__api_version__` | The `graphsense-lib-client` package on PyPI, OpenAPI spec version |

Both tracks live in the same repo and share `Makefile` helpers (`RELEASESEM` for the library track, `WEBAPISEM` for the client track) to mint and validate tags.

## How `setuptools_scm` derives the wheel version

Configured in `pyproject.toml`:

```toml
[tool.setuptools_scm]
version_scheme = "release-branch-semver"
local_scheme = "node-and-date"
```

What this produces, by branch state:

| State | Wheel version |
|---|---|
| HEAD == `vX.Y.Z` (stable tag) | `X.Y.Z` |
| HEAD == `vX.Y.Z-rc.N` | `X.Y.ZrcN` |
| HEAD == `vX.Y.Z-dev.N` | `X.Y.Z.devN` |
| Off-tag, on `master` / `develop` / `main` | `X.(Y+1).0.devN+g<sha>.d<date>` (next minor) |
| Off-tag, on `release/X.Y.x` | `X.Y.(Z+1).devN+g<sha>.d<date>` (next patch) |
| Off-tag, on any other branch (e.g. `feature/mcp`) | patch-bump form, same as `release/X.Y.x` |
| Dirty workspace | adds `.dirty` to the local segment |

Two consequences worth noting:

- The local segment (`+g<sha>.d<date>`) makes off-tag wheels uniquely identifiable but **forbidden on PyPI**. PyPI rejects any version with `+...`. This is intentional — it prevents accidental dev publishes. Stable tags don't get a local segment, so the official release flow is unaffected.
- The commit *date* in `.d<date>` changes if a commit is rebased or cherry-picked. If you need bit-for-bit reproducible version strings, that's the trade-off of `node-and-date`.

## Workflows by intent

### "I'm doing dev work on a feature branch"

Just push. Every push to `master`, `develop`, or `feature/**` produces:

- Docker image `ghcr.io/graphsense/graphsense-lib:<branch-slug>` — rolling tag, follows the branch HEAD.
- Docker image `ghcr.io/graphsense/graphsense-lib:<short-sha>` — immutable, what operators pin to.
- For `develop` only: also publishes the rolling `dev` alias.

No git tag required. No edit to `RELEASESEM`. Multiple feature branches no longer share a counter.

To test the wheel that your commit produces:

```bash
uv run python -m setuptools_scm    # prints the computed version
make build                         # builds the wheel into dist/
```

### "I want a clean named ref for this milestone"

Useful when another repo pins to `graphsense-lib` via git URL and you want a readable reference, or for a snapshot worth referring to in a discussion.

```bash
# Edit Makefile RELEASESEM to the desired tag value, e.g. 'v2.12.0-dev.2'.
make tag-version
git push origin --tags
```

CI then publishes a Docker image with the exact tag (no rolling alias — branch tags already serve that purpose).

Pick the next free `-dev.N` against the target version by checking existing tags:

```bash
git tag --list 'v2.12.0-dev.*'
```

Use plain SemVer pre-release form (`vX.Y.Z-dev.N`). **Do not** use `+<branch>.N` build metadata — branches are already namespaced by Docker tag.

### "I want to ship a release candidate"

```bash
# Edit Makefile RELEASESEM to e.g. 'v2.12.0-rc.1'.
make tag-version
git push origin --tags
```

CI publishes a Docker image tagged `vX.Y.Z-rc.N` and the rolling `rc` alias. No PyPI publish, no GitHub Release.

### "I'm shipping a stable library release"

1. Make sure `CHANGELOG.md` has a section for the version (the release workflow extracts it).
2. Edit `Makefile` `RELEASESEM` to `vX.Y.Z` (no suffix).
3. `make tag-version && git push origin --tags`.

CI fans out:

- Creates a GitHub Release using the changelog section.
- Publishes the wheel to PyPI.
- Publishes Docker images: `vX.Y.Z` + rolling `latest`.

### "I'm shipping a Web API + client release"

Separate track; uses the `webapi-vA.B.C` tag.

```bash
# 1. Update Makefile WEBAPISEM, e.g. 'v2.12.0'.
make update-api-version    # syncs src/graphsenselib/web/version.py
make sync-client-version   # syncs clients/python/pyproject.toml
make check-semver          # validates all four version sources align
git commit -am "bump webapi to 2.12.0"
make tag-version           # creates webapi-v2.12.0 tag
git push origin --tags
```

CI publishes the client wheel to PyPI. Docker is unaffected.

### "I'm stabilizing an older minor"

Once `2.13.0` is out, fixes for the `2.12.x` line live on a stabilization branch:

```bash
git checkout v2.12.0
git checkout -b release/2.12.x
git push -u origin release/2.12.x
```

`setuptools_scm` automatically computes patch-bump dev versions on `release/X.Y.x` branches (e.g. `2.12.1.devN+...`) instead of the default minor bump.

When ready, mint `v2.12.1` from that branch.

### "I'm doing a major version bump (3.0.0)"

`release-branch-semver` does not auto-detect majors. Mint the tag explicitly:

```bash
# Edit Makefile RELEASESEM to 'v3.0.0' (or 'v3.0.0-rc.1' first).
make tag-version
git push origin --tags
```

This is intentional: major releases should be deliberate.

## CI trigger reference

| Trigger | GitHub Release | PyPI library | PyPI client | Docker tags |
|---|---|---|---|---|
| Push tag `vX.Y.Z` | ✅ | ✅ | ❌ | `vX.Y.Z`, rolling `latest` |
| Push tag `vX.Y.Z-rc.N` | ❌ | ❌ | ❌ | `vX.Y.Z-rc.N`, rolling `rc` |
| Push tag `vX.Y.Z-dev.N` | ❌ | ❌ | ❌ | `vX.Y.Z-dev.N` only |
| Push tag `webapi-vA.B.C` | ❌ | ❌ | ✅ | (none) |
| Push to `master` | ❌ | ❌ | ❌ | `master`, `<short-sha>` |
| Push to `develop` | ❌ | ❌ | ❌ | `develop`, `<short-sha>`, rolling `dev` |
| Push to `feature/**` | ❌ | ❌ | ❌ | `<branch-slug>`, `<short-sha>` |

## Common pitfalls

- **Hand-editing the Python package version.** Don't. The wheel version is fully derived from git tags via `setuptools_scm`. Editing `pyproject.toml` `version` won't even help — the field is `dynamic`.
- **Using `+<branch>.N` build metadata in tags.** This was a workaround for the old `-dev.N` counter collision; it's no longer needed. Use plain SemVer pre-release form.
- **Pushing a stable tag without a CHANGELOG entry.** The release workflow extracts the section by exact version match. Missing section = workflow failure (correctly so — fix the changelog and re-tag if needed).
- **Pinning downstream Python deps to a non-tag commit.** Off-tag wheels carry a local segment, which PyPI rejects. For unstable cross-repo references, pin to a commit SHA (`graphsense-lib @ git+https://.../graphsense-lib@<sha>`) or mint a `-dev.N` tag for a clean ref.
- **Detached HEAD CI checkouts.** `release-branch-semver` needs the branch name to decide minor-vs.-patch bump. CI workflows that build the library use `actions/checkout@v4` with `fetch-depth: 0` for this reason — keep it that way.
- **Major bumps via `master`.** `release-branch-semver` only auto-bumps minor on `master`, never major. Mint `v3.0.0` explicitly.

## Quick command reference

```bash
# What version would my current checkout produce?
uv run python -m setuptools_scm

# What versions are configured (sanity check)?
make show-versions
make check-semver

# Mint and push a library tag
# (after editing RELEASESEM in Makefile)
make tag-version
git push origin --tags

# Mint and push a webapi tag
# (after editing WEBAPISEM and running update-api-version + sync-client-version)
make tag-version
git push origin --tags

# Build a wheel locally
make build
ls dist/

# List existing dev tags for a target version
git tag --list 'v2.12.0-dev.*'
```
