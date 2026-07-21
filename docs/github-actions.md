# GitHub Actions

The repository uses GitHub Actions for continuous integration and tagged binary releases. The workflows use the package's minimum Rust version, Rust 1.88.0, and always honor `Cargo.lock`.

## Continuous integration

`.github/workflows/ci.yml` runs for pull requests and pushes to `main`. It can also be called by another workflow. Superseded runs for the same ref are cancelled.

The workflow exposes these jobs, which can be selected as required checks in branch protection rules. Compilation, tests, and package verification use the declared MSRV.

- `format`: `cargo fmt --check`
- `clippy`: `cargo clippy --all-targets --all-features --locked -- -D warnings`
- `test`: `cargo test --all-features --locked`
- `release-build`: `cargo build --release --locked` followed by `cargo package --locked`
- `audit`: checks `Cargo.lock` against RustSec advisories on pushes and pull requests

The same RustSec audit runs when the release workflow calls CI, so a release is blocked by formatting, Clippy, tests, dependency advisories, package verification, and the release-profile build.

## Creating a release

GitHub Releases are created from stable semantic-version tags in the form `vX.Y.Z`. The tag must exactly match `package.version` in `Cargo.toml`. For example, version `0.2.0` must be released as `v0.2.0`.

Before tagging:

1. Update `package.version` and `Cargo.lock` in the normal development change.
2. Merge that change and confirm the CI checks pass.
3. Run any authorized external TAK Server preflight required for the release. GitHub Actions only runs the repository's local test suite and does not contact a TAK Server.
4. Create and push an annotated tag:

   ```bash
   git tag -a v0.2.0 -m "Release v0.2.0"
   git push origin v0.2.0
   ```

Pushing the tag starts `.github/workflows/release.yml`. Tags with prerelease suffixes, non-semantic names, or versions that differ from Cargo metadata fail before compilation.

## Release artifacts

After CI succeeds, native GitHub-hosted runners build `tak-bench` for:

| Platform | Rust target | Asset format |
| --- | --- | --- |
| Linux x86-64 | `x86_64-unknown-linux-gnu` | `.tar.gz` |
| Windows x86-64 | `x86_64-pc-windows-msvc` | `.zip` |
| macOS Intel | `x86_64-apple-darwin` | `.tar.gz` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `.tar.gz` |

Each archive is named `tak-bench-<tag>-<target>.<format>` and contains the executable, `README.md`, `LICENSE-APACHE`, and `LICENSE-MIT`. GitHub also provides its automatically generated source archives.

The release includes `SHA256SUMS`. On Linux or macOS, download all assets into one directory and verify them with:

```bash
sha256sum --check SHA256SUMS
```

On Windows PowerShell, compare an asset with its entry in `SHA256SUMS`:

```powershell
Get-FileHash .\tak-bench-v0.2.0-x86_64-pc-windows-msvc.zip -Algorithm SHA256
```

The binaries are not code-signed. SHA-256 checksums detect download corruption or modification but do not replace code signing.

## Failures and reruns

If validation, CI, or any platform build fails, neither crates.io nor the GitHub Release is published. Fix the problem on a new commit and create a new tag; do not move a published release tag.

The crates.io job publishes the single `tak-bench` package using the repository's `CRATES_IO_TOKEN` secret. A rerun skips an existing version only when its crates.io checksum matches the package built from the tag. Published crate versions are immutable.

If only the GitHub Release publication fails transiently, rerun the failed jobs in GitHub Actions. The workflow creates the release when it is absent, completes a partial draft, or replaces assets in an existing mutable release while retaining its release notes. If the repository has immutable releases enabled, assets in an already published release cannot be replaced.
