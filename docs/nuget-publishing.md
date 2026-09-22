# NuGet.org trusted publishing

Publishing uses GitHub OIDC and a short-lived NuGet API key from the organization-approved `NuGet/login@v1` action. No long-lived NuGet.org API key is required. Package ownership remains with `petersunde`.

The NuGet.org policy matches this repository, its publishing workflow filename and the `nuget-publishing` GitHub environment. It permits only new versions of this repository's existing package; creating other package names and unlisting versions are not allowed.

The environment allows `main` and `v*` release tags. Keep its branch/tag restrictions in place; do not broaden them to allow pull-request branches. Both normal publishing and authentication checks use this environment.

For a non-publishing authentication check, dispatch the publishing workflow from an allowed branch with `auth_only=true`. All build/release jobs are skipped; the check exchanges GitHub OIDC for a temporary credential without printing it. This proves token issuance, not a package upload. A normal successful publication and downstream package download provide the publishing acceptance evidence.

Old workflow runs and release tags retain their original workflow definitions. Do not rerun a pre-migration workflow after its old key is revoked. Use the current publishing workflow; for release recovery, dispatch from the current default branch and follow the repository's normal version/recovery guards.
