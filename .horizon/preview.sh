#!/bin/bash
set -euo pipefail

# Package a synthetic offline GUI installer with the CI-built toolchain.
# Uses the README's manifest and `surge pack` workflow; never builds or uploads.
repo_root="$PWD"
preview_root="$(mktemp -d /tmp/surge-preview.XXXXXX)"
mkdir -p "$preview_root/artifacts" "$preview_root/storage"
printf '#!/bin/sh\nsleep 3600\n' > "$preview_root/artifacts/preview-app"
chmod +x "$preview_root/artifacts/preview-app"
cat > "$preview_root/surge.yml" <<EOF
schema: 1
storage:
  provider: filesystem
  bucket: $preview_root/storage
apps:
  - id: preview-app
    name: Installer Preview
    main: preview-app
    installDirectory: $preview_root/installed
    channels: [stable]
    shortcuts: []
    installers: [offline-gui]
    target:
      rid: linux-x64
EOF
export SURGE_INSTALLER_UI_LAUNCHER="$repo_root/target/release/surge-installer-ui"
export SURGE_INSTALLER_BINARY="$repo_root/target/release/surge"
"$repo_root/target/release/surge" --manifest-path "$preview_root/surge.yml" pack \
    --version 1.0.0 --artifacts-dir "$preview_root/artifacts" \
    --output-dir "$preview_root/packages"
mapfile -t installers < <(find "$preview_root/installers" -type f)
test "${#installers[@]}" -eq 1
export LIBGL_ALWAYS_SOFTWARE=1
exec "${installers[0]}"
