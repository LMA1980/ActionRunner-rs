#!/usr/bin/env bash
set -euo pipefail

echo "================================================================================"
echo "  ActionRunner Service Account & Security Isolation Provisioning"
echo "================================================================================"

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: This script must be run as root (or via sudo)." >&2
    exit 1
fi

# 1. Create action-runner system user and group if not present
if ! id -u action-runner >/dev/null 2>&1; then
    echo "Creating system service account: action-runner"
    useradd -r -m -d /var/lib/action-runner -s /bin/bash -c "ActionRunner CI Daemon" action-runner
else
    echo "System service account action-runner already exists."
fi

# 2. Prepare workspace and config directories with strict ownership (0750 / 0700)
echo "Setting up /var/lib/action-runner and /etc/action-runner"
mkdir -p /var/lib/action-runner/workspace/repos
mkdir -p /var/lib/action-runner/workspace/logs
mkdir -p /var/lib/action-runner/workspace/.cargo
mkdir -p /var/lib/action-runner/web/dist
mkdir -p /etc/action-runner

chown -R action-runner:action-runner /var/lib/action-runner
chmod 750 /var/lib/action-runner
chmod 700 /etc/action-runner

# 3. Migrate webhook secret if present in /home/lma1980/.github_webhook_secret
if [ -f "/home/lma1980/.github_webhook_secret" ] && [ ! -f "/etc/action-runner/github_webhook_secret" ]; then
    echo "Copying webhook secret from /home/lma1980/.github_webhook_secret to /etc/action-runner/github_webhook_secret"
    cp /home/lma1980/.github_webhook_secret /etc/action-runner/github_webhook_secret
    chown action-runner:action-runner /etc/action-runner/github_webhook_secret
    chmod 600 /etc/action-runner/github_webhook_secret
fi

# 4. Migrate CI SSH Deploy Key if present
if [ -f "/home/lma1980/.ssh/id_k3_ci" ] && [ ! -f "/var/lib/action-runner/.ssh/id_k3_ci" ]; then
    echo "Provisioning CI deploy key for action-runner"
    mkdir -p /var/lib/action-runner/.ssh
    cp /home/lma1980/.ssh/id_k3_ci /var/lib/action-runner/.ssh/id_k3_ci
    if [ -f "/home/lma1980/.ssh/id_k3_ci.pub" ]; then
        cp /home/lma1980/.ssh/id_k3_ci.pub /var/lib/action-runner/.ssh/id_k3_ci.pub
    fi
    chown -R action-runner:action-runner /var/lib/action-runner/.ssh
    chmod 700 /var/lib/action-runner/.ssh
    chmod 600 /var/lib/action-runner/.ssh/id_k3_ci
fi

# 5. Install Sudoers configuration for user lma1980 diagnostics
echo "Installing sudoers diagnostics policy in /etc/sudoers.d/action-runner-diag"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp "${SCRIPT_DIR}/sudoers-action-runner" /etc/sudoers.d/action-runner-diag
chmod 440 /etc/sudoers.d/action-runner-diag
visudo -c -f /etc/sudoers.d/action-runner-diag

# 6. Install Systemd Service Unit
echo "Installing /etc/systemd/system/action-runner.service"
cp "${SCRIPT_DIR}/action-runner.service" /etc/systemd/system/action-runner.service
chmod 644 /etc/systemd/system/action-runner.service
systemctl daemon-reload

echo "================================================================================"
echo "  Provisioning complete!"
echo "  To test sudo diagnostics as lma1980:"
echo "    sudo -u action-runner -i"
echo "  To enable and start the service:"
echo "    sudo systemctl enable --now action-runner"
echo "================================================================================"
