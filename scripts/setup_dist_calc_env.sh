#!/usr/bin/env bash
# One-time setup on an Euler login node:
# - Install Miniconda into $HOME/miniconda3 if missing
# - Recreate the Conda environment "dist_calc_env" exactly from sdm_env.yml
set -Eeuo pipefail
IFS=$'\n\t'

# ---- Configuration ----
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ENVYML="$SCRIPT_DIR/dist_calc_env.yml"      # Conda recipe
CONDA_DIR="${HOME}/miniconda3"        # Conda root
ENV_NAME="dist_calc_env"                      # Env name (matches YAML)

# ---- Helpers ----
ensure_tools() {
  # Ensure minimal CLI tools are present
  if ! command -v wget >/dev/null 2>&1; then
    echo "Error: 'wget' not found on PATH." >&2
    exit 1
  fi
}

install_miniconda_if_missing() {
  # Install Miniconda into $CONDA_DIR if it is not present
  if [[ -d "$CONDA_DIR" ]]; then
    return 0
  fi
  echo ">>> Installing Miniconda into $CONDA_DIR"
  tmp_installer="$(mktemp /tmp/mini.XXXXXX.sh)"
  wget -qO "$tmp_installer" \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  bash "$tmp_installer" -b -p "$CONDA_DIR"
  rm -f "$tmp_installer"
}

init_conda_shell() {
  # Source conda shell hook without touching user dotfiles
  source "$CONDA_DIR/etc/profile.d/conda.sh"
  conda activate base
}

recreate_env_from_yaml() {
  # Remove existing env and recreate exactly from the YAML
  echo ">>> Rebuilding conda env '$ENV_NAME' from $ENVYML"
  conda env remove -n "$ENV_NAME" -y 2>/dev/null || true
  conda env create -f "$ENVYML"
}

# ---- Main ----
echo ">>> Starting setup for env '$ENV_NAME'"
ensure_tools
install_miniconda_if_missing
init_conda_shell
recreate_env_from_yaml

echo
echo "✅ Done. Use the environment with:"
echo "   source $CONDA_DIR/etc/profile.d/conda.sh && conda activate $ENV_NAME"
