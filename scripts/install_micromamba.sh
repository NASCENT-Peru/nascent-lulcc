#!/bin/bash
# install_micromamba.sh
# Install micromamba to user's home directory

# Installation directory (persistent, small footprint)
INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"

# Download micromamba
echo "========================================="
echo "Installing Micromamba"
echo "========================================="
echo "Installation directory: $INSTALL_DIR"
echo

# Detect architecture
ARCH=$(uname -m)
OS=$(uname -s)

echo "Detected system: $OS $ARCH"

# Download appropriate version
if [ "$OS" = "Linux" ] && [ "$ARCH" = "x86_64" ]; then
    MICROMAMBA_URL="https://micro.mamba.pm/api/micromamba/linux-64/latest"
else
    echo "ERROR: Unsupported system: $OS $ARCH"
    exit 1
fi

echo "Downloading micromamba from: $MICROMAMBA_URL"
echo

cd "$INSTALL_DIR" || exit 1

# Download and extract
curl -Ls "$MICROMAMBA_URL" | tar -xvj bin/micromamba

if [ $? -eq 0 ]; then
    echo
    echo "✓ Micromamba installed successfully to: $INSTALL_DIR/micromamba"
else
    echo "✗ Failed to install micromamba"
    exit 1
fi

# Make executable
chmod +x "$INSTALL_DIR/micromamba"

# Initialize shell configuration (optional)
echo
echo "========================================="
echo "Shell Configuration (Optional)"
echo "========================================="
echo
echo "To use micromamba easily, add this to your ~/.bashrc:"
echo
echo "  # Micromamba"
echo "  export MAMBA_EXE=\"$INSTALL_DIR/micromamba\""
echo "  export MAMBA_ROOT_PREFIX=\"$HOME/.micromamba\""
echo "  eval \"\$(\$MAMBA_EXE shell hook -s bash)\""
echo
echo "Or run these commands now:"
echo
echo "  echo 'export MAMBA_EXE=\"$INSTALL_DIR/micromamba\"' >> ~/.bashrc"
echo "  echo 'export MAMBA_ROOT_PREFIX=\"$HOME/.micromamba\"' >> ~/.bashrc"
echo "  echo 'eval \"\$(\\\$MAMBA_EXE shell hook -s bash)\"' >> ~/.bashrc"
echo "  source ~/.bashrc"
echo
echo "========================================="
echo "Installation Complete"
echo "========================================="
echo "Micromamba location: $INSTALL_DIR/micromamba"
echo "Version:"
"$INSTALL_DIR/micromamba" --version
echo
echo "Next steps:"
echo "1. Update your scripts to use: $INSTALL_DIR/micromamba"
echo "2. Run setup_environments.sh to create conda environments"
echo
