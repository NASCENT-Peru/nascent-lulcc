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

# Download and extract
echo "Downloading to temporary file..."
TMP_FILE=$(mktemp)
curl -Ls "$MICROMAMBA_URL" -o "$TMP_FILE"

if [ $? -ne 0 ]; then
    echo "✗ Failed to download micromamba"
    rm -f "$TMP_FILE"
    exit 1
fi

echo "Extracting archive..."
tar -xvjf "$TMP_FILE" -C "$INSTALL_DIR" --strip-components=1 bin/micromamba

if [ $? -eq 0 ] && [ -f "$INSTALL_DIR/micromamba" ]; then
    echo
    echo "✓ Micromamba extracted successfully to: $INSTALL_DIR/micromamba"
    rm -f "$TMP_FILE"
else
    echo "✗ Failed to extract micromamba"
    echo "Trying alternative extraction method..."
    
    # Alternative: extract to current directory first
    cd "$INSTALL_DIR" || exit 1
    tar -xvjf "$TMP_FILE"
    
    if [ -f "bin/micromamba" ]; then
        mv bin/micromamba .
        rm -rf bin
        echo "✓ Micromamba installed successfully to: $INSTALL_DIR/micromamba"
    else
        echo "✗ Failed to install micromamba"
        rm -f "$TMP_FILE"
        exit 1
    fi
    rm -f "$TMP_FILE"
fi

# Make executable
chmod +x "$INSTALL_DIR/micromamba"

# Verify installation
if [ ! -f "$INSTALL_DIR/micromamba" ]; then
    echo "✗ ERROR: micromamba file not found after installation"
    exit 1
fi

if [ ! -x "$INSTALL_DIR/micromamba" ]; then
    echo "✗ ERROR: micromamba is not executable"
    exit 1
fi

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
echo

# Verify and show version
if [ -x "$INSTALL_DIR/micromamba" ]; then
    echo "Version:"
    "$INSTALL_DIR/micromamba" --version
    echo
else
    echo "WARNING: Could not verify installation"
    echo "Please check: ls -lh $INSTALL_DIR/micromamba"
    echo
fi

echo "Next steps:"
echo "1. Add micromamba to your PATH (see instructions above)"
echo "2. Run setup_environments.sh to create conda environments"
echo
