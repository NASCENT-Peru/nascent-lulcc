#!/bin/bash
# install_micromamba_simple.sh
# Simplified micromamba installation script

set -e  # Exit on error

INSTALL_DIR="$HOME/.local/bin"
MAMBA_BIN="$INSTALL_DIR/micromamba"

echo "========================================="
echo "Installing Micromamba (Simple Method)"
echo "========================================="
echo "Installation directory: $INSTALL_DIR"
echo

# Create directory
mkdir -p "$INSTALL_DIR"

# Detect system
ARCH=$(uname -m)
OS=$(uname -s)
echo "System: $OS $ARCH"

# Set download URL
if [ "$OS" = "Linux" ] && [ "$ARCH" = "x86_64" ]; then
    # Use the direct download link for Linux x86_64
    MICROMAMBA_URL="https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-linux-64"
else
    echo "ERROR: Unsupported system: $OS $ARCH"
    exit 1
fi

echo "Downloading from: $MICROMAMBA_URL"
echo

# Download directly to target location
cd "$INSTALL_DIR"
curl -L "$MICROMAMBA_URL" -o micromamba

# Check download
if [ ! -f micromamba ]; then
    echo "ERROR: Download failed - file not found"
    exit 1
fi

FILE_SIZE=$(stat -c%s micromamba 2>/dev/null || stat -f%z micromamba 2>/dev/null || echo "0")
if [ "$FILE_SIZE" -lt 1000000 ]; then
    echo "ERROR: Downloaded file is too small ($FILE_SIZE bytes)"
    echo "This might be an error page or failed download"
    rm -f micromamba
    exit 1
fi

# Make executable
chmod +x micromamba

# Verify
if [ ! -x micromamba ]; then
    echo "ERROR: Failed to make executable"
    exit 1
fi

echo
echo "========================================="
echo "Installation Successful!"
echo "========================================="
echo "Location: $MAMBA_BIN"
echo "Size: $(ls -lh micromamba | awk '{print $5}')"
echo

# Test it
echo "Testing micromamba..."
./micromamba --version

echo
echo "========================================="
echo "Next Steps"
echo "========================================="
echo
echo "1. Add to your ~/.bashrc:"
echo
cat << 'EOF'
   export MAMBA_EXE="$HOME/.local/bin/micromamba"
   export MAMBA_ROOT_PREFIX="$HOME/.micromamba"
   eval "$($MAMBA_EXE shell hook -s bash)"
EOF
echo
echo "2. Run the commands:"
echo
cat << EOF
   echo 'export MAMBA_EXE="\$HOME/.local/bin/micromamba"' >> ~/.bashrc
   echo 'export MAMBA_ROOT_PREFIX="\$HOME/.micromamba"' >> ~/.bashrc
   echo 'eval "\$(\$MAMBA_EXE shell hook -s bash)"' >> ~/.bashrc
   source ~/.bashrc
EOF
echo
echo "3. Create environments:"
echo "   bash scripts/setup_environments.sh"
echo
echo "Done! ✓"
