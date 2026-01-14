#!/bin/bash
# troubleshoot_micromamba.sh
# Diagnose micromamba installation issues

echo "========================================="
echo "Micromamba Troubleshooting"
echo "========================================="
echo

# Check installation directory
INSTALL_DIR="$HOME/.local/bin"
MAMBA_BIN="$INSTALL_DIR/micromamba"

echo "Checking installation directory..."
echo "Expected location: $MAMBA_BIN"
echo

if [ -d "$INSTALL_DIR" ]; then
    echo "✓ Directory exists: $INSTALL_DIR"
    echo "  Contents:"
    ls -lah "$INSTALL_DIR"
else
    echo "✗ Directory does not exist: $INSTALL_DIR"
    echo "  Creating it now..."
    mkdir -p "$INSTALL_DIR"
fi
echo

# Check micromamba file
echo "Checking micromamba binary..."
if [ -f "$MAMBA_BIN" ]; then
    echo "✓ File exists: $MAMBA_BIN"
    echo "  Size: $(ls -lh "$MAMBA_BIN" | awk '{print $5}')"
    echo "  Permissions: $(ls -l "$MAMBA_BIN" | awk '{print $1}')"
    
    if [ -x "$MAMBA_BIN" ]; then
        echo "✓ File is executable"
        echo "  Testing version..."
        if "$MAMBA_BIN" --version 2>&1; then
            echo "✓ Micromamba works correctly!"
        else
            echo "✗ Micromamba exists but fails to run"
            echo "  Checking file type..."
            file "$MAMBA_BIN"
        fi
    else
        echo "✗ File is not executable"
        echo "  Attempting to fix..."
        chmod +x "$MAMBA_BIN"
        if [ -x "$MAMBA_BIN" ]; then
            echo "✓ Fixed! File is now executable"
        else
            echo "✗ Could not make file executable"
        fi
    fi
else
    echo "✗ File does not exist: $MAMBA_BIN"
    echo
    echo "Micromamba is not installed."
    echo "Please run one of these scripts:"
    echo "  bash scripts/install_micromamba_simple.sh  (recommended)"
    echo "  bash scripts/install_micromamba.sh"
fi
echo

# Check PATH
echo "Checking PATH..."
if echo "$PATH" | grep -q "$INSTALL_DIR"; then
    echo "✓ $INSTALL_DIR is in PATH"
else
    echo "✗ $INSTALL_DIR is NOT in PATH"
    echo "  You may need to use full path: $MAMBA_BIN"
fi
echo

# Check environment variable
echo "Checking MAMBA_EXE environment variable..."
if [ -n "$MAMBA_EXE" ]; then
    echo "✓ MAMBA_EXE is set: $MAMBA_EXE"
    if [ -x "$MAMBA_EXE" ]; then
        echo "✓ MAMBA_EXE points to valid executable"
    else
        echo "✗ MAMBA_EXE points to invalid location"
    fi
else
    echo "✗ MAMBA_EXE is not set"
    echo "  Add this to ~/.bashrc:"
    echo "    export MAMBA_EXE=\"$MAMBA_BIN\""
fi
echo

# Check if micromamba command works
echo "Checking if 'micromamba' command is available..."
if command -v micromamba &> /dev/null; then
    echo "✓ 'micromamba' command found in PATH"
    echo "  Location: $(command -v micromamba)"
    micromamba --version
else
    echo "✗ 'micromamba' command not found in PATH"
    echo "  This is OK if you use the full path: $MAMBA_BIN"
fi
echo

# Check environments directory
echo "Checking environments directory..."
ENV_DIR="/cluster/scratch/bblack/micromamba/envs"
if [ -d "$ENV_DIR" ]; then
    echo "✓ Environments directory exists: $ENV_DIR"
    echo "  Environments found:"
    if [ -n "$(ls -A "$ENV_DIR" 2>/dev/null)" ]; then
        ls -1 "$ENV_DIR"
    else
        echo "  (none - run setup_environments.sh)"
    fi
else
    echo "✗ Environments directory does not exist: $ENV_DIR"
    echo "  Run: bash scripts/setup_environments.sh"
fi
echo

# Summary
echo "========================================="
echo "Summary & Recommendations"
echo "========================================="
echo

if [ -x "$MAMBA_BIN" ]; then
    echo "✓ Micromamba is installed and working!"
    echo
    echo "Next steps:"
    echo "  1. Ensure ~/.bashrc has micromamba configuration"
    echo "  2. Run: bash scripts/setup_environments.sh"
    echo "  3. Test with: sbatch scripts/submit_calibrate_allocation_parameters.sh"
else
    echo "✗ Micromamba is NOT installed or not working"
    echo
    echo "Recommended action:"
    echo "  bash scripts/install_micromamba_simple.sh"
fi
echo
