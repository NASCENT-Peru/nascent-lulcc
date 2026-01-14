#!/bin/bash
# Quick Setup - Run this first time on HPC

echo "========================================="
echo "NASCENT LULCC - HPC Quick Setup"
echo "========================================="
echo

# 1. Install micromamba
echo "Step 1: Installing micromamba..."
bash scripts/install_micromamba.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to install micromamba"
    exit 1
fi
echo

# 2. Add to bashrc
echo "Step 2: Configuring shell..."
if ! grep -q "MAMBA_EXE" ~/.bashrc; then
    echo 'export MAMBA_EXE="$HOME/.local/bin/micromamba"' >> ~/.bashrc
    echo 'export MAMBA_ROOT_PREFIX="$HOME/.micromamba"' >> ~/.bashrc
    echo 'eval "$($MAMBA_EXE shell hook -s bash)"' >> ~/.bashrc
    echo "✓ Added micromamba configuration to ~/.bashrc"
else
    echo "✓ Micromamba already configured in ~/.bashrc"
fi
echo

# 3. Source bashrc
echo "Step 3: Reloading shell configuration..."
source ~/.bashrc
echo "✓ Shell configuration reloaded"
echo

# 4. Create environments
echo "Step 4: Creating conda environments..."
bash scripts/setup_environments.sh
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create environments"
    exit 1
fi
echo

# 5. Verify
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo
echo "Installed micromamba at:"
which micromamba 2>/dev/null || echo "$HOME/.local/bin/micromamba"
echo
echo "Created environments:"
$HOME/.local/bin/micromamba env list
echo
echo "Next steps:"
echo "  1. Test with: sbatch scripts/submit_calibrate_allocation_parameters.sh"
echo "  2. Monitor: squeue -u \$USER"
echo "  3. Check logs: cat logs/calibrate-allocation-params-*.out"
echo
echo "Done! 🎉"
