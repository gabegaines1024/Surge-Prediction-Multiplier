#!/bin/bash
# Quick setup script for Dask environment

echo "=============================================="
echo "  Dask Setup for 80M+ Row Processing"
echo "=============================================="
echo ""

# Check Python version
echo "1. Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "   Found: Python $python_version"

# Create virtual environment (optional but recommended)
echo ""
echo "2. Would you like to create a virtual environment? (y/n)"
read -r create_venv

if [[ $create_venv == "y" || $create_venv == "Y" ]]; then
    echo "   Creating virtual environment..."
    python3 -m venv venv_dask
    echo "   Activating virtual environment..."
    source venv_dask/bin/activate
    echo "   ✓ Virtual environment activated"
else
    echo "   Skipping virtual environment creation"
fi

# Install dependencies
echo ""
echo "3. Installing Dask and dependencies..."
pip install --upgrade pip
pip install -r requirements_dask.txt

# Verify installation
echo ""
echo "4. Verifying installation..."
python3 -c "import dask; import pandas; import numpy; import pyarrow; print('✓ All packages installed successfully!')"

echo ""
echo "=============================================="
echo "  Setup Complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Run: python retrieval.py"
echo "  2. Wait 30-50 minutes for processing"
echo "  3. Check ./processed_data/ for output"
echo ""
echo "For detailed guide, see: DASK_IMPLEMENTATION_GUIDE.md"
echo ""

