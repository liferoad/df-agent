#!/bin/bash
# Development environment setup script

set -e

echo "Setting up development environment for Dataflow Job Agent..."

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install pre-commit hooks
echo "Installing pre-commit hooks..."
pre-commit install

# Run pre-commit on all files to ensure everything is formatted correctly
echo "Running pre-commit on all files..."
pre-commit run --all-files

echo "✅ Development environment setup complete!"
echo ""
echo "Available commands:"
echo "  pre-commit run --all-files  # Run all hooks on all files"
echo "  pre-commit run              # Run hooks on staged files"
echo "  black .                     # Format Python code"
echo "  flake8 .                    # Lint Python code"
echo "  isort .                     # Sort imports"
echo "  pytest tests/              # Run tests"
