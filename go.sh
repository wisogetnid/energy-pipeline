#!/usr/bin/env bash
# filepath: /Users/tobiasvogel/repositories/energy-pipeline/go

set -e

# List available commands
function show_help {
  echo "Energy Pipeline - Available commands:"
  echo "  ./go.sh                    Show this help message"
  echo "  ./go.sh install            Install project with dependencies using uv"
  echo "  ./go.sh run                Run the complete energy pipeline"
  echo "  ./go.sh test               Run all unit tests"
  echo "  ./go.sh test-coverage      Run tests with coverage report"
  echo "  ./go.sh clean              Clean up generated files"
  echo "  ./go.sh create-env         Create a new virtual environment"
  echo "  ./go.sh lint               Lint code with pylint"
  echo "  ./go.sh format             Format code with black"
  echo "  ./go.sh check              Run tests and lint code"
}

# Main command router
case "$1" in
  "install")
    echo "Installing dependencies with uv..."
    uv sync --dev
    ;;
  "run")
    uv run python -m pipeline
    ;;
  "test")
    uv run --dev pytest pipeline/tests
    ;;
  "test-coverage")
    uv run pytest --cov=pipeline pipeline/tests --cov-report=term
    ;;
  "clean")
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    find . -type d -name "*.egg-info" -exec rm -rf {} +
    find . -type d -name "*.egg" -exec rm -rf {} +
    find . -type d -name ".pytest_cache" -exec rm -rf {} +
    find . -type d -name ".coverage" -exec rm -rf {} +
    rm -rf .coverage
    ;;
  "create-env")
    echo "uv automatically manages virtual environments. Run './go.sh install' to set up the project."
    ;;
  "lint")
    uv run pylint pipeline
    ;;
  "format")
    uv run black pipeline
    ;;
  "check")
    uv run pytest pipeline/tests
    uv run pylint pipeline
    ;;
  *)
    show_help
    ;;
esac