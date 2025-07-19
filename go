#!/usr/bin/env bash
# filepath: /Users/tobiasvogel/repositories/energy-pipeline/go

set -e

# List available commands
function show_help {
  echo "Energy Pipeline - Available commands:"
  echo "  ./go                    Show this help message"
  echo "  ./go install            Install required dependencies"
  echo "  ./go run                Run the complete energy pipeline"
  echo "  ./go test               Run all unit tests"
  echo "  ./go test-coverage      Run tests with coverage report"
  echo "  ./go clean              Clean up generated files"
  echo "  ./go create-env         Create a new virtual environment"
  echo "  ./go lint               Lint code with pylint"
  echo "  ./go format             Format code with black"
  echo "  ./go check              Run tests and lint code"
}

# Main command router
case "$1" in
  "install")
    pip install -r requirements.txt
    ;;
  "run")
    python -m pipeline
    ;;
  "test")
    pytest pipeline/tests
    ;;
  "test-coverage")
    pytest --cov=pipeline pipeline/tests --cov-report=term
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
    python -m venv .venv
    echo "Run 'source .venv/bin/activate' to activate the environment"
    ;;
  "lint")
    pylint pipeline
    ;;
  "format")
    black pipeline
    ;;
  "check")
    pytest pipeline/tests
    pylint pipeline
    ;;
  *)
    show_help
    ;;
esac