# List available commands
default:
    @just --list

# Install required dependencies
install:
    pip install -r requirements.txt

# Run the complete energy pipeline
run:
    python -m pipeline

# Run all unit tests
test:
    pytest pipeline/tests

# Run tests with coverage report
test-coverage:
    pytest --cov=pipeline pipeline/tests --cov-report=term

# Clean up generated files
clean:
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete
    find . -type d -name "*.egg-info" -exec rm -rf {} +
    find . -type d -name "*.egg" -exec rm -rf {} +
    find . -type d -name ".pytest_cache" -exec rm -rf {} +
    find . -type d -name ".coverage" -exec rm -rf {} +
    rm -rf .coverage


# Create a new environment
create-env:
    python -m venv .venv
    @echo "Run 'source .venv/bin/activate' to activate the environment"

# Lint code
lint:
    pylint pipeline

# Format code with black
format:
    black pipeline

# Run tests and lint code
check: test lint