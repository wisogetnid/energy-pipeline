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

# Run specific test modules
test-data-processing:
    pytest pipeline/tests/data_processing

test-data-retrieval:
    pytest pipeline/tests/data_retrieval

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

# Run specific pipeline components
retrieve-data:
    python -c "from pipeline.ui.data_retrieval_ui import DataRetrievalUI; DataRetrievalUI().run()"

convert-to-jsonl:
    python -c "from pipeline.ui.data_converter_ui import DataConverterUI; DataConverterUI().run()"

convert-to-parquet:
    python -c "from pipeline.ui.parquet_converter_ui import ParquetConverterUI; ParquetConverterUI().run()"

visualize:
    python -c "from pipeline.ui.visualization_ui import VisualizationUI; VisualizationUI().run()"

# Run visualizations on all data
visualize-all:
    python -c "from pipeline.data_visualisation.energy_efficiency import load_and_process_consumption_data, generate_consumption_patterns, generate_weekly_comparison, generate_weekday_weekend_pattern; import glob; from pathlib import Path; output_dir = Path('data/visualisations'); output_dir.mkdir(parents=True, exist_ok=True); [generate_consumption_patterns(*(load_and_process_consumption_data(f)), output_dir / Path(f).stem) and generate_weekly_comparison(*(load_and_process_consumption_data(f)), output_dir / Path(f).stem) and generate_weekday_weekend_pattern(*(load_and_process_consumption_data(f)), output_dir / Path(f).stem) for f in glob.glob('data/processed/*_consumption_*.parquet') or glob.glob('data/processed/*_consumption_*.jsonl')]"

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

# Fetch all resources for a specific date range
fetch-all-resources month="current":
    python -c "from pipeline.ui.data_retrieval_ui import DataRetrievalUI; from pipeline.data_retrieval import GlowmarktClient; from pipeline.utils.credentials import get_credentials; username, password, token = get_credentials(); ui = DataRetrievalUI(); ui.setup_client(username, password, token); ui.select_entity(); resources = ui.client.get_virtual_entity_resources(ui.selected_entity.get('veId')).get('resources', []); valid_resources = [r for r in resources if 'consumption' in r.get('classifier', '')]; ui.select_time_range(preset='{{month}}'); [ui._fetch_resource(r) for r in valid_resources];"