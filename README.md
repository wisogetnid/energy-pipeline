# Glowmarkt API dabbling

The Hildebrand Glowmarkt API (should) allows people to retrieve their energy data over an API.

You can run the project using `python -m pipeline`

## Tools
- [Bruno](https://www.usebruno.com/) scripts to manually explore the API
- Python for the data pipeline

## Python setup
python env setup, as suggested in a post on [staticnet.io](https://staticnet.io/macos-python-dev-env/)

- install python - I propose to use asdf for python version management. Install it, then execute `asdf install` on the root folder (using .tool-versions)
- use python virtual environments for local dependency management
    - use `source .venv/bin/activate` to start and `deactivate` to stop
    - install all required packages using `pip install .`
- run tests with `pytest`
- run the pipeline with `python -m pipeline`

## Todos
- use just
- actually write the code =o)