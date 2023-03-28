# Unit tests

## Running unit tests locally
From the module's root dir:
```
python3 -m venv .venv
source .venv/bin/activate
pip3 install --upgrade pip
pip3 install pytest jsonschema
deactivate
source .venv/bin/activate
pytest -v blockbreaker.py tests/test-blockbreaker.py
```
