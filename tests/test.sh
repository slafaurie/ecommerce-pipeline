#!/bin/bash

# show current files
ls

# show pytest
pip freeze | grep "pytest"

# Run tests
pytest tests