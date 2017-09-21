#!/bin/bash

# I think the correct way to do this is to add these
# dependencies via 'tests_require' in setup.py. I ran
# into issue getting scipy installed this way though,
# so for now these are basically copied from .travis.yml
pip install \
  future \
  pytest \
  scipy

# Don't depend on the code in the Docker image
# being up to date.
pip install .

# Run tests
py.test
