#!/bin/bash

pip install \
  future \
  pytest \
  scipy

pip install .

# Run tests
py.test
