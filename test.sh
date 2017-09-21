#!/bin/bash

pip install \
  future \
  pytest \
  scipy

pip install .

pip install requests>=2.9.1 pandas>=0.20.3 numpy>=1.13.1 osmnet>=0.1.4 pandana>=0.3.0 matplotlib>=2.0.2 geopy>=1.11.0 pyyaml>=3.11 scikit-learn>=0.18.2 

# Run tests
py.test
