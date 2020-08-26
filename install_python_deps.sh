#!bin/bash

set -e

# Dependencies
pip install -r requirements.txt

# Installation of torch without GPU support (lighter)
pip install torch==1.6.0+cpu -f https://download.pytorch.org/whl/torch_stable.html
