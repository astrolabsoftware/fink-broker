#!/bin/bash

pip install ruff --upgrade

ruff check --fix --preview .
ruff format --preview .
