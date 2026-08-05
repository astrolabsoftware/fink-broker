#!/bin/bash

pip install ruff --upgrade

ruff check --fix .
ruff format .
