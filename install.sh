#!/bin/bash

set -e
read -p "A new conda environment will be created. Please give it a name: " env
conda create -n $env --yes
conda install -n $env -c conda-forge \
--file requirements/build.txt \
--file requirements/run.txt \
--file requirements/test.txt \
--file requirements/doc.txt \
--yes
conda run -n $env python -m pip install -e .
echo "Installtion is complete."
