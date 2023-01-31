#!/usr/bin/env bash
today=$(date "+%Y-%m-%d")
find . -type f -name "*ipynb" | grep -v .ipynb_checkpoints | while read f; do git add "${f}"; done
git commit -m "Updating notebooks on ${today}"
git push origin master
