#!/usr/bin/env bash
set -e
git add .
git commit --amend --no-edit
git push -f

