# BuildBuddy goreman tools

This directory contains tools that make it easier to run multiple BuildBuddy
servers locally. You may want to do this in order to:

- Test remote execution locally
- Test distributed caching locally
- Something else!

## Pre-reqs

Before using these configs, you need to install goreman, the tool that runs
them. It's easy:

- `goreman` (go get github.com/mattn/goreman)

## How to run a remote execution stack locally:

`goreman -f tools/goreman/procfiles/Procfile.rexec start`

## How to run a distributed disk cache locally:

`goreman -f tools/goreman/procfiles/Procfile.ddisk start`
