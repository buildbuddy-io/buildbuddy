#!/bin/bash

bash -c "${BASH_SCRIPT}"
exit_code=$?
if [[ $exit_code != 0 ]]; then
  echo "error running script, exit code $exit_code"
  exit 1
fi

echo "successfully ran script"



