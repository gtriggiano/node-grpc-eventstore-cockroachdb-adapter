#! /bin/bash

source "${BASH_SOURCE%/*}/common.sh"

echo -n "Preparing for tests... "
cleanService &> /dev/null
setupCockroachDbInstance
echo "Done"

if [[ -n $1 ]]; then
  CMD="CODE_FOLDER=src mocha --require babel-register -b -w tests/index"
else
  CMD="CODE_FOLDER=lib mocha lib-tests/index"
fi

runAsService development $CMD

echo "Cleaning after CockroachDB adapter tests... "
cleanService &> /dev/null
cleanCockroachDbInstance &> /dev/null
echo "Done"
