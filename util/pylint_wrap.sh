#!/bin/bash

# The purpose of this script is to perform a contolled, minimalist
# PyLint operation upon a set of Python code, with the intention that
# the lint failures detected by this script are deemed severe enough
# to prevent a build from completing / installing.  The motivation
# for introducting this is because the Python setuptools (e.g the
# setup.py install command) INEXPLICABLY DO NOT return a failure
# status code when Python scripts fail to byte compile: e.g. syntax
# errors DO NOT cause the setuptools packaging machinery to return
# a non-zero status code, so Makefile-based builds do not halt
# appropriately, and non-usable eggs get installed.

Version=0.1.0
ThisBinDir=$(cd $(dirname "$0"); pwd)
PythonLint=`sh $ThisBinDir/findPython.sh`/bin/pylint
if [ ! -x $PythonLint ] ; then 
    echo "FATAL ERROR: did not find pylint at $PythonLint" >&2
    exit 1
fi

Tmp=/tmp/pylint-${LOGNAME}-${RANDOM}.txt
trap '\rm -f $Tmp' EXIT                     # Clean up anything left over

if [ "$1" = "-x" ] ; then 
    set -x
    shift
fi

$PythonLint \
    --output-format=colorized \
    --rcfile=/dev/null \
    --msg-template="{path}:{line}: [{msg_id}] {msg}" \
    --reports=no \
    --disable=C,R \
    $@ \
    > $Tmp 2>&1

# Only truly fatal errors are flagged, mere stylistic complaints are ignored
egrep -i "importerror|syntax|E0001|E1300|undefined" $Tmp
test $? -eq 0 && exit 2
exit 0
