#!/bin/bash

# config.sh: Help determine installation location for gdctools package,
#            by identifying existing Python installations as candidates.

InstallDir=

if [ -n "$1" ] ; then
    PythonExe=$1
    shift
else
    PythonExe=python
fi

if [ -n "$VIRTUAL_ENV" ] ; then
    InstallDir=$VIRTUAL_ENV
fi

if [ -z "$InstallDir" ] ; then
    # For convenience, give precedence to well-known directories @ Broad Institute
    BroadDirs="/local/firebrowse/latest /xchip/tcga/Tools/gdac/latest"
    for dir in $BroadDirs ; do
        if [ -d $dir ] ; then
            InstallDir=$dir
            break
        fi
    done
fi

if [ -z "$InstallDir" ] ; then
    Python=`type -P $PythonExe`
    if [ -n "$Python" ]; then
        InstallDir=`dirname $Python`
        InstallDir=`dirname $InstallDir`
    fi
fi

if [ -z "$InstallDir" ] ; then
    echo "Error: could not find a $PythonExe installation to use" >&2
    exit 1
fi

echo "$InstallDir"
exit 0
