#!/bin/bash

# findPython.sh: help determine installation location for this package,
#                by identifying existing candidate Python installations

InstallDir=

# Use any Python3 installation, or Python2 if >= 2.7
Python=`type -P python`
if [ -n "$Python" ] ; then
    case `python --version 2>&1 | awk '{print $NF}'` in
        2.7*| 3.*)
            InstallDir=`dirname $Python`
            InstallDir=`dirname $InstallDir`
            ;;
    esac
fi

if [ -z "$InstallDir" ] ; then
    # Nothing found: for convenience, look for well-known dirs @ Broad Institute
    BroadDirs="/local/firebrowse/latest /xchip/tcga/Tools/gdac/latest"
    for dir in $BroadDirs ; do
        if [ -d $dir ] ; then
            InstallDir=$dir
            break
        fi
    done
fi

if [ -z "$InstallDir" ] ; then
    echo "Error: could not find an appropriate python installation to use" >&2
    exit 1
fi

echo "$InstallDir"
exit 0
