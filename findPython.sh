#!/bin/bash

# findPython.sh: help determine installation location for this package,
#                by identifying existing candidate Python installations

InstallDir=

# For convenience, give precedence to well-known directories @ Broad Institute
BroadDirs="/local/firebrowse/latest"
for dir in $BroadDirs ; do
    if [ -d $dir ] ; then
        InstallDir=$dir
        break
    fi
done

if [ -z "$InstallDir" ] ; then
    Python=`type -P python`
    if [ -n "$Python" ] ; then
        if `python <<EOT
import sys
# If any python 3, or Python2 >= 2.7.9, or virtual env, then use it
major = int(sys.version_info[0])
minor = int(sys.version_info[1])
patch = int(sys.version_info[2])
if major > 2 or ((minor > 6 and patch > 9) or hasattr(sys,'real_prefix')):
    sys.exit(0)
sys.exit(1)
EOT` ;                          then
            InstallDir=`dirname $Python`
            InstallDir=`dirname $InstallDir`
        fi
    fi
fi

if [ -z "$InstallDir" ] ; then
    echo "Error: could not find an appropriate python installation to use" >&2
    exit 1
fi

echo "$InstallDir"
exit 0
