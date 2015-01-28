#!/bin/bash

BASE_URL="https://www.meteo.unican.es/work/DRM4G"
DRM4G_VERSION=2.2.0
DRM4G_DIR_INSTALATION=$PWD
DRM4G_HARDWARE=$(uname -m)

have_command () {
  type "$1" >/dev/null 2>/dev/null
}

require_command () {
  if ! have_command "$1"; then
    echo "Could not find required command '$1' in system PATH. Aborting."
    exit 1
  fi
}

require_python () {
    require_command "$PYTHON"
    # Support 2.5 >= python < 3.0 
    python_version=$($PYTHON <<EOF
import sys
print(sys.version_info[0]==2 and sys.version_info[1] >= 5 )
EOF
)

    if [ "$python_version" != "True" ]; then
        echo "Wrong version of python is installed" 
        echo "DRM4G requires Python version 2.5+"
        echo "It does not support your version of python: $($PYTHON -V 2>&1|sed 's/python//gi')"
    fi
}

usage () {
    cat <<EOF
This program installs DRM4G.

usage:
$0 [OPTIONS]

Options:

      -d, --dir DIRECTORY    Install DRM4G into a directory.
                             (Default: $DRM4G_DIR_INSTALATION)

      -v, --version          Version to install.
                             (Default: $DRM4G_VERSION)

      -h, --help             Print this help text.

EOF
}

while true
do
    case "$1" in
        -d|--dir)
            shift
            DRM4G_DIR_INSTALATION=$1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -v|--version)
            DRM4G_VERSION=$1       
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

cat <<EOF
==========================
DRM4G installation script
==========================

This script installs DRM4G $DRM4G_VERSION in '$DRM4G_DIR_INSTALATION'.

EOF

# Check gcc and python  
require_command gcc

require_python

DRM4G_BUNDLE=drm4g_${DRM4G_HARDWARE}_${DRM4G_VERSION}_tar.gz

# Download command
wget --no-check-certificate -O $DRM4G_BUNDLE $BASE_URL/$DRM4G_BUNDLE 
rc=$?
if [ $rc -ne 0 ]
then
    echo "Unable to download bunble $DRM4G_BUNDLE ..."
    exit 1
fi

echo "Installing DRM4G in directory '$DRM4G_DIR_INSTALATION' ..."
tar xvzf $DRM4G_BUNDLE -C $DRM4G_DIR_INSTALATION
rc=$?
if [ $rc -ne 0 ]
then
    "Unable to unpack the bunble $DRM4G_BUNDLE in '$VENVDIR'"
    exit 1
fi

cat <<EOF
===============================
Installation of DRM4G is done!
===============================

In order to work with DRM4G you have to enable its 
environment with the command:

    . $DRM4G_DIR_INSTALATION/bin/drm4g_init.sh

You need to run the above command on every new shell you 
open before using DRM4G, but just once per session.

EOF

exit 0
