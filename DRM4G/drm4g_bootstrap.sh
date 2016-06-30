#!/bin/bash 

#__version__  = '2.4.0'
#__author__   = 'Carlos Blanco'
#__revision__ = "$Id: install.sh 2986 2016-06-21 11:52:59Z carlos $"

BASE_URL="https://meteo.unican.es/work/DRM4G"
PIP_URL="https://bootstrap.pypa.io/get-pip.py"
DRM4G_DEPLOYMENT_DIR=$PWD
FILE_VERSIONS="drm4g_versions"
DRM4G_REQUIREMENTS="paramiko<2"

have_command () {
    type "$1" >/dev/null 2>/dev/null
}

require_command () {
    have_command "$1" 
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "Could not find required command '$1' in system PATH."
        exit 1
    fi
}

require_python () {
    require_command "python"
    # Support 2.5 > python >= 3.3 
    python_version=$(python <<EOF
import sys
print(sys.version_info[0]==2 and sys.version_info[1] > 5 or sys.version_info[0]==3 and sys.version_info[1] >= 3 )
EOF
)

    if [ "$python_version" != "True" ] 
    then
        echo "Wrong version of python is installed" 
        echo "DRM4G requires Python version Python (2.6+, 3.3+)"
        echo "It does not support your version of"
        echo "python: $(python -V 2>&1|sed 's/python//gi')"
        exit 1
    fi
}

download_drm4g () {
    wget -N -nv -q --no-check-certificate $BASE_URL/$DRM4G_BUNDLE
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to download bunble $DRM4G_BUNDLE from $BASE_URL ..."
        exit 1
    fi
}

download_drm4g_versions () {
    wget -N -nv -q --no-check-certificate $BASE_URL/$FILE_VERSIONS
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to download $FILE_VERSIONS from $BASE_URL ..."
        exit 1
    fi
}

download_get_pip () {
    wget -N -nv -q --no-check-certificate $PIP_URL
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to download pip from $PIP_URL ..."
        exit 1
    fi
}

install_drm4g_depencies_get_pip () {
    python get-pip.py -t $DRM4G_DEPLOYMENT_DIR/drm4g/libexec $DRM4G_REQUIREMENTS
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to install DRM4G depencies"
        exit 1
    fi
    rm get-pip.py
}

install_drm4g_depencies_pip () {
    pip -q install -t $DRM4G_DEPLOYMENT_DIR/drm4g/libexec $DRM4G_REQUIREMENTS
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to install DRM4G depencies"
        exit 1
    fi
}

unpack_drm4g() {
    tar xzf $DRM4G_BUNDLE --overwrite -C $DRM4G_DEPLOYMENT_DIR
    rc=$?
    if [ $rc -ne 0 ]
    then
        echo "ERROR: Unable to unpack the bunble $DRM4G_BUNDLE in $DRM4G_DEPLOYMENT_DIR"
        exit 1
    fi
    rm -rf $DRM4G_BUNDLE
}


usage () {
    cat <<EOF
This program installs DRM4G.

usage:
$0 [OPTIONS]

Options:

      -d, --dir DIRECTORY    Install DRM4G into a directory.
                             (Default: $DRM4G_DEPLOYMENT_DIR)

      -V, --version          Version to install.
                             (Default: $DRM4G_VERSION)

      -h, --help             Print this help text.

EOF
}

######### 
while test -n "$1"
do
    case "$1" in
        -d|--dir)
            shift
            DRM4G_DEPLOYMENT_DIR=$1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -V|--version)
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
EOF

# Check wget, bash and python  
require_command bash

require_command wget

require_python

if [ -n $DRM4G_VERSION  ]
then
    echo ""
    echo "--> Checking the last version of DRM4G ..."
    download_drm4g_versions
    DRM4G_VERSION=$(sort $FILE_VERSIONS | tail -1)
fi

echo ""
echo "--> DRM4G version selected: $DRM4G_VERSION"

DRM4G_BUNDLE="drm4g-${DRM4G_VERSION}.tar.gz"
echo ""
echo "--> Downloading $DRM4G_BUNDLE ..."

if [ -f $DRM4G_BUNDLE ]
then
    echo "WARNING: $DRM4G_BUNDLE already exists in directory $DRM4G_DEPLOYMENT_DIR"
    read -p "Are you sure you want to download it? [y/N] " response
    case $response in y|Y|yes|Yes) download_drm4g;; *);; esac
else
    download_drm4g
fi

echo ""
echo "--> Unpacking $DRM4G_BUNDLE in directory $DRM4G_DEPLOYMENT_DIR ..."

if [ -d "$DRM4G_DEPLOYMENT_DIR/drm4g" ]
then
    echo "WARNING: DRM4G already exists"
    read -p "Are you sure you want to install it? [y/N] " response
    case $response in y|Y|yes|Yes) $DRM4G_DEPLOYMENT_DIR/drm4g/bin/drm4g stop 2>/dev/null; unpack_drm4g;; *);; esac
else
    unpack_drm4g
fi

have_command "pip"
pip_rc=$?
if [ ! ${pip_rc} -eq 0 ] 
then
    download_get_pip
fi

echo ""
echo "--> Installing DRM4G python requirements locally ..."
if [ ! ${pip_rc} -eq 0 ]
then
    install_drm4g_depencies_get_pip
else
    install_drm4g_depencies_pip
fi

rm -rf $FILE_VERSIONS 

cat <<EOF
====================================
Installation of DRM4G $DRM4G_VERSION is done!
====================================

In order to work with DRM4G you have to enable its 
environment with the command:

    source $DRM4G_DEPLOYMENT_DIR/drm4g/bin/drm4g_init.sh

You need to run the above command on every new shell you 
open before using DRM4G, but just once per session.

EOF
