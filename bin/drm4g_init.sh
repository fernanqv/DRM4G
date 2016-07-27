DRM4G_DEPLOYMENT_BIN=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DRM4G_DEPLOYMENT_DIR=$( dirname $DRM4G_DEPLOYMENT_BIN )
if [[ ":$PATH:" != *":$DRM4G_DEPLOYMENT_BIN:"* ]]; then export PATH=$DRM4G_DEPLOYMENT_BIN:${PATH}; fi
if [[ ":$PYTHONPATH:" != *":$DRM4G_DEPLOYMENT_DIR/libexec:"* ]]; then export PYTHONPATH=$DRM4G_DEPLOYMENT_DIR/libexec:${PYTHONPATH}; fi
source ${DRM4G_DEPLOYMENT_BIN}/drm4g_autocomplete.sh
