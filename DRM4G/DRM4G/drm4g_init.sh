DRM4G_BIN=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [[ ":$PATH:" != *":$DRM4G_BIN:"* ]]; then export PATH=${PATH}:$DRM4G_BIN; fi
source ${DRM4G_BIN}/drm4g_autocomplete.sh
