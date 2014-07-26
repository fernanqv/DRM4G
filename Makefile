install: 
ifdef GW_LOCATION
	@cp gw_im_mad_drm4g.py $(GW_LOCATION)/bin/
	@cp gw_em_mad_drm4g.py $(GW_LOCATION)/bin/
	@cp gw_tm_mad_drm4g.py $(GW_LOCATION)/bin/
	@cp drm4g_env.py $(GW_LOCATION)/bin/
	@cp drm4g.py $(GW_LOCATION)/bin/drm4g
	@cp -rf drm4g $(GW_LOCATION)/libexec/
	@cp gw_drm4g_wrapper.sh $(GW_LOCATION)/etc/
	@cp logger.conf $(GW_LOCATION)/etc/
	@cp resources.conf $(GW_LOCATION)/etc/
	@chmod a+x $(GW_LOCATION)/bin/gw_im_mad_drm4g.py
	@chmod a+x $(GW_LOCATION)/bin/gw_tm_mad_drm4g.py
	@chmod a+x $(GW_LOCATION)/bin/gw_em_mad_drm4g.py
	@chmod a+x $(GW_LOCATION)/bin/drm4g
	@find $(GW_LOCATION) -depth -name .svn -exec rm -rf '{}' \;
	@echo "DRM4G MAD successfully installed"
else
	@echo "You must define the GW_LOCATION env variable"
	@exit 2
endif

