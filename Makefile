install: 
ifdef GW_LOCATION
	@cp bin/* $(GW_LOCATION)/bin/
	@cp etc/* $(GW_LOCATION)/etc/
	@cp -rf libexec/* $(GW_LOCATION)/libexec/
	@chmod a+x $(GW_LOCATION)/bin/gw_im_mad_drm4g.py
	@chmod a+x $(GW_LOCATION)/bin/gw_tm_mad_drm4g.py
	@chmod a+x $(GW_LOCATION)/bin/gw_em_mad_drm4g.py
	@echo "DRM4G MAD successfully installed"
else
	@echo "You must define the GW_LOCATION env variable"
	exit 2
endif

