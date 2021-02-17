from drm4g.core.im_mad import GwImMad


def test_GwImMad():
    gw_im_mad = GwImMad()
    args = "DISCOVER 0 S30 ARGS"
    gw_im_mad.do_DISCOVER(args)
    gw_im_mad.do_MONITOR(args, output=True)


if __name__ == "__main__":
    test_GwImMad()
