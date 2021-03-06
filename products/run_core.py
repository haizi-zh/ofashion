#!/usr/bin/env python
# coding=utf-8
import sys

from products import louis_vuitton
import global_settings as glob


__author__ = 'Zephyre'

if glob.DEBUG_FLAG:
    import pydevd

    pydevd.settrace(host=glob.DEBUG_HOST, port=glob.DEBUG_PORT, stdoutToServer=True, stderrToServer=True)
    print 'REMOTE DEBUG ENABLED'

module = louis_vuitton

module.get_logger().info(unicode.format(u'INITIALIZING {0}...', module.__name__.upper()))

for region in sys.argv[1:]:
    module.main(region)

