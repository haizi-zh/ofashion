import logging
import os
import sys
import datetime
import errno

__author__ = 'Zephyre'


class MStoreFileHandler(logging.FileHandler):
    def make_sure_path_exists(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    def __init__(self, path=u'.', filename=None, mode='a'):
        if not filename:
            filename = '.'.join(os.path.basename(sys.modules['__main__'].__file__).split('.')[:-1]).decode('utf-8')
        filename = unicode.format(u'{0}/{1}_{2}.log', path, filename, datetime.datetime.now().strftime('%Y%m%d'))
        self.make_sure_path_exists(os.path.split(filename)[0])
        super(MStoreFileHandler, self).__init__(filename.encode('utf-8'), mode, encoding='utf-8')
