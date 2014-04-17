import hashlib
from cStringIO import StringIO
import re
import urlparse

from PIL import Image
from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.contrib.pipeline.media import MediaPipeline

from scrapy.utils.misc import md5sum
from scrapy.http import Request
from scrapy.exceptions import DropItem, NotConfigured
#TODO: from scrapy.contrib.pipeline.media import MediaPipeline
from scrapy.contrib.pipeline.files import FileException, FilesPipeline, FSFilesStore, S3FilesStore, os
import upyun
from twisted.internet import defer, threads


class UPYUNFilesStore(object):
    UP_BUCKETNAME = None
    UP_USERNAME = None
    UP_PASSWORD = None

    # headers = {"x-gmkerl-rotate": "360"}

    def __init__(self, uri):
        assert uri.startswith('up://')
        info, self.dirpath = uri[5:].split('/', 1)
        self.UP_USERNAME, self.UP_PASSWORD, self.UP_BUCKETNAME = re.split('[:@]', info)

    def stat_file(self, path, info):
        def _onsuccess(checksum_info):
            checksum, modified_stamp = checksum_info
            return {'checksum': checksum, 'last_modified': modified_stamp}

        return self._get_info(path).addCallback(_onsuccess)

    def access_mysql_info(self, path):
        # with RoseVisionDb({"host": "localhost", "port": 3306, "schema": "celery", "username": "root", "password": "rose123"}) as db:
        #     rs = self.db.query_match(['path', 'checksum'], 'checksum_info', {'path': path})
        # kv = rs[0]
        # return {'checksum': checksum, 'last_modified': last_modified}
        return {''}

    def _get_info(self, path):
        """get info from MySQL"""
        return threads.deferToThread(self.access_mysql_info, path)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to UPYUN storage"""
        up = upyun.UpYun(self.UP_BUCKETNAME, self.UP_USERNAME, self.UP_PASSWORD, timeout=30,
                         endpoint=upyun.ED_AUTO)
        # headers = self.headers
        full_file = os.path.join(self.dirpath,path)
        return threads.deferToThread(up.put, '/'+full_file, buf.getvalue(), checksum=False)
        # return up.put('/'+full_file, buf.getvalue(), checksum=False,)


class MFImagesPipeline(ImagesPipeline):
    ImagesPipeline.STORE_SCHEMES['up'] = UPYUNFilesStore

    def __init__(self, store_uri, download_func=None):
        # if not store_uri:
        #     raise NotConfigured
        # self.store = self._get_store(store_uri)
        super(MFImagesPipeline, self).__init__(store_uri, download_func=download_func)

    @classmethod
    def from_settings(cls, settings):
        cls.MIN_WIDTH = settings.getint('IMAGES_MIN_WIDTH', 0)
        cls.MIN_HEIGHT = settings.getint('IMAGES_MIN_HEIGHT', 0)
        cls.EXPIRES = settings.getint('IMAGES_EXPIRES', 90)
        cls.THUMBS = settings.get('IMAGES_THUMBS', {})
        s3store = cls.STORE_SCHEMES['s3']
        s3store.AWS_ACCESS_KEY_ID = settings['AWS_ACCESS_KEY_ID']
        s3store.AWS_SECRET_ACCESS_KEY = settings['AWS_SECRET_ACCESS_KEY']

        upstore = cls.STORE_SCHEMES['up']

        cls.IMAGES_URLS_FIELD = settings.get('IMAGES_URLS_FIELD', cls.DEFAULT_FILES_URLS_FIELD)
        cls.IMAGES_RESULT_FIELD = settings.get('IMAGES_RESULT_FIELD', cls.DEFAULT_FILES_RESULT_FIELD)
        store_uri = settings['IMAGES_STORE']

        return cls(store_uri)

    def _get_store(self, uri):
        if os.path.isabs(uri):  # to support win32 paths like: C:\\some\dir
            scheme = 'file'
        else:
            scheme = urlparse.urlparse(uri).scheme
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(uri)

