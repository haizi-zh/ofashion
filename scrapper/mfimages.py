import hashlib
from cStringIO import StringIO
import re
import urlparse

from PIL import Image
import datetime
from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.contrib.pipeline.media import MediaPipeline

from scrapy.utils.misc import md5sum
from scrapy.http import Request
from scrapy.exceptions import DropItem, NotConfigured
#TODO: from scrapy.contrib.pipeline.media import MediaPipeline
from scrapy.contrib.pipeline.files import FileException, FilesPipeline, FSFilesStore, S3FilesStore, os
import time
import upyun
from twisted.internet import defer, threads
from scrapy import log
from utils.db import RoseVisionDb
import global_settings as glob


class UPYUNFilesStore(object):
    # UP_BUCKETNAME = None
    # UP_USERNAME = None
    # UP_PASSWORD = None

    # headers = {"x-gmkerl-rotate": "360"}

    def __init__(self, uri):
        assert uri.startswith('up://')
        info, self.dirpath = uri[5:].split('/', 1)
        self.UP_USERNAME, self.UP_PASSWORD, self.UP_BUCKETNAME = re.split('[:@]', info)
        self.db = RoseVisionDb()
        self.db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])

    def stat_file(self, path, info):
        def _onsuccess(info):
            if info:
                (checksum, last_modified, width, height, size, fmt ) = (
                info['checksum'], info['update_time'], info['width'], info['height'], info['size'],info['format'])
                last_modified = time.mktime(datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S').timetuple())
            else:
                checksum = last_modified = width = height = size = fmt = None

            return {'checksum': checksum, 'last_modified': last_modified, 'width': width, 'height': height,
                    'size': size, 'format': fmt}

        return self._get_info(path).addCallback(_onsuccess)

    def access_mysql_info(self, path):
        rs = self.db.query_match(['checksum', 'update_time', 'width', 'height', 'size', 'format'], 'images_store',
                                 {'path': path})
        if rs.num_rows() == 0:
            return None
        else:
            return rs.fetch_row(how=1)[0]

    def _get_info(self, path):
        """get info from MySQL"""
        return threads.deferToThread(self.access_mysql_info, path)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to UPYUN storage"""
        up = upyun.UpYun(self.UP_BUCKETNAME, self.UP_USERNAME, self.UP_PASSWORD, timeout=30,
                         endpoint=upyun.ED_AUTO)
        # headers = self.headers
        full_file = os.path.join(self.dirpath, path)
        return threads.deferToThread(up.put, '/' + full_file, buf.getvalue(), checksum=False)


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

    # def _get_store(self, uri):
    #     if os.path.isabs(uri):  # to support win32 paths like: C:\\some\dir
    #         scheme = 'file'
    #     else:
    #         scheme = urlparse.urlparse(uri).scheme
    #     store_cls = self.STORE_SCHEMES[scheme]
    #     return store_cls(uri)

    def convert_image(self, image, size=None):
        if image.format == 'PNG' and image.mode == 'RGBA':
            background = Image.new('RGBA', image.size, (255, 255, 255))
            background.paste(image, image)
            image = background.convert('RGB')
        elif image.mode != 'RGB':
            image = image.convert('RGB')

        if size:
            image = image.copy()
            image.thumbnail(size, Image.ANTIALIAS)

        buf = StringIO()
        if image.format == 'PNG':
            image.save(buf, 'PNG')
        elif image.format == 'BMP':
            image.save(buf, 'BMP')
        elif image.format == 'GIF':
            image.save(buf, 'GIF')
        elif image.format == 'TIFF':
            image.save(buf, 'TIFF')
        else:
            image.save(buf, 'JPEG')
        return image, buf

    def media_downloaded(self, response, request, info):
        referer = request.headers.get('Referer')

        if response.status != 200:
            log.msg(
                format='File (code: %(status)s): Error downloading image from %(request)s referred in <%(referer)s>',
                level=log.WARNING, spider=info.spider,
                status=response.status, request=request, referer=referer)
            raise FileException('download-error')

        if not response.body:
            log.msg(format='File (empty-content): Empty image from %(request)s referred in <%(referer)s>: no-content',
                    level=log.WARNING, spider=info.spider,
                    request=request, referer=referer)
            raise FileException('empty-content')

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        log.msg(format='File (%(status)s): Downloaded image from %(request)s referred in <%(referer)s>',
                level=log.DEBUG, spider=info.spider,
                status=status, request=request, referer=referer)
        self.inc_stats(info.spider, status)

        try:
            path = self.file_path(request, response=response, info=info)
            checksum = self.file_downloaded(response, request, info)
        except FileException as exc:
            whyfmt = 'File (error): Error processing image from %(request)s referred in <%(referer)s>: %(errormsg)s'
            log.msg(format=whyfmt, level=log.WARNING, spider=info.spider,
                    request=request, referer=referer, errormsg=str(exc))
            raise
        except Exception as exc:
            whyfmt = 'File (unknown-error): Error processing image from %(request)s referred in <%(referer)s>'
            log.err(None, whyfmt % {'request': request, 'referer': referer}, spider=info.spider)
            raise FileException(str(exc))

        orig_image = Image.open(StringIO(response.body))
        width, height = orig_image.size
        fmt = orig_image.format
        size = len(response.body)

        return {'url': request.url, 'path': path, 'checksum': checksum, 'width': width, 'height': height,
                'size': size, 'format': fmt}