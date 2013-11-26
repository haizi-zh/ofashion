#!/usr/bin/env python
# coding=utf-8

from Queue import Queue, Empty
import csv
import hashlib
import json
import logging
import codecs
import os
import _mysql
import re
import shutil
from threading import Thread
import time
import urllib
import urllib2
import pydevd
import global_settings as glob
import common as cm
from products import utils
from scripts import dbman
from scripts.sync_product import SyncProducts
from scripts.dbman import ProcessTags
import core

__author__ = 'Zephyre'

import sys
from PIL import Image

cmd_list = ('help', 'sandbox', 'resize', 'image_check', 'editor_price', 'import_tag', 'process_tags', 'sync')
ext_list = ('.jpg', '.jpeg', '.tif', '.tiff', '.png', 'bmp')
verbose = False
force_overwrite = False

debug_flag = False
debug_port = glob.DEBUG_PORT

logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
logger = logging.getLogger()


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def mstore_help():
    print str.format('Available commands are: {0}', ', '.join(cmd_list))


def mstore_error():
    default_error()


def to_sql(val):
    return unicode(val).replace('\\', '\\\\').replace('"', '\\"') if val else ''


def import_tag_mapping(args):
    idx = 0
    map_file = None
    region = 'cn'
    brand_id = None
    db_spec = glob.SPIDER_SPEC
    while True:
        if idx >= len(args):
            break
        hdr = args[idx]
        idx += 1
        if hdr == '-f':
            map_file = args[idx]
            idx += 1
        elif hdr == '-r':
            region = args[idx]
            idx += 1
        elif hdr == '--brand':
            brand_id = int(args[idx])
            idx += 1
        else:
            logger.critical('Invalid syntax.')
            return

    if not map_file or not brand_id:
        logger.critical('Invalid syntax.')
        return

    data = []
    with open(map_file, 'r') as f:
        rdr = csv.reader(f)
        for row in rdr:
            if row[0][:3] == codecs.BOM_UTF8:
                row[0] = row[0][3:]
            data.append([cm.unicodify(val) for val in row])

    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")
    db.query('START TRANSACTION')

    for rule in data:
        tag_name = rule[0]
        tag_text = rule[1]
        mapping_list = list(set(filter(lambda x: x, rule[1:])))
        m_val = json.dumps(mapping_list, ensure_ascii=False)
        db.query(
            unicode.format(u'SELECT * FROM products_tag_mapping WHERE brand_id={0} && region="{1}" && tag_name="{2}"',
                           brand_id, region, tag_name).encode('utf-8'))
        rs = db.store_result()

        if rs.num_rows() > 0:
            pid = rs.fetch_row(how=1)[0]['idmappings']
            db.query(
                unicode.format(u'UPDATE products_tag_mapping SET mapping_list="{0}" WHERE idmappings={1}',
                               to_sql(m_val), pid).encode('utf-8'))
        else:
            db.query(unicode.format(u'INSERT INTO products_tag_mapping (brand_id, region, tag_name, '
                                    u'tag_text, mapping_list) VALUES ({0}, "{1}", "{2}", "{3}", "{4}")',
                                    brand_id, to_sql(region), to_sql(tag_name), to_sql(tag_text),
                                    to_sql(m_val)).encode('utf-8'))

    db.query('SELECT * FROM products_tag_mapping WHERE mapping_list IS NULL')
    rs = db.store_result()
    for i in xrange(rs.num_rows()):
        record = rs.fetch_row(how=1)[0]
        tag_text = cm.unicodify(record['tag_text'])
        pid = record['idmappings']
        m_val = json.dumps([tag_text] if tag_text else [], ensure_ascii=False)
        db.query(
            unicode.format(u'UPDATE products_tag_mapping SET mapping_list="{0}" WHERE idmappings={1}', to_sql(m_val),
                           pid).encode('utf-8'))

    db.query('COMMIT')

    db.close()


def process_tags(args):
    last_update = None
    extra_cond = None

    if 'last-update' in args:
        last_update = args['last-update'][0]
    if 'cond' in args:
        extra_cond = args['cond']

    core.func_carrier(ProcessTags(last_update, extra_cond), 0.3)


def editor_price_processor(args):
    """
    处理editor的价格信息
    """
    hdr = args[0]
    if hdr == '--brand':
        brand_id = args[1]
    else:
        print 'Invalid syntax.'
        return

    db_spec = glob.EDITOR_SPEC
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")
    db.query('START TRANSACTION')

    db.query(str.format('SELECT idproducts, price, region FROM products WHERE brand_id={0}', brand_id))
    rs = db.store_result()
    tot = rs.num_rows()
    for i in xrange(rs.num_rows()):
        if i % 100 == 0:
            print str.format('PROCESSING {0}/{1}({2:.2%})', i, tot, float(i) / tot)

        record = rs.fetch_row(how=1)[0]
        pid = record['idproducts']
        price_body = record['price']
        region = record['region']
        if not price_body:
            continue

        ret = cm.process_price(price_body, region)

        # 转换后的category
        clause = unicode.format(u'price_rev={0}, currency_rev="{1}"', ret['price'], ret['currency'])
        db.query(unicode.format(u'UPDATE products SET {0} WHERE idproducts={1}', clause, pid).encode('utf-8'))

    db.query('COMMIT')
    db.close()
    pass


def resize(args):
    global verbose, force_overwrite
    arg_map = {'width': None, 'height': None, 'method': Image.BICUBIC, 'longest': None, 'ext': '.jpg'}
    idx = 0
    while idx < len(args):
        hdr = args[idx]
        idx += 1
        if hdr == '-w':
            param = args[idx]
            idx += 1
            arg_map['width'] = int(param)
        elif hdr == '-h':
            param = args[idx]
            idx += 1
            arg_map['height'] = int(param)
        elif hdr == '-l':
            param = args[idx]
            idx += 1
            arg_map['longest'] = int(param)
        elif hdr == '-m':
            param = args[idx]
            idx += 1
            if param.lower() == 'nearest':
                arg_map['method'] = Image.NEAREST
            elif param.lower() == 'BILINEAR':
                arg_map['method'] = Image.BILINEAR
            elif param.lower() == 'BICUBIC':
                arg_map['method'] = Image.BICUBIC
            elif param.lower() == 'ANTIALIAS':
                arg_map['method'] = Image.ANTIALIAS
            else:
                print str.format('Unknown interpolation algorithm: {0}', param)
                return
        elif hdr == '-p':
            param = args[idx]
            idx += 1
            arg_map['path'] = param
        elif hdr == '-o':
            param = args[idx]
            idx += 1
            arg_map['outpath'] = param
        elif hdr == '--prefix' or hdr == '--postfix':
            param = args[idx]
            idx += 1
            if hdr == '--prefix':
                arg_map['prefix'] = param
            else:
                arg_map['postfix'] = param
        elif hdr == '-e':
            param = args[idx]
            idx += 1
            if param in ext_list:
                arg_map['ext'] = param
            else:
                print str.format('Invalid extension name: {0}', param)
                return
        elif hdr == '-v':
            verbose = True
        elif hdr == '-f':
            force_overwrite = True

    if 'path' not in arg_map:
        print 'No source path specified.'
        return

    # If the specified path is a file or a directory
    path = arg_map['path']
    if os.path.isfile(path):
        arg_map['path'] = (os.path.normpath(path),)
    elif os.path.isdir(path):
        base = path
        arg_map['path'] = [os.path.normpath(os.path.join(base, fname)) for fname in
                           filter(lambda x: os.path.splitext(x)[-1] in ext_list, os.listdir(base))]
    else:
        print str.format('Invalid path specified: {0}', path)
        return

    if 'outpath' not in arg_map:
        print 'No output path specified.'
        return
    if not os.path.isdir(arg_map['outpath']):
        print str.format('Invalid output path: {0}', arg_map['outpath'])
        return

    w, h, l, m = arg_map['width'], arg_map['height'], arg_map['longest'], arg_map['method']
    if not w and not h and not l:
        print 'No dimension specified!'
        return

    tot = len(arg_map['path'])
    cnt = 0
    for path in arg_map['path']:
        cnt += 1
        if verbose:
            print str.format('Processing {0} out of {1}: {2}', cnt, tot, path)

        try:
            img = Image.open(path)
            if verbose:
                print str.format('Original: width={0}, height={1}, format={2}, mode={3}', img.size[0], img.size[1],
                                 img.format, img.mode)

            prefix = arg_map['prefix'] if 'prefix' in arg_map and arg_map['prefix'] else ''
            postfix = arg_map['postfix'] if 'postfix' in arg_map and arg_map['postfix'] else ''
            basename = os.path.splitext(os.path.split(path)[1])[0]
            filename = os.path.normpath(
                os.path.join(arg_map['outpath'], str.format('{0}{1}{2}{3}', prefix, basename, postfix, arg_map['ext'])))
            if os.path.isdir(filename):
                print str.format('ERROR! {0} is a directory.', filename)
            elif os.path.isfile(filename) and not force_overwrite:
                ret = raw_input(
                    str.format('WARNING! {0} already exists, press Y to proceed, others to skip:\t', filename))
                if ret != 'Y':
                    continue

            sz = img.size
            ratio = float(sz[0]) / sz[1]
            if w and h:
                pass
            elif not w and not h:
                if ratio >= 1:
                    # width >= height
                    w = l
                    h = w / ratio
                else:
                    # width < height
                    h = l
                    w = l * ratio
                    # img.resize((w,h),resample=m)
            elif w:
                h = w / ratio
            elif h:
                w = l * ratio

            w, h = int(w), int(h)
            img.resize((w, h), resample=m).save(filename)
        except IOError:
            print str.format('Cannot process {0}', path)

    print 'Done.'


class ImageDownloader(object):
    def __init__(self):
        self.queue = Queue(8)
        self.run_flag = False
        self.t = None
        self.timeout = 2
        self.downloaded = {}

    def download(self, url, path, callback=None):
        buf = None
        if url in self.downloaded:
            try:
                with open(self.downloaded[url], 'rb') as f:
                    buf = f.read()
            except IOError:
                pass

        if not buf:
            self.queue.put({'url': url, 'path': path, 'callback': callback})
        elif callback:
            try:
                callback(buf, url, self.downloaded[url])
            except:
                # 如果未成功，则从已完成列表中移除
                self.downloaded.pop(url)
                print sys.exc_info()

    def run(self):
        self.run_flag = True
        self.t = Thread(target=self.func)
        self.t.start()

    def func(self):
        while self.run_flag:
            try:
                data = self.queue.get(block=True, timeout=self.timeout)
            except Empty:
                continue

            response = utils.fetch_image(data['url'])
            callback = data['callback']
            if not response:
                raise ValueError
            if response['status'] != 200:
                raise urllib2.HTTPError(data['url'], response['status'], str.format('{0}', data['url']),
                                        response['header'], None)

            if callback:
                try:
                    callback(response['body'], response['url'], data['path'])
                    self.downloaded[data['url']] = data['path']
                except:
                    print sys.exc_info()

        self.run_flag = False
        self.t = None

    def stop(self):
        self.run_flag = False
        self.t.join()
        self.t = None


def sync(args):
    idx = 0
    cond = []
    src_spec = glob.SPIDER_SPEC
    dst_spec = glob.EDITOR_SPEC
    db_map = {'tmp': glob.TMP_SPEC, 'spider': glob.SPIDER_SPEC, 'editor': glob.EDITOR_SPEC,
              'release': glob.RELEASE_SPEC}
    while True:
        if idx >= len(args):
            break
        hdr = args[idx]
        idx += 1
        if hdr == '--cond':
            cond.append(args[idx])
            idx += 1
        elif hdr == '--src':
            tmp = args[idx]
            idx += 1
            src_spec = db_map[tmp]
        elif hdr == '--dst':
            tmp = args[idx]
            idx += 1
            dst_spec = db_map[tmp]
        elif hdr == '-D':
            pydevd.settrace('localhost', port=debug_port, stdoutToServer=True, stderrToServer=True)
        else:
            print str.format('INVALID PARAMETER: {0}', hdr)
            return

    core.func_carrier(SyncProducts(src_spec=src_spec, dst_spec=dst_spec, cond=cond), 1)


class ImageCheck(object):
    def __init__(self, db_spec, gen_checksum, refetch, image_validity, cond=None, update=False):
        self.gen_checksum = gen_checksum
        self.refetch = refetch
        self.image_validity = image_validity
        self.update_flag = update

        self.missing = 0
        self.dim_mismatch = 0
        self.size_mismatch = 0
        self.format_mismatch = 0
        self.checksum_mismatch = 0
        self.path_error = 0

        self.db = core.MySqlDb()
        self.db.conn(db_spec)

        self.progress = 0
        self.tot = 1
        if cond:
            if cm.iterable(cond):
                self.cond = cond
            else:
                self.cond = [cond]
        else:
            self.cond = ['1']

    def get_msg(self):
        summary = str.format(
            'Summary: {0} images, {1} missing, {2} resolution mismatch, {3} size mismatch, {4} checksum failed, '
            '{5} url/path mismatch',
            self.tot, self.missing, self.dim_mismatch, self.size_mismatch, self.checksum_mismatch, self.path_error)
        if self.tot > 0:
            return str.format('{0}/{1}({2:.1%}) PROCESSED {3}', self.progress, self.tot,
                              float(self.progress) / self.tot, summary)
        else:
            return str.format('{0}/{1} PROCESSED {2}', self.progress, self.tot, summary)

    def update_db(self, entry):
        old_checksum = entry.pop('old_checksum')
        new_checksum = entry.pop('new_checksum')

        self.db.start_transaction()
        try:
            if old_checksum != new_checksum:
                if self.db.query(
                        str.format('SELECT * FROM images_store WHERE checksum="{0}"', new_checksum)).num_rows() == 0:
                    record = {k: cm.unicodify(entry[k]) for k in entry}
                    record['checksum'] = new_checksum
                    self.db.update(record, 'images_store', str.format('checksum="{0}"', old_checksum))
                else:
                    record = {k: cm.unicodify(entry[k]) for k in entry}
                    if record:
                        self.db.update(record, 'images_store', str.format('checksum="{0}"', new_checksum))
                    self.db.update({'checksum': new_checksum}, 'products_image',
                                   str.format('checksum="{0}"', old_checksum))
                    self.db.execute(str.format('DELETE FROM images_store WHERE checksum="{0}"', old_checksum))
            else:
                record = {k: cm.unicodify(entry[k]) for k in entry}
                self.db.update(record, 'images_store', str.format('checksum="{0}"', old_checksum))
            self.db.commit()
        except:
            self.db.rollback()
            raise

    def run(self):
        downloader = ImageDownloader()
        downloader.run()

        storage_path = os.path.normpath(os.path.join(glob.STORAGE_PATH, 'products/images'))
        rs = self.db.query(str.format('SELECT DISTINCT p1.checksum,p1.width,p1.height,p1.format,p1.size,p1.url,p1.path,'
                                      'p2.brand_id,p2.model FROM images_store AS p1 '
                                      'JOIN products_image AS p2 ON p1.checksum=p2.checksum WHERE {0}',
                                      ' AND '.join(self.cond)))
        self.tot = rs.num_rows()
        self.progress = 0

        while True:
            tmp = rs.fetch_row(how=1)
            if not tmp:
                break
            record = tmp[0]
            self.progress += 1

            hash_url = hashlib.sha1(record['url']).hexdigest()
            if hash_url != os.path.splitext(os.path.split(record['path'])[-1])[0]:
                logger.error(str.format('Url/hash mismatch: {0} => {1}', record['url'], hash_url))
                self.path_error += 1
            full_path = os.path.normpath(os.path.join(storage_path, record['path']))
            update_entry = {}
            try:
                if self.image_validity:
                    img = Image.open(full_path)
                    img.crop((0, 0, 16, 16))
                    if not record['width'] or int(record['width']) != \
                            img.size[0] or not record['height'] or int(record['height']) != img.size[1]:
                        self.dim_mismatch += 1
                        update_entry['width'] = img.size[0]
                        update_entry['height'] = img.size[1]

                    if not record['format'] or record['format'] != img.format:
                        self.format_mismatch += 1
                        update_entry['format'] = img.format

                file_size = os.path.getsize(full_path)
                if not record['size'] or int(record['size']) != file_size:
                    self.size_mismatch += 1
                    update_entry['size'] = file_size

                if self.gen_checksum:
                    with open(full_path, 'rb') as f:
                        checksum = hashlib.md5(f.read()).hexdigest()
                    if record['checksum'] != checksum:
                        self.checksum_mismatch += 1
                        update_entry['old_checksum'] = record['checksum']
                        update_entry['new_checksum'] = checksum

                # 有不一致的地方，需要更新
                if update_entry and self.update_flag:
                    if 'old_checksum' not in update_entry:
                        update_entry['old_checksum'] = record['checksum']
                        update_entry['new_checksum'] = record['checksum']
                    self.update_db(update_entry)
            except IOError:
                # 下载图像以后的回调函数
                def func(body, url, full_path):
                    path, tmp = os.path.split(full_path)
                    base_name, ext = os.path.split(tmp)

                    # 检查是否为有效图像文件
                    tmp_name = str.format('tmp_{0}.{1}', checksum, ext)
                    with open(tmp_name, 'wb') as f:
                        f.write(body)
                    img = Image.open(tmp_name)
                    img.crop((0, 0, 32, 32))
                    shutil.move(tmp_name, full_path)

                    md5 = hashlib.md5()
                    md5.update(body)
                    new_checksum = md5.hexdigest()

                    self.update_db({'old_checksum': record['checksum'], 'new_checksum': new_checksum, 'res': img.size,
                                    'format': img.format, 'size': len(body), 'url': url, 'full_path': full_path})

                self.missing += 1
                logger.error(str.format('{0} / {1} missing!', record['model'], record['path']))
                if self.refetch:
                    downloader.download(record['url'], full_path, func)

        logger.info(self.get_msg())
        downloader.stop()


def release(param_dict):
    for brand in param_dict['brand']:
        core.func_carrier(dbman.PublishRelease(brand), 1)


def image_check(param_dict):
    """
    检查图片是否正常。
    参数：
    --checksum：同时加入图片的MD5校验
    --brand：指定
    :param param_dict:
    :return:
    """
    db_spec = glob.EDITOR_SPEC
    db_map = {'tmp': glob.TMP_SPEC, 'spider': glob.SPIDER_SPEC, 'editor': glob.EDITOR_SPEC,
              'release': glob.RELEASE_SPEC}
    cond = ['1']
    gen_checksum = False
    refetch = False
    image_validity = False

    for param_name, param_value in param_dict.items():
        if param_name == 'checksum':
            gen_checksum = True
        elif param_name == 'refetch':
            refetch = True
        elif param_name == 'db':
            db_spec = db_map[param_value[0]]
        elif param_name == 'cond':
            cond = param_value
        elif param_name == 'image-validity':
            image_validity = True
        else:
            logger.critical(str.format('Invalid syntax. Unknow parameter: {0}', param_name))
            return

    core.func_carrier(ImageCheck(db_spec=db_spec, gen_checksum=gen_checksum, refetch=refetch,
                                 image_validity=image_validity, cond=cond), 1)


def argument_parser(args):
    if len(args) < 2:
        return mstore_error

    cmd = args[1]

    # 解析命令行参数
    param_dict = {}
    q = Queue()
    for tmp in args[2:]:
        q.put(tmp)
    param_name = None
    param_value = None
    while not q.empty():
        tmp = q.get()
        if re.search(r'--(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            if param_name:
                param_dict[param_name] = param_value

            param_name = tmp
            param_value = None
        elif re.search(r'-(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            if param_name:
                param_dict[param_name] = param_value

            for tmp in list(tmp):
                param_dict[tmp] = None
            param_name = None
            param_value = None
        else:
            if param_name:
                if param_value:
                    param_value.append(tmp)
                else:
                    param_value = [tmp]
    if param_name:
        param_dict[param_name] = param_value

    if 'debug' in param_dict or 'D' in param_dict:
        if 'debug-port' in param_dict:
            port = int(param_dict['debug-port'][0])
        else:
            port = glob.DEBUG_PORT
        pydevd.settrace('localhost', port=port, stdoutToServer=True, stderrToServer=True)

    for k in ('debug', 'D', 'debug-port'):
        try:
            param_dict.pop(k)
        except KeyError:
            pass

    if cmd == 'help':
        return mstore_help
    elif cmd == 'resize':
        return lambda: resize(param_dict)
    elif cmd == 'editor_price':
        return lambda: editor_price_processor(param_dict)
    elif cmd == 'image-check':
        return lambda: image_check(param_dict)
    elif cmd == 'import_tag':
        return lambda: import_tag_mapping(param_dict)
    elif cmd == 'process-tags':
        return lambda: process_tags(param_dict)
    elif cmd == 'sync':
        return lambda: sync(param_dict)
    elif cmd == 'release':
        return lambda: release(param_dict)
    else:
        return mstore_error


if __name__ == "__main__":
    argument_parser(sys.argv)()
