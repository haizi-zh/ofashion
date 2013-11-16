# coding=utf-8
from Queue import Queue, Empty
import csv
import hashlib
import json
import logging
import codecs
import os
import _mysql
from threading import Thread
import time
import pydevd
import global_settings as glob
import common as cm
from products import utils
from scripts.sync_product import SyncProducts
import core

__author__ = 'Zephyre'

import sys
from PIL import Image

cmd_list = ('help', 'sandbox', 'resize', 'image_check', 'editor_price', 'import_tag', 'process_tags')
ext_list = ('.jpg', '.jpeg', '.tif', '.tiff', '.png', 'bmp')
verbose = False
force_overwrite = False

debug_flag = False
debug_port = glob.DEBUG_PORT

logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
logger = logging.getLogger()


def sand_box():
    """
    For test use.
    """
    for i in xrange(5):
        time.sleep(1)
        sys.stdout.write(str.format('{0}\r', i))
        sys.stdout.flush()

    sys.stdout.write('Done')
    pass


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


@static_var("elapsed_counter", 0)
@static_var("start_ts", time.time())
@static_var("interval", 30)
def log_indicator(reset=False, interval=None):
    if interval:
        log_indicator.interval = interval

    if reset:
        log_indicator.start_ts = time.time()
        log_indicator.elapsed_counter = 0
        return False
    else:
        val = int((time.time() - log_indicator.start_ts) / log_indicator.interval)
        if val > log_indicator.elapsed_counter:
            log_indicator.elapsed_counter = val
            return True
        else:
            return False


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
    idx = 0
    region = 'cn'
    brand_id = None
    db_spec = glob.EDITOR_SPEC
    db_spider = glob.SPIDER_SPEC
    while True:
        if idx >= len(args):
            break
        hdr = args[idx]
        idx += 1
        if hdr == '-D':
            pydevd.settrace('localhost', port=debug_port, stdoutToServer=True, stderrToServer=True)
        elif hdr == '-r':
            region = args[idx]
            idx += 1
        elif hdr == '--brand':
            brand_id = int(args[idx])
            idx += 1
        else:
            logger.critical('Invalid syntax.')
            return

    if not brand_id:
        logger.critical('Invalid syntax.')
        return

    db = _mysql.connect(host=db_spider['host'], port=db_spider['port'], user=db_spider['username'],
                        passwd=db_spider['password'], db=db_spider['schema'])
    db.query("SET NAMES 'utf8'")
    db.query(str.format('SELECT tag_name,mapping_list FROM products_tag_mapping WHERE brand_id={0} && region="{1}"',
                        brand_id, region))
    temp = db.store_result().fetch_row(maxrows=0)
    mapping_rules = dict((cm.unicodify(val[0]), cm.unicodify(val[1])) for val in temp)
    db.close()

    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")

    db.query(str.format('SELECT idproducts,extra FROM products WHERE brand_id={0} && region="{1}"', brand_id, region))
    rs = db.store_result()

    db.query('START TRANSACTION')
    for i in xrange(rs.num_rows()):
        record = rs.fetch_row(how=1)[0]
        extra = json.loads(record['extra'])
        tags = []
        for k in extra:
            if isinstance(extra[k], list):
                tags.extend(extra[k])
            else:
                tags.append(extra[k])
                extra[k] = [extra[k]]

        tags = set(tags)
        tag_names = []
        for v in tags:
            if v in mapping_rules and mapping_rules[v]:
                tag_names.extend(json.loads(mapping_rules[v]))
        tag_names = list(set(tag_names))

        db.query(unicode.format(u'UPDATE products SET tags="{0}",extra="{2}" WHERE idproducts={1}',
                                to_sql(json.dumps(tag_names, ensure_ascii=False)),
                                record['idproducts'],
                                to_sql(json.dumps(extra, ensure_ascii=False))
        ).encode('utf-8'))

    db.query('COMMIT')
    db.close()


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
        self.path_list = set([])

    def download(self, url, path):
        if path not in self.path_list:
            self.queue.put({'url': url, 'path': path})
            self.path_list.add(path)

    def run(self):
        self.run_flag = True
        self.t = Thread(target=self.func)
        self.t.start()

    def func(self):
        while self.run_flag:
            try:
                data = self.queue.get(block=True, timeout=self.timeout)
                response = utils.fetch_image(data['url'])
                if response is None or len(response['body']) == 0:
                    continue

                # 写入图片文件
                with open(data['path'], 'wb') as f:
                    f.write(response['body'])
            except Empty:
                pass
        self.run_flag = False
        self.t = None

    def stop(self):
        self.run_flag = False
        self.t.join()
        self.t = None


def sync_editor(args):
    obj = SyncProducts()
    core.func_carrier(obj, 1)


def image_check(args):
    """
    检查图片是否正常。
    参数：
    --checksum：同时加入图片的MD5校验
    --brand：指定
    :param args:
    :return:
    """
    idx = 0
    brand_id = None
    db_spec = glob.SPIDER_SPEC
    gen_checksum = False
    refetch = False
    while True:
        if idx >= len(args):
            break
        hdr = args[idx]
        idx += 1
        if hdr == '--brand':
            brand_id = int(args[idx])
            idx += 1
        elif hdr == '-D':
            pydevd.settrace('localhost', port=debug_port, stdoutToServer=True, stderrToServer=True)
        elif hdr == '--checksum':
            gen_checksum = True
        elif hdr == '--refetch':
            refetch = True
        elif hdr == '-d':
            database_name = args[idx]
            idx += 1
            if database_name == 'spider':
                db_spec = glob.SPIDER_SPEC
            elif database_name == 'editor':
                db_spec = glob.EDITOR_SPEC
            elif database_name == 'release':
                db_spec = glob.RELEASE_SPEC
            else:
                logger.critical(str.format('Invalid database specifier: {0}', database_name))
                return
        else:
            logger.critical('Invalid syntax.')
            return

    downloader = ImageDownloader()
    downloader.run()

    storage_path = glob.STORAGE_PATH
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")

    if brand_id:
        db.query(str.format('SELECT DISTINCT model FROM products WHERE brand_id={0}', brand_id))
    else:
        db.query(str.format('SELECT DISTINCT model FROM products'))
    rs = db.store_result()
    model_list = rs.fetch_row(maxrows=0, how=1)
    tot = len(model_list)
    cnt = 0
    missing = 0
    mismatch = 0
    logger.info(str.format('Total models: {0}', tot))
    log_indicator(reset=True, interval=1)
    model_cnt = -1

    for model in [val['model'] for val in model_list]:
        model_cnt += 1
        db.query(
            str.format('SELECT idproducts_image,path,width,height,url FROM products_image WHERE model="{0}"', model))
        rs_image = db.store_result().fetch_row(maxrows=0, how=1)
        for image in rs_image:
            # if log_indicator():
            if model_cnt % 50 == 0:
                report_str = str.format('{0} images, {3} models checked({4:.1%}). {1} missing, {2} size mismatch.',
                                        cnt, missing, mismatch, model_cnt, float(model_cnt) / len(model_list))
                logger.info(report_str)
                # sys.stdout.write(report_str + '\r')
                # print report_str

            path = image['path']
            full_path = os.path.join(storage_path, 'products/images', path)

            cki = gen_checksum
            try:
                img = Image.open(full_path)
                w = int(image['width'])
                h = int(image['height'])
                if w != img.size[0] or h != img.size[1]:
                    logger.error(str.format('{0} / {1} size mismatch!', model, path))
                    mismatch += 1

                    db.query(str.format('UPDATE products_image SET width={0}, height={1} WHERE idproducts_image={2}',
                                        img.size[0], img.size[1], image['idproducts_image']))

            except IOError:
                missing += 1
                logger.error(str.format('{0} / {1} missing!', model, path))
                if refetch:
                    downloader.download(image['url'], full_path)
                    cki = True
            finally:
                cnt += 1

            if cki:
                md5 = hashlib.md5()
                try:
                    with open(full_path, 'rb') as f:
                        md5.update(f.read())
                    db.query(
                        str.format('UPDATE products_image SET checksum="{0}" WHERE idproducts_image={1}',
                                   md5.hexdigest(),
                                   image['idproducts_image']))
                except IOError:
                    pass

    logger.info(
        str.format('Summary: {0} images, {3} models checked. {1} missing, {2} size mismatch.', cnt, missing, mismatch,
                   model_cnt))
    downloader.stop()


def argument_parser(args):
    if len(args) < 2:
        return mstore_error

    cmd = args[1]
    if cmd not in cmd_list:
        return mstore_error

    if cmd == 'help':
        return mstore_help
    elif cmd == 'resize':
        return lambda: resize(args[2:])
    elif cmd == 'editor_price':
        return lambda: editor_price_processor(args[2:])
    elif cmd == 'sandbox':
        return sand_box
    elif cmd == 'image_check':
        return lambda: image_check(args[2:])
    elif cmd == 'import_tag':
        return lambda: import_tag_mapping(args[2:])
    elif cmd == 'process_tags':
        return lambda: process_tags(args[2:])
    elif cmd == 'sync_editor':
        return lambda: sync_editor(args[2:])

    pass


if __name__ == "__main__":
    argument_parser(sys.argv)()