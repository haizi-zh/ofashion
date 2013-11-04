# coding=utf-8
import json

import os
import _mysql
import re
import global_settings as glob
import common as cm

__author__ = 'Zephyre'

import sys
import Image

cmd_list = ('help', 'sandbox', 'resize', 'image_check', 'editor_price')
ext_list = ('.jpg', '.jpeg', '.tif', '.tiff', '.png', 'bmp')
verbose = False
force_overwrite = False


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def mstore_help():
    print str.format('Available commands are: {0}', ', '.join(cmd_list))


def mstore_error():
    default_error()


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

        val = cm.unicodify(price_body)
        currency_map = {'cn': 'CNY', 'us': 'USD', 'uk': 'GBP', 'hk': 'HKD', 'sg': 'SGD', 'de': 'EUR', 'es': 'EUR',
                        'fr': 'EUR', 'it': 'EUR', 'jp': 'JPY', 'kr': 'KRW', 'mo': 'MOP', 'ae': 'AED', 'au': 'AUD',
                        'br': 'BRL', 'ca': 'CAD', 'my': 'MYR', 'ch': 'CHF', 'nl': 'EUR', 'ru': 'RUB'}
        currency = currency_map[region]

        if region in ('de', 'it'):
            val_new = re.sub(ur'\s', u'', val, flags=re.U).replace('.', '').replace(',', '.')
        elif region in ('fr',):
            val_new = re.sub(ur'\s', u'', val, flags=re.U).replace(',', '.')
        else:
            val_new = re.sub(ur'\s', u'', val, flags=re.U).replace(',', '')

        m = re.search(ur'[\d\.]+', val_new)
        if not m:
            price = ''
        else:
            price = float(m.group())

        ret = {'currency': currency, 'price': price}

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


def image_check(args):
    hdr = args[0]
    if hdr == '--brand':
        brand_id = int(args[1])
    else:
        print 'Invalid syntax.'
        return

    storage_path = glob.STORAGE_PATH
    db_spec = glob.SPIDER_SPEC
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")

    db.query(str.format('SELECT DISTINCT model FROM products WHERE brand_id={0}', brand_id))
    rs = db.store_result()
    model_list = rs.fetch_row(maxrows=0, how=1)
    tot = len(model_list)
    cnt = 0
    missing = 0
    mismatch = 0
    print str.format('Total models: {0}', tot)
    for model in [val['model'] for val in model_list]:
        db.query(str.format('SELECT path,width,height FROM products_image WHERE model="{0}"', model))
        rs_image = db.store_result().fetch_row(maxrows=0, how=1)
        # image_list = [val['path'] for val in rs_image]
        for image in rs_image:
            path = image['path']
            if cnt % 100 == 0:
                print str.format('{0} images checked', cnt)
            try:
                img = Image.open(os.path.join(storage_path, 'products/images', path))
                w = int(image['width'])
                h = int(image['height'])
                if w != img.size[0] or h != img.size[1]:
                    print str.format('{0} / {1} size mismatch!', model, path)
                    mismatch += 1
            except IOError:
                missing += 1
                print str.format('{0} / {1} missing!', model, path)
            finally:
                cnt += 1

    print str.format('{0} missing.', missing)
    print str.format('{0} size mismatch.', mismatch)
    pass


def sand_box():
    """
    For test use.
    """
    pass


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

    pass


if __name__ == "__main__":
    argument_parser(sys.argv)()