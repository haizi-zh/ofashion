import os

__author__ = 'Zephyre'

import sys
import Image

cmd_list = ('help', 'sandbox', 'resize')
ext_list = ('.jpg', '.jpeg', '.tif', '.tiff', '.png', 'bmp')
verbose = False
force_overwrite = False


def default_error():
    print 'Invalid syntax. Use mstore help for more information.'


def mstore_help():
    print str.format('Available commands are: {0}', ', '.join(cmd_list))


def mstore_error():
    default_error()


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


def sand_box():
    """
    For test use.
    """
    img = Image.open('1.jpg')
    dir(img)
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
    elif cmd == 'sandbox':
        return sand_box

    pass


if __name__ == "__main__":
    argument_parser(sys.argv)()