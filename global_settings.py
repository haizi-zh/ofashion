# coding=utf-8
import ConfigParser
import datetime
import json
import os
import sys
import pkgutil
import urllib
import scrapper.spiders
import inspect
import imp
import base64
from Crypto.Cipher import AES

__author__ = 'Zephyre'


def get_cfg_from_SAE(spider, conf_url):
    url = conf_url + spider
    t = urllib.urlopen(url)
    raw = t.read()

    key = 'keyforrosevision'
    cipher = AES.new(key, AES.MODE_ECB)
    return json.loads(cipher.decrypt(base64.b64decode(raw)).strip())


def _load_user_cfg(cfg_file=None, expire=600):
    """
    功能：加载配置文件。

    描述：在加载主配置文件cfg_file后，如果里面有IMPORT区域，则按照该区域的指示，加载子配置。需要注意的是expire参数。
    在加载IMPORT区域的时候，如果子配置文件已经存在，并且创建时间距离现在没有超过expire所指定的过期时间，则可以直接从本地读取子配置文件，
    不再需要根据IMPORT指示从中央服务器获取。

    @param cfg_file: 主配置文件。
    @param expire: 过期时间，单位为秒。
    @return:
    """

    def parse_val(val):
        """
        解析字符串val。如果是true/false，返回bool值；如果为整数，返回int值；如果为浮点数，返回float值。
        @param val:
        """
        if val.lower() == 'true':
            return True
        elif val.lower() == 'false':
            return False

        try:
            num = float(val)
            # 判断是浮点数还是整数
            if num == int(num):
                return int(num)
            else:
                return num
        except ValueError:
            pass

        try:
            # val为字符串，尝试是否可以解析为JSON
            return json.loads(val)
        except ValueError:
            pass

        # 尝试解析为日期字符串
        for ts_format in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y'):
            try:
                return datetime.datetime.strptime(val, ts_format)
            except ValueError:
                pass

        # 作为原始字符串返回
        return val

    def read_section(section):
        return {option: parse_val(config.get(section, option)) for option in config.options(section)}

    if not cfg_file:
        cfg_file = os.path.join(os.path.split(__file__)[0], 'mstore.cfg')

    # 加载mstore.cfg的设置内容
    config = ConfigParser.ConfigParser()
    config.optionxform = str

    try:
        with open(cfg_file, 'r') as cf:
            config.readfp(cf)
    except IOError:
        pass

    cfg_expire = None
    # 获得配置文件的过期时间
    if 'IMPORT_MISC' in config.sections():
        tmp = read_section('IMPORT_MISC')
        if 'EXPIRE' in tmp:
            cfg_expire = tmp['EXPIRE'] * 60

    refresh_cfg = True
    cached_path = 'cached_mstore.cfg'
    if cfg_expire and os.path.exists(cached_path):
        delta = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(cached_path)))
        if delta < datetime.timedelta(0, cfg_expire):
            refresh_cfg = False

    self_module = sys.modules[__name__]
    if not refresh_cfg:
        with open(cached_path, 'rb') as f:
            all_settings = json.load(f)
    else:
        all_settings = {}

        # 尝试读取远程配置文件
        if 'IMPORT' in config.sections():
            for imp_spec in sorted(read_section('IMPORT').values(),
                                   key=lambda val: val['priority'] if 'priority' in val else 0):
                name = imp_spec['name']
                conf_url = read_section('CONFIG_URL').values()[0]['url']
                data = get_cfg_from_SAE(name, conf_url)
                for key, value in data.items():
                    all_settings[key] = value

        section_list = filter(lambda val: val != 'IMPORT', config.sections())
        data = dict(map(lambda x, y: (x, y), section_list, map(read_section, section_list)))
        for key, value in data.items():
            all_settings[key] = value

        with open(cached_path, 'wb') as f:
            json.dump(all_settings, f)

    # 加载设置
    for key, value in all_settings.items():
        setattr(self_module, key, value)

# 切换工作目录
os.chdir(os.path.split(sys.modules[__name__].__file__)[0])
_load_user_cfg()


