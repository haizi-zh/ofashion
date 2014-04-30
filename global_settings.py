# coding=utf-8
import ConfigParser
import datetime
import json
import os
from subprocess import check_output, CalledProcessError
import sys
import pkgutil
import scrapper.spiders
import inspect
import imp

__author__ = 'Zephyre'


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

    # 尝试读取远程配置文件
    if 'IMPORT' in config.sections():
        for imp_spec in sorted(read_section('IMPORT').values(),
                               key=lambda val: val['priority'] if 'priority' in val else 0):
            # 检查本地缓存文件，并决定是否采用。

            path = imp_spec['path']
            sub_cfg_file = os.path.split(path)[-1]
            if False:
            # if not (sub_cfg_file in os.listdir('.') and (datetime.datetime.now() - datetime.datetime.fromtimestamp(
            #         os.path.getmtime(sub_cfg_file))).total_seconds() < expire):
                host = imp_spec['host'] if 'host' in imp_spec else '127.0.0.1'
                port = imp_spec['port'] if 'port' in imp_spec else 22
                username = imp_spec['username']
                head = 'pscp' if sys.platform in ('win32',) else 'scp'
                cmd_str = str.format('{4} -P {0} {1}@{2}:{3} {5}', port, username, host, path, head, sub_cfg_file)
                try:
                    check_output(cmd_str, shell=True)
                except CalledProcessError as e:
                    print e.message

            _load_user_cfg(cfg_file=sub_cfg_file)

    section_list = filter(lambda val: val != 'IMPORT', config.sections())
    data = dict(map(lambda x, y: (x, y), section_list, map(read_section, section_list)))
    self_module = sys.modules[__name__]
    for key, value in data.items():
        setattr(self_module, key, value)

# 切换工作目录
os.chdir(os.path.split(sys.modules[__name__].__file__)[0])
_load_user_cfg()
