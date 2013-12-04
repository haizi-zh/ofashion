# coding=utf-8
import re
import types
import global_settings as glob

__author__ = 'Zephyre'


def process_price(price, region, decimal=None, currency=None):
    def func(val):
        """
        去掉多余的空格，以及首尾的非数字字符
        :param val:
        """
        # val=unicode.format(u' {0} ',)
        if not re.search(r'\d', val):
            return ''
        val = re.sub(r'\s', '', val, flags=re.U)
        if val[0] in ('.', ','):
            val = val[1:]
        if val[-1] in ('.', ','):
            val = val[:-1]
        return val

    if isinstance(price, int) or isinstance(price, float):
        price = unicode(price)
    if not price or not price.strip():
        return None
    val = unicode.format(u' {0} ', unicodify(price))

    if not currency:
        # 如果price没有货币单位信息，则根据region使用默认值
        mt = re.search(r'\b([A-Z]{3})\b', price)
        if mt and mt.group(1) in glob.currency_info().keys():
            currency = mt.group(1)
        else:
            currency = glob.region_info()[region]['currency']

    # 提取最长的数字，分隔符字符串
    tmp = sorted([func(tmp) for tmp in re.findall(r'(?<=[^\d])[\d\s,\.]+(?=[^\d])', val, flags=re.U)],
                 key=lambda tmp: len(tmp), reverse=True)
    if not tmp:
        return None
        # 去除首尾的符号
    while True:
        tmp = tmp[0].strip()
        if not tmp:
            return None
        elif tmp[0] in ('.', ','):
            tmp = tmp[1:]
            continue
        elif tmp[-1] in ('.', ','):
            tmp = tmp[:-1]
            continue
        break


    # 判断小数点符号
    # 方法：如果,和.都有，谁在后面，谁就是分隔符。否则的话，看该符号是否在三的倍数位置上
    if decimal:
        pass
    elif (tmp.count('.') > 0 and tmp.count(',') == 1) or (tmp.count(',') > 0 and tmp.count('.') == 1):
        decimal = re.search(r'[\.,]', tmp[::-1]).group()
    elif (tmp.count('.') | tmp.count(',')) and not (tmp.count('.') & tmp.count(',')):
        # 只有一种符号出现
        c = re.search(r'[\.,]', tmp).group()
        # 符号位的位置。如果相互之间间隔为4，则说明是千位分隔符。
        pos = [val.start() for val in re.finditer(r'[\.,]', tmp)]
        pos.append(len(tmp))
        is_triple = reduce(lambda ret, val: ret and (val == 4), [pos[i + 1] - pos[i] for i in xrange(len(pos) - 1)],
                           True)
        if is_triple:
            decimal = list({',', '.'} - {c})[0]
        else:
            if tmp.count(c) == 1:
                decimal = c
            else:
                decimal = None
    elif tmp.count('.') == 0 and tmp.count(',') == 0:
        decimal = '.'
    else:
        decimal = None

    if not decimal:
        return None

    part = tmp.split(decimal)
    if len(part) == 1:
        part = part[0], '0'

    try:
        val = int(re.sub(r'[\.,]', '', part[0])) + float('.' + re.sub(r'[\.,]', '', part[1]))
    except (TypeError, ValueError):
        return None

    return {'currency': currency, 'price': val}


def unicodify(val):
    """
    Unicode化，并且strip
    :param val:
    :return:
    """
    if val is None:
        return None
    elif isinstance(val, str):
        return val.decode('utf-8').strip()
    else:
        return unicode(val).strip()


def iterable(val):
    """
    val是否iterable。注意：val为str的话，返回False。
    :param val:
    """
    if isinstance(val, types.StringTypes):
        return False
    else:
        try:
            iter(val)
            return True
        except TypeError:
            return False