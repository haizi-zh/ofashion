# coding=utf-8
import hashlib
import re
import types
import global_settings as glob

__author__ = 'Zephyre'


def guess_currency(price, region=None):
    # 如果下面这些符号出现在字符串中，则可以直接确定货币
    symbols = {u'€': 'EUR', 'HK$': 'HKD', 'AU$': 'AUD', 'CA$': 'CAD', 'US$': 'USD', u'£': 'GBP'}
    # 按照符号提取
    for s in symbols:
        if s in price:
            return symbols[s]

    # 如果$前面没有两个大写的字母，即没有出现CA$，AU $等情况，则说明货币是美元。
    if '$' in price:
        mt = re.search(r'[A-Z]{2}\s*\$', price, flags=re.U)
        if not mt:
            return 'USD'

    if u'¥' in price and region in ('cn', 'hk', 'mo', 'tw'):
        return 'CNY'

    # 若字符串中包含大写的三个字母，并且该标识出现在货币列表中，说明这三个字母组成的字符串是货币信息
    mt = re.search(r'([A-Z]{3})', price, flags=re.U)
    if mt and mt.group(1) in glob.currency_info().keys():
        return mt.group(1)
    else:
        # 未找到货币信息
        return None


def gen_fingerprint(brand, model):
    """
    根据单品的品牌编号和model，生成加盐处理后的MD5指纹
    @param brand:
    @param model:
    """
    salt_plain = 'roseVision88'
    salt = hashlib.md5(salt_plain).digest()

    idstores = str(brand) + model
    d1 = hashlib.md5(idstores).digest()
    return ''.join(map((lambda x, y: '{0:x}'.format((ord(x) + ord(y)) % 256)), d1, salt))


def process_price(price, region, decimal=None, currency=None):
    def func(val):
        """
        去掉多余的空格，以及首尾的非数字字符
        :param val:
        """
        # val=unicode.format(u' {0} ',)
        if not re.search(r'\d', val):
            return ''
        val = re.sub(r"[\s']", '', val, flags=re.U)
        if val[0] in ('.', ','):
            val = val[1:]
        if val[-1] in ('.', ','):
            val = val[:-1]
        return val

    if not price or not price.strip():
        return None
        # 如果包含了appel, call等字符，说明不是这不是价格信息
    for term in ['appel', 'call', 'appelez', 'chiamare']:
        if term in price.lower():
            return None

    if isinstance(price, int) or isinstance(price, float):
        price = unicode(price)
    if not price or not price.strip():
        return None
    val = unicode.format(u' {0} ', unicodify(price))

    if not currency:
        # 如果price没有货币单位信息，则根据根据price内容，尝试提取货币信息。
        currency = guess_currency(price, region=region)
        if not currency:
            # 如果无法提取货币信息，则使用region的默认值
            currency = glob.region_info()[region]['currency']

    # 提取最长的数字，分隔符字符串
    tmp = sorted([func(tmp) for tmp in re.findall(r"(?<=[^\d])[\d\s,'\.]+(?=[^\d])", val, flags=re.U)],
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

    if re.search(r'^0+', tmp):
        return None

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