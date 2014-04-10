# coding=utf-8
import urllib
from utils.utils_core import unicodify
from core import RoseVisionDb
import global_settings as gs
import datetime
import logging
import json
from utils import info

logging.basicConfig(filename='DataCheck.log', level=logging.DEBUG)


class DataCheck(object):
    """
    图片信息检验
    @param param_dict:
    """

    @classmethod
    def run(cls, logger=None, **kwargs):
        logging.info('PRODUCT CHECK STARTED!!!!')
        #set brand id list

        threshold = kwargs['threshold'] if 'threshold' in kwargs else 10

        if 'brand_list' in kwargs:
            brand_list = kwargs['brand_list']
        else:
            with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
                brand_list = db.query_match(['brand_id'],
                                            'products', distinct=True).fetch_row(maxrows=0)
                db.start_transaction()
                brand_list = [int(val[0]) for val in brand_list]

        for brand in brand_list:
            with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
                #=============================product check==================================================
                logging.info(unicode.format(u'{0} PROCESSING product check {1} / {2}',
                                            datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), brand,
                                            info.brand_info()[brand]['brandname_e']))
                rs = db.query_match(
                    ['idproducts', 'region', 'name', 'url', 'color', 'description', 'details', 'price_change'],
                    'products', {'brand_id': brand}).fetch_row(maxrows=0)
                db.start_transaction()
                for idproducts, region, name, url, color, desc, details, price_change in rs:
                    name_err = url_err = color_err = desc_err = details_err = price_change_err = False
                    #中英美区域name、description检验，只能包含中英文字符和标点，出现其他文字及符号标识为错误
                    if region in ['cn', 'us', 'uk']:
                        name_err = not region_pass(name)
                        desc_err = not region_pass(desc)
                    #url不含cjk字符，否则报错，quote生成新url，待用。
                    url_err = check_url(url)
                    if url_err:
                        url = urllib.quote(url, ":?=/")
                    #color为[]或者可json解析的字符串
                    if color != '[]' and color is not None:
                        try:
                            t = json.loads(color)
                            color_err = False
                        except:
                            color_err = True
                            print 'color:', color

                    if name and True in map(lambda x: x in name,
                                            ['&ensp;', '&emsp;', '&nbsp;', '&lt;', '&gt;', '&amp;', '&quot;', '&copy;',
                                             '&reg;', '&times;', '&divide;']):
                        name_err = True

                    if name_err or url_err or color_err or desc_err:
                        logging.error(
                            (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '--idproducts:', idproducts,
                             'name_err' if name_err else None,
                             'url_err' if url_err else None,
                             'color_err' if color_err else None,
                             'desc_err' if desc_err else None,
                            ))

                        #=============================price check==================================================
                        logging.info(unicode.format(u'{0} PROCESSING price check {1} / {2}',
                                                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), brand,
                                                    info.brand_info()[brand]['brandname_e']))
                        prs = db.query(str.format(
                            'SELECT * FROM (SELECT p2.idprice_history,p2.date,p2.price,p2.currency,p1.idproducts,p1.brand_id,'
                            'p1.region,p1.name,p1.model,p1.offline FROM products AS p1 JOIN products_price_history AS p2 ON '
                            'p1.idproducts=p2.idproducts '
                            'WHERE p1.brand_id={0} ORDER BY p2.date DESC) AS p3 GROUP BY p3.idproducts', brand))
                        # 以model为键值，将同一个model下，不同区域的价格放在一起。
                        records = prs.fetch_row(maxrows=0, how=1)
                        price_data = {}
                        for r in records:
                            model = r['model']
                            # 仅有那些price不为None，且offline为0的数据，才加入到price check中。
                            if r['price'] and int(r['offline']) == 0:
                                # 首先检查model是否已存在
                                if model not in price_data:
                                    price_data[model] = []
                                price_data[model].append(r)

                        # 最大值和最小值之间，如果差别过大，则说明价格可能有问题
                        for model in price_data:
                            for item in price_data[model]:
                                price = float(item['price'])
                                item['nprice'] = gs.currency_info()[item['currency']] * price

                            # 按照nprice大小排序
                            sorted_data = sorted(price_data[model], key=lambda item: item['nprice'])
                            max_price = sorted_data[-1]['nprice']
                            min_price = sorted_data[0]['nprice']
                            if min_price > 0 and max_price / min_price > threshold:
                                logging.warning(
                                    unicode.format(u'{0} WARNING: {1}:{7} MODEL={2}, {3} / {4} => {5} / {6}',
                                                   datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                   brand, model,
                                                   sorted_data[0]['nprice'], sorted_data[0]['region'],
                                                   sorted_data[-1]['nprice'], sorted_data[-1]['region'],
                                                   info.brand_info()[brand]['brandname_e']))

        logging.info('PRODUCT CHECK ENDED!!!!')


def region_pass(char):
    """判断信息是否是属于中国区域,只能包含中英文字符及标点，否则返回False"""
    if char and char.strip():
        uchar = unicodify(char)
        for i in uchar:
            if i and i.strip():
                if i == u'\t' or i == u'\r' or i == u' ' or is_latin(i) or is_chinese(i) or is_currency(
                        i) or is_special(i):
                    pass
                else:
                    return False
    return True


def check_url(char):
    """判断url是否包含cjk，包含返回True，并转义，否则返回False"""
    if char and char.strip():
        uchar = unicodify(char)
        for i in uchar:
            if i and i.strip():
                if is_cjk(i):
                    return True
                else:
                    return False
    return True


def is_latin(uchar):
    """判断一个unicode是否是拉丁字母、扩展字母、数字、标点符号"""
    if u'\u0020' <= uchar <= u'\u00ff':
        return True
    else:
        return False


def is_chinese(uchar):
    """判断一个unicode是否是汉字、标点符号"""
    #U+FF01 – U+FF0F，U+FF1A – U+FF20，U+FF3B – U+FF40，U+FF5B – U+FF5E
    if u'\u4e00' <= uchar <= u'\u9fa5' or u'\u3000' <= uchar <= u'\u303f' or u'\u2000' <= uchar <= u'\u206f' or \
                            u'\uff01' <= uchar <= u'\uff0f' or u'\uff1a' <= uchar <= u'\uff20' or \
                            u'\uff3b' <= uchar <= u'\uff40' or u'\uff5b' <= uchar <= u'\uff5e':
        return True
    else:
        return False


def is_currency(uchar):
    """判断一个unicode是否是货币符号"""
    if u'\u20a0' <= uchar <= u'\u20ba':
        return True
    else:
        return False


def is_special(uchar):
    """判断一个unicode是否是特殊符号: Spacing Modifier Letters,Letterlike Symbols
    """
    if u'\u02B0' <= uchar <= u'\u02ff' or u'\u2100' <= uchar <= u'\u214F':
        return True
    else:
        return False


def is_cjk(uchar):
    """判断一个unicode是否是cjk"""
    if u'\u4e00' <= uchar <= u'\u9fa5':
        return True
    else:
        return False


if __name__ == '__main__':
    t = DataCheck()
    t.run(brand_list=[10006])