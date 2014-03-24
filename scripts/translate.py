# coding=utf-8
import getopt
import logging
import sys
from utils.utils_core import get_logger
from core import RoseVisionDb
import goslate
import urllib2
import global_settings

__author__ = 'Ryan'


def get_sorted_region(db):
    """
    得到region的权重排序
    """
    rows = db.query(str.format('SELECT iso_code FROM region_info ORDER BY weight;'))
    result = []
    for row in rows.fetch_row(maxrows=0, how=1):
        result += [row['iso_code']]
    return result


def get_fingerprints(db, start=0, count=50):
    """
    取得不重复单品的fingerprint
    """
    rows = db.query(
        str.format('SELECT fingerprint from products group by fingerprint order by fingerprint limit {0}, {1}', start,
                   count))
    result = []
    for row in rows.fetch_row(maxrows=0, how=1):
        fingerprint = row['fingerprint']
        result += [fingerprint]
    return result


def get_product(db, fingerprint):
    """
    取得单品信息
    """
    rows = db.query(
        str.format('SELECT model,region,description,details FROM products WHERE fingerprint = "{0}"', fingerprint))
    result = {}
    for row in rows.fetch_row(maxrows=0, how=1):
        model = row['model']
        region = row['region']
        description = row['description']
        details = row['details']
        result[region] = {
            'model': model,
            'description': description,
            'details': details,
        }
    return result


def is_chs(val):
    """
    val是否含有简体中文
    @param val:
    """
    if val:
        flag = False

        for c in val.decode('utf-8'):
            if ord(c) >= 0x4e00 and ord(c) < 0x9fa5:
                flag = True

        if flag:
            try:
                val.decode('utf-8').encode('gb2312')
            except:
                flag = False

        return flag

    return False


def is_cht(val):
    """
    val是否含有繁体中文
    @param val:
    """
    if val:
        flag = False

        for c in val.decode('utf-8'):
            if ord(c) >= 0x4e00 and ord(c) < 0x9fa5:
                flag = True

        if flag:
            flag = False

            try:
                val.decode('utf-8').encode('gb2312')
            except:
                flag = True

        return flag

    return False


def is_eng(val):
    """
    val是否为英语
    @param val:
    """
    if val:
        for c in val.decode('utf-8'):
            if ord(c) > 127:
                return False

        return True
    else:
        return False


def check_cns_region(product_infos, key):
    """
    按顺序检查指定字段是否含有中文
    返回含有中文字段
    """
    cns = ['cn', 'hk', 'tw', 'mo']
    for region in cns:
        if region in product_infos:
            if key in product_infos[region]:
                temp = product_infos[region][key]
                if is_chs(temp):
                    return temp
                elif is_cht(temp):
                    return temp


def check_ens_region(product_infos, key):
    """
    按顺序检查指定字段是否是英文
    返回英文字段
    """
    ens = ['us', 'uk', 'ca']
    for region in ens:
        if region in product_infos:
            if key in product_infos[region]:
                temp = product_infos[region][key]
                if is_eng(temp):
                    return temp


def translate_text_to(gs, text, to, source='', backup_gs=None):
    logger = get_logger()
    try:
        text = text.encode('utf-8')
    except:
        pass

    if not source and is_cht(text):
        source = 'zh-cn'

    result = None
    try:
        result = gs.translate(text, to, source)
    except:
        if not backup_gs:
            logger.info(
                str.format("Error: gs translate error with text : {0}       source : {1}        target : {2}", text,
                           source,
                           to))
        pass
    if not result and backup_gs:
        try:
            result = backup_gs.translate(text, to, source)
        except:
            logger.info(
                str.format("Error: backupgs translate error with text : {0}       source : {1}        target : {2}",
                           text, source, to))
            pass

    return result


def get_proxy(region='us'):
    try:
        return getattr(global_settings, 'PROXY')[region][0]
    except (KeyError, IndexError, AttributeError):
        return None


def translate_main(start=0, count=100, logger=None, db_spec=None):
    if not logger:
        logger = get_logger()

    with RoseVisionDb(getattr(global_settings, db_spec)) as db:
        gs = goslate.Goslate()
        proxy_name = get_proxy()
        proxy = urllib2.ProxyHandler({'http': proxy_name}) if proxy_name else None
        opener = urllib2.build_opener(proxy)
        backup_gs = goslate.Goslate(opener=opener, debug=True)

        sorted_region = get_sorted_region(db)

        fingerprint_start = start
        fingerprint_count = count

        logger.info(str.format("Translate process start"))
        while 1:
            fingerprints = get_fingerprints(db, fingerprint_start, fingerprint_count)
            if not fingerprints:
                logger.info(str.format("Translate process end"))
                break
            else:
                logger.info(
                    str.format("Translate process offset : {0} count : {1}", fingerprint_start, len(fingerprints)))
                fingerprint_start += fingerprint_count

            for fingerprint in fingerprints:

                is_exist = db.query_match({'fingerprint'}, 'products_translate',
                                          {'fingerprint': fingerprint}).num_rows()
                if is_exist:
                    continue

                product_infos = get_product(db, fingerprint)

                # 按权重排序
                product_infos = sorted(product_infos.items(), key=lambda e: sorted_region.index(e[0]))
                product_infos = {e[0]: e[1] for e in product_infos}

                final_description_cn = None
                final_details_cn = None
                final_description_en = None
                final_details_en = None

                description_cn = check_cns_region(product_infos, 'description')
                details_cn = check_cns_region(product_infos, 'details')

                if is_chs(description_cn):
                    final_description_cn = description_cn
                elif is_cht(description_cn):
                    final_description_cn = translate_text_to(gs, description_cn, 'zh-cn', source='zh-cn',
                                                             backup_gs=backup_gs)

                if is_chs(details_cn):
                    final_details_cn = details_cn
                elif is_cht(details_cn):
                    final_details_cn = translate_text_to(gs, details_cn, 'zh-cn', source='zh-cn', backup_gs=backup_gs)

                description_en = check_ens_region(product_infos, 'description')
                details_en = check_ens_region(product_infos, 'details')

                if is_eng(description_en):
                    final_description_en = description_en
                if is_eng(details_en):
                    final_details_en = details_en

                try:
                    if not final_description_cn:
                        for region, info in product_infos.items():
                            if product_infos[region]['description']:
                                final_description_cn = translate_text_to(gs, product_infos[region]['description'],
                                                                         'zh-cn', backup_gs=backup_gs)
                                break
                    if not final_details_cn:
                        for region, info in product_infos.items():
                            if product_infos[region]['details']:
                                final_details_cn = translate_text_to(gs, product_infos[region]['details'], 'zh-cn',
                                                                     backup_gs=backup_gs)
                                break
                    if not final_description_en:
                        for region, info in product_infos.items():
                            if region != 'cn':  # 尽量不从中文翻译到其他外语
                                if product_infos[region]['description']:
                                    final_description_en = translate_text_to(gs, product_infos[region]['description'],
                                                                             'en', backup_gs=backup_gs)
                                    break
                    if not final_details_en:
                        for region, info in product_infos.items():
                            if region != 'cn':  # 尽量不从中文翻译到其他外语
                                if product_infos[region]['details']:
                                    final_details_en = translate_text_to(gs, product_infos[region]['details'], 'en',
                                                                         backup_gs=backup_gs)
                                    break

                    if not final_description_en and final_description_cn:
                        final_description_en = translate_text_to(gs, final_description_cn, 'en', 'zh-cn',
                                                                 backup_gs=backup_gs)
                    if not final_details_en and final_details_cn:
                        final_details_en = translate_text_to(gs, final_details_cn, 'en', 'zh-cn', backup_gs=backup_gs)
                except:
                    pass

                insert_dict = {}
                if final_description_cn:
                    insert_dict['description_cn'] = final_description_cn
                if final_details_cn:
                    insert_dict['details_cn'] = final_details_cn
                if final_description_en:
                    insert_dict['description_en'] = final_description_en
                if final_details_en:
                    insert_dict['details_en'] = final_details_en

                if insert_dict:
                    insert_dict['fingerprint'] = fingerprint
                    result = db.query_match({'fingerprint'}, 'products_translate', {'fingerprint': fingerprint})
                    try:
                        if result.num_rows() == 0:
                            db.insert(insert_dict, 'products_translate')
                        else:
                            db.update(insert_dict, 'products_translate', str.format('fingerprint="{0}"', fingerprint))
                    except:
                        logger.info(str.format("Error: Insert or update sql error with {0}", insert_dict))
                        pass
                else:
                    logger.info(str.format("Error: No insert_dict for fingerprint : {0}", fingerprint))


class TranslateTasker(object):
    running = False

    @classmethod
    def run(cls, **kwargs):
        cls.running = True

        logger = kwargs['logger'] if 'logger' in kwargs else get_logger()
        logger.info(str.format("Translate tasker start"))

        db_spec = kwargs['db_spec'] if 'db_spec' in kwargs else 'DB_SPEC'
        start = kwargs['start'] if 'start' in kwargs else 0
        count = kwargs['count'] if 'count' in kwargs else 100

        translate_main(start, count, logger, db_spec)

        logger.info(str.format("Translate tasker end"))

        cls.running = False

    @classmethod
    def is_running(cls):
        return cls.running


if __name__ == '__main__':
    pass
    # logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
    # logger = logging.getLogger()
    #
    # logger.info(str.format("Script start"))
    #
    # start = 0
    # count = 100
    # opts, args = getopt.getopt(sys.argv[1:], "s:c:")
    # for opt, arg in opts:
    #     if opt == '-s':
    #         start = int(arg)
    #     elif opt == '-c':
    #         count = int(arg)
    #
    # translate_main(start, count, logger)
    #
    # logger.info(str.format("Script end"))
    # pass
