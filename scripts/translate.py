# coding=utf-8

__author__ = 'Ryan'

from core import MySqlDb
import goslate
import urllib2
import socket


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
    rows = db.query(str.format('SELECT fingerprint from products group by fingerprint limit {0}, {1}', start, count))
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
        for c in val.decode('utf-8'):
            if ord(c) >= 0x4e00 and ord(c) < 0x9fa5:
                return True

    return False


def is_eng(val):
    """
    val是否为cjk字符集
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
    返回含有中文字段所在的region
    """
    cns = ['cn', 'hk', 'tw', 'mo']
    for region in cns:
        if region in product_infos:
            if key in product_infos[region]:
                return product_infos[region][key]


def check_ens_region(product_infos, key):
    """
    按顺序检查指定字段是否是英文
    返回英文字段所在的region
    """
    ens = ['us', 'uk', 'ca']
    for region in ens:
        if region in product_infos:
            if key in product_infos[region]:
                return product_infos[region][key]


def translate_text_to(gs, text, to, source='', backup_gs=None):
    try:
        text = text.encode('utf-8')
    except:
        pass

    result = None
    try:
        result = gs.translate(text, to, source)
    except:
        pass
    if not result and backup_gs:
        try:
            result = backup_gs.translate(text, to, source)
        except:
            pass

    return result


def translate_main():
    db_spec = {
        "host": "127.0.0.1", "port": 3306,
        "username": "rose", "password": "rose123",
        "schema": "translateTest"
    }
    db = MySqlDb()
    db.conn(db_spec)

    gs = goslate.Goslate()
    proxy = urllib2.ProxyHandler({'http': '173.230.131.197:8888'})
    opener = urllib2.build_opener(proxy)
    backup_gs = goslate.Goslate(opener=opener, debug=True)

    sorted_region = get_sorted_region(db)
    fingerprint_start = 0
    fingerprint_count = 50
    while 1:
        fingerprints = get_fingerprints(db, fingerprint_start, fingerprint_count)
        if not fingerprints:
            break
        else:
            fingerprint_start += fingerprint_count

        for fingerprint in fingerprints:
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
            if is_chs(details_cn):
                final_details_cn = details_cn

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
                            final_details_cn = translate_text_to(gs, product_infos[region]['details'], 'zh-cn', backup_gs=backup_gs)
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
                                final_details_en = translate_text_to(gs, product_infos[region]['details'], 'en', backup_gs=backup_gs)
                                break

                if not final_description_en and final_description_cn:
                    final_description_en = translate_text_to(gs, final_description_cn, 'en', 'zh-cn', backup_gs=backup_gs)
                if not final_details_en and final_details_cn:
                    final_details_en = translate_text_to(gs, final_details_cn, 'en', 'zh-cn', backup_gs=backup_gs)
            except :
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
                    pass

    db.close()


translate_main()
