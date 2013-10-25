# coding=utf-8

import logging
import logging.config
import re
import pylev
import common as cm

__author__ = 'Zephyre'


def get_logger():
    logging.config.fileConfig('addr_filter.cfg')
    return logging.getLogger('firenzeLogger')


def gen_func_chain(logger):
    return (lambda v: filter_escape(v, logger=logger)), (lambda v: filter_brand_name(v, threshold=2, logger=logger)), \
           (lambda v: filter_tel_level_1(v, logger=logger)), (lambda v: filter_shop_hdr_level_1(v, logger=logger)), \
           (lambda v: filter_floor_level_1(v, logger=logger)), (lambda v: filter_reformat(v, logger=logger))


def filter_escape(v, logger):
    """
    消除HTML escape字符
    :param v:
    :param logger:
    """
    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()
    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']
    if addr is None:
        return record, modified
    escape_list = re.findall(ur'&.+?;', addr, flags=re.I)
    if len(escape_list) > 0:
        logger.info(unicode.format(u'Escape characters detected: {0}', u', '.join(escape_list)))
        record[u'addr_e_rev'] = cm.reformat_addr(addr)
        modified = True
    return record, modified


def filter_reformat(v, logger):
    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()
    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']
    if addr is None:
        return record, modified

    new_addr = cm.reformat_addr(addr)
    if new_addr != addr:
        logger.info(unicode.format(u'Address reformatted: {0} => {1}', addr, new_addr))
        record[u'addr_e_rev'] = new_addr
        modified = True

    return record, modified


def filter_shop_hdr_level_1(v, logger):
    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()
    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']
    if addr is None:
        return record, modified

    new_addr = re.sub(ur'^shop\s+(no\s*\.*)*\s*[\d-]+\s*,', u'', addr, flags=re.I)
    if new_addr != addr:
        logger.info(unicode.format(u'Shop header removed: {0} => {1}', addr, new_addr))
        record[u'addr_e_rev'] = new_addr
        modified = True

    return record, modified


def filter_floor_level_1(v, logger):
    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()
    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']
    if addr is None:
        return record, modified

    # 针对前三项进行处理
    addr_terms = tuple(addr.split(u','))
    term_list = addr_terms[:3]
    new_term_list = tuple(
        filter((lambda v: (re.search(ur'^\s*(\d+/?f|level\s+\d+)\s*$', v, flags=re.I) is None)),
               (temp.strip() for temp in term_list)))

    new_term_list = tuple(
        filter((lambda v: (re.search(ur'^\s*\d+(st|nd|th)\s+(floor|level)\s*$', v, flags=re.I) is None)),
               (temp.strip() for temp in new_term_list)))

    new_term_list = tuple(
        filter((lambda v: (re.search(ur'^\s*(f\d+|\d+f)\.?\s*$', v, flags=re.I) is None)),
               (temp.strip() for temp in new_term_list)))

    new_term_list = tuple(
        filter((lambda v: (re.search(ur'^\s*(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|ground)'
                                     ur'\s*floor\s*$', v, flags=re.I) is None)),
               (temp.strip() for temp in new_term_list)))

    if len(term_list) != len(new_term_list):
    #     检测到楼层信息
        temp = list(new_term_list)
        temp.extend(addr_terms[3:])
        new_addr = u', '.join(temp)
        logger.info(unicode.format(u'Floor info removed: {0} => {1}', addr, new_addr))
        record[u'addr_e_rev'] = new_addr
        modified = True

    return record, modified


def filter_tel_level_1(v, logger):
    """
    过滤电话号码
    :param v:
    :param logger:
    :return:
    """

    def re_split(text, pattr):
        temp = tuple((temp.start(), temp.end()) for temp in re.finditer(pattr, text, flags=re.I))
        match_list = tuple(text[v[0]:v[1]] for v in temp)

        start_pos = [v[0] for v in temp]
        start_pos.append(len(text))
        end_pos = [v[1] for v in temp]
        end_pos.insert(0, 0)
        split_list = map(lambda x, y: text[x:y], end_pos, start_pos)

        return split_list, match_list

    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()
    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']

    if addr is None:
        return record, modified

    split_list, match_list = re_split(addr, ur'(\bf\b|\bt\b|\btel\b|\bfax\b)*[:\s\.]*(\++\s*[\d\s\(\)-]{5,})')

    if len(match_list) > 0:
        old_addr = addr
        record[u'addr_e_rev'] = cm.reformat_addr(u', '.join(split_list))
        for temp in match_list:
            m = re.search(ur'(\bf\b|\bfax\b)+[:\s\.]*(\++\s*[\d\s\(\)-]{5,})', temp, flags=re.I)
            if m:
                if record[u'fax']:
                    logger.info(unicode.format(u'Fax conflict: original={0}, new={1}', record[u'fax'], m.group(2)))
                else:
                    record[u'fax'] = m.group(2)
                    logger.info(unicode.format(u'Fax added: {0}', m.group(2)))
            else:
                m = re.search(ur'\++\s*[\d\s\(\)-]{5,}', temp, flags=re.I)
                if m:
                    if record[u'tel']:
                        logger.info(
                            unicode.format(u'Telephone conflict: original={0}, new={1}', record[u'tel'], m.group()))
                    else:
                        record[u'tel'] = m.group()
                        logger.info(unicode.format(u'Tel added: {0}', m.group()))

        logger.info(
            unicode.format(u'Phone number found. Old addr: {0}, new addr: {1}', old_addr, record[u'addr_e_rev']))
        modified = True

    return record, modified


def filter_brand_name(v, threshold, logger=None):
    # 如果地址以品牌名称开始
    """
    去除地址开头是品牌名称的情况
    :param record:
    :param threshold: Levenshtein距离的阈值
    :return:
    """
    record, modified = v
    logger = logging.getLogger() if logger is None else logger
    record = record.copy()

    addr = record[u'addr_e_rev'] if record[u'addr_e_rev'] is not None else record[u'addr_e']
    if addr is None:
        return record, modified
    addr_list = tuple(temp.strip() for temp in addr.split(u','))
    if len(addr_list) <= 1:
        return record, modified
    str1 = addr_list[0].lower()
    str2 = record[u'brandname_e'].strip().lower()
    dist = pylev.levenshtein(str1, str2)
    if dist < threshold:
        logger.info(
            unicode.format(u'{0} is similar to {1}, idstores={2}', addr, record[u'brandname_e'], record[u'idstores']))
        record[u'addr_e_rev'] = u', '.join(addr_list[1:])
        modified = True
    return record, modified


def addr_filter(db, extra=None, update=False, logger=None, block_size=500):
    extra = tuple(u'true', ) if not extra else tuple(unicode.format(u'({0})', temp) for temp in extra)
    logger = logging.getLogger() if not logger else logger
    filter_chain = gen_func_chain(logger)

    # 总数量
    db.query(unicode.format(u'SELECT COUNT(idstores) FROM stores WHERE {0}', u' && '.join(extra)).encode('utf-8'))
    total_cnt = int(db.store_result().fetch_row()[0][0])
    idx = 0

    # 分段处理
    idstores_offset = 0
    while True:
        cond = list(extra)
        cond.append(unicode.format(u'(idstores>{0})', idstores_offset))
        db.query(
            unicode.format(u'SELECT * FROM stores WHERE {0} LIMIT {1}', u' && '.join(cond), block_size).encode('utf-8'))
        record_list = tuple(cm.unicodize(temp) for temp in db.store_result().fetch_row(maxrows=0, how=1))
        if len(record_list) == 0:
            break
        for record in record_list:
            idx += 1
            idstores_offset = int(record[u'idstores'])
            logger.info(
                unicode.format(u'Processing {0}/{1} ({2:.2%}), idstores={3}', idx, total_cnt, float(idx) / total_cnt,
                               idstores_offset))
            temp_chain = list(filter_chain)
            temp_chain.insert(0, (record, False))
            new_record, modified = reduce(lambda val, func: func(val), temp_chain)

            if update and modified:
                cm.update_record(db, (unicode.format(u'idstores={0}', record[u'idstores']),), u'stores', new_record)

    logger.info(u'DONE')
