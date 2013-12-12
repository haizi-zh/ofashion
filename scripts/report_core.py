# coding=utf-8

import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import global_settings as gs
import datetime
from core import MySqlDb


__author__ = 'Zephyre'


def get_fetched_report(db, time_range_str):
    # 有哪些品牌有新抓取的爬虫
    if time_range_str:
        rs = db.query_match(['DISTINCT brand_id'], 'products',
                            extra=[str.format('update_time>"{0}"', time_range_str[0]),
                                   str.format('update_time<"{0}"', time_range_str[1])])
    else:
        rs = db.query_match(['DISTINCT brand_id'], 'products')
    brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]

    def proc_by_brand(brand):
        if time_range_str:
            rs = db.query_match(['COUNT(DISTINCT model)'], 'products', {'brand_id': brand},
                                extra=[str.format('update_time>"{0}"', time_range_str[0]),
                                       str.format('update_time<"{0}"', time_range_str[1])])
        else:
            rs = db.query_match(['COUNT(DISTINCT model)'], 'products', {'brand_id': brand})
        cnt_tot = int(rs.fetch_row()[0][0])

        def func1(region):
            if time_range_str:
                rs = db.query_match(['COUNT(*)'], 'products', {'brand_id': brand, 'region': region},
                                    extra=[str.format('update_time>"{0}"', time_range_str[0]),
                                           str.format('update_time<"{0}"', time_range_str[1])])
            else:
                rs = db.query_match(['COUNT(*)'], 'products', {'brand_id': brand, 'region': region})
            return rs.fetch_row()[0][0]

        cnt_by_region = '/'.join(map(func1, ['cn', 'us', 'fr', 'uk', 'it']))
        return unicode.format(
            u'<tr><td>{0}</td><td>{1}</td><td>{2}</td><td>{3}</td></tr>', brand, gs.brand_info()[brand]['brandname_e'],
            cnt_tot, cnt_by_region
        )

    return '\r'.join(map(proc_by_brand, brand_list))


def spider_prog_report(param_dict):
    """
    产生爬虫进度报告
    """
    report_tpl = u'''
                <h1>全球扫货指南：单品信息抓取系统简报</h1>
                <p>系统自动邮件，请勿回复。</p>
                <p>统计时间段：{STAT-DATE-RANGE}</p>
                <h2>新抓取的单品：</h2>
                <p><table style="width:100%;" cellpadding="2" cellspacing="0" border="1" bordercolor="#000000">
                    <tbody>
                        <tr>
                            <td>品牌编号</td>
                            <td>品牌名称</td>
                            <td>单品总数</td>
                            <td>主要市场的分布情况（中/美/法/英/意）</td>
                        </tr>
                        {NEW-PRODUCTS}
                    </tbody>
                </table></p>
                <h2>已经发布的单品：</h2>
                <p><table style="width:100%;" cellpadding="2" cellspacing="0" border="1" bordercolor="#000000">
                    <tbody>
                        <tr>
                            <td>品牌编号</td>
                            <td>品牌名称</td>
                            <td>单品总数</td>
                            <td>主要市场的分布情况（中/美/法/英/意）</td>
                        </tr>
                        {FETCHED-PRODUCTS}
                    </tbody>
                </table></p>
                <h2>价格变化趋势：</h2>
                <p>暂无</p>
                '''
    recipients = [unicode.format(u'{0} <{1}>', item[0], item[1]) for item in gs.EMAIL_ADDR.items()]
        # ['Haizi Zheng <haizi.zh@gmail.com>', 'Haizi Zheng <haizi.zh@qq.com>']
    recipent_addrs = gs.EMAIL_ADDR.values()# ['haizi.zh@gmail.com', 'haizi.zh@qq.com']

    cur = datetime.datetime.now()
    from_time = cur - datetime.timedelta(1)
    time_range_str = map(lambda v: v.strftime('%Y-%m-%d %H:%M:%S'), [from_time, cur])
    stat_date_range = unicode.format(u'从{0}至{1}', time_range_str[0], time_range_str[1])

    db = MySqlDb()
    db.conn(gs.DB_SPEC)

    new_products = get_fetched_report(db, time_range_str)
    fetched_products = get_fetched_report(db, None)

    db.close()

    msg = MIMEText(
        report_tpl.replace('{STAT-DATE-RANGE}', stat_date_range).replace('{NEW-PRODUCTS}', new_products).replace(
            '{FETCHED-PRODUCTS}', fetched_products),
        _subtype='html', _charset='utf-8')
    msg['Subject'] = u'单品信息抓取系统简报'
    msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
    msg['To'] = ', '.join(recipients)

    server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
    #server = smtplib.SMTP('smtp.gmail.com', 587)
    #server.ehlo()
    #server.starttls()
    server.login('buddy@mfashion.com.cn', 'rose123')
    server.sendmail('buddy@mfashion.com.cn', recipent_addrs, msg.as_string())

    pass
