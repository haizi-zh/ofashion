# coding=utf-8
import random

import smtplib
import re
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

    # 确定收信人
    try:
        group = getattr(gs, 'REPORTS')['DATA_STATUS']
        if not isinstance(group, list):
            group = [group]
        recipients = {}
        for g in group:
            for key, value in getattr(gs, 'EMAIL_GROUPS')[g].items():
                recipients[key] = value
                # recipent_addrs = gs.EMAIL_ADDR.values()  # ['haizi.zh@gmail.com', 'haizi.zh@qq.com']
    except (TypeError, AttributeError, KeyError):
        return

    cur = datetime.datetime.now()
    # 时间跨度：一天
    delta = datetime.timedelta(1)
    from_time = cur - delta
    time_range_str = map(lambda v: v.strftime('%Y-%m-%d %H:%M:%S'), [from_time, cur])
    stat_date_range = unicode.format(u'从{0}至{1}', time_range_str[0], time_range_str[1])

    with MySqlDb(getattr(gs, 'DB_SPEC')) as db:
        new_products = get_fetched_report(db, time_range_str)
        fetched_products = get_fetched_report(db, None)

    msg = MIMEText(
        report_tpl.replace('{STAT-DATE-RANGE}', stat_date_range).replace('{NEW-PRODUCTS}', new_products).replace(
            '{FETCHED-PRODUCTS}', fetched_products),
        _subtype='html', _charset='utf-8')
    msg['Subject'] = u'单品信息抓取系统简报'
    msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
    msg['To'] = ', '.join([unicode.format(u'{0} <{1}>', item[0], item[1]) for item in recipients.items()])

    server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
    # 使用gmail发送邮件
    #server = smtplib.SMTP('smtp.gmail.com', 587)
    #server.ehlo()
    #server.starttls()

    server.login('buddy@mfashion.com.cn', 'rose123')
    server.sendmail('buddy@mfashion.com.cn', recipients.values(), msg.as_string())
    server.quit()


def process_log(param):
    ProcessLog(param).run()


class ProcessLog(object):
    """
    分析log文件，报告错误位置
    """

    def __init__(self, param=None):
        if 'log-path' in param:
            self.log_path = param['log_path'][0]
        else:
            self.log_path = None

        if 'interval' in param:
            self.interval = param['interval'][0]
        else:
            self.interval = 1

        if 'start' in param and 'stop' in param:
            self.start_time = param['start'][0]
            self.stop_time = param['stop'][0]
        else:
            cur = datetime.datetime.now()
            to_time = cur.strftime('%Y%m%d%H%M%S')
            from_time = (cur - datetime.timedelta(self.interval)).strftime('%Y%m%d%H%M%S')
            self.start_time = from_time
            self.stop_time = to_time

    def run(self):
        '''
        通过文件名判断文件创建时间、货号、new or update
        不使用文件属性判断，防止文件误删恢复影响创建时间、修改时间
        '''
        if not self.log_path:
            return

        # 确定收信人
        try:
            group = getattr(gs, 'REPORTS')['DEV_STATUS']
            if not isinstance(group, list):
                group = [group]
            recipients = {}
            for g in group:
                for key, value in getattr(gs, 'EMAIL_GROUPS')[g].items():
                    recipients[key] = value
                    # recipent_addrs = gs.EMAIL_ADDR.values()  # ['haizi.zh@gmail.com', 'haizi.zh@qq.com']
        except (TypeError, AttributeError, KeyError):
            return

        files = os.listdir(self.log_path)
        process_error = {}
        for file in files:
            #通过文件名获取信息
            # file = 'update_10192_20140304172558.log'
            file_name = file.split('.')[0]
            if file_name.split('_')[0] == 'update':
                (update, model, dt) = file_name.split('_')
            else:
                update = ''
                dt = file_name.split('_')[-1]
                dt = dt + '000000'
                model = '_'.join(file_name.split('_')[1:-1])

            #文件创建时间晚于stop_time,文件跳过不处理
            if int(self.stop_time) < int(dt):
                continue
            else:
                f = open('%s%s%s' % (self.log_path, os.sep, file), 'rb')
                error_count = 0
                error_time = ''
                error_info = ''
                error_line = 0

                #默认文件最后生成时间为now
                last_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                process_error[file] = []

                lines = f.readlines()
                i = 0
                error_list = []

                #获取文件最后生成时间
                for l in xrange(len(lines) - 1, 0, -1):
                    last = re.findall(r'(\S+)-(\S+)-(\S+) (\S+):(\S+):(\S+)\+\d{4} \[\S+\]', lines[l])
                    if last:
                        last_time = ''.join(last[0])
                        break

                #文件创建结束时间早于start_time,文件跳过不处理
                if int(self.start_time) > int(last_time):
                    continue

                #整理错误行号列表
                while i < len(lines):
                    if re.findall(r'\S+-\S+-\S+ \S+:\S+:\S+\+\d{4} \[\S+\] [ERROR|Error]', lines[i]):
                        error_list.append(i)
                        t = []
                        while lines[i + 1].startswith('\t') and (lines[i + 1].strip() != ''):
                            t.append(i + 1)
                            i += 1
                        if t:
                            error_list.append(t)
                    i += 1
                # print(error_list)

                process_error[file] = []
                for index in xrange(0, len(error_list)):
                    if type(error_list[index]) is not list:
                        error_lineno = error_list[index] + 1
                        error_time = ''.join(re.findall(r'(\S+)-(\S+)-(\S+) (\S+):(\S+):(\S+)\+\d{4} \[\S+\] ERROR:',
                                                        lines[error_list[index]])[0])
                        error_info = ''.join(
                            re.findall(r'\S+-\S+-\S+ \S+:\S+:\S+\+\d{4} \[\S+\] (ERROR: .*) <GET.*?>(.*)',
                                       lines[error_list[index]])[0])

                        if process_error[file] and process_error[file][-1]['error_info'] != error_info:
                            process_error[file].append(
                                {'line_no': error_lineno,
                                 'error_time': error_time,
                                 'error_info': error_info,
                                 'error_count': 1,
                                 'Traceback': []})
                        elif process_error[file] and process_error[file][-1]['error_info'] == error_info:
                            process_error[file][-1]['error_count'] += 1
                        else:
                            process_error[file].append(
                                {'line_no': error_lineno,
                                 'error_time': error_time,
                                 'error_info': error_info,
                                 'error_count': 1,
                                 'Traceback': []})

                    else:
                        for i in range(error_list[index][-1], error_list[index][0] + 1, -1):
                            detail = re.findall(r'File "(/home/rose/MStore\S+)", line (\S+), in (\S+)',
                                                lines[i])
                            if detail:
                                # print detail, i,temp['Traceback'][i+1]
                                (error_file, error_file_line, error_function) = detail[0]
                                error_detail = lines[i + 1].strip()
                                (exception, exception_detail) = \
                                    re.findall(r'exceptions.(\S+): (.*)', lines[error_list[index][-1]])[0]
                                # print (exception,exception_detail)
                                process_error[file][-1]['Traceback'].append(
                                    [error_file, error_file_line, error_function, error_detail, exception,
                                     exception_detail])
                                break
        # print process_error

        self.sendemail(process_error, recipients)

    @staticmethod
    def sendemail(data, recipients):
        # msg = MIMEText(
        # report_tpl.replace('{STAT-DATE-RANGE}', stat_date_range).replace('{NEW-PRODUCTS}', new_products).replace(
        #     '{FETCHED-PRODUCTS}', fetched_products),
        # _subtype='html', _charset='utf-8')

        colors = ['#C0C0C0', '#FFFF00', '#FAEBD7', '#7FFFD4', '#00FF00', '#CC99FF', '#FFCC66', '#0099FF']
        report = ''
        for file in data:
            color = random.choice(colors)
            if data[file]:
                for error in data[file]:
                    if error['Traceback']:
                        report += u'<tr><td style="background-color: %s">%s</td><td>' % (color, file) + \
                                  u'</td><td>'.join(
                                      [str(error['line_no']), error['error_time'], str(error['error_count']),
                                       error['error_info'], \
                                       error['Traceback'][0][4], error['Traceback'][0][5], error['Traceback'][0][0],
                                       error['Traceback'][0][1], \
                                       error['Traceback'][0][2], error['Traceback'][0][3]]) + u'</td></tr>'
                    else:
                        report += u'<tr><td style="background-color: %s">%s</td><td>' % (color, file) + \
                                  u'</td><td>'.join(
                                      [str(error['line_no']), error['error_time'], str(error['error_count']),
                                       error['error_info']]) + \
                                  u'<td>无</td>' * 6 + u'</td></tr>'
        # print report

        content = u"""
        <h1>log文件分析报告</h1>
        <table cellpadding="2" cellspacing="0" border="1" bordercolor="#000000">
        <tbody>
            <tr>
                <th>log文件</th>
                <th>错误行号</th>
                <th>错误时间</th>
                <th>错误次数</th>
                <th width="50%">scrapy error</th>
                <th>Traceback ERROR</th>
                <th>Traceback ERROR INFO</th>
                <th>Traceback file</th>
                <th>Traceline</th>
                <th>Traceback function</th>
                <th>Traceback content</th>

            </tr>
                {0}
        </tbody>
        </table>
        """

        msg = MIMEText(unicode.format(content, report), _subtype='html', _charset='utf-8')
        # msg = MIMEMultipart('alternative')
        msg['Subject'] = u'MFashion Logs文件处理报告'
        msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
        msg['To'] = ', '.join([unicode.format(u'{0} <{1}>', item[0], item[1]) for item in recipients.items()])

        server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
        server.login('buddy@mfashion.com.cn', 'rose123')
        server.sendmail('buddy@mfashion.com.cn', recipients.values(), msg.as_string())
        server.quit()

