# coding=utf-8
from utils.utils_core import unicodify, get_logger
from core import RoseVisionDb
import global_settings as gs
import os
import re
import random
import smtplib
from email.mime.text import MIMEText
import datetime


class ParseLog(object):
    """
    分析log文件，报告错误位置
    """

    @classmethod
    def run(cls, logger=None, **param):
        '''
        通过文件名判断文件创建时间、货号、new or update
        不使用文件属性判断，防止文件误删恢复影响创建时间、修改时间
        '''

        logger = logger if 'logger' in param else get_logger()
        logger.info('PARSE LOG STARTED!!!')

        max_err_cnt = 10
        if 'log-path' in param:
            log_path = param['log-path']
        else:
            log_path = os.sep.join((getattr(gs, 'STORAGE_PATH'), 'products', 'log'))

        if 'interval' in param:
            interval = param['interval']
        else:
            interval = 1

        if 'start' in param and 'stop' in param:
            start_time = param['start']
            stop_time = param['stop']
        else:
            cur = datetime.datetime.now()
            to_time = cur.strftime('%Y%m%d%H%M%S')
            from_time = (cur - datetime.timedelta(interval)).strftime('%Y%m%d%H%M%S')
            start_time = from_time
            stop_time = to_time

        # 确定收信人
        try:
            group = getattr(gs, 'REPORTS')['CRAWLER_STATUS']
            if not isinstance(group, list):
                group = [group]
            recipients = {}
            for g in group:
                for key, value in getattr(gs, 'EMAIL_GROUPS')[g].items():
                    recipients[key] = value
                    # recipent_addrs = gs.EMAIL_ADDR.values()  # ['haizi.zh@gmail.com', 'haizi.zh@qq.com']
        except (TypeError, AttributeError, KeyError):
            return

        files = os.listdir(log_path)
        process_error = {}
        for file in files:
            #通过文件名获取信息
            # file = 'update_10192_20140304172558.log'
            file_name = os.path.basename(os.path.splitext(file)[0])
            if os.path.splitext(file)[1] != '.log':
                continue
            # file_name = file.split('.')[0]
            if re.search(r'^update', file_name) and re.findall(r'_\d{14}', file_name):
                tmp = re.split(r'_', file_name)
                if len(tmp) == 2:
                    (update, dt) = tmp
                    model = 'MIX'
                elif len(tmp) == 3:
                    (update, model, dt) = tmp
            elif re.search(r'^\d{5}_', file_name) and re.findall(r'_\d{8}', file_name):
                update = ''
                dt = re.findall(r'_(\d{8})', file_name)[0]
                dt += '000000'
                model = '_'.join(file_name.split('_')[1:-1])

            #文件创建时间晚于stop_time,文件跳过不处理
            if int(stop_time) < int(dt):
                continue
            else:
                f = open('%s%s%s' % (log_path, os.sep, file), 'rb')
                error_count = 0
                error_time = ''
                error_info = ''
                error_line = 0

                #默认文件最后生成时间为now
                last_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                lines = f.readlines()
                i = 0
                error_list = []

                #获取文件最后生成时间
                for l in xrange(len(lines) - 1, 0, -1):
                    last = re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[[^\[\]]*\]', lines[l])
                    if last:
                        last_time = ''.join(last[0])
                        break

                #文件创建结束时间早于start_time,文件跳过不处理
                if int(start_time) > int(last_time):
                    continue

                process_error[file] = []
                #整理错误行号列表
                while i < len(lines):
                    if re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4} \[\S+\] (ERROR|Error)', lines[i]):
                        error_list.append(i)
                        t = []
                        while i < len(lines) - 1 and lines[i + 1].startswith('\t') and (lines[i + 1].strip() != ''):
                            t.append(i + 1)
                            i += 1
                        if t:
                            error_list.append(t)
                    i += 1
                # print(error_list)

                process_error[file] = []
                for index in xrange(0, len(error_list)):
                    if len(process_error[file]) >= max_err_cnt:
                        break

                    if type(error_list[index]) is not list:
                        error_lineno = error_list[index] + 1
                        # 使用下面这个
                        # re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4} \[\S+\] (ERROR: .*)', lines[error_list[index]])
                        error_time = ''.join(
                            re.findall(r'(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\+\d{4} \[\S+\] ERROR:',
                                       lines[error_list[index]])[0])

                        tmp2 = re.findall(
                            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4} \[\S+\] (ERROR: .*) <GET.*?>(.*)',
                            lines[error_list[index]])
                        if tmp2:
                            error_info = ''.join(tmp2[0])
                        else:
                            tmp2 = re.findall(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4} \[\S+\] (ERROR: .*) (.*)',
                                              lines[error_list[index]])
                            if tmp2:
                                error_info = ''.join(tmp2[0])
                            else:
                                error_info = 'UNKNOWN ERROR'

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
                                (error_file, error_file_line, error_function) = detail[0]
                                error_detail = lines[i + 1].strip()
                                if re.findall(r'exceptions.(\S+): ', lines[error_list[index][-1]]):
                                    exception = re.findall(r'exceptions.(\S+): ', lines[error_list[index][-1]])[0]
                                else:
                                    exception = ''
                                if re.findall(r'exceptions.\S+: (.*)', lines[error_list[index][-1]]):
                                    exception_detail = \
                                        re.findall(r'exceptions.\S+: (.*)', lines[error_list[index][-1]])[0]
                                else:
                                    exception_detail = ''
                                process_error[file][-1]['Traceback'].append(
                                    [error_file, error_file_line, error_function, error_detail, exception,
                                     exception_detail])
                                break

        cls.sendemail(process_error, recipients)
        logger.info('PARSE LOG EMAIL SENDED!!!')
        logger.info('PARSE LOG ENDED!!!')

    @staticmethod
    def sendemail(data, recipients):

        colors = ['#C0C0C0', '#FFFF00', '#FAEBD7', '#7FFFD4', '#00FF00', '#CC99FF', '#FFCC66', '#0099FF']
        report = ''
        for file in data:
            color = random.choice(colors)
            if data[file]:
                for error in data[file]:
                    if error['Traceback']:
                        report += u'<tr><td style="background-color: %s">%s</td><td>' % (
                            color, file) + u'</td><td>'.join(
                            [str(error['line_no']), error['error_time'], str(error['error_count']),
                             error['error_info'], error['Traceback'][0][4], error['Traceback'][0][5],
                             error['Traceback'][0][0],
                             error['Traceback'][0][1], error['Traceback'][0][2],
                             error['Traceback'][0][3]]) + u'</td></tr>'
                    else:
                        tmp_str = u'</td><td>'.join([unicode.format(u'{0}', tmp) for tmp in [error['line_no'],
                                                                                             error['error_time'],
                                                                                             error['error_count'],
                                                                                             unicodify(
                                                                                                 error['error_info'])]])

                        report += u'<tr><td style="background-color: %s">%s</td><td>' % (
                            color, file) + u'</td><td>'.join(
                            map(lambda x: unicode(str(x if x is not '' else 'none')), [error['line_no'],
                                                                                       error['error_time'],
                                                                                       error['error_count'],
                                                                                       error['error_info']])) + \
                                  u'<td></td><td></td><td></td><td></td><td></td><td></td></tr>'

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