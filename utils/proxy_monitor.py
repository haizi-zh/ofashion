# coding=utf-8
import urllib
import smtplib
from email.mime.text import MIMEText


def send_email(region):
    content = u"""
            region:{region} proxy error found!!!!
            """.format(region=region)
    msg = MIMEText(content, _subtype='html', _charset='utf-8')

    mailto_list = ["jingwei.zheng@qq.com"]
    msg['Subject'] = u'{region} Proxy Error!'.format(region=region)
    msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
    msg['To'] = ', '.join(mailto_list)

    server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
    server.login('buddy@mfashion.com.cn', 'rose123')
    server.sendmail('buddy@mfashion.com.cn', mailto_list, msg.as_string())
    server.quit()


def proxy_monitor():
    # us_proxy = {'http': 'http://173.255.255.30:8888'}
    base_url = {'cn': 'zh_CN', 'us': 'en_US', 'fr': 'fr_FR', 'uk': 'en_GB', 'jp': 'ja_JP', }
    PROXIES = [
        {'us': "http://173.255.255.30:8888"},  #linode-Fremont, CA, USA:
        {'jp': "http://106.187.97.29:8888"},  #linode-Tokyo, JP:
        {'uk': "http://178.79.128.219:8888"},  #linode-London, UK:
        {'fr': "http://176.58.90.76:8888"},  #hostvirtual-Paris, France:
        # {'ca':"http://162.248.221.119:8888"},       #hostvirtual- Toronto, Canada:
    ]

    for p in PROXIES:
        new_url = "http://secure.chanel.com/global-service/frontend/pricing/%s/fashion/A92216Y25912/?format=json" % \
                  base_url[p.keys()[0]]

        try:
            t = urllib.urlopen(new_url, proxies={'http': p.values()[0]})
            print t.read(), t.getcode()
        except:
            send_email(p.keys()[0])


if __name__ == '__main__':
    proxy_monitor()