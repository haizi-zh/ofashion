# coding=utf-8
from HTMLParser import HTMLParser

__author__ = 'Zephyre'

import urllib2
import ZenithWatch

def y3continents(url, tag, sel, fetchStore):
    """
    获得y-3的洲信息
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url)
    html = response.read()

    class y3continentsParser(HTMLParser):
        """
        处理y-3的洲信息
        """

        def __init__(self, classTag, selTag, excludeTag):
            HTMLParser.__init__(self)
            self.__isSelectRegionOpen = False
            self.__isProcessing = False
            self.__continentUrl = None
            self.continentData = {}
            self.storeData = []

            self.classTag = classTag
            self.selTag = selTag
            self.excludeTag = excludeTag
            # 处理商店信息
            self.procStore = False
            self.tmpKey = None
            self.parsingAddr = False

        def handle_endtag(self, tag):
            HTMLParser.handle_endtag(self, tag)
            self.__isSelectRegionOpen = False
            self.__continentUrl = None
            if self.__isProcessing:
                if self.procStore:
                    if tag.__eq__('li'):
                        # 完成一个商店条目
                        # 去除多余空格
                        for k in self.storeItem.keys():
                            self.storeItem[k] = self.storeItem[k].strip()
                        self.storeData.append(self.storeItem)
                        self.storeItem = None
                    elif tag.__eq__('p'):
                        # 结束地址读取
                        self.parsingAddr = False
                        self.tmpKey = None
                if tag.__eq__('ul'):
                    # 无论是在哪种模式下，遇到/ul都结束
                    self.__isProcessing = False

        def handle_startendtag(self, tag, attrs):
            HTMLParser.handle_startendtag(self, tag, attrs)
            self.__isSelectRegionOpen = False
            self.__continentUrl = None

            if self.parsingAddr and tag.__eq__('br'):
                # 正在读取地址
                self.storeItem['Address'] += '\r\n'
                self.tmpKey = 'Address'

        def handle_starttag(self, tag, attrs):
            HTMLParser.handle_starttag(self, tag, attrs)

            # 添加attrs字典
            attrDict = {}

            def attrDictHlp(val):
                attrDict[val[0].strip()] = val[1].strip()

            map(attrDictHlp, attrs)
            if 'class' in attrDict and attrDict['class'].__eq__(self.classTag):
                if self.procStore:
                    self.__isProcessing = True
                else:
                    self.__isSelectRegionOpen = True
                return
            elif self.__isProcessing and self.procStore:
                # 处理商店信息
                if tag.__eq__('li'):
                    # 新建一个商店
                    self.storeItem = {}
                elif 'class' in attrDict and attrDict['class'].__eq__('store-name'):
                    self.tmpKey = 'Name'
                elif 'class' in attrDict and attrDict['class'].__eq__('store-address'):
                    self.parsingAddr = True
                    self.tmpKey = 'Address'
            elif self.__isProcessing and 'href' in attrDict and not self.procStore:
                # 找到大陆信息
                self.__continentUrl = attrDict['href']
                return

            self.__isSelectRegionOpen = False
            self.__continentUrl = None

        def handle_data(self, data):
            """

            """
            data = data.strip()
            if self.__isSelectRegionOpen and data.__eq__(self.selTag):
                self.__isProcessing = True
            if self.__continentUrl is not None and not data.__eq__(self.excludeTag):
                self.continentData[data] = self.__continentUrl
            if self.procStore and self.__isProcessing and self.tmpKey is not None:
                # 准备添加条目
                if self.tmpKey in self.storeItem:
                    self.storeItem[self.tmpKey] += data.strip()
                else:
                    self.storeItem[self.tmpKey] = data.strip()
                self.tmpKey = None

            self.__isSelectRegionOpen = False
            self.__continentUrl = None

    parser = y3continentsParser(tag, sel, "Y-3 Stores")
    parser.procStore = fetchStore
    parser.feed(html)
    if fetchStore:
        return parser.storeData
    else:
        return parser.continentData


def run():

    def procCity(url):
        try:
            stores = y3continents(url, 'store-list', None, True)
        except urllib2.HTTPError:
            print ('Error loading %s' % url)
            return []
        print(stores)
        return stores

    def procCountry(url):
        try:
            result = y3continents(url, 'store-finder-filter-title', 'Select City', False)
        except urllib2.HTTPError:
            print ('Error loading %s' % url)
            return []
        cities = result.keys()
        stores = []
        for c in cities:
            print 'Fetching in city: %s' % c
            stores.extend(procCity(result[c]))
        return stores

    def procRegion(url):
        try:
            result = y3continents(url, 'store-finder-filter-title', 'Select Country', False)
        except urllib2.HTTPError:
            print ('Error loading %s' % url)
            return []

        countries = result.keys()
        stores = []
        for c in countries:
            print 'Fetching in country: %s' % c
            stores.extend(procCountry(result[c]))
        return stores

    def procRoot():
        url = 'http://www.y-3.com/store-finder'
        result = y3continents(url, 'store-finder-filter-title', 'Select Region', False)
        regions = result.keys()
        stores = []
        r = 'Asia'
        # for r in regions:
        print 'Fetching in region: %s' % r
        stores.extend(procRegion(result[r]))
        return stores

    # storeList = procRoot()
    storeList = procRegion('http://www.y-3.com/store-finder/asia')
    print(storeList)
    # url = 'http://www.y-3.com/store-finder/europe/de/berlin'
    # print(y3continents(url, 'store-list', None, True))
