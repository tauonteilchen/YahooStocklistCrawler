import urllib2
import threading
import sys
from BeautifulSoup import BeautifulSoup
import time
import datetime


#class ING_DIBA_STOCK_CRAWLER(threading.Thread):
#    def __init__(self):
#        threading.Thread.__init__(self)
class ING_DIBA_STOCK_CRAWLER():
    def __init__(self):
        self.URL = "https://wertpapiere.ing-diba.de/DE/showpage.aspx?pageID=29"
    def fetch_stock_count(self):
        page = urllib2.urlopen(self.URL)
        soup = BeautifulSoup(page)
        print(soup)


#crawler = ING_DIBA_STOCK_CRAWLER()
#crawler.fetch_stock_count()

class YAHOO_STOCK_CRAWLER(threading.Thread):
    def __init__(self,page_number=0):
        threading.Thread.__init__(self)
        self.web_index = page_number*20
        self.current_page = page_number
        self.stocks_of_page = []
        self.try_counter = 50
        self.stop = False

    def get_stock_count(self):
        page = urllib2.urlopen('https://de.finance.yahoo.com/lookup/stocks?s=*&t=S&m=ALL&r=%i' % (self.web_index))
        soup = BeautifulSoup(page)
        pagination = soup.find("div", attrs={'id' : 'pagination'}).text
        start = pagination.find("von") + 3
        stop = pagination.find("|")-1
        str_count = pagination[start:stop].replace('.','').replace(' ','')
        return int(str_count)

    def __stop(self):
        self.stop = True

    def run(self):
        stock_count = 0
        cycle = 0
        while cycle < self.try_counter:
            if self.stop:
                return
            page = None
            try:
                page = urllib2.urlopen('https://de.finance.yahoo.com/lookup/stocks?s=*&t=S&m=ALL&r=%i' % (self.web_index))
            except:
                cycle +=1
                time.sleep(0.1)
                continue
            soup = BeautifulSoup(page)

            stocks = []
            stocks.extend(soup.findAll("tr", attrs={'class' : 'yui-dt-odd'}))
            stocks.extend(soup.findAll("tr", attrs={'class' : 'yui-dt-even'}))
            stock_count = len(stocks)
            
            if stock_count == 0:
                cycle +=1
                time.sleep(0.1)
                continue

            for stock in stocks:
                columns = stock.findAll("td")

                innerColumnThree = columns[3].text.replace('.', '').replace(',','.')
                float_price = 0.0
                try:
                    float_price = float(innerColumnThree)
                except:
                    pass
                if float_price == 0.0:
                    continue

                if columns[0].text.isspace() or (columns[0].text == ""):
                    continue
                if columns[2].text.isspace() or (columns[2].text == ""):
                    continue
                stockLine = "%s; %s" % (columns[0].text,columns[2].text)

                self.stocks_of_page.append(stockLine + '\n')
            return


now = datetime.datetime.now()
crawler = YAHOO_STOCK_CRAWLER()
stock_count = crawler.get_stock_count()
page_count = stock_count / 20
print(stock_count)
file_name = "yahoo_stock_list.csv"
csv_file = open(file_name, 'w')
thread_count = 100
threads = []
try:

    for j in range(0,thread_count):
        index = (j)
        page = j
        thread = YAHOO_STOCK_CRAWLER(index)
        threads += [thread]
        time.sleep(0.01)
        thread.start()
    pos = thread_count

    while pos < page_count:
        for i in range(0,len(threads)):
            if not threads[i].is_alive():
                if len(threads[i].stocks_of_page) == 0:
                    sys.stdout.write('-')
                else:
                    sys.stdout.write('|')
                sys.stdout.flush()
                for stock in threads[i].stocks_of_page:
                    csv_file.write(stock)
                    csv_file.flush()
                pos += 1
                if pos > page_count:
                    break
                thread = YAHOO_STOCK_CRAWLER(pos)
                thread.start()
                threads[i] = thread
    for i in range(0,len(threads)):
        threads[i].join()
    for thread in threads:
        for stock in thread.stocks_of_page:
            csv_file.write(stock)
        csv_file.flush()

finally:
    csv_file.close()
    diff = datetime.datetime.now() - now
    print diff

