from pandas import read_html, read_csv
import requests
import re
from multiprocessing import Process, Queue
from datetime import datetime
from threading import Thread
import urllib.request, urllib.error, urllib.parse
from time import sleep, time
import MySQLdb
import warnings

warnings.simplefilter(action = "ignore")

# add request time and write to disk after queue is empty

def marketOpen():
    now = datetime.now()

    openTime = now.replace(hour=8,minute=31,second=0,microsecond=0)
    closeTime = now.replace(hour=15,minute=0,second=0,microsecond=0)
    if now>=openTime and now<closeTime:
        return True
    else:
        return False

def get_tickers():

    print("%s getting tickers" % str(datetime.now()))
    page = urllib.request.urlopen("ftp://ftp.nasdaqtrader.com/SymbolDirectory/options.txt")
    df = read_csv(page,sep="|")

    dfs = df.groupby('Underlying Symbol')
    tickers = []
    for df in dfs:
        if len(df[1])>10:
            tickers.append(df[0])

    print(len(tickers))

    f = open('C:/Users/Nick/OneDrive/collector/tickers.csv','w')

    for ticker in tickers:
            f.write(ticker+'\n')
    f.close()
    print("%s getting tickers complete" % str(datetime.now()))


def network_worker(ticker_queue):
    s = requests.session()
    request_time = None

    data_list = []
    while marketOpen() or ticker_queue.qsize()>0:
        if request_time == None:

            request_time = str(datetime.now()).split(".")[0]
            request_time = request_time.split(" ")[1]
        ticker = ticker_queue.get()
        ticker = ticker.replace('\n','')

        start = -1
        end = -1
        price = -1
        trycount = 0

        while (start == -1 or end == -1) and trycount<5:
            trycount += 1
            update_time = str(datetime.now()).split(".")[0]
            try:
                html_page = s.get('http://www.marketwatch.com/investing/stock/%s/options?countrycode=US&showAll=True' % ticker.replace('-','.'))

            except Exception as e:
                print(e)
                continue

            if 'No Option Chain found' in html_page.text or 'Symbol Lookup' in html_page.text:
                break

            html_text = html_page.text.encode('utf-8').strip()

            start = html_text.find('<p class="data bgLast">'.encode('utf-8'))
            end = html_text.find('</p>'.encode('utf-8'), start)

            try:
                price = float(html_text[start+23:end])
            except:
                continue



            start = html_text.find('<p class="data bgLast">'.encode('utf-8'))
            end = html_text.find('</table>'.encode('utf-8'), start)

            if (start == -1 or end == -1) and trycount>3:
                print("ERROR " + ticker)


        if start != -1 and end != -1 and price != -1:
            #work_queue.put((str(datetime.now()).replace(':','.'), ticker, price, html_page.text[start:end+9]))
            filename = 'c:/to_process/%s %s %s %s.html' % ((request_time.replace(":","."), update_time.replace(":","."), ticker, price))

            data_list.append((filename, html_page.text[start:end+9]))

        if ticker_queue.qsize()==0:
            request_time = None
            while len(data_list)>0:
                i = data_list.pop()
                f = open(i[0], 'w')
                f.write(i[1])
                f.close()





if __name__ == '__main__':

    get_tickers()


    ticker_queue = Queue()


    # wait for market open

    while marketOpen() == False:
        sleep(1)

    for i in range(35):
        t = Thread(target=network_worker, args = (ticker_queue,)).start()
        sleep(.1)

    #for i in range(3):
        #p = Process(target = data_cruncher, args = (work_queue,)).start()

    while marketOpen():
        f = open('C:/Users/Nick/OneDrive/collector/tickers.csv', 'r')

        for i in f.readlines():
            ticker_queue.put(i)
        f.close()
        #print "New queue length: "+str(ticker_queue.qsize())

        now = time()
        while ticker_queue.qsize()>0:
            sleep(1)
            #print str(work_queue.qsize()) + ' ',

        print('\n%s Queue completed in %i seconds' % (str(datetime.now()), time()-now))

        while time()-now < 900:
            #print str(work_queue.qsize()) + ' ',
            sleep(1)

    while ticker_queue.qsize()>0:
        sleep(1)

    del(ticker_queue)
