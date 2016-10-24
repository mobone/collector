from multiprocessing import Process, Queue
import MySQLdb
from datetime import datetime
from os import listdir, remove
from os.path import isfile, join
from time import sleep
import re
import os
from pandas import read_csv, read_html, DataFrame, to_datetime
import warnings
from random import shuffle

warnings.simplefilter(action = "ignore")

# add request time

computername = os.environ['COMPUTERNAME']
if computername == "APPSERVER":
    path = "c:/to_process/"
    num_cpu = 3
else:
    path = "//appserver/to_process/"
    num_cpu = 8

def connect():
    con = MySQLdb.connect(host="192.168.1.20", user="new_root", passwd="C00kie32!", db="options_test")
    c = con.cursor()
    return (con, c)

def xnull(s):
    s = str(s)
    if s == 'nan':
        return None
    if s == 'quote':
        return None
    return s

def data_cruncher(work_queue):
    (con, c) = connect()
    computername = os.environ['COMPUTERNAME']

    if computername == "APPSERVER":
        path = "c:/to_process/"
    else:
        path = "//appserver/to_process/"

    while (work_queue.qsize()>0):
        filename = work_queue.get()

        try:
            f = open(path+filename,'rt')
            html_text = f.read()
            f.close()
            remove(path+filename)
        except Exception as e:
            continue

        current_work_unit = filename.replace('.html','').split(' ')

        (request_time, update_date, update_time) = (current_work_unit[0], current_work_unit[1], current_work_unit[2])
        (ticker, last_price) = (current_work_unit[3], current_work_unit[4])

        html_text = html_text.replace('<tr class="chainrow acenter understated ', '</table><table class="chainrow acenter understated ')
        html_text = html_text.replace('\n','').replace('\r','')

        tables = html_text.split('<table class="chainrow acenter understated')

        for table in tables:
            table = '<table class="chainrow acenter understated '+str(table)
            date = re.search('(January|February|March|April|May|June|July|August|September|October|November|December)\d{4}',table)
            if date is None:
                continue

            date = date.group(0)

            df = read_html(table, header=0)[0]
            if 'Change' not in df.columns:
                continue
            try:
                cur_price_placement = df[df['Change'].str.contains("Current price")==True]
            except:
                continue
            if len(cur_price_placement)>0:
                df = df.drop(cur_price_placement.index[0])

            df = df[:-1]

            call_table = df[['Last','Change','Vol','Bid','Ask','Open Int.','Strike']]
            put_table = df[['Last.1','Change.1','Vol.1','Bid.1','Ask.1','Open Int..1']]
            call_table.columns = ['Last','Change','Vol','Bid','Ask','Open_Int','Strike']
            put_table.columns = ['Last','Change','Vol','Bid','Ask','Open_Int']
            put_table['Strike'] = call_table.loc[:,['Strike']]

            call_table['Type'] = 'C'
            put_table['Type'] = 'P'

            final_table = call_table.append(put_table)
            final_table['Expiration_Date'] = date
            final_table['Last_Stock_Price'] = last_price
            final_table['Ticker'] = ticker
            final_table['Update_Date'] = datetime.strptime(update_date, '%Y-%m-%d')


            update_time =  update_time.split(".")[:3]
            request_time =  request_time.split(".")[:3]


            final_table['Update_Time'] = datetime.strptime(update_time, '%H.%M.%S')
            final_table['Request_Time'] = datetime.strptime(update_time, '%H.%M.%S')


            db_table = []
            for row in final_table.iterrows():
                row = row[1]
                db_row = []
                for key in final_table.columns:
                    value = xnull(row[key])
                    db_row.append(value)

                db_table.append(tuple(db_row))

            sql = """INSERT INTO marketwatch_data (`Last_Option_Price`, `Change_Option_Price`, `Vol`, `Bid`, `Ask`, `Open_Int`, `Strike`, `Type_Option`,
             `Expiration_Date`, `Last_Stock_Price`, `Ticker`, `Update_Date`,
             `Update_Time`, `Request_Time`)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            #print(sql)
            c.executemany(sql , db_table)
        con.commit()

if __name__ == '__main__':
    work_queue = Queue()

    files_list = [ f for f in listdir(path) if isfile(join(path,f)) ]
    shuffle(files_list)
    for i in files_list:
        work_queue.put(i)

    for i in range(num_cpu):
        p = Process(target = data_cruncher, args = (work_queue,)).start()
        sleep(1)

    print('%s Loaded Queue %i' % (str(datetime.now()), work_queue.qsize()))
        # wait for queue to empty
    while work_queue.qsize()>0:
        sleep(600)
        print('%s Queue Update %i' % (str(datetime.now()), work_queue.qsize()))
