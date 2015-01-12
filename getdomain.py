# -*- coding: utf-8 -*-

import requests
import urlparse
import task
import Queue
import threading
from BeautifulSoup import BeautifulSoup
from pymongo import Connection


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

header = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWeb'
                        'Kit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25'}
requests.adapters.DEFAULT_RETRIES = 100


def getURL(url, worker_Queue):
    con = Connection()
    db = con.domain
    posts = db.url
    if url.startswith('http'):
        try:
            res = requests.get(url, headers=header, timeout=5)
            # url = etree.HTML(res.content).xpath('//@href')
            soup = BeautifulSoup(res.content).findAll()
            lasturl = []
            for u in soup:
                c_url = u.get('href')
                try:
                    if c_url.startswith('http'):
                        parse = urlparse.urlparse(c_url)
                        c_url1 = parse.scheme + '://' + parse.hostname
                        lasturl.append(c_url1)
                except:
                    pass
            for i in set(lasturl):
                dict1 = {'domain': "%s" % i}
                post = posts.find_one(dict1)
                if not post:
                    posts.insert(dict1)
                    print i
                task.crawler_worker.delay(i)
                print i
                worker_Queue.put(i)
        except requests.Timeout:
            pass
        except requests.ConnectionError, e:
            print url, e
        finally:
            con.close()


def workerQueue(worker_Queue):
    while True:
        url = worker_Queue.get()
        getURL(url, worker_Queue)


def main():
    worker_Queue = Queue.Queue()
    url1 = raw_input('baseURL:')
    num_threads = 100
    for i in range(num_threads):
        t = threading.Thread(target=workerQueue, args=(worker_Queue,))
        t.daemon = True
        t.start()
    worker_Queue.put(url1)
    worker_Queue.join()


if __name__ == '__main__':
    main()