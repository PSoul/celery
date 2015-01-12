#/bin/env python2.7
# -*- coding: utf-8 -*-

BACKEND_URL = 'redis://localhost:6379/0'
BROKER_URL = 'redis://localhost:6379/0'

header = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWeb"
                        "Kit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"}

from pymongo import Connection
from gevent import socket
from celery import Celery
from lxml import etree
import urlparse
import requests


socket.setdefaluttimeout = 2
requests.adapters.DEFAULT_RETRIES = 20
app = Celery(backend=BACKEND_URL, broker=BROKER_URL)


@app.task(ignore_result=True)
def crawler_worker(url):
    con = Connection()
    db = con.domain
    posts = db.url
    try:
        try:
            ip = socket.gethostbyname(urlparse.urlparse(url).hostname)
        except:
            ip = ''
        ip_dict = {'ip': ip}
        res = requests.get(url, headers=header, timeout=3)
        update_dict = {'domain': "%s" % url}
        if res.status_code == 400:
            pass
            # print url + res.content
        elif res.status_code:
            try:
                chars = filter(lambda x: 'charset' in x.lower(), etree.HTML(res.content).xpath('//meta/@content'))[0].split('=')[1]
            except:
                try:
                    chars = filter(lambda x: 'charset' in x.lower(), etree.HTML(res.content).xpath('//script/@charset'))[0].split('=')[1]
                except:
                    chars = 'utf-8'
            posts.update(update_dict, {"$set": {'chars': chars}})
            res.encoding = chars
            try:
                title = etree.HTML(res.content).xpath('//title')[0].text
            except:
                try:
                    title = etree.HTML(res.content).xpath('//')[0].text
                except:
                    title = ''
            posts.update(update_dict, {"$set": ip_dict})
            posts.update(update_dict, {"$set": {'title': title}})
            posts.update(update_dict, {"$set": dict(res.headers)})
    except requests.ConnectionError:
        pass
    except requests.Timeout:
        print 'timeout'
    finally:
        con.close()