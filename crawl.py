#!/usr/bin/env python3

import urllib
import urllib.request as rq
import sqlite3 as sq
import datetime as dt
import json
import sys
from optparse import OptionParser
import multiprocessing as mp

def crawl(url):
    url = url.strip()
    rsp = None
    try:
        rsp = rq.urlopen(url)
        rc = rsp.read()
        hdrs = rsp.getheaders()
    except Exception as e:
        return {"ok":False, "url":url, "x":e}
    finally:
        if(rsp): rsp.close()
    sphead = {"Date", "Content-Type", "Content-Length", "Server", "Set-Cookie"}
    res = {"ok":True, "url":url, "status":rsp.status, "statex":rsp.reason, "Date":None, "Content-Type":None, "Content-Length":None, "Server":None, "Set-Cookie":[], "headers":{}, "content":rc}
    for hnm, hvl in hdrs:
        if hnm in res:
            addto = res
        else:
            addto = res["headers"]
        if hnm in addto and addto[hnm] is not None:
            if type(addto[hnm]) is list:
                addto[hnm].append(hvl)
            else:
                addto[hnm] = [addto[hnm],hvl]
        else:
            addto[hnm] = hvl
#    if type(res["Date"]) is str:
#        try:
#            res["Date"] = dt.datetime.strptime(res["Date"], "%a, %d %b %Y %H:%M:%S %Z")
#        except ValueError:
#            res["Date"] = None
#    else:
#        res["Date"] = None
    if res["Content-Length"] is None:
        res["ContentLength"] = len(rc)
    else:
        res["ContentLength"] = res["Content-Length"]
    res["ContentType"]=res["Content-Type"]
    res["headers"]=json.dumps(res["headers"], check_circular=False, sort_keys=True, separators=(",",":"))
    res["SetCookie"]=json.dumps(res["Set-Cookie"], check_circular=False, separators=(",",":"))
    del res["Content-Length"]
    del res["Content-Type"]
    del res["Set-Cookie"]
    return res

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-j", "--jobs", dest="jobcnt", type="int", default=4, metavar="N", help="use N jobs for crawling (default 4)")
    opts, _ = parser.parse_args()
    with mp.Pool(processes=opts.jobcnt) as pool:
        res = pool.map(crawl, sys.stdin)
    db = sq.connect("meta.db")
    try:
        with db:
            c = db.cursor()
            c.execute("SELECT * FROM sqlite_master WHERE name='meta' AND type='table'")
            if c.fetchone() is None:
                c.execute("CREATE TABLE meta (url text, status integer, statex string, date text, ctype text, clen integer, server text, cookies text, headers text, cont blob)")
            for row in res:
                if row["ok"]:
                    c.execute('''INSERT INTO meta
                                VALUES (:url, :status, :statex, :Date, :ContentType, :ContentLength, :Server, :SetCookie, :headers, :content)''', row)
                else:
                    print("Exception occurred while processing", row["url"], ":", row["x"].args[0], file=sys.stderr)
    except sq.Error as e:
        print("A database error occurred:", e.args[0], file=sys.stderr)
    #for url in sys.stdin:
        #crawl(url.strip())
