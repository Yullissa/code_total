#!/usr/bin/python3.5
# -*- coding: utf-8 -*-
import json
import sys
import subprocess
import datetime
import requests
import traceback
import pymongo
import random
import time
from concurrent.futures import ProcessPoolExecutor


def get_htc_key_value(chunk):
    coll = pymongo.MongoClient('10.103.17.110:27017,10.103.17.113:27017,10.103.17.122:27017,10.103.17.142:27017,'
                               '10.103.33.134:27017,10.120.15.25:27017,10.120.16.14:27017,10.120.16.16:27017').staticFeature.document
    s = requests.Session()
    success = 0
    try:
        last = 'None'
        for line in chunk:
            host = random.choice(['10.136.29.27', '10.136.29.26', '10.120.181.3', '10.120.180.27', '10.120.180.22'])
            url = 'http://{}:9093/service/topic_cluster'.format(host)
            doc = json.loads(line)
            docid = doc.get('_id')
            last = docid
            if docid is None:
                print('{} have no docid!'.format(line), file=sys.stderr, flush=True)
                continue
            ctype = doc.get('ctype', 'news')
            if ctype == 'video':
                continue
            res = s.post(url, data=line.encode('utf8'),
                         timeout=10)
            data = json.loads(res.text)
            keys = data.get('htc_key', [[], []])
            vals = data.get('htc_value', [[], []])
            # print('docid {} htc_key {} htc_vals {}'.format(docid, keys, vals), flush=True)
            coll.update({'_id': docid}, {'$set': {'htc_key': keys, 'htc_value': vals}})
            # print('update {} done'.format(docid), file=sys.stderr, flush=True)
            success += 1
        print('my work done last docid {}'.format(last), file=sys.stderr, flush=True)
    except Exception as e:
        print('{} {} {}'.format(datetime.datetime.now(), e, traceback.format_exc()), file=sys.stderr,
              flush=True)
    return success


def main():
    hdfs_path = '/user/azkaban/camus/{{indata_str_documents_info,indata_str_vertical_documents}}/hourly/{}/*/*'
    st = datetime.datetime(2017, 6, 1)
    ed = datetime.datetime(2018, 3, 8)
    delt = datetime.timedelta(days=1)
    ct = 0
    doc_lst = []
    jobs = []
    with ProcessPoolExecutor(10) as executor:
        while st <= ed:
            dt = ed.strftime('%Y-%m-%d')
            p = subprocess.Popen('hadoop fs -text {}'.format(hdfs_path.format(dt)), shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            while p.poll() is None:
                try:
                    line = p.stdout.readline()
                    if isinstance(line, bytes):
                        line = line.decode('utf8')
                    line = line.strip()
                    if line:
                        doc_lst.append(line)
                        if len(doc_lst) >= 100:
                            while len(jobs) > 20:
                                njobs = []
                                for ft in jobs:
                                    if ft.done():
                                        ct += ft.result()
                                    else:
                                        njobs.append(ft)
                                jobs = njobs
                                print('wait some jobs done', file=sys.stderr, flush=True)
                                time.sleep(1)
                            print('{} now {} doc success! '.format(datetime.datetime.now(), ct), file=sys.stderr,
                                  flush=True)
                            jobs.append(executor.submit(get_htc_key_value, doc_lst))
                            doc_lst = []
                    else:
                        p.terminate()
                except Exception as e:
                    print('{} {} {}'.format(datetime.datetime.now(), e, traceback.format_exc()), file=sys.stderr,
                          flush=True)
            if p.returncode == 0:
                print('Subprogram success')
            else:
                print('Subprogram failed')
            print('now finish {}'.format(dt), file=sys.stderr, flush=True)
            ed -= delt
        if len(doc_lst) > 0:
            jobs.append(executor.submit(get_htc_key_value, doc_lst))
        while len(jobs) > 0:
            njobs = []
            for ft in jobs:
                if ft.done():
                    ct += ft.result()
                else:
                    njobs.append(ft)
            jobs = njobs
            print('wait some jobs done', file=sys.stderr, flush=True)
            time.sleep(1)


if __name__ == '__main__':
    main()