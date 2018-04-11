
# -*- coding: UTF-8 -*-
import requests
import json
import sys
import pymongo
from pymongo import MongoClient

def postmongo(textid,htc_value,htc_key):
    client = MongoClient('mongodb://10.103.17.110:27017,10.103.17.113:27017,10.103.17.122:27017,10.103.17.142:27017,10.103.33.134:27017,10.120.15.25:27017,10.120.16.14:27017,10.120.16.16:27017')
    db = client.staticFeature
    # for item in db.document.find({"_id":textid}):
    #     print(item)
    db.document.update({"_id":textid},{"$set":{'htc_value':htc_value,"htc_key":htc_key}})
    return

def main():
    for eachline in sys.stdin:
        try:
            # print(eachline)
            textdata = json.loads(eachline)
            ctype = textdata['ctype']
            # print (ctype)
            if ctype != "video":
                textid = textdata['_id']
                s = requests.Session()
                res = s.post("http://10.120.180.22:9093/service/topic_cluster",data = eachline.encode('utf-8'))
                res = res.text
                htcresult = json.loads(res)
                htc_value = htcresult['htc_value']
                htc_key = htcresult['htc_key']
                # print("htc_key =  " ,htc_key)
                # print ("htc_value = ",htc_value)
                postmongo(textid,htc_value,htc_key)
        except:
            print("wrong in ", eachline)
            pass

if __name__ == '__main__':    
    # main(sys.argv[1])
    main()
