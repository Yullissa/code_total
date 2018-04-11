# -*- coding: UTF-8 -*-
import requests
import json
import sys
from pymongo import MongoClient
import pymongo

def postmongo(textid,htc_value,htc_key):
    # 127.0.0.1:27017
    client = MongoClient('mongodb://127.0.0.1:27017/')
    '''connect pythondb'''
    # db = client.staticFeature
    db = client.pythondb 
    # post = {"_id":textid,
    #         "htc_keycp":htc_key,
    #         "htc_valuecp":htc_value}
    # posts = db.posts
    # post_id = posts.insert_one(post).inserted_id
    # collection.update({'_id':ObjectId(id),'$inc':{'hit':1}})

    # for item in db.document.find({"_id":textid}):
    #     print (item)
    db.posts.insert_one({"_id":textid},{"$set":{'adf':htc_value,"asdf":htc_key}})
    return 

def main():
    with open("text.txt","rb") as fileread:
        for eachline in fileread:            
            textdata = json.loads(eachline)
            textid = textdata['_id']
            ctype = textdata['ctype']
            print (ctype)
            if ctype != "video": 
                print (textid)
                s = requests.Session()
                res = s.post("http://10.120.180.22:9093/service/topic_cluster",data = eachline)
                # res = '\''+res.text+'\''
                res = res.text
                keyresult = json.loads(res)
                htc_value = keyresult['htc_value']
                print ("htc_value = ",htc_value)
                htc_key = keyresult['htc_key']
                print("htc_key =  " ,htc_key)
                postmongo(textid,htc_value,htc_key)
            

if __name__ == '__main__':    
    # main(sys.argv[1])
    main()