#!/usr/bin/python3 
# -*- coding: utf-8 -*-
"""
@File Name  : test_1.py
@Author     : LeeCQ
@Date-Time  : 2020/1/19 22:00

"""
import json
# t1 = json.load(open(r'C:\code\Py\HTTP\Download_9C\data\1.txt'))
import pymongo
client = pymongo.MongoClient("mongodb+srv://root:root123456@test-ivnj5.azure.mongodb.net/test?retryWrites=true&w=majority")
db = client.test
collect = db['9c']


