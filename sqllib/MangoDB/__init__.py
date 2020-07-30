#!/usr/bin/python3 
# -*- coding: utf-8 -*-
"""
@File Name  : __init__.py
@Author     : LeeCQ
@Date-Time  : 2020/1/13 14:22

https://www.runoob.com/python3/python-mongodb.html
https://www.runoob.com/mongodb/mongodb-tutorial.html

ORM 框架
"""
import pymongo

client = pymongo.MongoClient("mongodb+srv://root:root123456@test-ivnj5.azure.mongodb.net/test?retryWrites=true&w=majority")
db = client.test
col = db['book']

col.insert_one()