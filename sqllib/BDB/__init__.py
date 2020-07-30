#!/usr/bin/python3 
# -*- coding: utf-8 -*-
"""
@File Name  : __init__.py
@Author     : LeeCQ
@Date-Time  : 2020/1/13 17:15

https://blog.csdn.net/langkew/article/details/48480069 Berkeley DB

Berkeley DB
Berkeley DB是一个纯Java的开源的文件数据库，介于关系数据库与内存数据库之间，使用方式与内存数据库类似，
它提供的是一系列直接访问数据库的函数，而不是像关系数据库那样需要网络通讯、SQL解析等步骤。

爬虫框架WebCollector就用到了BerkeleyDB。pip
"""

import bsddb3

db = bsddb3.db
db = db.DB()
