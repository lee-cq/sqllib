#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 18:32
@Author : LeeCQ
@File Name: 写字节对象-BLOB.py
"""

import pymysql
import sys

# 读取图片文件
# blob最大只能存65K的文件

# fp = open("test.jpg",'rb',encoding='utf-8')
fp = open("./06a01.jpg", 'rb')
img = fp.read()
fp.close()
# 创建连接
conn = pymysql.connect(host='localhost',
                       port=3306,
                       user='root',
                       passwd='123456',
                       db='test',
                       charset='utf8',
                       use_unicode=True, )
# 创建游标
cursor = conn.cursor()
# img = b'12312312'
# 注意使用Binary()函数来指定存储的是二进制
# cursor.execute("INSERT INTO `images` SET `img`= %s" % pymysql.Binary(img))
# print(img)
# sql = "INSERT INTO images (img) VALUES  %s"
# cursor.execute(sql, pymysql.Binary(img))

cursor.execute('insert into images (imgs) values (%s)', [img])

# 提交，不然无法保存新建或者修改的数据
conn.commit()

# 关闭游标
cursor.close()
# 关闭连接
conn.close()
