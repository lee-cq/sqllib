# -*- coding: utf-8 -*-
"""
@Time   : 2019/11/26 12:19
@Author : LeeCQ
@File Name: 02_查询.py

语法：
    SELECT column_name,column_name
    FROM table_name
    [WHERE Clause]
    [LIMIT N][ OFFSET M]
"""

import MySQLdb

# 打开数据库连接
db = MySQLdb.connect("localhost", "testuser", "test123", "TESTDB", charset='utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# SQL 查询语句
sql = "SELECT * FROM EMPLOYEE WHERE INCOME > %s" % (1000)
try:

    cursor.execute(sql)  # 执行SQL语句
    # 获取所有记录列表
    results = cursor.fetchall()
    for row in results:
        fname = row[0]
        lname = row[1]
        age = row[2]
        sex = row[3]
        income = row[4]
        # 打印结果
        print("fname=%s,lname=%s,age=%s,sex=%s,income=%s" % \
              (fname, lname, age, sex, income))
except:
    print("Error: unable to fecth data")

# 关闭数据库连接
db.close()
