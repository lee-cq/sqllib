#!/usr/bin/python3 
# -*- coding: utf-8 -*-
"""
@File Name  : game_num.py
@Author     : LeeCQ
@Date-Time  : 2020/1/13 12:30

https://plugins.jetbrains.com/pluginManager/?action=download&id=DBN&build=PC-191.8026.44&uuid=a59f1549-d4cd-4d46-ab22-be8a4837bcb7
"""
import sqlite3

conn = sqlite3.connect('./sup/test.db')


books = [(1, 1, 'Cook Recipe', 3.12, 1),
         (2, 3, 'Python Intro', 17.5, 2),
         (3, 2, 'OS Intro', 13.6, 2),
         ]

cur = conn.cursor()

# create tables
cur.execute('''CREATE TABLE IF NOT  EXISTS category
      (id int primary key, sort int, name text)''')
cur.execute('''CREATE TABLE IF NOT  EXISTS book 
      (id int primary key,
       sort int,
       name text,
       price real,
       category int,
       FOREIGN KEY (category) REFERENCES category(id))''')

# save the changes
conn.commit()

# close the connection with the database
cur.close()


cur = conn.cursor()
# execute "INSERT"
cur.execute("INSERT INTO category VALUES (1, 1, 'kitchen')")

# using the placeholder
cur.execute("INSERT INTO category (id , sort, name)VALUES (?, ?, ?)", (3, 2, 'computer'))

# execute multiple commands
cur.executemany('INSERT INTO book VALUES (?, ?, ?, ?, ?)', books)

conn.commit()
conn.close()



