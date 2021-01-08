#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Setup module for core.

@File Name  : setup.py
@Author     : LeeCQ
@Date-Time  : 2020/7/30 11:42
"""
import os
from setuptools import setup, find_packages

PACKAGE = "sqllib"
DESCRIPTION = "对MySQL, SQLite等数据库进行接口集成."
AUTHOR = "Lee CQ"
AUTHOR_EMAIL = "lee-cq@qq.com"
URL = "https://leecq.cn"

TOPDIR = os.path.dirname(__file__) or "."
VERSION = __import__(PACKAGE).__version__

with open("readme.md", encoding='utf8') as fp:
    LONG_DESCRIPTION = fp.read()


with open("requirements.txt") as fp:
    requires = fp.read().split('\n')

with open('./updateLog', encoding='utf8') as fp:
    update_log = fp.read()


setup_args = {
    'version': VERSION,
    'description': DESCRIPTION,
    'long_description': LONG_DESCRIPTION,
    'author': AUTHOR,
    'author_email': AUTHOR_EMAIL,
    'license': "Apache License 2.0",
    'url': URL,
    'keywords': ["MySQL", "API", "SQLite", 'DB'],
    'packages': find_packages(exclude=["tests*"]),
    # 'package_data': {'aliyunsdkcore': ['data/*.json', '*.pem', "vendored/*.pem"],
    #                  'aliyunsdkcore.vendored.requests.packages.certifi': ['cacert.pem']},
    'platforms': 'any',
    'install_requires': requires,
    'classifiers': [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development',
    ]
}

setup(name=PACKAGE, **setup_args)
