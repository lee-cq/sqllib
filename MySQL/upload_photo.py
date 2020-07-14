#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File Name  : upload_photo.py
@Author     : LeeCQ
@Date-Time  : 2019/12/5 13:48

<<<<<<< HEAD
上传图片数据到Tencent MySQL；

"""
import time
import os
import _md5

from tencent_MySQL import TencentMySQL


class UploadPhoto:
    DIR_ROOT = 'D:/H1/照片整理/'

    def __init__(self):
        self._sql = TencentMySQL('root', 'lichao187', 'test')

    def __get_dir_path(self):
        """"""
        return [self.DIR_ROOT + _p for _p in os.listdir(self.DIR_ROOT)]

    def __get_photo_path(self):
        os.path.isdir()

    @staticmethod
    def __read_photo(photo_path):
        """"""
        with open(photo_path, 'rb') as fp:
            return fp.read()

    def upload_photo(self, photo_path):
        """

        :param photo_path:
        :return:
        """
        _s = time.time()
        data = self.__read_photo(photo_path)
        path, name = os.path.split(photo_path)
        md5 = _md5.md5(data).hexdigest()
        print('_MD5 : ', time.time() - _s)

        self.create_table()
        res = self._sql.insert('photo', MD5=md5, NAME=name, PATH=path, DATA=data)
        print('UPLOAD: ', time.time() - _s)

        return res

    def create_table(self):
        command = ('CREATE TABLE IF NOT EXISTS `photo` ( '
                   'MD5     CHAR(32)    UNIQUE, '
                   'NAME    VARCHAR(99), '
                   'PATH    VARCHAR(99), '
                   'DATA    MEDIUMBLOB'
                   ')')
        return self._sql.write_db(command)


if __name__ == '__main__':
    a = UploadPhoto()
    print(a.upload_photo('D:/H1/照片整理/最美家园.jpg'))

