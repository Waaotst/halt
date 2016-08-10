import os
import shutil
import sqlite3
import tempfile

import pytest

from halt import insert

def dbcreate(db):
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        cur.execute('pragma foreign_keys = off')
        cur.execute("CREATE TABLE Test(Name Text, Password Text, MashConfig)")
        con.commit()

def get_all_data(db):
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        cur.execute('select * from Test')
        return cur.fetchall()

class TestHalt():

    def setup_class(cls):
        cls.tdir = tempfile.mkdtemp()

    def teardown_class(cls):
        shutil.rmtree(cls.tdir)

    def setup_method(self, func):
        '''create fresh database for each test method'''
        db = os.path.join(self.tdir, func.__name__+'.db')
        try:
            os.remove(db)
        except FileNotFoundError:
            pass
        dbcreate(db)
        self.db = db

    def test_insert(self):
        data = {'Name': 'bob', 'Password': 'password'}
        insert(self.db, 'Test', data, mash=False, commit=True, con=False)
        assert ([('bob', 'password', None)] == get_all_data(self.db))

    def test_insert_only_mash(self):
        data = {'random': 15}
        insert(self.db, 'Test', data, mash=True, commit=True, con=False)
        assert ([(None, None, '{"random": 15}')] == get_all_data(self.db))

    def test_isnert_with_mash_and_columns(self):
        data = {'Name': 'bob', 'random': 15}
        insert(self.db, 'Test', data, mash=True, commit=True, con=False)
        assert ([('bob', None, '{"random": 15}')] == get_all_data(self.db))



