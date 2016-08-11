import os
import shutil
import tempfile

import pytest

from halt import insert
from halt import load_column
from halt import delete
from halt import update
from halt import objectify
from halt import HaltException

from test_util import *

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

    def test_insert_with_mash_and_columns(self):
        data = {'Name': 'bob', 'random': 15}
        insert(self.db, 'Test', data, mash=True, commit=True, con=False)
        assert ([('bob', None, '{"random": 15}')] == get_all_data(self.db))

    def test_load_column(self):
        data = {'Name': 'bob', 'Password': 'pass', 'random': 15}
        insert(self.db, 'Test', data, mash=True)
        assert 'bob' == load_column(self.db, 'Test', ('Name',))[0][0]
        assert ('bob', 'pass') == load_column(self.db,
                                             'Test', ('Name', 'Password'))[0]

    def test_delete(self):
        data = {'Name': 'bob', 'Password': 'pass', 'random': 15}
        insert(self.db, 'Test', data, mash=True)
        delete(self.db, 'Test', "where Name == 'bob'")
        assert not get_all_data(self.db)

    @pytest.mark.here
    def test_update_columns(self):
        data = {'Name': 'bob', 'Password': 'pass', 'random': 15}
        insert(self.db, 'Test', data, mash=True)

        new_data = {'Name': 'tom', 'random2': 15}
        update(self.db, 'Test', new_data, mash=False, cond="where Name == 'bob'")
        assert [('tom', 'pass', '{"random": 15}')] == get_all_data(self.db)

    @pytest.mark.here
    def test_update_only_mash(self):
        data = {'Name': 'bob', 'Password': 'pass', 'r': 1, 'r2':2}
        insert(self.db, 'Test', data, mash=True)

        new_data = {'r': 20, 'r3': 3}
        update(self.db, 'Test', new_data, mash=True, cond="where Name == 'bob'")
        results = get_all_data(self.db)
        assert results[0][:2] == ('bob', 'pass')
        dict_cmp({"r": 20, "r2": 2, "r3": 3}, objectify(results[0][2]))

    @pytest.mark.here
    def test_update_with_mash_and_columns(self):
        data = {'Name': 'bob', 'Password': 'pass', 'r': 1, 'r2':2}
        insert(self.db, 'Test', data, mash=True)

        new_data = {'Name': 'tom', 'r': 20, 'r3': 3}
        update(self.db, 'Test', new_data, mash=True, cond="where Name == 'bob'")
        results = get_all_data(self.db)
        assert results[0][:2] == ('tom', 'pass')
        dict_cmp({"r": 20, "r2": 2, "r3": 3}, objectify(results[0][2]))

