import sqlite3

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


def dict_cmp(a, b):
    for k, v in a.items():
        assert v == b[k]
    for k, v in b.items():
        assert v == a[k]
