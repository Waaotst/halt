import logging
import sqlite3
import json

def stringify(obj):
    return json.dumps(obj)


def objectify(string):
    '''turn a str back into its object form'''
    return json.loads(string)


def table_columns(cur, table):
    '''returns all table columns'''
    cur.execute('select * from %s' % table)
    return [f[0] for f in cur.description]


def prep_first_time_mash(column_names, update):
    mash = {}
    for thing in dict(update):
        if thing not in column_names:
            mash[thing] = update[thing]
            del update[thing]
    update['MashConfig'] = stringify(mash)
    return update


def seperate_mash(updates, column_names):
    '''
    returns a dict with updates for all columns
        and a dict with updates for mash
    '''
    column_updates = {}

    for column in column_names:
        if column in updates:
            column_updates[column] = updates[column]
            del updates[column]
    mash_updates = dict(updates)
    return column_updates, mash_updates



def do_con(db, con):
    if not con:
        con = sqlite3.connect(db)
    return con
