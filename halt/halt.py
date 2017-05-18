'''
    Pretty simple databse helper
    MashConfig is a Column that stores a dictionary.
    It will automatically jsonify objects up to depth 1 (not nested)
'''

import os
import sqlite3
import logging

from halt.util import stringify
from halt.util import objectify
from halt.util import table_columns
from halt.util import do_con
from halt.util import prep_first_time_mash
from halt.util import seperate_mash

class HaltException(Exception):pass

def load_column(db, table, columns, cond=''):
    ''' load a column or columns, do not use with mashconfig'''
    assert type(columns) in (list, tuple)
    with sqlite3.connect(db) as con:
        column_str = ', '.join(column for column in columns)
        query = 'select ' + column_str + ' from ' + table + ' ' + cond
        cur = con.cursor()
        cur.execute(query)
        return cur.fetchall()


def load_row(db, table, cond='', headers=True):
    '''load a row/rows, handles mashconfig'''
    with sqlite3.connect(db) as con:
        query = 'SELECT * FROM {} {}'.format(table, cond)
        cur = con.cursor()
        cur.execute(query)
        results =  cur.fetchall()

        # TODO split into function
        column_names = table_columns(cur, table)
        try:
            i = column_names.index('MashConfig')
        except IndexError:
            do_mash = False
            return results
        else:
            do_mash = True

        # Turn the mashconfig into objects
        if do_mash:
            new_results = []
            for row in results:
                if row[i]:
                    new_row = row[:i] + (objectify(row[i]),) + row[i+1:]
                else:
                    new_row = row
                new_results.append(new_row)
            results = new_results

        # Returns a dict of headings to row value
        if headers:
            new_results = []
            for row in results:
                new_row = {header: row[i] for i, header in enumerate(table_columns(cur, table))}
                new_results.append(new_row)
            results = new_results
        return results


def insert(db, table, update, mash=False, commit=True, con=False):
    """
    will error on update, only for creating new rows

    :update: dict will have keys used as columns and values used as row data
    :mash: if you want to do mash config or not
    :commit: change to false to not commit
    :con: pass in a pre existing connection object
    """
    con = do_con(db, con)
    cur = con.cursor()

    # stop mutability
    update = dict(update)
    if mash:
        column_names = table_columns(cur, table)
        update = prep_first_time_mash(column_names, update)

    columns = ', '.join(update.keys())
    placeholders = ':' + ', :'.join(update.keys())
    query = 'insert into %s (%s) VALUES (%s)'\
                        % (table, columns, placeholders)

    try:
        cur.execute(query, update)
        if commit:
            rowid = cur.lastrowid
    except Exception as err:
        cur.close()
        con.close()
        raise HaltException(err)

    if commit:
        con.commit()
        return rowid
    else:
        return con

# Todo commit stuff
def update(db, table, updates, cond='', mash=False, commit=True, con=False):
    """
    updates or creates.
    first all keys that match columns in the table are updated
    if mash is true it will udpate the rest into mash.

    :updates: all key, values to be updates

    WARNING: when mash = True only update one row..
    """
    con = do_con(db, con)
    cur = con.cursor()

    # stop mutability
    updates = dict(updates)

    column_names = table_columns(cur, table)
    column_updates, mash_updates = seperate_mash(updates, column_names)

    if mash:
        query = 'select MashConfig from ' + table + ' ' + cond
        cur.execute(query)
        current_mash = cur.fetchone()[0]
        if current_mash:
            mash_updates = dict(objectify(current_mash), **updates)

    # do the queries
    if mash_updates:
        all_updates = dict(column_updates, **{'MashConfig': mash_updates})
    else:
        all_updates = column_updates
    tupled = [(k, v) for k, v in all_updates.items()]
    placeholders = ', '.join(k + ' =?' for k, v in tupled)
    query = 'UPDATE {} SET {} {}'.format(table, placeholders, cond)
    values = []
    for k, v in tupled:
        if isinstance(v, (tuple, list, dict)):
            values.append(stringify(v))
        else:
            values.append(v)

    cur.execute(query, values)

    if commit:
        con.commit()
    else:
        return con


def delete(db, table, cond=''):
    '''
    deletes rows which match the condition
    '''
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        query = "delete from %s %s" % (table, cond)
        cur.execute(query)
        con.commit()
