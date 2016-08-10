import os
import sqlite3
import logging

from halt.util import stringify
from halt.util import objectify
from halt.util import table_columns
from halt.util import do_con
from halt.util import prep_first_time_mash
from halt.util import seperate_mash


def load_column(db, table, *columns, cond=''):
    with sqlite3.connect(db) as con:
        column_str = ', '.join(column for column in columns)
        query = 'select ' + column_str + ' from ' + table + ' ' + cond
        cur = con.cursor()
        cur.execute(query)
        return cur.fetchall()


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
    except sqlite3.InterfaceError:
        cur.close()
        con.close()
        raise

    if commit:
        con.commit()
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

    column_names = table_columns(cur, table)
    column_updates, mash_updates = seperate_mash(updates, column_names)

    mash_updates = {}
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
