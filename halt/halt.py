import os
import sqlite3
import logging

from halt.util import stringify
from halt.util import objectify
from halt.util import table_columns
from halt.util import dict_to_2tuple
from halt.util import do_con


def load_column(db, table, *column, cond):
    '''
    yields the results
    '''
    with sqlite3.connect(db):
        query = 'select ' + column + ' from ' + table + ' ' + condition
        cur = con.cursor()
        cur.execute(query)
        yield cur.fetchall()


def _prepare_insert_with_mash(column_names, update):
    mash = {}
    for thing in dict(update):
        if thing not in column_names:
            mash[thing] = update[thing]
            del update[thing]
    update['MashConfig'] = stringify(mash)
    return update


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
        update = _prepare_insert_with_mash(column_names, update)

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


def update(db, table, update, cond, mash=False):
    """
    updates first all columns that are in the dict,
    if mash is true it will udpate the rest into mash.
    """

    con = _con(con)
    cur = con.cursor()

    column_updates = {}

    for column in table_columns(cur, table):
        if column in update:
            column_updates[column] = update[column]
            del update[column]


    # all the is left in update will be mashed
    mash_updates = {}
    if mashconfig:
        query = 'select MashConfig from ' + table + ' ' + condition
        cur.execute(query)
        current_mash = cur.fetchone()[0]
        if current_mash:
            mash_updates = dict(objectify(current_mash)).update(update)


    # do the queries
    if mash_updates and not column_updates:
        update_str = stringify(dict_to_2tuple(mash_updates))
        query = 'UPDATE ' + table + ' SET MashConfig= (?) ' + condition
        cur.execute(query, (update_str, ))
    else:
        update_values = stringify(dict_to_2tuple(column_updates))
        cur.execute(query, obj)
        con.commit()


def delete(db, table, cond):
    '''
    deletes rows which match the condition
    '''
    with sqlite3.connect(db):
        cur = con.cursor()
        query = "delete from %s %s" % (table, cond)
        cur.execute(query)
        con.commit()
