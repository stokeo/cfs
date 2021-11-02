'''
database.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''

from .logging import logging  # Ensure use of custom logger class
from cassandra.cluster import Cluster

log = logging.getLogger(__name__)


class Connection(object):
    '''
    This class wraps a Cassandra connection object. It should be used instead of any
    native Cassandra cursor

    It provides methods to directly execute CQL commands and creates Cassandra
    cursors dynamically.

    Instances are not thread safe. They can be passed between threads,
    but must not be called concurrently.

    Attributes
    ----------

    :conn:     apsw connection object
    '''

    def __init__(self, file_=None, hosts=None):
        """ file_ argument useful for sqlite or other file based database """
        if hosts:
            self.cluster = Cluster(hosts)
        else:
            self.cluster = Cluster()
        self.conn = self.cluster.connect("cfs")

    def close(self):
        # return
        self.conn.shutdown()

    def get_size(self):
        '''Return size of database file'''
        return 0

    def query(self, *a, **kw):
        '''Return iterator over results of given statement

        If the caller does not retrieve all rows the iterator's close() method
        should be called as soon as possible to terminate the SQL statement
        (otherwise it may block execution of other statements). To this end,
        the iterator may also be used as a context manager.
        '''
        return ResultSet(self.conn.execute(*a, **kw))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows '''
        return self.conn.execute(*a, **kw)

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""
        return

    def has_val(self, *a, **kw):
        '''Execute statement and check if it gives result rows'''

        res = self.conn.execute(*a, **kw)
        for row in res:
            return True
        return False

    def get_val(self, *a, **kw):
        """Execute statement and return first element of first result row.

        If there is no result row, raises `NoSuchRowError`. If there is more
        than one row, raises `NoUniqueValueError`.
        """

        return self.get_row(*a, **kw)[0]

    def get_list(self, *a, **kw):
        """Execute select statement and returns result list"""

        return list(self.query(*a, **kw))

    def get_row(self, *a, **kw):
        """Execute select statement and return first row.

        If there are no result rows, raises `NoSuchRowError`. If there is more
        than one result row, raises `NoUniqueValueError`.
        """

        res = self.conn.execute(*a, **kw)
        final_res = None
        for row in res:
            if final_res:
                raise NoUniqueValueError()
            final_res = row

        if not final_res:
            raise NoSuchRowError()

        return final_res


class NoUniqueValueError(Exception):
    '''Raised if get_val or get_row was called with a query
    that generated more than one result row.
    '''

    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    '''Raised if the query did not produce any result rows'''

    def __str__(self):
        return 'Query produced 0 result rows'


class ResultSet(object):
    '''
    Provide iteration over encapsulated apsw cursor. Additionally,
    `ResultSet` instances may be used as context managers to terminate
    the query before all result rows have been retrieved.
    '''

    def __init__(self, cur):
        self.cur = cur
        self.cur.__iter__()  # necessary to make next() method work

    def __next__(self):
        return next(self.cur)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # self.cur.close()
        return

    def close(self):
        '''Terminate query'''
        # self.cur.close()
        return
