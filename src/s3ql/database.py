'''
database.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.


Module Attributes:
-----------

:initsql:      SQL commands that are executed whenever a new
               connection is created.
'''

from .logging import logging, QuietError  # Ensure use of custom logger class
from .common import get_seq_no
from .metadata import (download_metadata, dump_metadata, upload_metadata,
                       dump_and_upload_metadata)
from . import CURRENT_FS_REV
import apsw
from ast import literal_eval
from base64 import b64decode, b64encode
import binascii
import os
import shutil
import tempfile
import time
import trio

log = logging.getLogger(__name__)

sqlite_ver = tuple([int(x) for x in apsw.sqlitelibversion().split('.')])
if sqlite_ver < (3, 7, 0):
    raise QuietError('SQLite version too old, must be 3.7.0 or newer!\n')


initsql = (
           # WAL mode causes trouble with e.g. copy_tree, so we don't use it at the moment
           # (cf. http://article.gmane.org/gmane.comp.db.sqlite.general/65243).
           # However, if we start using it we must initiaze it *before* setting
           # locking_mode to EXCLUSIVE, otherwise we can't switch the locking
           # mode without first disabling WAL.
           'PRAGMA synchronous = OFF',
           'PRAGMA journal_mode = OFF',
           # 'PRAGMA synchronous = NORMAL',
           # 'PRAGMA journal_mode = WAL',

           'PRAGMA foreign_keys = OFF',
           'PRAGMA locking_mode = EXCLUSIVE',
           'PRAGMA recursize_triggers = on',
           'PRAGMA page_size = 4096',
           'PRAGMA wal_autocheckpoint = 25000',
           'PRAGMA temp_store = FILE',
           'PRAGMA legacy_file_format = off',
           )


class Connection(object):
    '''
    This class wraps an APSW connection object. It should be used instead of any
    native APSW cursors.

    It provides methods to directly execute SQL commands and creates apsw
    cursors dynamically.

    Instances are not thread safe. They can be passed between threads,
    but must not be called concurrently.

    Attributes
    ----------

    :conn:     apsw connection object
    '''

    def __init__(self, file_):
        self.conn = apsw.Connection(file_)
        self.file = file_

        cur = self.conn.cursor()

        for s in initsql:
            cur.execute(s)

    def close(self):
        self.conn.close()

    def get_size(self):
        '''Return size of database file'''

        if self.file is not None and self.file not in ('', ':memory:'):
            return os.path.getsize(self.file)
        else:
            return 0

    def query(self, *a, **kw):
        '''Return iterator over results of given SQL statement

        If the caller does not retrieve all rows the iterator's close() method
        should be called as soon as possible to terminate the SQL statement
        (otherwise it may block execution of other statements). To this end,
        the iterator may also be used as a context manager.
        '''

        return ResultSet(self.conn.cursor().execute(*a, **kw))

    def execute(self, *a, **kw):
        '''Execute the given SQL statement. Return number of affected rows '''

        self.conn.cursor().execute(*a, **kw)
        return self.changes()

    def rowid(self, *a, **kw):
        """Execute SQL statement and return last inserted rowid"""

        self.conn.cursor().execute(*a, **kw)
        return self.conn.last_insert_rowid()

    def has_val(self, *a, **kw):
        '''Execute statement and check if it gives result rows'''

        res = self.conn.cursor().execute(*a, **kw)
        try:
            next(res)
        except StopIteration:
            return False
        else:
            # Finish the active SQL statement
            res.close()
            return True

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

        res = self.conn.cursor().execute(*a, **kw)
        try:
            row = next(res)
        except StopIteration:
            raise NoSuchRowError()
        try:
            next(res)
        except StopIteration:
            # Fine, we only wanted one row
            pass
        else:
            # Finish the active SQL statement
            res.close()
            raise NoUniqueValueError()

        return row

    def last_rowid(self):
        """Return rowid most recently inserted in the current thread"""

        return self.conn.last_insert_rowid()

    def changes(self):
        """Return number of rows affected by most recent sql statement"""

        return self.conn.changes()


class SqliteMetaBackend(object):
    def __init__(self, backend=None, cachepath=None):
        self.backend = backend
        self.cachepath = cachepath
        self.db = None
        self.param = None
        self._metadata_upload_task = None
        self.get_metadata(backend, cachepath)

    def get_metadata(self, backend, cachepath):
        '''Retrieve metadata'''
        seq_no = get_seq_no(backend)

        # When there was a crash during metadata rotation, we may end up
        # without an s3ql_metadata object.
        meta_obj_name = 's3ql_metadata'
        if meta_obj_name not in backend:
            meta_obj_name += '_new'

        # Check for cached metadata
        db = None
        param = None
        if os.path.exists(cachepath + '.params'):
            param = self.load_params()
            if param['seq_no'] < seq_no:
                log.info('Ignoring locally cached metadata (outdated).')
                param = backend.lookup(meta_obj_name)
            elif param['seq_no'] > seq_no:
                raise QuietError("File system not unmounted cleanly, run fsck!",
                                 exitcode=30)
            else:
                log.info('Using cached metadata.')
                db = Connection(cachepath + '.db')
        else:
            param = backend.lookup(meta_obj_name)

        # Check for unclean shutdown
        if param['seq_no'] < seq_no:
            raise QuietError('Backend reports that fs is still mounted elsewhere, aborting.',
                             exitcode=31)

        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError('File system revision too old, please run `s3qladm upgrade` first.',
                             exitcode=32)
        elif param['revision'] > CURRENT_FS_REV:
            raise QuietError('File system revision too new, please update your '
                             'S3QL installation.', exitcode=33)

        # Check that the fs itself is clean
        if param['needs_fsck']:
            raise QuietError("File system damaged or not unmounted cleanly, run fsck!",
                             exitcode=30)
        if time.time() - param['last_fsck'] > 60 * 60 * 24 * 31:
            log.warning('Last file system check was more than 1 month ago, '
                        'running fsck.s3ql is recommended.')

        # Download metadata
        if not db:
            db = download_metadata(backend, cachepath + '.db')

            # Drop cache
            if os.path.exists(cachepath + '-cache'):
                shutil.rmtree(cachepath + '-cache')

        self.save_params()

        self.param = param
        self.db = db

    def mark_metadata_dirty(self):
        '''Mark metadata as dirty and increase sequence number'''
        self.param['seq_no'] += 1
        self.param['needs_fsck'] = True
        self.save_params()
        self.backend['s3ql_seq_no_%d' % self.param['seq_no']] = b'Empty'
        self.param['needs_fsck'] = False

    def dump_and_upload_metadata(self):
        dump_and_upload_metadata(self.backend, self.db, self.param)

    def metadata_upload_task(self, backend_pool,
                             metadata_upload_interval):
        self._metadata_upload_interval = MetadataUploadTask(
            backend_pool, self.param, self.db, metadata_upload_interval)
        return self._metadata_upload_task

    def load_params(self):
        with open(self.cachepath + '.params', 'rb') as fh:
            return thaw_basic_mapping(fh.read())

    def save_params(self):
        filename = self.cachepath + '.params'
        tmpname = filename + '.tmp'
        with open(tmpname, 'wb') as fh:
            fh.write(freeze_basic_mapping(self.param))
            # Fsync to make sure that the updated sequence number is committed to
            # disk. Otherwise, a crash immediately after mount could result in both
            # the local and remote metadata appearing to be out of date.
            fh.flush()
            os.fsync(fh.fileno())

        # we need to flush the dirents too.
        # stackoverflow.com/a/41362774
        # stackoverflow.com/a/5809073
        os.rename(tmpname, filename)
        dirfd = os.open(os.path.dirname(filename), os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    def make_nfsindex(self):
        self.db.execute('CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')

    def del_nfsindex(self):
        self.db.execute('DROP INDEX IF EXISTS ix_contents_inode')

    def close(self):
        seq_no = get_seq_no(self.backend)
        if self._metadata_upload_task.db_mtime == os.stat(self.cachepath + '.db').st_mtime:
            log.info('File system unchanged, not uploading metadata.')
            del self.backend['s3ql_seq_no_%d' % self.param['seq_no']]
            self.param['seq_no'] -= 1
            self.db.save_params()
        elif seq_no == self.param['seq_no']:
            self.param['last-modified'] = time.time()
            self.db.dump_and_upload_metadata()
            self.db.save_params()
        else:
            log.error('Remote metadata is newer than local (%d vs %d), '
                      'refusing to overwrite!', seq_no, self.param['seq_no'])
            log.error('The locally cached metadata will be *lost* the next time the file system '
                      'is mounted or checked and has therefore been backed up.')
            for name in (self.cachepath + '.params', self.cachepath + '.db'):
                for i in range(4)[::-1]:
                    if os.path.exists(name + '.%d' % i):
                        os.rename(name + '.%d' % i, name + '.%d' % (i + 1))
                os.rename(name, name + '.0')

        log.info('Cleaning up local metadata...')

        self.db.execute('ANALYZE')
        self.db.execute('VACUUM')
        self.db.close()


class MetadataUploadTask:
    '''
    Periodically upload metadata. Upload is done every `interval`
    seconds, and whenever `event` is set. To terminate thread,
    set `quit` attribute as well as `event` event.
    '''

    def __init__(self, backend_pool, param, db, interval):
        super().__init__()
        self.backend_pool = backend_pool
        self.param = param
        self.db = db
        self.interval = interval
        self.db_mtime = os.stat(db.file).st_mtime
        self.event = trio.Event()
        self.quit = False

        # Can't assign in constructor, because Operations instance needs
        # access to self.event as well.
        self.fs = None

    async def run(self):
        log.debug('started')

        assert self.fs is not None

        while not self.quit:
            if self.interval is None:
                await self.event.wait()
            else:
                with trio.move_on_after(self.interval):
                    await self.event.wait()
            self.event = trio.Event()  # reset
            if self.quit:
                break

            new_mtime = os.stat(self.db.file).st_mtime
            if self.db_mtime == new_mtime:
                log.info('File system unchanged, not uploading metadata.')
                continue

            log.info('Dumping metadata...')
            fh = tempfile.TemporaryFile()
            dump_metadata(self.db, fh)

            with self.backend_pool() as backend:
                seq_no = get_seq_no(backend)
                if seq_no > self.param['seq_no']:
                    log.error('Remote metadata is newer than local (%d vs %d), '
                              'refusing to overwrite and switching to failsafe mode!',
                              seq_no, self.param['seq_no'])
                    self.fs.failsafe = True
                    fh.close()
                    break

                fh.seek(0)
                self.param['last-modified'] = time.time()

                # Temporarily decrease sequence no, this is not the final upload
                self.param['seq_no'] -= 1
                await trio.to_thread.run_sync(
                    upload_metadata, backend, fh, self.param)
                self.param['seq_no'] += 1

                fh.close()
                self.db_mtime = new_mtime

        # Break reference loop
        self.fs = None

        log.debug('finished')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('started')
        self.quit = True
        self.event.set()


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

    def __next__(self):
        return next(self.cur)

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cur.close()

    def close(self):
        '''Terminate query'''

        self.cur.close()


class ThawError(Exception):
    def __str__(self):
        return 'Malformed serialization data'


def thaw_basic_mapping(buf):
    '''Reconstruct dict from serialized representation

    *buf* must be a bytes-like object as created by
    `freeze_basic_mapping`. Raises `ThawError` if *buf* is not a valid
    representation.

    This procedure is safe even if *buf* comes from an untrusted source.
    '''

    try:
        d = literal_eval(buf.decode('utf-8'))
    except (UnicodeDecodeError, SyntaxError, ValueError):
        raise ThawError()

    # Decode bytes values
    for (k, v) in d.items():
        if not isinstance(v, bytes):
            continue
        try:
            d[k] = b64decode(v)
        except binascii.Error:
            raise ThawError()

    return d


def freeze_basic_mapping(d):
    '''Serialize mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary type (i.e.,
    `str`, `bytes`, `int`, `float`, `complex`, `bool` or None).

    The output is a bytestream that can be used to reconstruct the mapping. The
    bytestream is not guaranteed to be deterministic. Look at
    `checksum_basic_mapping` if you need a deterministic bytestream.
    '''

    els = []
    for (k, v) in d.items():
        if not isinstance(k, str):
            raise ValueError('key %s must be str, not %s' % (k, type(k)))

        if (not isinstance(v, (str, bytes, bytearray, int, float, complex, bool))
                and v is not None):
            raise ValueError('value for key %s (%s) is not elementary' % (k, v))

        # To avoid wasting space, we b64encode non-ascii byte values.
        if isinstance(v, (bytes, bytearray)):
            v = b64encode(v)

        # This should be a pretty safe assumption for elementary types, but we
        # add an assert just to be safe (Python docs just say that repr makes
        # "best effort" to produce something parseable)
        (k_repr, v_repr) = (repr(k), repr(v))
        assert (literal_eval(k_repr), literal_eval(v_repr)) == (k, v)

        els.append(('%s: %s' % (k_repr, v_repr)))

    buf = '{ %s }' % ', '.join(els)
    return buf.encode('utf-8')
