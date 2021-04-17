'''
metadata.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError  # Ensure use of custom logger class
from .database import Connection, NoSuchRowError
from . import BUFSIZE, CTRL_INODE, ROOT_INODE, CURRENT_FS_REV
from .common import (pretty_print_size, time_ns, get_seq_no,
                     freeze_basic_mapping, thaw_basic_mapping)
from .deltadump import INTEGER, BLOB, dump_table, load_table
from .backends.common import NoSuchObject, CorruptedObjectError
import os
import shutil
import tempfile
import time
import trio
import bz2
import stat

log = logging.getLogger(__name__)

# Has to be kept in sync with create_tables()!
DUMP_SPEC = [
             ('objects', 'id', (('id', INTEGER, 1),
                                ('size', INTEGER),
                                ('refcount', INTEGER))),

             ('blocks', 'id', (('id', INTEGER, 1),
                               ('hash', BLOB, 32),
                               ('size', INTEGER),
                               ('obj_id', INTEGER, 1),
                               ('refcount', INTEGER))),

             ('inodes', 'id', (('id', INTEGER, 1),
                               ('uid', INTEGER),
                               ('gid', INTEGER),
                               ('mode', INTEGER),
                               ('mtime_ns', INTEGER),
                               ('atime_ns', INTEGER),
                               ('ctime_ns', INTEGER),
                               ('size', INTEGER),
                               ('rdev', INTEGER),
                               ('locked', INTEGER),
                               ('refcount', INTEGER))),

             ('inode_blocks', 'inode, blockno',
              (('inode', INTEGER),
               ('blockno', INTEGER, 1),
               ('block_id', INTEGER, 1))),

             ('symlink_targets', 'inode', (('inode', INTEGER, 1),
                                           ('target', BLOB))),

             ('names', 'id', (('id', INTEGER, 1),
                              ('name', BLOB),
                              ('refcount', INTEGER))),

             ('contents', 'parent_inode, name_id',
              (('name_id', INTEGER, 1),
               ('inode', INTEGER, 1),
               ('parent_inode', INTEGER))),

             ('ext_attributes', 'inode', (('inode', INTEGER),
                                          ('name_id', INTEGER),
                                          ('value', BLOB))),
]


class SqliteMetaBackend(object):
    def __init__(self, backend=None, cachepath=None,
                 mkfs=False, mkfsopts=None):
        self.backend = backend
        self.cachepath = cachepath
        self.db = None
        self.param = None
        self._metadata_upload_interval = None

        if mkfs:
            self.mkfs(mkfsopts)
        else:
            self.get_metadata(backend, cachepath)

    def mkfs(self, options):
        db = Connection(self.cachepath + '.db')
        self.db = db
        create_tables(db)
        init_tables(db)

        param = dict()
        param['revision'] = CURRENT_FS_REV
        param['seq_no'] = int(time.time())
        param['label'] = options.label
        param['max_obj_size'] = options.max_obj_size * 1024
        param['needs_fsck'] = False
        param['inode_gen'] = 0
        param['last_fsck'] = time.time()
        param['last-modified'] = time.time()
        self.param = param

        log.info('Dumping metadata...')
        self.dump_and_upload_metadata()

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
            raise QuietError(
                'Backend reports that fs is still mounted elsewhere, aborting.',
                exitcode=31)

        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError(
                'File system revision too old, please run `s3qladm upgrade` first.',
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

        self.param = param
        self.db = db

        self.save_params()

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
        return self._metadata_upload_interval

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
        self.db.execute(
            'CREATE INDEX IF NOT EXISTS ix_contents_inode ON contents(inode)')

    def del_nfsindex(self):
        self.db.execute('DROP INDEX IF EXISTS ix_contents_inode')

    def blocks_count(self):
        return self.db.get_val("SELECT COUNT(id) FROM objects")

    def inodes_count(self):
        return self.db.get_val("SELECT COUNT(id) FROM inodes")

    def fs_size(self):
        return self.db.get_val('SELECT SUM(size) FROM blocks')

    def parent_inode(self, inodeid):
        return self.db.get_val(
            "SELECT parent_inode FROM contents WHERE inode=?",
            (inodeid,))

    def get_dirent_inode(self, parent_inode, name):
        return self.db.get_val(
            "SELECT inode FROM contents_v WHERE name=? AND parent_inode=?",
            (name, parent_inode))

    def readlink(self, inodeid):
        return self.db.get_val(
            "SELECT target FROM symlink_targets WHERE inode=?",
            (inodeid,))

    def readdir(self, inodeid, off):
        return self.db.query(
            "SELECT name_id, name, inode FROM contents_v "
            'WHERE parent_inode=? AND name_id > ? ORDER BY name_id',
            (inodeid, off-3))

    def getxattr(self, inodeid, name):
        return self.db.get_val(
            'SELECT value FROM ext_attributes_v WHERE inode=? AND name=?',
            (inodeid, name))

    def listxattr(self, inodeid):
        return self.db.query('SELECT name FROM ext_attributes_v WHERE inode=?',
                             (inodeid,))

    def setxattr(self, inodeid, name, value):
        return self.db.execute(
            'INSERT OR REPLACE INTO ext_attributes (inode, name_id, value) '
            'VALUES(?, ?, ?)', (inodeid, self._add_name(name), value))

    def removexattr(self, inodeid, name):
        name_id = self._del_name(name)
        return self.db.execute(
            'DELETE FROM ext_attributes WHERE inode=? AND name_id=?',
            (inodeid, name_id))

    def link(self, name, inodeid, parent_inode_id):
        self.db.execute(
            "INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
            (self._add_name(name), inodeid, parent_inode_id))

    def symlink(self, inodeid, target):
        self.db.execute('INSERT INTO symlink_targets (inode, target) VALUES(?,?)',
                        (inodeid, target))

    def list_directory(self, parent_inode, off):
        return self.db.query(
            'SELECT name_id, inode FROM contents WHERE parent_inode=? '
            'AND name_id > ? ORDER BY name_id', (parent_inode, off))

    def is_directory(self, inodeid):
        return self.db.has_val('SELECT 1 FROM contents WHERE parent_inode=?',
                               (inodeid,))

    def batch_list_dir(self, batch_size, parent_inode):
        return self.db.get_list(
            'SELECT name, name_id, inode FROM contents_v WHERE '
            'parent_inode=? LIMIT %d' % batch_size, (parent_inode,))

    def copy_tree_files(self, cur_id, new_id):
        self.db.execute('INSERT INTO symlink_targets (inode, target) '
                        'SELECT ?, target FROM symlink_targets WHERE inode=?',
                        (new_id, cur_id))
        self.db.execute('INSERT INTO ext_attributes (inode, name_id, value) '
                        'SELECT ?, name_id, value FROM ext_attributes WHERE inode=?',
                        (new_id, cur_id))
        self.db.execute('UPDATE names SET refcount = refcount + 1 WHERE '
                        'id IN (SELECT name_id FROM ext_attributes WHERE inode=?)',
                        (cur_id,))

        processed = self.db.execute(
            'INSERT INTO inode_blocks (inode, blockno, block_id) '
            'SELECT ?, blockno, block_id FROM inode_blocks '
            'WHERE inode=?', (new_id, cur_id))
        self.db.execute(
            'REPLACE INTO blocks (id, hash, refcount, size, obj_id) '
            'SELECT id, hash, refcount+COUNT(id), size, obj_id '
            'FROM inode_blocks JOIN blocks ON block_id = id '
            'WHERE inode = ? GROUP BY id', (new_id,))
        return processed

    def copy_tree_dirs(self, name_id, id_new, target_id):
        self.db.execute(
            'INSERT INTO contents (name_id, inode, parent_inode) VALUES(?, ?, ?)',
            (name_id, id_new, target_id))
        self.db.execute('UPDATE names SET refcount=refcount+1 WHERE id=?',
                        (name_id,))

    def make_copy_visible(self, inodeid, tmpid):
        self.db.execute('UPDATE contents SET parent_inode=? WHERE parent_inode=?',
                        (inodeid, tmpid))

    def delete_dirent(self, name, parent_inode):
        name_id = self._del_name(name)
        self.db.execute(
            "DELETE FROM contents WHERE name_id=? AND parent_inode=?",
            (name_id, parent_inode))

    def _add_name(self, name):
        '''Get id for *name* and increase refcount

        Name is inserted in table if it does not yet exist.
        '''
        try:
            name_id = self.db.get_val('SELECT id FROM names WHERE name=?',
                                      (name,))
        except NoSuchRowError:
            name_id = self.db.rowid(
                'INSERT INTO names (name, refcount) VALUES(?,?)', (name, 1))
        else:
            self.db.execute('UPDATE names SET refcount=refcount+1 WHERE id=?',
                            (name_id,))
        return name_id

    def _del_name(self, name):
        '''Decrease refcount for *name*

        Name is removed from table if refcount drops to zero. Returns the
        (possibly former) id of the name.
        '''
        (name_id, refcount) = self.db.get_row(
            'SELECT id, refcount FROM names WHERE name=?', (name,))

        if refcount > 1:
            self.db.execute('UPDATE names SET refcount=refcount-1 WHERE id=?',
                            (name_id,))
        else:
            self.db.execute('DELETE FROM names WHERE id=?', (name_id,))

        return name_id

    def rename(self, id_p_old, name_old, id_p_new, name_new):
        name_id_new = self._add_name(name_new)
        name_id_old = self._del_name(name_old)

        self.db.execute(
            "UPDATE contents SET name_id=?, parent_inode=? WHERE name_id=? "
            "AND parent_inode=?", (name_id_new, id_p_new,
                                   name_id_old, id_p_old))

        return name_id_new, name_id_old

    def replace_target(self, name_new, name_old, id_old, id_p_old, id_p_new):
        name_id_new = self.get_val('SELECT id FROM names WHERE name=?',
                                   (name_new,))
        self.db.execute(
            "UPDATE contents SET inode=? WHERE name_id=? AND parent_inode=?",
            (id_old, name_id_new, id_p_new))

        # Delete old name
        name_id_old = self._del_name(name_old)
        self.db.execute('DELETE FROM contents WHERE name_id=? AND parent_inode=?',
                        (name_id_old, id_p_old))

    def extstat(self):
        entries = self.db.get_val("SELECT COUNT(rowid) FROM contents")
        blocks = self.blocks_count()
        inodes = self.inodes_count()
        fs_size = self.db.get_val('SELECT SUM(size) FROM inodes') or 0
        dedup_size = self.db.get_val('SELECT SUM(size) FROM blocks') or 0

        # Objects that are currently being uploaded/compressed have size == -1
        compr_size = self.db.get_val('SELECT SUM(size) FROM objects '
                                     'WHERE size > 0') or 0

        return (entries, blocks, inodes, fs_size, dedup_size, compr_size,
                self.db.get_size())

    def create_inode(self, kw):
        ATTRIBUTES = ('mode', 'refcount', 'uid', 'gid', 'size', 'locked',
                      'rdev', 'atime_ns', 'mtime_ns', 'ctime_ns', 'id')

        bindings = tuple(kw[x] for x in ATTRIBUTES if x in kw)
        columns = ', '.join(x for x in ATTRIBUTES if x in kw)
        values = ', '.join('?' * len(kw))

        return self.db.rowid(
            'INSERT INTO inodes (%s) VALUES(%s)' % (columns, values),
            bindings)

    def get_inode(self, inodeid):
        return self.db.get_row(
            "SELECT * FROM inodes WHERE id=? ", (inodeid,))

    def delete_inode(self, inodeid):
        self.db.execute(
            'UPDATE names SET refcount = refcount - 1 WHERE '
            'id IN (SELECT name_id FROM ext_attributes WHERE inode=?)',
            (inodeid,))
        # self.db.execute(
        #     'DELETE FROM names WHERE refcount=0 AND '
        #     'id IN (SELECT name_id FROM ext_attributes WHERE inode=?)',
        #     (inodeid,))
        self.db.execute('DELETE FROM names WHERE refcount=0')
        self.db.execute('DELETE FROM ext_attributes WHERE inode=?', (inodeid,))
        self.db.execute('DELETE FROM symlink_targets WHERE inode=?',
                        (inodeid,))

    def update_inode(self, inode, update_attrs):
        update_str = ', '.join('%s=?' % x for x in update_attrs)
        self.db.execute("UPDATE inodes SET %s WHERE id=?" % update_str,
                        [getattr(inode, x) for x in update_attrs] + [inode.id])

    def cache_delete_inode(self, inodeid):
        self.db.execute('DELETE FROM inodes WHERE id=?', (inodeid,))

    def update_object_size(self, obj_id, obj_size):
        self.db.execute('UPDATE objects SET size=? WHERE id=?',
                        (obj_size, obj_id))

    def nullify_block_hash(self, obj_id):
        self.db.execute('UPDATE blocks SET hash=NULL WHERE obj_id=?',
                        (obj_id,))

    def get_block_id(self, inodeid, blockno):
        try:
            return self.db.get_val('SELECT block_id FROM inode_blocks '
                                   'WHERE inode=? AND blockno=?',
                                   (inodeid, blockno))
        except NoSuchRowError:
            return None

    def block_id_by_hash(self, hash_):
        return self.db.get_val('SELECT id FROM blocks WHERE hash=?', (hash_,))

    def create_object(self):
        return self.db.rowid(
            'INSERT INTO objects (refcount, size) VALUES(1, -1)')

    def create_block(self, obj_id, hash_, size):
        return self.db.rowid(
            'INSERT INTO blocks (refcount, obj_id, hash, size) '
            'VALUES(?,?,?,?)', (1, obj_id, hash_, size))

    def increment_block_ref(self, blockid):
        self.db.execute('UPDATE blocks SET refcount=refcount+1 WHERE id=?',
                        (blockid,))

    def inode_add_block(self, inodeid, blockid, blockno):
        self.db.execute(
            'INSERT OR REPLACE INTO inode_blocks (block_id, inode, blockno) '
            'VALUES(?,?,?)', (blockid, inodeid, blockno))

    def inode_del_block(self, inode, blockno):
        self.db.execute('DELETE FROM inode_blocks WHERE inode=? AND blockno=?',
                        (inode, blockno))

    def get_objid(self, blockid):
        return self.db.get_val('SELECT obj_id FROM blocks WHERE id=?',
                               (blockid,))

    def deref_block(self, block_id):
        refcount = self.db.get_val('SELECT refcount FROM blocks WHERE id=?',
                                   (block_id,))
        if refcount > 1:
            log.debug('decreased refcount for block: %d', block_id)
            self.db.execute('UPDATE blocks SET refcount=refcount-1 WHERE id=?',
                            (block_id,))
            return None

        log.debug('removing block %d', block_id)
        obj_id = self.db.get_val('SELECT obj_id FROM blocks WHERE id=?',
                                 (block_id,))
        self.db.execute('DELETE FROM blocks WHERE id=?', (block_id,))
        (refcount, size) = self.db.get_row(
            'SELECT refcount, size FROM objects WHERE id=?',
            (obj_id,))
        if refcount > 1:
            log.debug('decreased refcount for obj: %d', obj_id)
            self.db.execute(
                'UPDATE objects SET refcount=refcount-1 WHERE id=?',
                (obj_id,))
            return

        log.debug('removing object %d', obj_id)
        self.db.execute('DELETE FROM objects WHERE id=?', (obj_id,))
        return obj_id, size

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


def restore_metadata(fh, dbfile):
    '''Read metadata from *fh* and write into *dbfile*

    Return database connection to *dbfile*.

    *fh* must be able to return an actual file descriptor from
    its `fileno` method.

    *dbfile* will be created with 0600 permissions. Data is
    first written into a temporary file *dbfile* + '.tmp', and
    the file is renamed once all data has been loaded.
    '''

    tmpfile = dbfile + '.tmp'
    fd = os.open(tmpfile, os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                 stat.S_IRUSR | stat.S_IWUSR)
    try:
        os.close(fd)

        db = Connection(tmpfile)
        db.execute('PRAGMA locking_mode = NORMAL')
        db.execute('PRAGMA synchronous = OFF')
        db.execute('PRAGMA journal_mode = OFF')
        create_tables(db)

        for (table, _, columns) in DUMP_SPEC:
            log.info('..%s..', table)
            load_table(table, columns, db=db, fh=fh)
        db.execute('ANALYZE')

        # We must close the database to rename it
        db.close()
    except:
        os.unlink(tmpfile)
        raise

    os.rename(tmpfile, dbfile)

    return Connection(dbfile)


def cycle_metadata(backend, keep=10):
    '''Rotate metadata backups'''

    # Since we always overwrite the source afterwards, we can
    # use either copy or rename - so we pick whatever is faster.
    if backend.has_native_rename:
        cycle_fn = backend.rename
    else:
        cycle_fn = backend.copy

    log.info('Backing up old metadata...')
    for i in range(keep)[::-1]:
        try:
            cycle_fn("s3ql_metadata_bak_%d" % i, "s3ql_metadata_bak_%d" % (i + 1))
        except NoSuchObject:
            pass

    # If we use backend.rename() and crash right after this instruction,
    # we will end up without an s3ql_metadata object. However, fsck.s3ql
    # is smart enough to use s3ql_metadata_new in this case.
    try:
        cycle_fn("s3ql_metadata", "s3ql_metadata_bak_0")
    except NoSuchObject:
        # In case of mkfs, there may be no metadata object yet
        pass
    cycle_fn("s3ql_metadata_new", "s3ql_metadata")

    # Note that we can't compare with "is" (maybe because the bound-method
    # is re-created on the fly on access?)
    if cycle_fn == backend.copy:
        backend.delete('s3ql_metadata_new')


def dump_metadata(db, fh):
    '''Dump metadata into fh

    *fh* must be able to return an actual file descriptor from
    its `fileno` method.
    '''

    locking_mode = db.get_val('PRAGMA locking_mode')
    try:
        # Ensure that we don't hold a lock on the db
        # (need to access DB to actually release locks)
        db.execute('PRAGMA locking_mode = NORMAL')
        db.has_val('SELECT rowid FROM %s LIMIT 1' % DUMP_SPEC[0][0])

        for (table, order, columns) in DUMP_SPEC:
            log.info('..%s..', table)
            dump_table(table, order, columns, db=db, fh=fh)

    finally:
        db.execute('PRAGMA locking_mode = %s' % locking_mode)


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # size == -1 indicates block has not been uploaded yet
    conn.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL,
        size      INT NOT NULL
    )""")

    # Table of known data blocks
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE blocks (
        id        INTEGER PRIMARY KEY,
        hash      BLOB(32) UNIQUE,
        refcount  INT,
        size      INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id)
    )""")

    # Table with filesystem metadata
    # The number of links `refcount` to an inode can in theory
    # be determined from the `contents` table. However, managing
    # this separately should be significantly faster (the information
    # is required for every getattr!)
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime_ns  INT NOT NULL,
        atime_ns  INT NOT NULL,
        ctime_ns  INT NOT NULL,
        refcount  INT NOT NULL,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )""")

    # Further Blocks used by inode (blockno >= 1)
    conn.execute("""
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        block_id    INTEGER NOT NULL REFERENCES blocks(id),
        PRIMARY KEY (inode, blockno)
    )""")

    # Symlinks
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )""")

    # Names of file system objects
    conn.execute("""
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )""")

    # Table of filesystem objects
    # rowid is used by readdir() to restart at the correct position
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),

        UNIQUE (parent_inode, name_id)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,

        PRIMARY KEY (inode, name_id)
    )""")

    # Shortcuts
    conn.execute("""
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id
    """)
    conn.execute("""
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """)


def init_tables(conn):
    # Insert root directory
    now_ns = time_ns()
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
         | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
         os.getuid(), os.getgid(), now_ns, now_ns, now_ns, 1))

    # Insert control inode, the actual values don't matter that much
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                 (CTRL_INODE, stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR,
                  0, 0, now_ns, now_ns, now_ns, 42))

    # Insert lost+found directory
    inode = conn.rowid(
        "INSERT INTO inodes (mode,uid,gid,mtime_ns,atime_ns,ctime_ns,refcount) "
        "VALUES (?,?,?,?,?,?,?)",
        (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
         os.getuid(), os.getgid(), now_ns, now_ns, now_ns, 1))
    name_id = conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)',
                         (b'lost+found', 1))
    conn.execute(
        "INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
        (name_id, inode, ROOT_INODE))


def stream_write_bz2(ifh, ofh):
    '''Compress *ifh* into *ofh* using bz2 compression'''

    compr = bz2.BZ2Compressor(9)
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = compr.compress(buf)
        if buf:
            ofh.write(buf)
    buf = compr.flush()
    if buf:
        ofh.write(buf)


def stream_read_bz2(ifh, ofh):
    '''Uncompress bz2 compressed *ifh* into *ofh*'''

    decompressor = bz2.BZ2Decompressor()
    while True:
        buf = ifh.read(BUFSIZE)
        if not buf:
            break
        buf = decompressor.decompress(buf)
        if buf:
            ofh.write(buf)

    if decompressor.unused_data or ifh.read(1) != b'':
        raise CorruptedObjectError('Data after end of bz2 stream')


def download_metadata(backend, db_file, name='s3ql_metadata'):
    with tempfile.TemporaryFile() as tmpfh:
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            stream_read_bz2(fh, tmpfh)

        log.info('Downloading and decompressing metadata...')
        backend.perform_read(do_read, name)

        log.info("Reading metadata...")
        tmpfh.seek(0)
        return restore_metadata(tmpfh, db_file)


def dump_and_upload_metadata(backend, db, param):
    with tempfile.TemporaryFile() as fh:
        log.info('Dumping metadata...')
        dump_metadata(db, fh)
        upload_metadata(backend, fh, param)


def upload_metadata(backend, fh, param):
    log.info("Compressing and uploading metadata...")
    def do_write(obj_fh):
        fh.seek(0)
        stream_write_bz2(fh, obj_fh)
        return obj_fh
    obj_fh = backend.perform_write(do_write, "s3ql_metadata_new",
                                   metadata=param, is_compressed=True)
    log.info('Wrote %s of compressed metadata.',
             pretty_print_size(obj_fh.get_obj_size()))

    log.info('Cycling metadata backups...')
    cycle_metadata(backend)


def load_params(cachepath):
    with open(cachepath + '.params', 'rb') as fh:
        return thaw_basic_mapping(fh.read())


def save_params(cachepath, param):
    filename = cachepath + '.params'
    tmpname = filename + '.tmp'
    with open(tmpname, 'wb') as fh:
        fh.write(freeze_basic_mapping(param))
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
