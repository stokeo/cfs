'''
fs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging
from . import deltadump, CTRL_NAME, CTRL_INODE
from .backends.common import NoSuchObject, CorruptedObjectError
from .common import parse_literal, time_ns
from .database import NoSuchRowError
from io import BytesIO
import collections
import errno
import pyfuse3
from pyfuse3 import FUSEError
import math
import os
import stat
import struct
import time
import trio

# We work in bytes
CTRL_NAME = CTRL_NAME.encode('us-ascii')

# standard logger for this module
log = logging.getLogger(__name__)

# During long-running operations, yield to event loop with at least
# this interval.
CHECKPOINT_INTERVAL = 0.05

# ACL_ERRNO is the error code returned for requests that try
# to modify or access extendeda attributes associated with ACL.
# Since we currently don't know how to keep these in sync
# with permission bits, we cannot support ACLs despite having
# full support for extended attributes.
#
# What errno to use for ACL_ERRNO is a bit tricky. acl_set_fd(3)
# returns ENOTSUP if the file system does not support ACLs. However,
# this function uses the setxattr() syscall internally, and
# setxattr(3) states that ENOTSUP means that the file system does not
# support extended attributes at all. A test with btrfs mounted with
# -o noacl shows that the actual errno returned by setxattr() is
# EOPNOTSUPP. Also, some Python versions do not know about
# errno.ENOTSUPP and errno(3) says that on Linux, EOPNOTSUPP and ENOTSUP
# have the same value (despite this violating POSIX).
#
# All in all, the situation seems complicated, so we try to use
# EOPNOTSUPP with a fallback on ENOTSUP just in case.

if not hasattr(errno, 'EOPNOTSUPP'):
    ACL_ERRNO = errno.ENOTSUP
else:
    ACL_ERRNO = errno.EOPNOTSUPP


class _TruncSetattrFields:
    """A private class used for O_TRUNC.

    This is needed because pyfuse3.SetattrFields fields are read only"""
    def __getattr__(self, name):
        """Only return ``True`` for ``update_size`` and ``update_mtime`.
        Anything else will return ``False``"""
        return name in ('update_size', 'update_mtime')


"""We only need a single instance of this read-only class"""
_TruncSetattrFields = _TruncSetattrFields()


class Operations(pyfuse3.Operations):
    """A full-featured file system for online data storage

    This class implements low-level FUSE operations and is meant to be passed to
    pyfuse3.init().

    The ``access`` method of this class always gives full access, independent of
    file permissions. If the FUSE library is initialized with ``allow_other`` or
    ``allow_root``, the ``default_permissions`` option should therefore always
    be passed as well.


    Attributes:
    -----------

    :cache:       Holds information about cached blocks
    :inode_cache: A cache for the attributes of the currently opened inodes.
    :open_inodes: dict of currently opened inodes. This is used to not remove
                  the blocks of unlinked inodes that are still open.
    :upload_task: Trio task for metadata uploads
    :failsafe: Set when backend problems are encountered. In that case, fs only
               allows read access.
    :broken_blocks: Caches information about corrupted blocks to avoid repeated (pointless)
                    attempts to retrieve them. This attribute is a dict (indexed by inodes)
                    of sets of block indices. Broken blocks are removed from the cache
                    when an inode is forgotten.

    Directory Entry Types
    ----------------------

    S3QL is quite agnostic when it comes to directory entry types. Every
    directory entry can contain other entries *and* have a associated data,
    size, link target and device number. However, S3QL makes some provisions for
    users relying on unlink()/rmdir() to fail for a directory/file. For that, it
    explicitly checks the st_mode attribute.
    """

    supports_dot_lookup = True
    enable_acl = False
    enable_writeback_cache = True

    def __init__(self, block_cache, db, max_obj_size, inode_cache,
                 upload_task=None):
        super().__init__()

        self.inodes = inode_cache
        self.db = db
        self.upload_task = upload_task
        self.open_inodes = collections.defaultdict(lambda: 0)
        self.max_obj_size = max_obj_size
        self.cache = block_cache
        self.failsafe = False
        self.broken_blocks = collections.defaultdict(set)

        # Root inode is always open
        self.open_inodes[pyfuse3.ROOT_INODE] += 1

    async def destroy(self):
        await self.forget(list(self.open_inodes.items()))
        self.inodes.destroy()

    async def lookup(self, id_p, name, ctx):
        return self._lookup(id_p, name, ctx).entry_attributes()

    def _lookup(self, id_p, name, ctx):
        log.debug('started with %d, %r', id_p, name)

        if name == CTRL_NAME:
            inode = self.inodes[CTRL_INODE]

            # Make sure the control file is only writable by the user
            # who mounted the file system (but don't mark inode as dirty)
            object.__setattr__(inode, 'uid', os.getuid())
            object.__setattr__(inode, 'gid', os.getgid())

        elif name == '.':
            inode = self.inodes[id_p]

        elif name == '..':
            id_ = self.db.parent_inode(id_p)
            inode = self.inodes[id_]

        else:
            try:
                id_ = self.db.get_dirent_inode(name, id_p)
            except NoSuchRowError:
                raise FUSEError(errno.ENOENT)
            inode = self.inodes[id_]

        self.open_inodes[inode.id] += 1
        return inode

    async def getattr(self, id_, ctx):
        log.debug('started with %d, %d', id_, CTRL_INODE)
        if id_ == CTRL_INODE:
            # Make sure the control file is only writable by the user
            # who mounted the file system (but don't mark inode as dirty)
            inode = self.inodes[CTRL_INODE]
            object.__setattr__(inode, 'uid', os.getuid())
            object.__setattr__(inode, 'gid', os.getgid())
            return inode.entry_attributes()

        return self.inodes[id_].entry_attributes()

    async def readlink(self, id_, ctx):
        log.debug('started with %d', id_)
        now_ns = time_ns()
        inode = self.inodes[id_]
        if inode.atime_ns < inode.ctime_ns or inode.atime_ns < inode.mtime_ns:
            inode.atime_ns = now_ns
        try:
            return self.db.readlink(id_)
        except NoSuchRowError:
            log.warning('Inode does not have symlink target: %d', id_)
            raise FUSEError(errno.EINVAL)

    async def opendir(self, id_, ctx):
        log.debug('started with %d', id_)
        return id_

    async def readdir(self, id_, off, token):
        log.debug('started with %d, %d', id_, off)
        if off == 0:
            off = -1

        inode = self.inodes[id_]
        if inode.atime_ns < inode.ctime_ns or inode.atime_ns < inode.mtime_ns:
            inode.atime_ns = time_ns()

        # NFS treats offsets 1 and 2 special, so we have to exclude
        # them.
        with self.db.readdir(id_, off-3) as res:
            for (next_, name, cid_) in res:
                if not pyfuse3.readdir_reply(
                        token, name, self.inodes[cid_].entry_attributes(), next_+3):
                    break
                self.open_inodes[cid_] += 1

    async def getxattr(self, id_, name, ctx):
        log.debug('started with %d, %r', id_, name)
        # Handle S3QL commands
        if id_ == CTRL_INODE:
            if name == b's3ql_pid?':
                return ('%d' % os.getpid()).encode()

            elif name == b's3qlstat':
                return self.extstat()

            raise FUSEError(errno.EINVAL)

        # http://code.google.com/p/s3ql/issues/detail?id=385
        elif name in (b'system.posix_acl_access',
                      b'system.posix_acl_default'):
            raise FUSEError(ACL_ERRNO)

        else:
            try:
                value = self.db.getxattr(id_, name)
            except NoSuchRowError:
                raise FUSEError(pyfuse3.ENOATTR)
            return value

    async def listxattr(self, id_, ctx):
        log.debug('started with %d', id_)
        names = list()
        for (name,) in self.db.listxattr(id_):
            names.append(name)
        return names

    async def setxattr(self, id_, name, value, ctx):
        log.debug('started with %d, %r, %r', id_, name, value)

        # Handle S3QL commands
        if id_ == CTRL_INODE:
            if name == b's3ql_flushcache!':
                self.inodes.flush()
                await self.cache.flush()

            elif name == b's3ql_dropcache!':
                self.inodes.drop()
                await self.cache.drop()

            elif name == b'copy':
                try:
                    tup = parse_literal(value, (int, int))
                except ValueError:
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                await self.copy_tree(*tup)

            elif name == b'upload-meta':
                if self.upload_task is not None:
                    self.inodes.flush()
                    self.upload_task.event.set()
                else:
                    raise FUSEError(errno.ENOTTY)

            elif name == b'lock':
                try:
                    id_ = parse_literal(value, int)
                except ValueError:
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                await self.lock_tree(id_)

            elif name == b'rmtree':
                try:
                    tup = parse_literal(value, (int, bytes))
                except ValueError:
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                await self.remove_tree(*tup)

            elif name == b'logging':
                try:
                    (lvl, modules)= parse_literal(value, (int, str))
                except (ValueError, KeyError):
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                update_logging(lvl, modules.split(',') if modules else None)

            elif name == b'cachesize':
                try:
                    self.cache.cache.max_size = parse_literal(value, int)
                except ValueError:
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                log.debug('updated cache size to %d bytes', self.cache.cache.max_size)

            elif name == b'dirhardlink':
                try:
                    targ_ino, lnkp_ino, lnk_name = parse_literal(value, (int, int, str))
                    bytes_lnk_name = lnk_name.encode()
                except ValueError:
                    log.warning('Received malformed command via control inode')
                    raise FUSEError.EINVAL()
                await self.link(targ_ino, lnkp_ino, bytes_lnk_name, None)

            else:
                log.warning('Received unknown command via control inode')
                raise FUSEError(errno.EINVAL)

        # http://code.google.com/p/s3ql/issues/detail?id=385
        elif name in (b'system.posix_acl_access',
                      b'system.posix_acl_default'):
            raise FUSEError(ACL_ERRNO)

        else:
            if self.failsafe or self.inodes[id_].locked:
                raise FUSEError(errno.EPERM)

            if len(value) > deltadump.MAX_BLOB_SIZE:
                raise FUSEError(errno.EINVAL)

            self.db.setxattr(id_, name, value)
            self.inodes[id_].ctime_ns = time_ns()

    async def removexattr(self, id_, name, ctx):
        log.debug('started with %d, %r', id_, name)

        if self.failsafe or self.inodes[id_].locked:
            raise FUSEError(errno.EPERM)

        try:
            changes = self.db.removexattr(id_, name)
        except NoSuchRowError:
            raise FUSEError(pyfuse3.ENOATTR)

        if changes == 0:
            raise FUSEError(pyfuse3.ENOATTR)

        self.inodes[id_].ctime_ns = time_ns()

    async def lock_tree(self, id0):
        '''Lock directory tree'''

        if self.failsafe:
            raise FUSEError(errno.EPERM)

        log.debug('started with %d', id0)
        queue = [ (id0, -1) ]
        self.inodes[id0].locked = True
        while queue:
            (id_p, off) = queue.pop()
            log.debug('Processing directory (%d, %d)', id_p, off)
            processed = 0
            with self.db.list_directory(id_p, off) as res:
                for (name_id, id_) in res:
                    self.inodes[id_].locked = True

                    if self.db.is_directory(id_):
                        queue.append((id_, -1))

                    # Break every once in a while - note that we can't yield
                    # right here because there's an active DB query.
                    processed += 1
                    if processed > 200:
                        queue.append((id_p, name_id))
                        break

            await trio.lowlevel.checkpoint()

        log.debug('finished')

    async def remove_tree(self, id_p0, name0):
        '''Remove directory tree'''

        if self.failsafe:
            raise FUSEError(errno.EPERM)

        log.debug('started with %d, %s', id_p0, name0)

        if self.inodes[id_p0].locked:
            raise FUSEError(errno.EPERM)

        id0 = self._lookup(id_p0, name0, ctx=None).id
        queue = [id0]  # Directories that we still need to delete
        batch_size = 200  # Entries to process before checkpointing
        stamp = time.time()  # Time of last checkpoint
        while queue:  # For every directory
            id_p = queue.pop()
            is_open = id_p in self.open_inodes

            # Per https://sqlite.org/isolation.html, results of removing rows
            # during select are undefined. Therefore, process data in chunks.
            # This is also a nice opportunity to release the GIL...
            query_chunk = self.db.batch_list_dir(batch_size, id_p)
            reinserted = False
            for (name, id_) in query_chunk:
                if self.db.is_directory(id_):
                    # First delete subdirectories
                    if not reinserted:
                        queue.append(id_p)
                        reinserted = True
                    queue.append(id_)
                else:
                    if is_open:
                        pyfuse3.invalidate_entry_async(id_p, name)
                    await self._remove(id_p, name, id_, force=True)

            if query_chunk and not reinserted:
                # Make sure to re-insert the directory to process the remaining
                # contents and delete the directory itself.
                queue.append(id_p)

            dt = time.time() - stamp
            batch_size = int(batch_size * CHECKPOINT_INTERVAL / dt)
            batch_size = min(batch_size, 200) # somewhat arbitrary...
            batch_size = max(batch_size, 20000)
            log.debug('Adjusting batch_size to %d and yielding', batch_size)
            await trio.lowlevel.checkpoint()
            log.debug('re-acquired lock')
            stamp = time.time()

        if id_p0 in self.open_inodes:
            log.debug('invalidate_entry(%d, %r)', id_p0, name0)
            pyfuse3.invalidate_entry_async(id_p0, name0)
        await self._remove(id_p0, name0, id0, force=True)

        await self.forget([(id0, 1)])
        log.debug('finished')

    async def copy_tree(self, src_id, target_id):
        '''Efficiently copy directory tree'''

        if self.failsafe:
            raise FUSEError(errno.EPERM)

        log.debug('started with %d, %d', src_id, target_id)

        # To avoid lookups and make code tidier
        make_inode = self.inodes.create_inode
        db = self.db

        # Copy target attributes
        # These come from setxattr, so they may have been deleted
        # without being in open_inodes
        try:
            src_inode = self.inodes[src_id]
            target_inode = self.inodes[target_id]
        except KeyError:
            raise FUSEError(errno.ENOENT)
        for attr in ('atime_ns', 'ctime_ns', 'mtime_ns', 'mode', 'uid', 'gid'):
            setattr(target_inode, attr, getattr(src_inode, attr))

        # We first replicate into a dummy inode, so that we
        # need to invalidate only once.
        now_ns = time_ns()
        tmp = make_inode(mtime_ns=now_ns, ctime_ns=now_ns, atime_ns=now_ns,
                         uid=0, gid=0, mode=0, refcount=0)

        queue = [(src_id, tmp.id, -1)]
        id_cache = dict()
        while queue:
            (src_id, target_id, off) = queue.pop()
            log.debug('Processing directory (%d, %d, %d)', src_id, target_id, off)
            processed = 0
            with db.readdir(src_id, off) as res:
                for (name_id, name, id_) in res:
                    # exclude snapshots directory
                    # TODO avoid hardcoded dir name (use inode property for ex)
                    if name == b'.snapshots':
                        continue

                    # Make sure that all blocks are in the database
                    if id_ in self.open_inodes:
                        await self.cache.start_flush(id_)

                    if id_ not in id_cache:
                        inode = self.inodes[id_]
                        inode_new = make_inode(refcount=1, mode=inode.mode, size=inode.size,
                                               uid=inode.uid, gid=inode.gid,
                                               mtime_ns=inode.mtime_ns, atime_ns=inode.atime_ns,
                                               ctime_ns=inode.ctime_ns, rdev=inode.rdev)

                        id_new = inode_new.id

                        if inode.refcount != 1:
                            id_cache[id_] = id_new

                        processed += db.copy_tree_files(id_new, id_)

                        if db.is_directory(id_):
                            queue.append((id_, id_new, -1))
                    else:
                        id_new = id_cache[id_]
                        self.inodes[id_new].refcount += 1

                    db.copy_tree_dirs(name, id_new, target_id)

                    # Break every once in a while - note that we can't yield
                    # right here because there's an active DB query.
                    processed += 1
                    if processed > 200:
                        queue.append((src_id, target_id, name_id))
                        break

            await trio.lowlevel.checkpoint()

        # Make replication visible
        db.make_copy_visible(target_inode.id, tmp.id)
        del self.inodes[tmp.id]
        pyfuse3.invalidate_inode(target_inode.id)

        log.debug('finished')

    async def unlink(self, id_p, name, ctx):
        log.debug('started with %d, %r', id_p, name)
        if self.failsafe:
            raise FUSEError(errno.EPERM)

        inode = self._lookup(id_p, name, ctx)

        if stat.S_ISDIR(inode.mode):
            raise FUSEError(errno.EISDIR)

        await self._remove(id_p, name, inode.id)

        await self.forget([(inode.id, 1)])

    async def rmdir(self, id_p, name, ctx):
        log.debug('started with %d, %r', id_p, name)
        if self.failsafe:
            raise FUSEError(errno.EPERM)

        inode = self._lookup(id_p, name, ctx)

        if self.inodes[id_p].locked:
            raise FUSEError(errno.EPERM)

        if not stat.S_ISDIR(inode.mode):
            raise FUSEError(errno.ENOTDIR)

        await self._remove(id_p, name, inode.id)

        await self.forget([(inode.id, 1)])

    async def _remove(self, id_p, name, id_, force=False):
        '''Remove entry `name` with parent inode `id_p`

        `id_` must be the inode of `name`. If `force` is True, then
        the `locked` attribute is ignored.
        '''

        log.debug('started with %d, %r', id_p, name)

        now_ns = time_ns()

        # Check that there are no child entries
        if self.db.is_directory(id_):
            log.debug("Attempted to remove entry with children: %s",
                      self.db.get_path(id_p, name))
            raise FUSEError(errno.ENOTEMPTY)

        if self.inodes[id_p].locked and not force:
            raise FUSEError(errno.EPERM)

        self.db.delete_dirent(name, id_p)

        inode = self.inodes[id_]
        inode.refcount -= 1
        inode.ctime_ns = now_ns

        inode_p = self.inodes[id_p]
        inode_p.mtime_ns = now_ns
        inode_p.ctime_ns = now_ns

        if inode.refcount == 0 and id_ not in self.open_inodes:
            log.debug('removing from cache')
            await self.cache.remove(id_, 0, int(math.ceil(inode.size / self.max_obj_size)))
            # Since the inode is not open, it's not possible that new blocks
            # get created at this point and we can safely delete the inode
            self.db.delete_inode(id_)
            del self.inodes[id_]

        log.debug('finished')

    async def symlink(self, id_p, name, target, ctx):
        log.debug('started with %d, %r, %r', id_p, name, target)

        if self.failsafe:
            raise FUSEError(errno.EPERM)

        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)

        # Unix semantics require the size of a symlink to be the length
        # of its target. Therefore, we create symlink directory entries
        # with this size. If the kernel ever learns to open and read
        # symlinks directly, it will read the corresponding number of \0
        # bytes.
        inode = self._create(id_p, name, mode, ctx, size=len(target))
        self.db.symlink(inode.id, target)
        self.open_inodes[inode.id] += 1
        return inode.entry_attributes()

    async def rename(self, id_p_old, name_old, id_p_new, name_new, flags, ctx):
        if flags:
            raise FUSEError(errno.ENOTSUP)

        log.debug('started with %d, %r, %d, %r', id_p_old, name_old, id_p_new, name_new)
        if name_new == CTRL_NAME or name_old == CTRL_NAME:
            log.warning('Attempted to rename s3ql control file (%s -> %s)',
                        self.db.get_path(id_p_old, name_old),
                        self.db.get_path(id_p_new, name_new))
            raise FUSEError(errno.EACCES)

        if (self.failsafe or self.inodes[id_p_old].locked
                or self.inodes[id_p_new].locked):
            raise FUSEError(errno.EPERM)

        inode_old = self._lookup(id_p_old, name_old, ctx)

        try:
            inode_new = self._lookup(id_p_new, name_new, ctx)
        except FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            else:
                target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(id_p_old, name_old, id_p_new, name_new,
                          inode_old.id, inode_new.id)
            await self.forget([(inode_old.id, 1), (inode_new.id, 1)])
        else:
            self.db.rename(id_p_old, name_old, id_p_new, name_new)
            await self.forget([(inode_old.id, 1)])

    def _replace(self, id_p_old, name_old, id_p_new, name_new,
                 id_old, id_new):

        now_ns = time_ns()

        if self.db.is_directory(id_new):
            log.info("Attempted to overwrite entry with children: %s",
                     self.db.get_path(id_p_new, name_new))
            raise FUSEError(errno.EINVAL)

        # Replace target
        self.db.replace_target(name_new, name_old, id_old, id_p_old, id_p_new)

        inode_new = self.inodes[id_new]
        inode_new.refcount -= 1
        inode_new.ctime_ns = now_ns

        inode_p_old = self.inodes[id_p_old]
        inode_p_old.ctime_ns = now_ns
        inode_p_old.mtime_ns = now_ns

        inode_p_new = self.inodes[id_p_new]
        inode_p_new.ctime_ns = now_ns
        inode_p_new.mtime_ns = now_ns

        if inode_new.refcount == 0 and id_new not in self.open_inodes:
            self.cache.remove(id_new, 0,
                              int(math.ceil(inode_new.size / self.max_obj_size)))
            # Since the inode is not open, it's not possible that new blocks
            # get created at this point and we can safely delete the inode
            self.db.delete_inode(id_new)
            del self.inodes[id_new]

    async def link(self, id_, new_id_p, new_name, ctx):
        log.debug('started with %d, %d, %r', id_, new_id_p, new_name)

        if new_name == CTRL_NAME or id_ == CTRL_INODE:
            log.warning('Attempted to create s3ql control file at %s',
                        self.db.get_path(new_id_p, new_name))
            raise FUSEError(errno.EACCES)

        now_ns = time_ns()
        inode_p = self.inodes[new_id_p]

        if inode_p.refcount == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
                        new_name, new_id_p)
            raise FUSEError(errno.EINVAL)

        if self.failsafe or inode_p.locked:
            raise FUSEError(errno.EPERM)

        inode_p.ctime_ns = now_ns
        inode_p.mtime_ns = now_ns

        self.db.link(new_name, id_, new_id_p)
        inode = self.inodes[id_]
        inode.refcount += 1
        inode.ctime_ns = now_ns

        self.open_inodes[inode.id] += 1
        return inode.entry_attributes()

    async def setattr(self, id_, attr, fields, fh, ctx):
        """Handles FUSE setattr() requests"""
        inode = self.inodes[id_]
        if fh is not None:
            assert fh == id_
        now_ns = time_ns()

        if self.failsafe or inode.locked:
            raise FUSEError(errno.EPERM)

        if fields.update_mode:
            inode.mode = attr.st_mode

        if fields.update_uid:
            inode.uid = attr.st_uid

        if fields.update_gid:
            inode.gid = attr.st_gid

        if fields.update_atime:
            inode.atime_ns = attr.st_atime_ns

        if fields.update_mtime:
            inode.mtime_ns = attr.st_mtime_ns

        inode.ctime_ns = now_ns

        # This needs to go last, because the call to cache.remove and cache.get
        # are asynchronous and may thus evict the *inode* object from the cache.
        if fields.update_size:
            len_ = attr.st_size

            # Determine blocks to delete
            last_block = len_ // self.max_obj_size
            cutoff = len_ % self.max_obj_size
            total_blocks = int(math.ceil(inode.size / self.max_obj_size))

            # Adjust file size
            inode.size = len_

            # Delete blocks and truncate last one if required
            if cutoff == 0:
                await self.cache.remove(id_, last_block, total_blocks)
            else:
                await self.cache.remove(id_, last_block + 1, total_blocks)
                try:
                    async with self.cache.get(id_, last_block) as fh:
                        fh.truncate(cutoff)
                except NoSuchObject as exc:
                    log.warning('Backend lost block %d of inode %d (id %s)!',
                                last_block, id_, exc.key)
                    raise

                except CorruptedObjectError as exc:
                    log.warning('Backend returned malformed data for block %d of inode %d (%s)',
                                last_block, id_, exc)
                    self.failsafe = True
                    self.broken_blocks[id_].add(last_block)
                    raise FUSEError(errno.EIO)

        return inode.entry_attributes()

    async def mknod(self, id_p, name, mode, rdev, ctx):
        log.debug('started with %d, %r', id_p, name)
        if self.failsafe:
            raise FUSEError(errno.EPERM)
        inode = self._create(id_p, name, mode, ctx, rdev=rdev)
        self.open_inodes[inode.id] += 1
        return inode.entry_attributes()

    async def mkdir(self, id_p, name, mode, ctx):
        log.debug('started with %d, %r', id_p, name)
        if self.failsafe:
            raise FUSEError(errno.EPERM)
        inode = self._create(id_p, name, mode, ctx)
        self.open_inodes[inode.id] += 1
        return inode.entry_attributes()

    def extstat(self):
        '''Return extended file system statistics'''
        log.debug('started')

        # Flush inode cache to get better estimate of total fs size
        self.inodes.flush()

        (entries, blocks, inodes, fs_size,
         dedup_size, compr_size) = self.db.extstat()

        return struct.pack('QQQQQQQQQQQQ', entries, blocks, inodes, fs_size,
                           dedup_size, compr_size, self.db.get_size(),
                           *self.cache.get_usage())

    async def statfs(self, ctx):
        log.debug('started')

        stat_ = pyfuse3.StatvfsData()

        # Get number of blocks & inodes
        blocks = self.db.blocks_count()
        inodes = self.db.inodes_count()
        size = self.db.fs_size()

        if size is None:
            size = 0

        # file system block size, i.e. the minimum amount of space that can
        # be allocated. This doesn't make much sense for S3QL, so we just
        # return the average size of stored blocks.
        stat_.f_frsize = max(4096, size // blocks) if blocks != 0 else 4096

        # This should actually be the "preferred block size for doing IO.  However, `df` incorrectly
        # interprets f_blocks, f_bfree and f_bavail in terms of f_bsize rather than f_frsize as it
        # should (according to statvfs(3)), so the only way to return correct values *and* have df
        # print something sensible is to set f_bsize and f_frsize to the same value. (cf.
        # http://bugs.debian.org/671490)
        stat_.f_bsize = stat_.f_frsize

        # size of fs in f_frsize units. Since backend is supposed to be unlimited,
        # always return a half-full filesystem, but at least 1 TB)
        fs_size = max(2 * size, 1024 ** 4)

        stat_.f_blocks = fs_size // stat_.f_frsize
        stat_.f_bfree = (fs_size - size) // stat_.f_frsize
        stat_.f_bavail = stat_.f_bfree  # free for non-root

        total_inodes = max(2 * inodes, 1000000)
        stat_.f_files = total_inodes
        stat_.f_ffree = total_inodes - inodes
        stat_.f_favail = total_inodes - inodes  # free for non-root

        return stat_

    async def open(self, id_, flags, ctx):
        log.debug('started with %d', id_)
        if ((flags & os.O_RDWR or flags & os.O_WRONLY)
                and (self.failsafe or self.inodes[id_].locked)):
            raise FUSEError(errno.EPERM)

        if flags & os.O_TRUNC:
            if not (flags & os.O_RDWR or flags & os.O_WRONLY):
                #  behaviour is not defined in POSIX, we opt for an error
                raise FUSEError(errno.EINVAL)
            attr = await self.getattr(id_, ctx)
            if stat.S_ISREG(attr.st_mode):
                attr.st_mtime_ns = time_ns()
                attr.st_size = 0
                await self.setattr(id_, attr, _TruncSetattrFields, None, ctx)
            elif stat.S_ISFIFO(attr.st_mode) or stat.S_ISBLK(attr.st_mode):
                #  silently ignore O_TRUNC when FIFO or terminal block device
                pass
            else:
                #  behaviour is not defined in POSIX, we opt for an error
                raise FUSEError(errno.EINVAL)

        return pyfuse3.FileInfo(fh=id_, keep_cache=True)

    async def access(self, id_, mode, ctx):
        '''Check if requesting process has `mode` rights on `inode`.

        This method always returns true, since it should only be called
        when permission checking is disabled (if permission checking is
        enabled, the `default_permissions` FUSE option should be set).
        '''
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613

        log.debug('started with %d', id_)
        return True

    async def create(self, id_p, name, mode, flags, ctx):
        log.debug('started with id_p=%d, %s', id_p, name)
        if self.failsafe:
            raise FUSEError(errno.EPERM)

        try:
            id_ = self.db.get_dirent_inode(name, id_p)
        except NoSuchRowError:
            inode = self._create(id_p, name, mode, ctx)
        else:
            await self.open(id_, flags, ctx)
            inode = self.inodes[id_]

        self.open_inodes[inode.id] += 1
        return (pyfuse3.FileInfo(fh=inode.id), inode.entry_attributes())

    def _create(self, id_p, name, mode, ctx, rdev=0, size=0):
        if name == CTRL_NAME:
            log.warning('Attempted to create s3ql control file at %s',
                        self.db.get_path(id_p, name))
            raise FUSEError(errno.EACCES)

        now_ns = time_ns()
        inode_p = self.inodes[id_p]

        if inode_p.locked:
            raise FUSEError(errno.EPERM)

        if inode_p.refcount == 0:
            log.warning('Attempted to create entry %s with unlinked parent %d',
                        name, id_p)
            raise FUSEError(errno.EINVAL)
        inode_p.mtime_ns = now_ns
        inode_p.ctime_ns = now_ns

        if inode_p.mode & stat.S_ISGID:
            gid = inode_p.gid
            if stat.S_ISDIR(mode):
                mode |= stat.S_ISGID
        else:
            gid = ctx.gid
        inode = self.inodes.create_inode(
            mtime_ns=now_ns, ctime_ns=now_ns, atime_ns=now_ns, uid=ctx.uid,
            gid=gid, mode=mode, refcount=1, rdev=rdev, size=size)

        self.db.link(name, inode.id, id_p)

        return inode

    async def read(self, fh, offset, length):
        '''Read `size` bytes from `fh` at position `off`

        Unless EOF is reached, returns exactly `size` bytes.
        '''
        log.debug('started with %d, %d, %d', fh, offset, length)
        buf = BytesIO()
        inode = self.inodes[fh]

        # Make sure that we don't read beyond the file size. This
        # should not happen unless direct_io is activated, but it's
        # cheap and nice for testing.
        size = inode.size
        length = min(size - offset, length)

        while length > 0:
            tmp = await self._readwrite(fh, offset, length=length)
            buf.write(tmp)
            length -= len(tmp)
            offset += len(tmp)

        # Inode may have expired from cache
        inode = self.inodes[fh]

        if inode.atime_ns < inode.ctime_ns or inode.atime_ns < inode.mtime_ns:
            inode.atime_ns = time_ns()

        return buf.getvalue()

    async def write(self, fh, offset, buf):
        '''Handle FUSE write requests.'''
        log.debug('started with %d, %d, datalen=%d', fh, offset, len(buf))

        if self.failsafe or self.inodes[fh].locked:
            raise FUSEError(errno.EPERM)

        total = len(buf)
        minsize = offset + total
        while buf:
            written = await self._readwrite(fh, offset, buf=buf)
            offset += written
            buf = buf[written:]

        # Update file size if changed
        # Fuse does not ensure that we do not get concurrent write requests,
        # so we have to be careful not to undo a size extension made by
        # a concurrent write.
        now_ns = time_ns()
        inode = self.inodes[fh]
        inode.size = max(inode.size, minsize)
        inode.mtime_ns = now_ns
        inode.ctime_ns = now_ns
        # commit changes to database
        self.inodes.setattr(inode)

        return total

    async def _readwrite(self, id_, offset, *, buf=None, length=None):
        """Read or write as much as we can.

        If *buf* is None, read and return up to *length* bytes.

        If *length* is None, write from *buf* and return the number of
        bytes written.

        This is one method to reduce code duplication.
        """

        # Calculate required block
        blockno = offset // self.max_obj_size
        offset_rel = offset - blockno * self.max_obj_size

        if id_ in self.broken_blocks and blockno in self.broken_blocks[id_]:
            raise FUSEError(errno.EIO)

        if length is None:
            write = True
            length = len(buf)
        elif buf is None:
            write = False
        else:
            raise TypeError("Don't know what to do!")

        # Don't try to write/read into the next block
        if offset_rel + length > self.max_obj_size:
            length = self.max_obj_size - offset_rel

        try:
            async with self.cache.get(id_, blockno) as fh:
                fh.seek(offset_rel)
                if write:
                    fh.write(buf[:length])
                else:
                    buf = fh.read(length)

        except NoSuchObject as exc:
            log.error('Backend lost block %d of inode %d (id %s)!',
                      blockno, id_, exc.key)
            self.failsafe = True
            self.broken_blocks[id_].add(blockno)
            raise FUSEError(errno.EIO)

        except CorruptedObjectError as exc:
            log.error('Backend returned malformed data for block %d of inode %d (%s)',
                      blockno, id_, exc)
            self.failsafe = True
            self.broken_blocks[id_].add(blockno)
            raise FUSEError(errno.EIO)

        if write:
            return length
        elif len(buf) == length:
            return buf
        else:
            # If we can't read enough, add null bytes
            return buf + b"\0" * (length - len(buf))

    async def fsync(self, fh, datasync):
        log.debug('started with %d, %s', fh, datasync)
        if not datasync:
            self.inodes.flush_id(fh)

        for blockno in range(0, self.inodes[fh].size // self.max_obj_size + 1):
            self.cache.flush_local(fh, blockno)

    async def forget(self, forget_list):
        log.debug('started with %s', forget_list)

        for (id_, nlookup) in forget_list:
            self.open_inodes[id_] -= nlookup

            if self.open_inodes[id_] == 0:
                del self.open_inodes[id_]
                if id_ in self.broken_blocks:
                    del self.broken_blocks[id_]

                inode = self.inodes[id_]
                if inode.refcount == 0:
                    log.debug('removing %d from cache', id_)
                    await self.cache.remove(
                        id_, 0, inode.size // self.max_obj_size + 1)
                    # Since the inode is not open, it's not possible that new blocks
                    # get created at this point and we can safely delete the inode
                    self.db.delete_inode(id_)
                    del self.inodes[id_]

    async def fsyncdir(self, fh, datasync):
        log.debug('started with %d, %s', fh, datasync)
        if not datasync:
            self.inodes.flush_id(fh)

    async def releasedir(self, fh):
        log.debug('started with %d', fh)

    async def release(self, fh):
        log.debug('started with %d', fh)

    async def flush(self, fh):
        log.debug('started with %d', fh)


def update_logging(level, modules):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    if level == logging.DEBUG:
        logging.disable(logging.NOTSET)
        if 'all' in modules:
            root_logger.setLevel(logging.DEBUG)
        else:
            for module in modules:
                logging.getLogger(module).setLevel(logging.DEBUG)

    else:
        logging.disable(logging.DEBUG)
