'''
metadata.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError  # Ensure use of custom logger class
from .database import Connection, NoSuchRowError
from . import BUFSIZE, CTRL_INODE, ROOT_INODE, CURRENT_FS_REV
from .backends.comprenc import ComprencBackend
from .common import time_ns, get_backend
from .backends.common import CorruptedObjectError
from contextlib import contextmanager
import atexit
import bz2
import json
import os
import stat
import time
import trio


log = logging.getLogger(__name__)


class MetadataBackend(object):
    def __init__(self, backend=None, cachepath=None,
                 mkfs=False, options=None):
        self.backend = backend
        self.cachepath = cachepath
        self.param = None
        self.nodeid = None
        self._metadata_upload_task = None
        self.requests = None
        self.prepared_requests = dict()
        self.db = Connection(self.cachepath, [options.db_host])
        if mkfs:
            self.nodeid = 1
            save_persistent_config({"nodeid": self.nodeid})
        else:
            p_config = load_persistent_config()
            if p_config:
                self.nodeid = p_config["nodeid"]
            else:
                self.nodeid = self.find_free_nodeid()
                save_persistent_config({"nodeid": self.nodeid})

        self.inodeid_key = 'last_inodeid'
        self.objectid_key = 'last_objectid'
        self.blockid_key = 'last_blockid'

        # local node counts and sums
        self._inode_count_key = "inode_count"
        self._object_count_key = "object_count"
        self._entries_count_key = "entries_count"
        self._blocksize_sum_key = "blocksize_sum"
        self._inodesize_sum_key = "inodesize_sum"
        self._objectsize_sum_key = "objectsize_sum"

        self.general_param_keys = [
            'revision', 'seq_no', 'max_obj_size', 'needs_fsck', 'inode_gen',
            'last_fsck', 'last-modified']
        self.node_param_keys = [
            self.inodeid_key, self.objectid_key, self.blockid_key,
            self._inode_count_key, self._object_count_key,
            self._entries_count_key, self._blocksize_sum_key,
            self._inodesize_sum_key, self._objectsize_sum_key]
        self.inode_param_keys = [
            self.inodeid_key, self.objectid_key, self.blockid_key]
        self.all_param_keys = self.general_param_keys + self.node_param_keys

        if mkfs:
            self.mkfs(options)

        self.get_metadata(backend)
        ino_id, obj_id, blk_id = self.check_available_ids()
        self.param[self.inodeid_key] = ino_id
        self.param[self.objectid_key] = obj_id
        self.param[self.blockid_key] = blk_id
        self._inoid_gen = inodeid_gen(last_id=ino_id)
        self._objid_gen = inodeid_gen(last_id=obj_id)
        self._blkid_gen = inodeid_gen(last_id=blk_id)

        # local node counts and sums
        self._inode_count = self.param.get(self._inode_count_key, 0)
        self._object_count = self.param.get(self._object_count_key, 0)
        self._entries_count = self.param.get(self._entries_count_key, 0)
        self._blocksize_sum = self.param.get(self._blocksize_sum_key, 0)
        self._inodesize_sum = self.param.get(self._inodesize_sum_key, 0)
        self._objectsize_sum = self.param.get(self._objectsize_sum_key, 0)

        self.prepare_requests()

    def check_available_ids(self):
        has_ino = self.db.has_val(
            "SELECT * FROM inodes_refcount WHERE inode=%s",
            (self.param[self.inodeid_key]+1,))
        if has_ino:
            free_ino = self.find_free_id()
        else:
            free_ino = self.param[self.inodeid_key]

        has_obj = self.db.has_val(
            "SELECT * FROM objects_refcount WHERE id=%s",
            (self.param[self.objectid_key]+1,))
        if has_obj:
            free_obj = self.find_free_id(category=1)
        else:
            free_obj = self.param[self.objectid_key]

        has_blk = self.db.has_val(
            "SELECT * FROM blocks_refcount WHERE id=%s",
            (self.param[self.blockid_key]+1,))
        if has_blk:
            free_blk = self.find_free_id(category=2)
        else:
            free_blk = self.param[self.blockid_key]

        log.debug("free ids : {} {} {}".format(free_ino, free_obj, free_blk))
        return (free_ino, free_obj, free_blk)

    def mkfs(self, options):
        create_keyspace(self.db)
        create_tables(self.db)
        init_tables(self.db)

        param = dict()
        param['revision'] = CURRENT_FS_REV
        param['seq_no'] = int(time.time())
        # param['label'] = options.label
        param['max_obj_size'] = options.max_obj_size * 1024
        param['needs_fsck'] = False
        param['inode_gen'] = 0
        param['last_fsck'] = int(time.time())
        param['last-modified'] = int(time.time())
        param[self.inodeid_key] = 0 + (self.nodeid << 32)
        param[self.objectid_key] = 0 + (self.nodeid << 32)
        param[self.blockid_key] = 0 + (self.nodeid << 32)

        for k, v in param.items():
            if k == "needs_fsck":
                v = int(v)
                nodeid = 0
            elif k in self.node_param_keys:
                if k in self.inode_param_keys:
                    v = v - (self.nodeid << 32)
                nodeid = self.nodeid
            self.db.execute(("INSERT INTO params (name, node, value) "
                             "VALUES (%s, %s, %s)"),
                            (k, nodeid, v))

        plain_backend = get_backend(options, raw=True)
        atexit.register(plain_backend.close)

        if 's3ql_metadata' in plain_backend:
            raise QuietError("Refusing to overwrite existing file system! "
                             "(use `s3qladm clear` to delete)")
        data_pw = None
        backend = ComprencBackend(data_pw, ('lzma', 2), plain_backend)
        atexit.register(backend.close)
        write_empty_metadata_file(backend)

    def prepare_requests(self):
        self.requests = {
            "blocks_count": ("SELECT SUM(value) FROM params WHERE name='{}'"
                             .format(self._object_count_key)),
            "inodes_count": ("SELECT SUM(value) FROM params WHERE name='{}'"
                             .format(self._inode_count_key)),
            "fs_size": ("SELECT SUM(value) FROM params WHERE name='{}'"
                        .format(self._blocksize_sum_key)),

            "entries_count": ("SELECT SUM(value) FROM params WHERE name='{}'"
                              .format(self._entries_count_key)),
            "fs_full_size": ("SELECT SUM(value) FROM params WHERE name='{}'"
                             .format(self._inodesize_sum_key)),
            "objectsize_sum": ("SELECT SUM(value) FROM params WHERE name='{}'"
                               .format(self._objectsize_sum_key)),

            "parent_inode": ("SELECT parent_inode "
                             "FROM parent_inodes WHERE inode=?"),
            "dirent_inode": ("SELECT inode FROM contents "
                             "WHERE parent_inode=? AND name=?"),
            "readdir": ("SELECT name, inode FROM contents "
                        "WHERE parent_inode=? ORDER BY name"),
            "batch_list_dir": ("SELECT name, inode FROM contents "
                               "WHERE parent_inode=? "
                               "ORDER BY name LIMIT ?"),
            "delete_dirent": ("DELETE FROM contents "
                              "WHERE name=? AND parent_inode=?"),
            "is_directory": "SELECT inode FROM contents WHERE parent_inode=?",
            "link1": ("INSERT INTO contents (parent_inode, name, inode) "
                      "VALUES (?, ?, ?)"),
            "link2": ("INSERT INTO parent_inodes (inode, parent_inode) "
                      "VALUES (?, ?)"),
            "rename1": ("SELECT inode FROM contents "
                        "WHERE parent_inode=? AND name=?"),
            "rename5": "DELETE FROM parent_inodes WHERE inode=?",

            "make_copy_visible1": ("SELECT parent_inode, name, inode "
                                   "FROM contents WHERE parent_inode=?"),
            "make_copy_visible4": "DELETE FROM contents WHERE parent_inode=?",

            "getxattr": ("SELECT value FROM ext_attributes "
                         "WHERE inode=? AND name=?"),
            "listxattr": "SELECT name FROM ext_attributes WHERE inode=?",
            "setxattr": ("INSERT INTO ext_attributes (inode, name, value) "
                         "VALUES (?, ?, ?)"),
            "removexattr": ("DELETE FROM ext_attributes "
                            "WHERE inode=? AND name=?"),
            "readlink": "SELECT target FROM symlink_targets WHERE inode=?",
            "symlink": ("INSERT INTO symlink_targets (inode, target) "
                        "VALUES (?, ?)"),

            "copy_tree_files01": ("SELECT inode, target FROM symlink_targets "
                                  "WHERE inode=?"),
            "copy_tree_files02": ("INSERT INTO symlink_targets (inode, target)"
                                  " VALUES (?, ?)"),
            "copy_tree_files04": ("SELECT inode, name, value "
                                  "FROM ext_attributes WHERE inode=?"),
            "copy_tree_files05": ("INSERT INTO ext_attributes (inode, name, "
                                  "value) VALUES (?, ?, ?)"),
            "copy_tree_files08": ("SELECT inode, blockno, block_id "
                                  "FROM inodes_blocks WHERE inode=?"),
            "copy_tree_files09": ("INSERT INTO inodes_blocks (inode, blockno, "
                                  "block_id) VALUES (?, ?, ?)"),
            "block_incr_refcount": ("UPDATE blocks_refcount "
                                    "SET refcount = refcount + ? WHERE id=?"),

            "create_inode": ("INSERT INTO inodes (mode, uid, gid, size, "
                             "locked,rdev, atime_ns, mtime_ns, ctime_ns, id) "
                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            "inode_incr_refc": ("UPDATE inodes_refcount "
                                "SET refcount = refcount + 1 "
                                "WHERE inode = ?"),

            "get_inode1": "SELECT * FROM inodes WHERE id=?",
            "get_inode2": "SELECT * FROM inodes_refcount WHERE inode=?",

            "delete_inode1": ("UPDATE inodes_refcount "
                              "SET refcount = refcount - 1 WHERE inode=?"),
            "delete_inode2": ("SELECT refcount FROM inodes_refcount "
                              "WHERE inode=?"),
            "delete_inode3": "DELETE FROM ext_attributes WHERE inode=?",
            "delete_inode4": "DELETE FROM symlink_targets WHERE inode=?",

            "update_inode1": ("UPDATE inodes SET mode=?, uid=?, gid=?, size=?,"
                              " locked=?, rdev=?, atime_ns=?, mtime_ns=?, "
                              "ctime_ns=? WHERE id=?"),
            # "update_inode2": "UPDATE inodes_refcount SET refcount=?",

            "cache_delete_inode1": "DELETE FROM inodes WHERE id=?",
            "cache_delete_inode2": "DELETE FROM inodes_refcount WHERE inode=?",

            "create_object1": ("INSERT INTO objects (id, size) "
                               "VALUES (?, -1)"),
            "create_object2": ("UPDATE objects_refcount "
                               "SET refcount = refcount + 1 WHERE id=?"),

            "update_object_size": "UPDATE objects SET size=? WHERE id=?",

            "nullify_block_hash1": ("SELECT blockid FROM blocks_by_objid "
                                    "WHERE objid=?"),
            "nullify_block_hash2": "SELECT hash FROM blocks WHERE id=?",
            "nullify_block_hash3": "UPDATE blocks SET hash=NULL WHERE id=?",
            "nullify_block_hash4": "DELETE FROM blocks_by_hash WHERE hash=?",

            "get_block_id": ("SELECT block_id FROM inodes_blocks "
                             "WHERE inode=? AND blockno=?"),

            "inode_add_block": ("INSERT INTO inodes_blocks (inode, blockno, "
                                "block_id) VALUES (?,?,?)"),
            "inode_del_block": ("DELETE FROM inodes_blocks WHERE inode=? "
                                "AND blockno=?"),
            "block_id_by_hash": ("SELECT blockid FROM blocks_by_hash "
                                 "WHERE hash=?"),

            "create_block1": ("INSERT INTO blocks (id, hash, size, obj_id) "
                              "VALUES (?,?,?,?)"),
            "create_block2": ("UPDATE blocks_refcount "
                              "SET refcount = refcount + 1 WHERE id = ?"),
            "create_block3": ("INSERT INTO blocks_by_hash (hash, blockid) "
                              "VALUES (?, ?)"),
            "create_block4": ("INSERT INTO blocks_by_objid (objid, blockid) "
                              "VALUES (?, ?)"),

            "increment_block_ref": ("UPDATE blocks_refcount "
                                    "SET refcount=refcount+1 WHERE id=?"),

            "get_objid": "SELECT obj_id FROM blocks WHERE id=?",

            "deref_block1": ("SELECT hash, size, obj_id FROM blocks "
                             "WHERE id=?"),
            "deref_block2": ("SELECT refcount FROM blocks_refcount "
                             "WHERE id=?"),
            "deref_block3": ("UPDATE blocks_refcount SET refcount=refcount-1 "
                             "WHERE id=?"),
            "deref_block4": "DELETE FROM blocks WHERE id=?",
            "deref_block5": "DELETE FROM blocks_by_hash WHERE hash=?",
            "deref_block6": "DELETE FROM blocks_by_objid WHERE objid=?",
            "object_size": "SELECT size FROM objects WHERE id=?",
            "deref_block8": "SELECT refcount FROM objects_refcount WHERE id=?",
            "deref_block9": ("UPDATE objects_refcount SET refcount=refcount-1 "
                             "WHERE id=?"),
            "deref_block10": "DELETE FROM objects WHERE id=?",
        }

        for name, req in self.requests.items():
            try:
                self.prepared_requests[name] = self.db.conn.prepare(req)
            except:
                print(name)
                print(req)
                raise

    def get_metadata(self, backend):
        '''Retrieve metadata'''
        param = dict()
        for pname in self.all_param_keys:
            try:
                if pname in self.general_param_keys:
                    nodeid = 0
                elif pname in self.node_param_keys:
                    nodeid = self.nodeid

                param[pname] = self.db.get_val(
                    "SELECT value FROM params WHERE name=%s AND node=%s",
                    (pname, nodeid))
                if pname in self.inode_param_keys:
                    param[pname] = param[pname] + (self.nodeid << 32)

            except NoSuchRowError:
                if pname in self.inode_param_keys:
                    param[pname] = 0 + (self.nodeid << 32)
                    self.db.execute(
                        ("INSERT INTO params (name, node, value) "
                         "VALUES (%s, %s, %s)"),
                        (pname, self.nodeid, 0))

        # Check revision
        if param['revision'] < CURRENT_FS_REV:
            raise QuietError(
                'File system revision too old, please run `s3qladm upgrade` '
                'first.',
                exitcode=32)
        elif param['revision'] > CURRENT_FS_REV:
            raise QuietError(
                'File system revision too new, please update your '
                'S3QL installation.', exitcode=33)

        # Check that the fs itself is clean
        if param['needs_fsck']:
            raise QuietError(
                "File system damaged or not unmounted cleanly, run fsck!",
                exitcode=30)
        if time.time() - param['last_fsck'] > 60 * 60 * 24 * 31:
            log.warning('Last file system check was more than 1 month ago, '
                        'running fsck.s3ql is recommended.')

        self.param = param

    def find_free_nodeid(self):
        i = 1
        low_taken = 0
        high_free = 0x7FFFFFFF
        while True:
            try:
                self.db.get_val(
                    "SELECT value FROM params WHERE name=%s AND node=%s",
                    ('last_inodeid', i))
            except NoSuchRowError:
                if i == low_taken + 1:
                    return i
                high_free = i
                i -= (high_free - low_taken) // 2 or 1
            else:
                low_taken = i
                i += (high_free - low_taken) // 2 or 1

    def find_free_id(self, category=0):
        requests = ["SELECT * FROM inodes_refcount WHERE inode=%s",
                    "SELECT * FROM objects_refcount WHERE id=%s",
                    "SELECT * FROM blocks_refcount WHERE id=%s"]
        if not 0 <= category < 3:
            category = 0
        request = requests[category]
        i = 1
        low_taken = 0
        high_free = 0x7FFFFFFF
        while True:
            try:
                self.db.get_val(request, (i + (self.nodeid << 32),))
            except NoSuchRowError:
                if i == low_taken + 1:
                    return i + (self.nodeid << 32)
                high_free = i
                i -= (high_free - low_taken) // 2 or 1
            else:
                low_taken = i
                i += (high_free - low_taken) // 2 or 1

    def mark_metadata_dirty(self):
        '''Mark metadata as dirty and increase sequence number'''
        pass

    def dump_and_upload_metadata(self):
        pass

    def metadata_upload_task(self, backend_pool,
                             metadata_upload_interval):
        self._metadata_upload_task = MetadataUploadTask(
            backend_pool, self.param, self.db, metadata_upload_interval)
        return self._metadata_upload_task

    def load_params(self):
        pass

    def save_params(self):
        pass

    def make_nfsindex(self):
        pass

    def del_nfsindex(self):
        pass

    def blocks_count(self):
        log.debug("blocks_count")
        return self.db.get_val(self.prepared_requests["blocks_count"])

    def inodes_count(self):
        log.debug("inodes_count")
        return self.db.get_val(self.prepared_requests["inodes_count"])

    def fs_size(self):
        log.debug("fs_size")
        return self.db.get_val(self.prepared_requests["fs_size"])

    def parent_inode(self, inodeid):
        log.debug("{}".format(inodeid))
        return self.db.get_val(self.prepared_requests["parent_inode"],
                               (inodeid,))

    def get_dirent_inode(self, name, parent_inode):
        log.debug("{}, {}".format(parent_inode, name))
        return self.db.get_val(self.prepared_requests["dirent_inode"],
                               (parent_inode, name))

    def readdir(self, inodeid, off):
        log.debug("{}, {}".format(inodeid, off))
        names = self.db.execute(self.prepared_requests["readdir"],
                                (inodeid,))

        @contextmanager
        def ctxwrap():
            def gen():
                for i, row in enumerate(names):
                    if i > off:
                        yield (i, row.name, row.inode)
            yield gen()

        return ctxwrap()

    def list_directory(self, parent_inode, off):
        log.debug("{}, {}".format(parent_inode, off))
        names = self.db.execute(self.prepared_requests["readdir"],
                                (parent_inode,))

        @contextmanager
        def ctxwrap():
            def gen():
                for i, row in enumerate(names):
                    if i > off:
                        yield (i, row.inode)
            yield gen()

        return ctxwrap()

    def batch_list_dir(self, batch_size, parent_inode):
        log.debug("{}, {}".format(batch_size, parent_inode))
        return self.db.get_list(self.prepared_requests["batch_list_dir"],
                                (parent_inode, batch_size))

    def delete_dirent(self, name, parent_inode):
        log.debug("{}, {}".format(name, parent_inode))
        self.db.execute(self.prepared_requests["delete_dirent"],
                        (name, parent_inode))
        self._entries_count -= 1

    def is_directory(self, inodeid):
        log.debug("{}".format(inodeid))
        return self.db.has_val(self.prepared_requests["is_directory"],
                               (inodeid,))

    def link(self, name, inodeid, parent_inode_id, incr_entry_count=True):
        log.debug("{} {} {}".format(name, inodeid, parent_inode_id))
        self.db.execute(self.prepared_requests["link1"],
                        (parent_inode_id, name, inodeid))
        self.db.execute(self.prepared_requests["link2"],
                        (inodeid, parent_inode_id))
        if incr_entry_count:
            self._entries_count += 1

    def rename(self, id_p_old, name_old, id_p_new, name_new):
        log.debug("{} {} {} {}".format(id_p_old, name_old, id_p_new, name_new))
        inode = self.db.get_val(self.prepared_requests["rename1"],
                                (id_p_old, name_old))
        self.db.execute(self.prepared_requests["link1"],
                        (id_p_new, name_new, inode))
        self.db.execute(self.prepared_requests["link2"], (inode, id_p_new))
        self.db.execute(self.prepared_requests["delete_dirent"],
                        (name_old, id_p_old))
        self.db.execute(self.prepared_requests["rename5"], (inode,))

    def make_copy_visible(self, inodeid, tmpid):
        log.debug("{} {}".format(inodeid, tmpid))
        tmp_parents = self.db.query(
            self.prepared_requests["make_copy_visible1"],
            (tmpid,))
        for row in tmp_parents:
            self.link(row.name, row.inode, inodeid, incr_entry_count=False)

        self.db.execute(self.prepared_requests["make_copy_visible4"], (tmpid,))

    def copy_tree_dirs(self, name, id_new, target_id):
        log.debug("{} {} {}".format(name, id_new, target_id))
        self.link(name, id_new, target_id)

    def replace_target(self, name_new, name_old, id_old, id_p_old, id_p_new):
        log.debug("{} {} {} {} {}".format(name_new, name_old, id_old, id_p_old,
                                          id_p_new))
        self.link(name_new, id_old, id_p_new, incr_entry_count=False)
        self.db.execute(self.prepared_requests["delete_dirent"],
                        (name_old, id_p_old))
        # TODO do we have to delete old entry in parent_inodes too ?

    def getxattr(self, inodeid, name):
        """ extended attribute value associated with name """
        log.debug("{} {}".format(inodeid, name))
        return self.db.get_val(self.prepared_requests["getxattr"],
                               (inodeid, name))

    def listxattr(self, inodeid):
        """ list of extended attributes keys """
        log.debug("{}".format(inodeid))
        return self.db.query(self.prepared_requests["listxattr"],
                             (inodeid,))

    def setxattr(self, inodeid, name, value):
        log.debug("{} {} {}".format(inodeid, name, value))
        return self.db.execute(self.prepared_requests["setxattr"],
                               (inodeid, name, value))

    def removexattr(self, inodeid, name):
        log.debug("{} {}".format(inodeid, name))
        return self.db.execute(self.prepared_requests["removexattr"],
                               (inodeid, name))

    def readlink(self, inodeid):
        log.debug("{}".format(inodeid))
        return self.db.get_val(self.prepared_requests["readlink"],
                               (inodeid,))

    def symlink(self, inodeid, target):
        log.debug("{} {}".format(inodeid, target))
        self.db.execute(self.prepared_requests["symlink"],
                        (inodeid, target))

    def copy_tree_files(self, new_id, cur_id):
        log.debug("{} {}".format(cur_id, new_id))

        for cur_symlink in self.db.query(
                self.prepared_requests["copy_tree_files01"],
                (cur_id,)):
            self.db.execute(self.prepared_requests["copy_tree_files02"],
                            (new_id, cur_symlink.target))

        for eattr in self.db.query(self.prepared_requests["copy_tree_files04"],
                                   (cur_id,)):
            self.db.execute(self.prepared_requests["copy_tree_files05"],
                            (new_id, eattr.name, eattr.value))

        processed = 0
        for block in self.db.query(
                self.prepared_requests["copy_tree_files08"], (cur_id,)):
            self.db.execute(
                self.prepared_requests["copy_tree_files09"],
                (new_id, block.blockno, block.block_id))
            processed += 1

            # add 1 to block_id refcount
            self.db.execute(self.prepared_requests["block_incr_refcount"],
                            (1, block.block_id))

        return processed

    def extstat(self):
        log.debug("extstat")
        entries = self.db.get_val(self.prepared_requests["entries_count"])
        blocks = self.blocks_count()
        inodes = self.inodes_count()
        fs_size = self.db.get_val(self.prepared_requests["fs_full_size"])
        dedup_size = self.fs_size() or 0

        # Objects that are currently being uploaded/compressed have size == -1
        compr_size = self.db.get_val(
            self.prepared_requests["objectsize_sum"]) or 0

        return (entries, blocks, inodes, fs_size, dedup_size, compr_size,
                self.db.get_size())

    def create_inode(self, kw):
        log.debug("{}".format(kw))
        ATTRIBUTES = ('mode', 'uid', 'gid', 'size', 'locked',
                      'rdev', 'atime_ns', 'mtime_ns', 'ctime_ns')
        bindings = list()
        for attr in ATTRIBUTES:
            if attr == "locked":
                bindings.append(False)
            else:
                bindings.append(kw.get(attr, 0))

        bindings = tuple(bindings)

        inode_id = next(self._inoid_gen)
        self.db.execute(self.prepared_requests["create_inode"],
                        bindings + (inode_id,))
        self.db.execute(self.prepared_requests["inode_incr_refc"],
                        (inode_id,))
        self.param[self.inodeid_key] = inode_id
        self._inode_count += 1
        self._inodesize_sum += kw.get("size", 0)
        return inode_id

    def get_inode(self, inodeid):
        log.debug("{}".format(inodeid))
        ino = self.db.get_row(self.prepared_requests["get_inode1"],
                              (inodeid,))
        refc = self.db.get_row(self.prepared_requests["get_inode2"],
                               (inodeid,))
        return (ino.mode, refc.refcount, ino.uid, ino.gid, ino.size,
                ino.locked, ino.rdev, ino.atime_ns, ino.mtime_ns, ino.ctime_ns,
                ino.id)

    def delete_inode(self, inodeid):
        log.debug("{}".format(inodeid))
        self.db.execute(self.prepared_requests["delete_inode1"], (inodeid,))
        refcount = self.db.get_val(self.prepared_requests["delete_inode2"],
                                   (inodeid,))
        if refcount == 0:
            self.db.execute(self.prepared_requests["delete_inode3"],
                            (inodeid,))
            self.db.execute(self.prepared_requests["delete_inode4"],
                            (inodeid,))

    def update_inode(self, inode, update_attrs):
        log.debug("{} {}".format(inode, update_attrs))
        # update_str = ', '.join('%s=?' % x for x in update_attrs
        #                        if x != 'refcount')
        # request = "UPDATE inodes SET %s WHERE id=?" % update_str
        bindings = [getattr(inode, x) for x in update_attrs
                    if x != 'refcount'] + [inode.id]
        log.debug(bindings)
        old_ino = self.db.get_row(self.prepared_requests["get_inode1"],
                                  (inode.id,))
        self.db.execute(self.prepared_requests["update_inode1"], bindings)
        self._inodesize_sum += getattr(inode, "size") - old_ino.size
        # self.db.execute(self.prepared_requests["update_inode2"],
        #                 (getattr(inode, 'refcount'),))

    def cache_delete_inode(self, inodeid):
        log.debug("{}".format(inodeid))
        ino = self.db.get_row(self.prepared_requests["get_inode1"],
                              (inodeid,))
        self.db.execute(self.prepared_requests["cache_delete_inode1"],
                        (inodeid,))
        # currently commented out, keeping track of all inodes ever used
        # in inodes_refcount let us find last inode number used with dichotomy
        # search in case of fs crash
        # self.db.execute(self.prepared_requests["cache_delete_inode2"],
        #                 (inodeid,))
        self._inode_count -= 1
        self._inodesize_sum -= ino.size
        return 1  # cannot know how many rows were deleted (fix in
        # inode_cache.py ?)

    def create_object(self):
        log.debug("create object")
        obj_id = next(self._objid_gen)
        self.db.execute(self.prepared_requests["create_object1"],
                        (obj_id,))
        self.db.execute(self.prepared_requests["create_object2"],
                        (obj_id,))
        self.param[self.objectid_key] = obj_id
        self._object_count += 1

        return obj_id

    def update_object_size(self, obj_id, obj_size):
        """ called after successfull block upload """
        log.debug("{} {}".format(obj_id, obj_size))
        oldsize = self.db.get_val(self.prepared_requests["object_size"],
                                  (obj_id,))
        self.db.execute(self.prepared_requests["update_object_size"],
                        (obj_size, obj_id))
        if oldsize == -1:
            self._objectsize_sum += obj_size
        else:
            self._objectsize_sum += obj_size - oldsize

    def nullify_block_hash(self, obj_id):
        log.debug("{}".format(obj_id))
        blockid = self.db.get_val(
            self.prepared_requests["nullify_block_hash1"], (obj_id))
        hash_ = self.db.get_val(self.prepared_requests["nullify_block_hash2"],
                                (blockid,))
        self.db.execute(self.prepared_requests["nullify_block_hash3"],
                        (blockid,))
        self.db.execute(self.prepared_requests["nullify_block_hash4"],
                        (hash_,))

    def get_block_id(self, inodeid, blockno):
        log.debug("{} {}".format(inodeid, blockno))
        try:
            return self.db.get_val(self.prepared_requests["get_block_id"],
                                   (inodeid, blockno))
        except NoSuchRowError:
            return None

    def inode_add_block(self, blockid, inodeid, blockno):
        log.debug("{} {} {}".format(inodeid, blockid, blockno))
        self.db.execute(self.prepared_requests["inode_add_block"],
                        (inodeid, blockno, blockid))

    def inode_del_block(self, inode, blockno):
        log.debug("{} {}".format(inode, blockno))
        self.db.execute(self.prepared_requests["inode_del_block"],
                        (inode, blockno))

    def block_id_by_hash(self, hash_):
        log.debug("{}".format(hash_))
        return self.db.get_val(self.prepared_requests["block_id_by_hash"],
                               (hash_,))

    def create_block(self, obj_id, hash_, size):
        log.debug("{} {} {}".format(obj_id, hash_, size))
        blockid = next(self._blkid_gen)
        self.db.execute(self.prepared_requests["create_block1"],
                        (blockid, hash_, size, obj_id))
        self.db.execute(self.prepared_requests["create_block2"],
                        (blockid,))
        self.db.execute(self.prepared_requests["create_block3"],
                        (hash_, blockid))
        self.db.execute(self.prepared_requests["create_block4"],
                        (obj_id, blockid))
        self.param[self.blockid_key] = blockid
        self._blocksize_sum += size
        return blockid

    def increment_block_ref(self, blockid):
        log.debug("{}".format(blockid))
        self.db.execute(self.prepared_requests["increment_block_ref"],
                        (blockid,))

    def get_objid(self, blockid):
        log.debug("{}".format(blockid))
        return self.db.get_val(self.prepared_requests["get_objid"],
                               (blockid,))

    def deref_block(self, block_id):
        log.debug("{}".format(block_id))
        hash_, size, obj_id = self.db.get_row(
            self.prepared_requests["deref_block1"], (block_id,))
        refcount = self.db.get_val(self.prepared_requests["deref_block2"],
                                   (block_id,))
        if refcount > 1:
            log.debug('decreased refcount for block: %d', block_id)
            self.db.execute(self.prepared_requests["deref_block3"],
                            (block_id,))
            return None, None

        log.debug('removing block %d', block_id)
        self.db.execute(self.prepared_requests["deref_block4"], (block_id,))
        self.db.execute(self.prepared_requests["deref_block5"], (hash_,))
        self.db.execute(self.prepared_requests["deref_block6"], (obj_id,))
        self._blocksize_sum -= size
        obj_size = self.db.get_val(
            self.prepared_requests["object_size"], (obj_id,))
        refcount = self.db.get_val(
            self.prepared_requests["deref_block8"], (obj_id,))
        if refcount > 1:
            log.debug('decreased refcount for obj: %d', obj_id)
            self.db.execute(
                self.prepared_requests["deref_block9"], (obj_id,))
            return None, obj_size

        log.debug('removing object %d', obj_id)
        self.db.execute(self.prepared_requests["deref_block10"], (obj_id,))
        self._object_count -= 1
        self._objectsize_sum -= obj_size
        return obj_id, obj_size

    def get_path(self, id_, name=None):
        if name is None:
            path = list()
        else:
            if not isinstance(name, bytes):
                raise TypeError('name must be of type bytes')
            path = [name]

        maxdepth = 255

        while id_ != ROOT_INODE:
            # This can be ambiguous if directories are hardlinked
            id_p = self.db.get_val(self.prepared_requests["parent_inode"],
                                   (id_,))
            rows = self.db.query(self.prepared_requests["readdir"], (id_p,))
            for r in rows:
                if r.inode == id_:
                    name2 = r.name

            path.append(name2)
            id_ = id_p
            maxdepth -= 1
            if maxdepth == 0:
                raise RuntimeError(
                    'Failed to resolve name "%s" at inode %d to path',
                    name, id_)

        path.append(b'')
        path.reverse()

        return b'/'.join(path)

    def close(self):
        self.param[self._inode_count_key] = self._inode_count
        self.param[self._object_count_key] = self._object_count
        self.param[self._entries_count_key] = self._entries_count
        self.param[self._blocksize_sum_key] = self._blocksize_sum
        self.param[self._inodesize_sum_key] = self._inodesize_sum
        self.param[self._objectsize_sum_key] = self._objectsize_sum

        # write params
        for pname in self.all_param_keys:
            if pname in self.general_param_keys:
                nodeid = 0
            elif pname in self.node_param_keys:
                nodeid = self.nodeid

            if pname in self.inode_param_keys:
                value = self.param[pname] - (self.nodeid << 32)
            else:
                value = self.param[pname]

            self.db.execute(
                "INSERT INTO params (name, node, value) VALUES (%s, %s, %s)",
                (pname, nodeid, value))

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
        # self.db_mtime = os.stat(db.file).st_mtime
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

        # Break reference loop
        self.fs = None

        log.debug('finished')

    def stop(self):
        '''Signal thread to terminate'''

        log.debug('started')
        self.quit = True
        self.event.set()


def create_keyspace(conn):
    conn.execute("""
    CREATE KEYSPACE IF NOT EXISTS cfs
    WITH replication = {'class': 'SimpleStrategy',
                        'replication_factor': 1}
    """)


def create_tables(conn):
    # Table of storage objects
    # Refcount is included for performance reasons
    # size == -1 indicates block has not been uploaded yet
    conn.execute("""
    CREATE TABLE objects (
        id        bigint,
        size      bigint,
        PRIMARY KEY (id)
    )""")
    conn.execute("""
    CREATE TABLE objects_refcount (
        id        bigint,
        refcount  counter,
        PRIMARY KEY (id)
    )""")

    # Table of known data blocks
    # Refcount is included for performance reasons
    conn.execute("""
    CREATE TABLE blocks (
        id        bigint,
        hash      blob,
        refcount  bigint,
        size      bigint,
        obj_id    bigint,
        PRIMARY KEY (id)
    )""")
    conn.execute("""
    CREATE TABLE blocks_refcount (
        id        bigint,
        refcount  counter,
        PRIMARY KEY (id)
    )""")

    conn.execute("""
    CREATE TABLE blocks_by_hash (
        hash    blob,
        blockid bigint,
        PRIMARY KEY (hash, blockid)
    )""")

    conn.execute("""
    CREATE TABLE blocks_by_objid (
        objid   bigint,
        blockid bigint,
        PRIMARY KEY (objid, blockid)
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
        id        bigint,
        uid       bigint,
        gid       bigint,
        mode      int,
        mtime_ns  bigint,
        atime_ns  bigint,
        ctime_ns  bigint,
        size      bigint,
        rdev      bigint,
        locked    boolean,
        PRIMARY KEY (id)
    )""")

    conn.execute("""
    CREATE TABLE inodes_refcount (
        inode       bigint,
        refcount    counter,
        PRIMARY KEY (inode)
    )""")

    # Further Blocks used by inode (blockno >= 1)
    conn.execute("""
    CREATE TABLE inodes_blocks (
        inode       bigint,
        blockno     bigint,
        block_id    bigint,
        PRIMARY KEY (inode, blockno)
    )""")

    # Symlinks
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     bigint,
        target    blob,
        PRIMARY KEY (inode)
    )""")

    # parent_inode from inode
    conn.execute("""
    CREATE TABLE parent_inodes (
        inode         bigint,
        parent_inode  bigint,
        PRIMARY KEY (inode)
    )""")

    # Table of filesystem objects
    conn.execute("""
    CREATE TABLE contents (
        parent_inode bigint,
        name         blob,
        inode        bigint,
        PRIMARY KEY (parent_inode, name)
    )""")

    # Extended attributes
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     bigint,
        name      blob,
        value     blob,
        PRIMARY KEY (inode, name)
    )""")

    conn.execute("""
    CREATE TABLE params (
        name    text,
        node    bigint,
        value   bigint,
        PRIMARY KEY (name, node)
    )""")


def init_tables(conn):
    # Insert root directory
    now_ns = time_ns()
    root_mode = (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                 | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,size,"
        "rdev) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (ROOT_INODE, root_mode, os.getuid(), os.getgid(), now_ns, now_ns,
         now_ns, 0, 0))
    conn.execute("UPDATE inodes_refcount SET refcount = refcount + 1 "
                 "WHERE inode=%s", (ROOT_INODE,))

    ctrl_mode = stat.S_IFREG | stat.S_IRUSR | stat.S_IWUSR
    # Insert control inode, the actual values don't matter that much
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,size,"
        "rdev) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (CTRL_INODE, ctrl_mode, 0, 0, now_ns, now_ns, now_ns, 0, 0))
    conn.execute("UPDATE inodes_refcount SET refcount = refcount + 1 "
                 "WHERE inode=%s", (CTRL_INODE,))

    # Insert lost+found directory
    lostf_inodeid = 3
    lostf_mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
    conn.execute(
        "INSERT INTO inodes (id,mode,uid,gid,mtime_ns,atime_ns,ctime_ns,size,"
        "rdev) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (lostf_inodeid, lostf_mode, os.getuid(), os.getgid(), now_ns, now_ns,
         now_ns, 0, 0))
    conn.execute("UPDATE inodes_refcount SET refcount = refcount + 1 "
                 "WHERE inode=%s",
                 (lostf_inodeid,))
    conn.execute(
        "INSERT INTO contents (parent_inode, name, inode) VALUES(%s,%s,%s)",
        (ROOT_INODE, b'lost+found', lostf_inodeid))
    conn.execute("INSERT INTO parent_inodes (inode, parent_inode) "
                 "VALUES (%s, %s)", (lostf_inodeid, ROOT_INODE))


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
    pass


def dump_and_upload_metadata(backend, db, param):
    pass


def upload_metadata(backend, fh, param):
    pass


def write_empty_metadata_file(backend):
    def do_write(obj_fh):
        return obj_fh
    backend.perform_write(do_write, "s3ql_metadata", metadata={},
                          is_compressed=False)


def load_params(cachepath):
    pass


def save_params(cachepath, param):
    pass


def inodeid_gen(last_id=0):
    n = last_id + 1
    while True:
        yield n
        n += 1


def load_persistent_config():
    try:
        with open(os.path.expanduser("~/.s3ql/nodeconf"), "r") as fp:
            return json.load(fp)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        return None


def save_persistent_config(conf):
    with open(os.path.expanduser("~/.s3ql/nodeconf"), "w") as fp:
        return json.dump(conf, fp)
