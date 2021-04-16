'''
mkfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .backends.comprenc import ComprencBackend
from .backends import s3
from .common import (get_backend, split_by_n, freeze_basic_mapping)
from .metadata import SqliteMetaBackend
from .parse_args import ArgumentParser
from getpass import getpass
from base64 import b64encode
import os
import shutil
import sys
import atexit

log = logging.getLogger(__name__)


def parse_args(args):

    parser = ArgumentParser(
        description="Initializes an S3QL file system")

    parser.add_cachedir()
    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_backend_options()
    parser.add_version()
    parser.add_storage_url()

    parser.add_argument("-L", default='', help="Filesystem label",
                        dest="label", metavar='<name>',)
    parser.add_argument("--max-obj-size", type=int, default=10240, metavar='<size>',
                        help="Maximum size of storage objects in KiB. Files bigger than this "
                           "will be spread over multiple objects in the storage backend. "
                           "Default: %(default)d KiB.")
    parser.add_argument("--plain", action="store_true", default=False,
                        help="Create unencrypted file system.")

    options = parser.parse_args(args)

    return options


def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    if options.max_obj_size < 1024:
        # This warning should never be converrted to an exception
        log.warning('Maximum object sizes less than 1 MiB will degrade '
                    'performance.', extra={ 'force_log': True })

    plain_backend = get_backend(options, raw=True)
    atexit.register(plain_backend.close)

    log.info("Before using S3QL, make sure to read the user's guide, especially\n"
             "the 'Important Rules to Avoid Losing Data' section.")

    if isinstance(plain_backend, s3.Backend) and '.' in plain_backend.bucket_name:
        log.warning('S3 Buckets with names containing dots cannot be '
                    'accessed using SSL!'
                    '(cf. https://forums.aws.amazon.com/thread.jspa?threadID=130560)')

    if 's3ql_metadata' in plain_backend:
        raise QuietError("Refusing to overwrite existing file system! "
                         "(use `s3qladm clear` to delete)")

    if not options.plain:
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
            if not wrap_pw == getpass("Confirm encryption password: "):
                raise QuietError("Passwords don't match.")
        else:
            wrap_pw = sys.stdin.readline().rstrip()
        wrap_pw = wrap_pw.encode('utf-8')

        # Generate data encryption passphrase
        log.info('Generating random encryption key...')
        fh = open('/dev/urandom', "rb", 0)  # No buffering
        data_pw = fh.read(32)
        fh.close()

        backend = ComprencBackend(wrap_pw, ('lzma', 2), plain_backend)
        backend['s3ql_passphrase'] = data_pw
        backend['s3ql_passphrase_bak1'] = data_pw
        backend['s3ql_passphrase_bak2'] = data_pw
        backend['s3ql_passphrase_bak3'] = data_pw
    else:
        data_pw = None

    backend = ComprencBackend(data_pw, ('lzma', 2), plain_backend)
    atexit.unregister(plain_backend.close)
    atexit.register(backend.close)
    cachepath = options.cachepath

    # There can't be a corresponding backend, so we can safely delete
    # these files.
    if os.path.exists(cachepath + '.db'):
        os.unlink(cachepath + '.db')
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    log.info('Creating metadata tables...')
    db = SqliteMetaBackend(backend=backend, cachepath=cachepath + '.db',
                           mkfs=True, mkfsopts=options)
    param = db.param

    backend.store('s3ql_seq_no_%d' % param['seq_no'], b'Empty')
    with open(cachepath + '.params', 'wb') as fh:
        fh.write(freeze_basic_mapping(param))
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    if data_pw is not None:
        print('Please store the following master key in a safe location. It allows ',
              'decryption of the S3QL file system in case the storage objects holding ',
              'this information get corrupted:',
              '---BEGIN MASTER KEY---',
              ' '.join(split_by_n(b64encode(data_pw).decode(), 4)),
              '---END MASTER KEY---',
              sep='\n')


if __name__ == '__main__':
    main(sys.argv[1:])
