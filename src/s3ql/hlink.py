'''
hlink.py - addition to s3ql

'''

from .logging import logging, setup_logging, QuietError
from .common import assert_fs_owner
from .parse_args import ArgumentParser
import pyfuse3
import os
import stat
import sys
import textwrap

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Create a hardlink named <linkname> to <target>.
        '''))

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument('target', help='target directory',
                        type=(lambda x: x.rstrip('/')))
    parser.add_argument('linkname', help='link name',
                        type=(lambda x: x.rstrip('/')))

    options = parser.parse_args(args)

    return options


def main(args=None):
    ''' creates hardlinks to directories '''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    if not os.path.exists(options.target):
        raise QuietError('Target directory %r does not exist' % options.target)

    if not os.path.exists(options.linkname):
        raise QuietError('Link name %r already exist.' % options.linkname)

    parent = os.path.dirname(os.path.abspath(options.linkname))
    if not os.path.exists(parent):
        raise QuietError('Link name parent %r does not exist' % parent)

    fstat_t = os.stat(options.target)
    fstat_p = os.stat(parent)
    if not stat.S_ISDIR(fstat_t.st_mode):
        raise QuietError('Target %r is not a directory' % options.target)

    if not stat.S_ISDIR(fstat_p.st_mode):
        raise QuietError('Link parent %r is not a directory' % parent)

    if fstat_p.st_dev != fstat_t.st_dev:
        raise QuietError('Source and target are not on the same file system.')

    if os.path.ismount(options.target):
        raise QuietError('%s is a mount point.' % options.target)

    ctrlfile = assert_fs_owner(options.target)
    pyfuse3.link(ctrlfile, 'dirhardlink',
                 ('(%d, %d, %s)' % (fstat_t.st_ino, fstat_p.st_ino, options.linkname)).encode())


if __name__ == '__main__':
    main(sys.argv[1:])
