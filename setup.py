#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
setup.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

try:
    import setuptools
except ModuleNotFoundError:
    raise SystemExit('Setuptools package not found. Please install from '
                     'https://pypi.python.org/pypi/setuptools')
from setuptools import Extension
from setuptools.command.test import test as TestCommand

from distutils.version import LooseVersion
import os
import subprocess
import re
import sys
from glob import glob
import faulthandler
faulthandler.enable()

basedir = os.path.abspath(os.path.dirname(sys.argv[0]))
DEVELOPER_MODE = os.path.exists(os.path.join(basedir, 'MANIFEST.in'))
if DEVELOPER_MODE:
    print('MANIFEST.in exists, running in developer mode')

# Add S3QL sources
sys.path.insert(0, os.path.join(basedir, 'src'))
sys.path.insert(0, os.path.join(basedir, 'util'))
import s3ql


class build_docs(setuptools.Command):
    description = 'Build Sphinx documentation'
    user_options = [
        ('fresh-env', 'E', 'discard saved environment'),
        ('all-files', 'a', 'build all files'),
    ]
    boolean_options = ['fresh-env', 'all-files']

    def initialize_options(self):
        self.fresh_env = False
        self.all_files = False

    def finalize_options(self):
        pass

    def run(self):
        try:
            from sphinx.application import Sphinx
            from docutils.utils import SystemMessage
        except ModuleNotFoundError:
            raise SystemExit('This command requires Sphinx to be installed.') from None

        fix_docutils()

        dest_dir = os.path.join(basedir, 'doc')
        src_dir = os.path.join(basedir, 'rst')

        confoverrides = {}
        confoverrides['version'] = s3ql.VERSION
        confoverrides['release'] = s3ql.RELEASE

        for builder in ('html', 'latex', 'man'):
            print('Running %s builder...' % builder)
            self.mkpath(os.path.join(dest_dir, builder))
            app = Sphinx(srcdir=src_dir, confdir=src_dir,
                         outdir=os.path.join(dest_dir, builder),
                         doctreedir=os.path.join(dest_dir, 'doctrees'),
                         buildername=builder, confoverrides=confoverrides,
                         freshenv=self.fresh_env)
            self.fresh_env = False
            self.all_files = False

            try:
                if self.all_files:
                    app.builder.build_all()
                else:
                    app.builder.build_update()
            except SystemMessage as err:
                print('reST markup error:',
                      err.args[0].encode('ascii', 'backslashreplace'),
                      file=sys.stderr)

        # These shouldn't be installed by default
        for name in ('expire_backups.1', 'pcp.1'):
            os.rename(os.path.join(dest_dir, 'man', name),
                      os.path.join(basedir, 'contrib', name))

        print('Running pdflatex...')
        for _ in range(3):
            with open('/dev/null', 'wb') as null:
                subprocess.check_call(['pdflatex', '-interaction', 'batchmode', 'manual.tex'],
                                      cwd=os.path.join(dest_dir, 'latex'), stdout=null)
        os.rename(os.path.join(dest_dir, 'latex', 'manual.pdf'),
                  os.path.join(dest_dir, 'manual.pdf'))


class pytest(TestCommand):

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(['tests'])
        sys.exit(errno)


def main():

    with open(os.path.join(basedir, 'README.rst'), 'r') as fh:
        long_desc = fh.read()

    compile_args = ['-Wall', '-Wextra', '-Wconversion', '-Wsign-compare']

    # Enable all fatal warnings only when compiling from Mercurial tip.
    # (otherwise we break forward compatibility because compilation with newer
    # compiler may fail if additional warnings are added)
    if DEVELOPER_MODE:
        if os.environ.get('CI') != 'true':
            compile_args.append('-Werror')

        # Value-changing conversions should always be explicit.
        compile_args.append('-Werror=conversion')

        # Note that (i > -1) is false if i is unsigned (-1 will be converted to
        # a large positive value). We certainly don't want to do this by
        # accident.
        compile_args.append('-Werror=sign-compare')

        compile_args.append('-Wno-unused-function')

    required_pkgs = ['apsw >= 3.7.0',
                     'cryptography',
                     'requests',
                     'defusedxml',
                     'dugong >= 3.4, < 4.0',
                     'google-auth',
                     'google-auth-oauthlib',

                     # Need trio.lowlevel
                     'trio >= 0.15',
                     'pyfuse3 >= 3.2.0, < 4.0']
    if sys.version_info < (3, 7, 0):
        required_pkgs.append('async_generator')

    setuptools.setup(
          name='s3ql',
          zip_safe=True,
          version=s3ql.VERSION,
          description='a full-featured file system for online data storage',
          long_description=long_desc,
          author='Nikolaus Rath',
          author_email='Nikolaus@rath.org',
          url='https://bitbucket.org/nikratio/s3ql/',
          download_url='https://bitbucket.org/nikratio/s3ql/downloads',
          license='GPLv3',
          classifiers=['Development Status :: 5 - Production/Stable',
                       'Environment :: No Input/Output (Daemon)',
                       'Environment :: Console',
                       'License :: OSI Approved :: GNU Library or Lesser General Public License (GPLv3)',
                       'Topic :: Internet',
                       'Operating System :: POSIX',
                       'Topic :: System :: Archiving'],
          platforms=[ 'POSIX', 'UNIX', 'Linux' ],
          keywords=['FUSE', 'backup', 'archival', 'compression', 'encryption',
                    'deduplication', 'aws', 's3' ],
          package_dir={'': 'src'},
          packages=setuptools.find_packages('src'),
          provides=['s3ql'],
          ext_modules=[Extension('s3ql.deltadump', ['src/s3ql/deltadump.c'],
                                 extra_compile_args=compile_args,
                                 extra_link_args=[ '-lsqlite3'])],
          data_files=[ ('share/man/man1',
                          [ os.path.join('doc/man/', x) for x
                            in glob(os.path.join(basedir, 'doc', 'man', '*.1')) ]) ],
          entry_points={ 'console_scripts':
                        [
                         'mkfs.s3ql = s3ql.mkfs:main',
                         'fsck.s3ql = s3ql.fsck:main',
                         'mount.s3ql = s3ql.mount:main',
                         'umount.s3ql = s3ql.umount:main',
                         's3qlcp = s3ql.cp:main',
                         's3qlstat = s3ql.statfs:main',
                         's3qladm = s3ql.adm:main',
                         's3qlctrl = s3ql.ctrl:main',
                         's3qllock = s3ql.lock:main',
                         's3qlln = s3ql.hlink:main',
                         's3qlrm = s3ql.remove:main',
                         's3ql_oauth_client = s3ql.oauth_client:main',
                         's3ql_verify = s3ql.verify:main',
                         ]
                        },
          install_requires=required_pkgs,
          tests_require=['pytest >= 3.7'],
          cmdclass={'upload_docs': upload_docs,
                    'build_cython': build_cython,
                    'build_sphinx': build_docs,
                    'pytest': pytest },
          command_options={ 'sdist': { 'formats': ('setup.py', 'bztar') } },
         )

class build_cython(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Compile .pyx to .c"

    def initialize_options(self):
        pass

    def finalize_options(self):
        # Attribute defined outside init
        #pylint: disable=W0201
        self.extensions = self.distribution.ext_modules

    def run(self):
        cython = None
        for c in ('cython3', 'cython'):
            try:
                version = subprocess.check_output([c, '--version'],
                                                  universal_newlines=True, stderr=subprocess.STDOUT)
                cython = c
            except FileNotFoundError:
                pass
        if cython is None:
            raise SystemExit('Cython needs to be installed for this command') from None

        hit = re.match('^Cython version (.+)$', version)
        if not hit or LooseVersion(hit.group(1)) < "0.17":
            raise SystemExit('Need Cython 0.17 or newer, found ' + version)

        cmd = [cython, '-Wextra', '-f', '-3',
               '-X', 'embedsignature=True']
        if DEVELOPER_MODE:
            cmd.append('-Werror')

        # Work around http://trac.cython.org/cython_trac/ticket/714
        cmd += ['-X', 'warn.maybe_uninitialized=False']

        for extension in self.extensions:
            for file_ in extension.sources:
                (file_, ext) = os.path.splitext(file_)
                path = os.path.join(basedir, file_)
                if ext != '.c':
                    continue
                if os.path.exists(path + '.pyx'):
                    print('compiling %s to %s' % (file_ + '.pyx', file_ + ext))
                    if subprocess.call(cmd + [path + '.pyx']) != 0:
                        raise SystemExit('Cython compilation failed')


class upload_docs(setuptools.Command):
    user_options = []
    boolean_options = []
    description = "Upload documentation"

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call(['rsync', '-aHv', '--del', os.path.join(basedir, 'doc', 'html') + '/',
                               'ebox.rath.org:/srv/www.rath.org/s3ql-docs/'])
        subprocess.check_call(['rsync', '-aHv', '--del', os.path.join(basedir, 'doc', 'manual.pdf'),
                               'ebox.rath.org:/srv/www.rath.org/s3ql-docs/'])


def fix_docutils():
    '''Work around https://bitbucket.org/birkenfeld/sphinx/issue/1154/'''

    import docutils.parsers
    from docutils.parsers import rst
    old_getclass = docutils.parsers.get_parser_class

    # Check if bug is there
    try:
        old_getclass('rst')
    except AttributeError:
        pass
    else:
        return

    def get_parser_class(parser_name):
        """Return the Parser class from the `parser_name` module."""
        if parser_name in ('rst', 'restructuredtext'):
            return rst.Parser
        else:
            return old_getclass(parser_name)
    docutils.parsers.get_parser_class = get_parser_class

    assert docutils.parsers.get_parser_class('rst') is rst.Parser


if __name__ == '__main__':
    main()
