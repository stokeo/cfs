.. -*- mode: rst -*-

.. _storage_backends:

==================
 Storage Backends
==================

S3QL supports different *backends* to store data at different service
providers and using different protocols. A *storage url* specifies a
backend together with some backend-specific information and uniquely
identifies an S3QL file system. The form of the storage url depends on
the backend and is described for every backend below.

Furthermore, every S3QL commands that accepts a storage url also
accepts a :cmdopt:`--backend-options` parameter than can be used to
pass backend-specific options to the backend module. The available
options are documented with the respective backends below.

All storage backends respect the :envvar:`!http_proxy` (for plain HTTP
connections) and :envvar:`!https_proxy` (for SSL connections)
environment variables.

.. note::

   Storage backends are not necessarily compatible. Don't expect that
   you can e.g. copy the data stored by the local backend into Amazon
   S3 using some non-S3QL tool and then access it with S3QL's S3
   backend. If you want to copy file systems from one backend to
   another, you need to use the :file:`clone_fs.py` script (from the
   :file:`contrib` directory in the S3QL tarball).


Google Storage
==============

.. program:: gs_backend

`Google Storage <https://cloud.google.com/storage/>`_ is an online
storage service offered by Google. In order to use it with S3QL, make
sure that you enable the JSON API in the `GCP Console API Library
<https://console.cloud.google.com/apis/library/>`_

The Google Storage backend uses OAuth2 authentication or ADC_
(Application Default Credentials).

.. _ADC: https://cloud.google.com/docs/authentication/production

To use OAuth2 authentication, specify ``oauth2`` as the backend login
and a valid OAuth2 refresh token as the backend password. To obtain a
refresh token, you can use the :ref:`s3ql_oauth_client <oauth_client>`
program. It will instruct you to open a specific URL in your browser,
enter a code and authenticate with your Google account. Once this
procedure is complete, :ref:`s3ql_oauth_client <oauth_client>` will
print out the refresh token. Note that you need to do this procedure
only once, the refresh token will remain valid until you explicitly
revoke it.

To use ADC, specify ``adc`` as the backend login and use an arbitrary
value for the backend password.

To create a Google Storage bucket, you can use e.g. the `Google
Storage Manager`_. The storage URL for accessing the bucket in S3QL is
then ::

   gs://<bucketname>/<prefix>

Here *bucketname* is the name of the bucket, and *prefix* can be an
arbitrary prefix that will be prepended to all object names used by
S3QL. This allows you to store several S3QL file systems in the same
Google Storage bucket.

The Google Storage backend accepts the following backend options:

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. _`Google Storage Manager`: https://console.cloud.google.com/storage/browser


Amazon S3
=========

.. program:: s3_backend

`Amazon S3 <http://aws.amazon.com/s3>`_ is the online storage service
offered by `Amazon Web Services (AWS) <http://aws.amazon.com/>`_. To
use the S3 backend, you first need to sign up for an AWS account. The
account is free, you will pay only for the amount of storage and
traffic that you actually use. After that, you need to create a bucket
that will hold the S3QL file system, e.g. using the `AWS Management
Console <https://console.aws.amazon.com/s3/home>`_. For best
performance, it is recommend to create the bucket in the
geographically closest storage region.

The storage URL for accessing S3 buckets in S3QL has the form ::

    s3://<region>/<bucket>/<prefix>

*prefix* can be an arbitrary prefix that will be prepended to all
object names used by S3QL. This allows you to store several S3QL file
systems in the same S3 bucket. For example, the storage URL ::

   s3://ap-south-1/foomart.net/data/s3ql_backup/

refers to the *foomart.net* bucket in the *ap-south-1* region. All
storage objects that S3QL stores in this bucket will be prefixed with
*data/s3ql_backup/*.

Note that the backend login and password for accessing S3 are not the
user id and password that you use to log into the Amazon Webpage, but
the *AWS access key id* and *AWS secret access key* shown under `My
Account/Access Identifiers
<https://aws-portal.amazon.com/gp/aws/developer/account/index.html?ie=UTF8&action=access-key>`_.

The Amazon S3 backend accepts the following backend options:

.. option:: no-ssl

   Disable encrypted (https) connections and use plain HTTP instead.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: sse

    Enable server side encryption. Both costs & benefits of S3 server
    side encryption are probably rather small, and this option does
    *not* affect any client side encryption performed by S3QL itself.

.. option:: it

   Use INTELLIGENT_TIERING storage class for new objects.
   See `AWS S3 Storage classes <https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html>`_

.. option:: ia

   Use STANDARD_IA (infrequent access) storage class for new objects.
   See `AWS S3 Storage classes <https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html>`_

.. option:: oia

   Use ONEZONE_IA (infrequent access) storage class for new objects.
   See `AWS S3 Storage classes <https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html>`_

.. option:: rrs

   Enable reduced redundancy storage for newly created objects
   (overwrites the *ia* option).

   When enabling this option, it is strongly recommended to
   periodically run :ref:`s3ql_verify <s3ql_verify>`, because objects
   that are lost by the storage backend may cause subsequent data loss
   even later in time due to the data de-duplication feature of S3QL (see
   :ref:`backend_reliability` for details).


.. _openstack_backend:

OpenStack/Swift
===============

.. program:: swift_backend

OpenStack_ is an open-source cloud server application suite. Swift_ is
the cloud storage module of OpenStack. Swift/OpenStack storage is
offered by many different companies.

There are two different storage URL for the OpenStack backend that
make use of different authentication APIs. For legacy (v1)
authentication, the storage URL is ::

   swift://<hostname>[:<port>]/<container>[/<prefix>]

for Keystone (v2 and v3) authentication, the storage URL is ::

   swiftks://<hostname>[:<port>]/<region>:<container>[/<prefix>]

Note that when using Keystone authentication, you can (and have to)
specify the storage region of the container as well. Also note that when
using Keystone v3 authentication, the :var:`domain` option is required.

In both cases, *hostname* name should be the name of the
authentication server.  The storage container must already exist (most
OpenStack providers offer either a web frontend or a command line tool
for creating containers). *prefix* can be an arbitrary prefix that
will be prepended to all object names used by S3QL, which can be used
to store multiple S3QL file systems in the same container.

When using legacy authentication, the backend login and password
correspond to the OpenStack username and API Access Key. When using
Keystone authentication, the backend password is your regular
OpenStack password and the backend login combines you OpenStack
username and tenant/project in the form :var:`<tenant>:<user>`.
If no tenant is required, the OpenStack username alone may be used as
backend login. For Keystone v2 :var:`<tenant>` needs to be the
tenant name (:envvar:`!OS_TENANT_NAME` in the OpenStack RC File).
For Keystone v3 :var:`<tenant>` needs to be the project ID
(:envvar:`!OS_TENANT_ID` in the OpenStack RC File).

The OpenStack backend accepts the following backend options:

.. option:: no-ssl

   Use plain HTTP to connect to the authentication server. This option
   does not directly affect the connection to the storage
   server. Whether HTTPS or plain HTTP is used to connect to the
   storage server is determined by the authentication server.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: disable-expect100

   If this option is specified, S3QL does not use the ``Expect:
   continue`` header (cf. `RFC2616, section 8.2.3`__) when uploading
   data to the server. This can be used to work around broken storage
   servers that don't fully support HTTP 1.1, but may decrease
   performance as object data will be transmitted to the server more
   than once in some circumstances.

.. option:: no-feature-detection

   If this option is specified, S3QL does not try to dynamically detect
   advanced features of the Swift backend. In this case S3QL can only
   use the least common denominator of supported Swift versions and
   configurations.

.. option:: domain

   If this option is specified, S3QL will use the Keystone v3 API. The
   default domain ID for OpenStack installations is :var:`default`. If this
   option is specified without setting the :var:`project-domain` option, this
   will be used for both the project and the user domain.
   You need to provide the domain ID not the domain name to this option.
   If your provider did not give you a domain ID, then it is most likely
   :var:`default`.

.. option:: domain-is-name

   If your provider only supplies you with the name of your domain and not the uuid,
   you need to set this :var:`domain-is-name` option, whereby the :var:`domain` is used as the domain name,
   not the domain id.

.. option:: project-domain

   In simple cases, the project domain will be the same as the auth
   domain. If the :var:`project-domain` option is not specified, it will be
   assumed to be the same as the user domain.
   You need to provide the domain ID not the domain name to this option.
   If your provider did not give you a domain ID, then it is most likely
   :var:`default`.

.. option:: project-domain-is-name

   If your provider only supplies you with the name of your project domain and not the uuid,
   you need to set this :var:`project-domain-name` option, whereby the :var:`project-domain` is used
   as the name of the project domain, not the id of the project domain.
   If project-domain-is-name is not set, it is assumed the same as domain-is-name.

.. option:: tenant-is-name

   Some providers use the tenant name to specify the storage location, and others use the tenant id.
   If your provider uses the tenant name and not the id, you need to set this :var:`tenant-is-name` option.
   If :var:`tenant-is-name` is provided, the :var:`<tenant>` component of the login is used as the tenant
   name, not the tenant id.

.. option:: identity-url

   If your provider does not use hostname:port/v3/auth/tokens but instead has another identity URL, you can use this option.
   It allows to replace /v3/auth/tokens with another path like for example /identity/v3/auth/tokens

.. __: http://tools.ietf.org/html/rfc2616#section-8.2.3
.. _OpenStack: http://www.openstack.org/
.. _Swift: http://openstack.org/projects/storage/

.. NOTE::

   The Swift API unfortunately lacks a number of features that S3QL
   normally makes use of. S3QL works around these deficiencies as much
   as possible. However, this means that storing data using the Swift
   backend generally requires more network round-trips and transfer
   volume than the other backends. Also, S3QL requires Swift storage
   servers to provide immediate consistency for newly created objects.


Rackspace CloudFiles
====================

Rackspace_ CloudFiles uses OpenStack_ internally, so it is possible to
just use the OpenStack/Swift backend (see above) with
``auth.api.rackspacecloud.com`` as the host name. For convenince,
there is also a special ``rackspace`` backend that uses a storage URL
of the form ::

   rackspace://<region>/<container>[/<prefix>]

The storage container must already exist in the selected
region. *prefix* can be an arbitrary prefix that will be prepended to
all object names used by S3QL and can be used to store several S3QL
file systems in the same container.

You can create a storage container for S3QL using the `Cloud Control
Panel <https://mycloud.rackspace.com/>`_ (click on *Files* in the
topmost menu bar).

The Rackspace backend accepts the same backend options as the
:ref:`OpenStack backend <openstack_backend>`.

.. _Rackspace: http://www.rackspace.com/


S3 compatible
=============

.. program:: s3c_backend

The S3 compatible backend allows S3QL to access any storage service
that uses the same protocol as Amazon S3. The storage URL has the form ::

   s3c://<hostname>:<port>/<bucketname>/<prefix>

Here *bucketname* is the name of an (existing) bucket, and *prefix*
can be an arbitrary prefix that will be prepended to all object names
used by S3QL. This allows you to store several S3QL file systems in
the same bucket.

The S3 compatible backend accepts the following backend options:

.. option:: no-ssl

   Disable encrypted (https) connections and use plain HTTP instead.

.. option:: ssl-ca-path=<path>

   Instead of using the system's default certificate store, validate
   the server certificate against the specified CA
   certificates. :var:`<path>` may be either a file containing
   multiple certificates, or a directory containing one certificate
   per file.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. option:: disable-expect100

   If this option is specified, S3QL does not use the ``Expect:
   continue`` header (cf. `RFC2616, section 8.2.3`__) when uploading
   data to the server. This can be used to work around broken storage
   servers that don't fully support HTTP 1.1, but may decrease
   performance as object data will be transmitted to the server more
   than once in some circumstances.

.. __: http://tools.ietf.org/html/rfc2616#section-8.2.3

.. option:: dumb-copy

   If this option is specified, S3QL assumes that a COPY request to
   the storage server has succeeded as soon as the server returns a
   ``200 OK`` status. The `S3 COPY API`_ specifies that the
   storage server may still return an error in the request body (see
   the `copy proposal`__ for the rationale), so this
   option should only be used if you are certain that your storage
   server only returns ``200 OK`` when the copy operation has been
   completely and successfully carried out. Using this option may be
   neccessary if your storage server does not return a valid response
   body for a successful copy operation.

.. _`S3 COPY API`: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html
.. __: https://doc.s3.amazonaws.com/proposals/copy.html


Backblaze B2
============

.. program:: b2_backend

Backblaze B2 is a cloud storage with its own API.

The storage URL for backblaze b2 storage is ::

   b2://<bucket-name>[/<prefix>]

*bucket-name* is an (existing) bucket which has to be accessible with
the provided account key. The *prefix* will be appended to all names
used by S3QL and can be used to hold separate S3QL repositories in the
same bucket.

It is also possible to use an application key. The required key capabilities
are the following:

   - `listBuckets`
   - `listFiles`
   - `readFiles`
   - `writeFiles`
   - `deleteFiles`

.. option:: disable-versions

   If versioning of the bucket is not enabled, this option can be set.
   When deleting objects, the bucket will not be scanned for all file versions
   because it will be implied that only the one (the most recent) version of a
   file exists. This will use only one class B transaction instead of
   (possibly) multiple class C transactions.

.. option:: retry-on-cap-exceeded

   If there are data/transaction caps set for the backblaze account, this option
   controls if operations should be retried as cap counters are reset every day.
   Otherwise the exception would abort the program.

.. option:: test-mode-fail-some-uploads

   This option puts the backblaze B2 server into test mode by adding a special header
   to the upload requests. The server will then randomly fail some uploads. Use
   only to test the failure resiliency of the backend implementation as it causes
   unnecessary traffic, delays and transactions.

.. option:: test-mode-expire-some-tokens

   Similarly to the option above this lets the server fail some authorization tokens
   in order to test the reauthorization of the backend implementation.

.. option:: test-mode-force-cap-exceeded

   Like above this option instructs the server to behave as if the data/transaction
   caps were exceeded. Use this only to test the backend implementation for correct/desired
   behavior. Can be useful in conjunction with *retry-on-cap-exceeded* option.

.. option:: tcp-timeout

   Specifies the timeout used for TCP connections. If no data can be
   exchanged with the remote server for longer than this period, the
   TCP connection is closed and re-established (default: 20 seconds).

.. _Backblaze B2 API: https://www.backblaze.com/b2/docs/


Local
=====

S3QL is also able to store its data on the local file system. This can
be used to backup data on external media, or to access external
services that S3QL can not talk to directly (e.g., it is possible to
store data over SSH by first mounting the remote system using sshfs_
and then using the local backend to store the data in the sshfs
mountpoint).

The storage URL for local storage is ::

   local://<path>

Note that you have to write three consecutive slashes to specify an
absolute path, e.g. `local:///var/archive`. Also, relative paths will
automatically be converted to absolute paths before the authentication
file (see :ref:`authinfo`) is read, i.e. if you are in the
`/home/john` directory and try to mount `local://s3ql`, the
corresponding section in the authentication file must match the
storage url `local:///home/john/s3ql`.

The local backend does not accept any backend options.

.. _sshfs: http://fuse.sourceforge.net/sshfs.html
