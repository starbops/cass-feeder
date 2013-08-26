#!/usr/bin/env python
# file: get_content.py

import os
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

SERVLIST_1 = ['140.113.121.170']
KEYSPACE_1 = 'ks'
COLUMNFAMILY_1 = 'cf'
SERVLIST_2 = ['192.168.100.103', '192.168.100.105',
              '192.168.100.107', '192.168.100.111'
]
KEYSPACE_2 = 'SECMAP'
COLUMNFAMILY_2 = 'SUMMARY'

def main():
    target_filename = sys.argv[1]

    frontend = ConnectionPool(KEYSPACE_1, SERVLIST_1)
    frontend_cf = ColumnFamily(frontend, COLUMNFAMILY_1)
    print "connection to frontend established"
    taskid = frontend_cf.get(target_filename).values()
    print "taskid: " + str(taskid)
    frontent.dispose()

    backend = ConnectionPool(KEYSPACE_2, SERVLIST_2)
    backend_cf = ColumnFamily(backend, COLUMNFAMILY_2)
    print "connection to backend established"
    content = backend_cf.get(taskid, ['content']).values()
    print "file size: " + str(len(content)) + " bytes"
    backend.dispose()

    full_filename = os.getcwd() + target_filename
    try:
        os.makedirs(full_filename)
    except OSError as err:
        print "[WARN] " + str(err)
    with open(full_filename, 'w') as file_handle:
        file_handle.write(content)
    print "file location: " + full_filename


if __name__ == '__main__':
    main()
