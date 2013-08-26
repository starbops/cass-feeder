#!/usr/bin/env python
# file: get_content.py

import os
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

SERVLIST = ['192.168.100.103', '192.168.100.105',
              '192.168.100.107', '192.168.100.111'
]
KEYSPACE = 'SECMAP'
COLUMNFAMILY = 'SUMMARY'

def main():
    serverlist = sys.argv[1]
    keyspace = sys.argv[2]
    columnfamily = sys.argv[3]
    target_filename = sys.argv[4]

    frontend = ConnectionPool(keyspace, serverlist)
    frontend_cf = ColumnFamily(frontend, columnfamily)
    print "connection to frontend established"
    taskid = frontend_cf.get(target_filename).values()
    print "taskid: " + str(taskid)
    frontent.dispose()

    backend = ConnectionPool(KEYSPACE, SERVLIST)
    backend_cf = ColumnFamily(backend, COLUMNFAMILY)
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
