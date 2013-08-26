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

def usage():
    return "Usage:\npython %s <serv> <keyspace> <col_fam> <key>" % sys.argv[0]

def main():
    try:
        serverlist = sys.argv[1]
        keyspace = sys.argv[2]
        columnfamily = sys.argv[3]
        target_filename = sys.argv[4]
    except IndexError as err:
        print usage()
        exit(-1)

    try:
        frontend = ConnectionPool(keyspace, [serverlist])
        frontend_cf = ColumnFamily(frontend, columnfamily)
    except Exception as err:
        print "[ERROR] " + str(err)
        exit(-1)
    print "connection to frontend established"
    try:
        taskid = frontend_cf.get(target_filename).values()
    except pycassa.NotFoundException as err:
        print "[ERROR] " + str(err)
        exit(-1)
    finally:
        frontend.dispose()
        print "connection closed"
    print "taskid: " + str(taskid)

    try:
        backend = ConnectionPool(KEYSPACE, SERVLIST)
        backend_cf = ColumnFamily(backend, COLUMNFAMILY)
    except Exception as err:
        print "[ERROR] " + str(err)
        exit(-1)
    print "connection to backend established"
    try:
        content = backend_cf.get(taskid[0], ['content']).values()[0]
    except pycassa.NotFoundException as err:
        print "[ERROR] " + str(err)
        exit(-1)
    finally:
        backend.dispose()
        print "connection closed"
    print "file size: " + str(len(content)) + " bytes"

    full_filename = os.getcwd() + target_filename
    full_filepath = os.path.dirname(full_filename)
    try:
        os.makedirs(full_filepath)
    except OSError as err:
        print "[WARN] " + str(err)
    with open(full_filename, 'wb') as file_handle:
        file_handle.write(content)
    print "file location: " + full_filename

if __name__ == '__main__':
    main()

