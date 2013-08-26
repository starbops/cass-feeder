#!/usr/bin/env python
# file: get_content.py

import os
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

SERVERLIST = ['192.168.100.103', '192.168.100.105',
            '192.168.100.107', '192.168.100.111'
]
KEYSPACE = 'SECMAP'
COLUMNFAMILY = 'SUMMARY'

def get_values(servlst, ks, cf, key):
    try:
        pool = ConnectionPool(ks, servlst)
        cf_handle = ColumnFamily(pool, cf)
        result = cf_handle.get(key).values()
    except pycassa.NotFoundException as err:
        print "[ERROR] " + key + " not found"
        exit(-1)
    except Exception as err:
        print "[ERROR] " + str(err)
        exit(-1)
    finally:
        pool.dispose()

    return result

def set_file(filename, file_content):
    full_filename = os.getcwd() + filename
    full_filepath = os.path.dirname(full_filename)

    try:
        os.makedirs(full_filepath)
    except OSError as err:
        print "[WARN] " + str(err)

    try:
        with open(full_filename, 'wb') as file_handle:
            file_handle.write(file_content)
    except IOError as err:
        print "[ERROR] " + str(err)
        exit(-1)

    return "%s" % full_filename

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

    taskid_list = get_values(serverlist, keyspace,
                             columnfamily, target_filename)
    for taskid in taskid_list:
        print taskid
        content_list = get_values(SERVERLIST, KEYSPACE, COLUMNFAMILY, taskid)

    content = content_list[0]
    file_location = set_file(target_filename, content)
    print file_location

if __name__ == '__main__':
    main()
