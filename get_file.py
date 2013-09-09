#!/usr/bin/env python
# file: get_content.py

import os
import re
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

D_PREFIX = "/mnt"
SERVERLIST = ['192.168.100.103', '192.168.100.105',
            '192.168.100.107', '192.168.100.111'
]
KEYSPACE = 'SECMAP'
COLUMNFAMILY = 'SUMMARY_TEST'

def get_values(servlst, ks, cf, key):
    #print key
    try:
        pool = ConnectionPool(ks, servlst)
        cf_handle = ColumnFamily(pool, cf)
        result = cf_handle.get(key).values()
    except pycassa.NotFoundException as err:
        print "[ERROR] " + key + " not found"
        result = ""
    except Exception as err:
        print "[ERROR] " + str(err)
        exit(-1)
    finally:
        pool.dispose()

    return result

def write_file(filename, file_content):
    full_filename = os.getcwd() + re.sub(r"^([a-zA-Z]):(.*)",
                                         r"\\\1\2",
                                         filename.replace("/", "\\"))
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

def get_file(serverlist,
             keyspace,
             columnfamily,
             target_filename):
    fn = D_PREFIX + re.sub(r"^([a-zA-Z]):(.*)", r"/\1\2", target_filename.replace("\\", "/"))
    taskid_list = get_values(serverlist.split(), keyspace,
                             columnfamily, fn)
    if taskid_list:
        for taskid in taskid_list:
            print taskid
            content_list = get_values(SERVERLIST, KEYSPACE,
                                      COLUMNFAMILY, taskid)

        content = content_list[0]
        print target_filename
        file_location = write_file(target_filename, content)
    else:
        file_location = ""
    return file_location

