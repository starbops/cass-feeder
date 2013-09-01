#!/usr/bin/env python

import hashlib
import os
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

F_SERVLIST = ["140.113.121.170"]
F_KS = "ks"
F_CF = "cf"
B_SERVLIST = ["192.168.100.103", "192.168.100.105",
              "192.168.100.107", "192.168.100.111"
]
B_KS = "SECMAP"
B_CF = "SUMMARY"
BLOCKSIZE = 65536

def get_filelist(root):
    filelist = []

    return [os.path.abspath(os.path.join(curdir, flnm))
            for curdir, subdirs, flnms in os.walk(root)
            for flnm in flnms
    ]

def gen_taskid(hasherlist, filename, file_content):
    [hasher.update(file_content) for hasher in hasherlist]
    hash_str = ''.join([hasher.hexdigest() for hasher in hasherlist])
    return hash_str + str(os.path.getsize(filename))

def feeder(filename, device_uuid, taskid, file_content):
    frontend = {}
    backend = {}
    try:
        frontend['pool'] = ConnectionPool(F_KS, F_SERVLIST)
        frontend['handle'] = ColumnFamily(frontend['pool'], F_CF)
        backend['pool'] = ConnectionPool(B_KS, B_SERVLIST)
        backend['handle'] = ColumnFamily(backend['pool'], B_CF)
    except pycassa.NotFoundException as err:
        print "[ERROR] " + str(err)
        exit(-1)
    except pycassa.AllServersUnavailable as err:
        print "[ERROR] " + str(err)
        exit(-1)

    frontend['handle'].insert(filename, {device_uuid: taskid})
    backend['handle'].insert(taskid, {'content': file_content})

    frontend['pool'].dispose()
    backend['pool'].dispose()

def main():
    try:
        start_point = sys.argv[1]
    except IndexError as err:
        print "[ERROR] " + str(err)
        exit(-1)

    filelist = get_filelist(start_point)
    if not filelist:
        print "[WARN] No files were found"
        exit(-1)

    for filename in filelist:
        file_content = ""
        with open(filename, "rb") as f:
            file_content = f.read(BLOCKSIZE)

        hasherlist = [hashlib.md5(), hashlib.sha1()]
        taskid = gen_taskid(hasherlist, filename, file_content)
        feeder(filename, 'UUID', taskid, file_content)

if __name__ == "__main__":
    main()
