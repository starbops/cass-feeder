#!/usr/bin/env python
# file: feeder.py

import hashlib
import dbus
import os
import re
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

PREFIX = "/mnt"
F_SERVLIST = ["localhost"]
F_KS = "ks"
F_CF = "cf"
B_SERVLIST = ["192.168.100.103", "192.168.100.105",
              "192.168.100.107", "192.168.100.111"
]
B_KS = "SECMAP"
B_CF = "SUMMARY"
BLOCKSIZE = 65536

def get_uuid_mntpoint():
    """Return a dictionary about UUID & mount point."""
    bus = dbus.SystemBus()
    ud_manager_obj = bus.get_object("org.freedesktop.UDisks",
                                    "/org/freedesktop/UDisks")
    ud_manager = dbus.Interface(ud_manager_obj,
                                'org.freedesktop.UDisks')
    uuid_list = []
    mntpoint_list = []

    for dev in ud_manager.EnumerateDevices():
        device_obj = bus.get_object("org.freedesktop.UDisks", dev)
        device_props = dbus.Interface(device_obj, dbus.PROPERTIES_IFACE)
        mntpoint = str(device_props.Get('org.freedesktop.UDisks.Device',
                                      "DeviceMountPaths"))
        iduuid = str(device_props.Get('org.freedesktop.UDisks.Device',
                                      "IdUuid"))
        try:
            mntpoint_list.append(re.search('.*\'(/.*?)\'.*', mntpoint).group(1))
        except AttributeError:
            continue

        if iduuid != '':
            uuid_list.append(iduuid)
    return dict(zip(uuid_list, mntpoint_list))

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

def feeder(start_point_list):
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

    counter = 0
    for sp in start_point_list:
        filelist = get_filelist(start_point_list[sp])
        if not filelist:
            print "[WARN] No files were found."
            continue

        for filename in filelist:
            file_content = ""
            with open(filename, "rb") as f:
                file_content = f.read(BLOCKSIZE)

            hasherlist = [hashlib.md5(), hashlib.sha1()]
            taskid = gen_taskid(hasherlist, filename, file_content)
            print "%s\n%s\n%s" % (sp, filename, taskid)
            frontend['handle'].insert(filename, {sp: taskid})
            backend['handle'].insert(taskid, {'content': file_content})
            print "Uploaded\n"
            counter += 1

    print "Total " + str(counter) + " file(s) uploaded."

    frontend['pool'].dispose()
    backend['pool'].dispose()

def main():
    try:
        start_point_list = {'UUID': sys.argv[1]}
    except IndexError as err:
        start_point_list = [start_point
                            for start_point in get_uuid_mntpoint()
                            if start_point.startswith('/mnt')
        ]

    feeder(start_point_list)

if __name__ == "__main__":
    main()
