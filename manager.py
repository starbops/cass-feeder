#!/usr/bin/env python
# file: manager.py

import dbus
import getopt
import hashlib
import os
import re
import subprocess as subp
import sys

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

SECMAP_SERVLST = ['192.168.100.103', '192.168.100.105',
                 '192.168.100.107', '192.168.100.111'
]

def uuidpath():
    """Return a dictionary about UUID & mount point."""
    bus = dbus.SystemBus()
    ud_manager_obj = bus.get_object("org.freedesktop.UDisks",
                                    "/org/freedesktop/UDisks")
    ud_manager = dbus.Interface(ud_manager_obj,
                                'org.freedesktop.UDisks')
    uuidlst = []
    mntpthlst = []

    for dev in ud_manager.EnumerateDevices():
        device_obj = bus.get_object("org.freedesktop.UDisks", dev)
        device_props = dbus.Interface(device_obj, dbus.PROPERTIES_IFACE)
        mntpth = str(device_props.Get('org.freedesktop.UDisks.Device',
                                      "DeviceMountPaths"))
        iduuid = str(device_props.Get('org.freedesktop.UDisks.Device',
                                      "IdUuid"))
        try:
            mntpthlst.append(re.search('.*\'(/.*?)\'.*', mntpth).group(1))
        except AttributeError:
            continue

        if iduuid != '':
            uuidlst.append(iduuid)
    return dict(zip(uuidlst, mntpthlst))

def insert_key(handle, key, col_val_list):
    """Insert key with column name and column value."""
    col_val_list = iter(col_val_list)
    for col, val in zip(col_val_list, col_val_list):
        handle.insert(key, {col: val})
        print "[INFO] Key '" + key + "' inserted."

def hashfile(file_name, hasherlst, file_size, blocksize=65536):
    """Generate a string containing md5, sha1, and file size."""
    print "[INFO] Hashing file '" + file_name + "'."
    if file_size == 0:
        [hasher.update('') for hasher in hasherlst]
    else:
        with open(file_name, 'rb') as afile:
            buf = afile.read(blocksize)
            while buf:
                [hasher.update(buf) for hasher in hasherlst]
                buf = afile.read(blocksize)

    hashstr = ''.join([hasher.hexdigest() for hasher in hasherlst])
    result = hashstr + str(file_size)
    return result

def inblacklist(nm, blklst):
    """Check if current directory is in blacklist."""
    for blk in blklst:
        if nm.startswith(blk):
            return True
    return False

def import_tree(h, devuuid, pth, blklst=[]):
    """Insert keys from a given point recursively (UUID)."""
    for drnm, sbdrnms, flnms in os.walk(pth):
        print "[INFO] Entering '" + drnm + "'."
        flnmabslst = [os.path.abspath(os.path.join(drnm, flnm))
                          for flnm in flnms
        ]
        for flnmabs in flnmabslst:
            if inblacklist(flnmabs, blklst):
                continue
            else:
                try:
                    hashstr = hashfile(flnmabs,
                                       [hashlib.md5(), hashlib.sha1()],
                                       os.path.getsize(flnmabs))
                    insert_key(h, flnmabs, [devuuid, hashstr])
                except Exception as err:
                    print "[ERROR] " + str(err) + "."
    print "[INFO] Importation done."

def delete_key(handle, key, col):
    """Delete whole row or multiple columns."""
    if len(col) != 0:
        handle.remove(key, col);
        print "[INFO] Remove " + str(col) + " from '" + key + "'."
    else:
        handle.remove(key);
        print "[INFO] Remove '" + key + "'."

def get_content(taskid):
    """Get file content from other cassandra servers."""
    result = ""
    try:
        (p, c) = connect_server(SECMAP_SERVLST, 'SECMAP', 'SUMMARY')
    except Exception as err:
        print "[ERROR] " + str(err) + "\n[ERROR] Connection aborted."
    else:
        try:
            content = get_key(c, taskid, ['content'], False)
            result = content.values()[0]
        except pycassa.NotFoundException as err:
            print "[ERROR] " + str(err) + "."
        p.dispose()
    return result

def set_file(file_name, content):
    """Write content into file."""
    try:
        os.makedirs(os.getcwd() + os.path.dirname(file_name))
    except OSError as err:
        print "[ERROR] " + str(err) + "."
    full_file_name = os.getcwd() + file_name
    with open(full_file_name, 'w') as f:
        f.write(content)

    if re.match(r".*DLL.*",
            subp.check_output(['file', full_file_name])):
        new_file_name = full_file_name + ".dll"
    else:
        new_file_name = full_file_name + ".exe"
    os.system("mv " + full_file_name + " " + new_file_name)

def get_key(handle, key, col, download=True):
    """Return OrderedDict of the specified key (along with column name)."""
    if not col:
        result = handle.get(key)
    else:
        result = handle.get(key, col)
    if download:
        for content in result.values():
            set_file(key, get_content(content))
    return result

def list_info(handle, key_list):
    """Show the number of total rows/show column names of the specified key."""
    if len(key_list) == 0:
        result = dict(handle.get_range(column_count=0,
                      filter_empty=False))
        print "Total", len(result), "row(s)."
        #return result
    else:
        result_col = get_key(handle, key_list[0], '').keys()
        print "'" + key_list[0] + "' has " + str(result_col) + " column(s)."
        return result_col

def complex_get(handle, key_list, col_list):
    """Return keys given columns."""
    if len(key_list) == 0:
        result = dict(handle.get_range(columns=col_list,
                      buffer_size=1024)).keys()
        for x in result:
            print x
    else:
        result = dict(handle.multiget(key_list, col_list,
                      buffer_size=1024))
        for x in result.values():
            print x.items()[0][1]
    return result

def connect_server(address, key_space, column_family):
    """Establish connection."""
    pool = ConnectionPool(key_space, address)
    print "[INFO] Connection to '" + key_space + "' established."
    cf = ColumnFamily(pool, column_family)
    print "[INFO] Column family '" + column_family + "' used."
    return pool, cf

def helpmsg():
    """Show help messages."""
    print "Usage: ./manager.py -h serv -k keyspace -c col_fam -i key col val"
    print "       ./manager.py -h serv -k keyspace -c col_fam -I [path]"
    print "       ./manager.py -h serv -k keyspace -c col_fam -d key [col]"
    print "       ./manager.py -h serv -k keyspace -c col_fam -l key col"
    print "       ./manager.py -h serv -k keyspace -c col_fam -L [key]"
    print "       ./manager.py -h serv -k keyspace -c col_fam -M [key_str] col_str"
