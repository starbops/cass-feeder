#!/usr/bin/env python
# file: feeder.py

import dbus
import hashlib
import os
import re

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

def hashfile(filename, hasherlst, file_size, blocksize=65536):
    """Generate a string containing md5, sha1, and file size."""
    print "[INFO] Hashing file '" + filename + "'."
    if file_size == 0:
        [hasher.update('') for hasher in hasherlst]
    else:
        with open(filename, 'rb') as afile:
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

