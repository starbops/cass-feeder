#!/usr/bin/env python
# -*- coding: utf-8 -*-
# file: manager.py

import dbus, getopt, hashlib, os, pycassa, re, sys
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

blklst = ['nicklistfifo', '/dev', 'run', '/var/lib/dpkg/info', '/run/vmblock-fuse', '/sys/kernel/debug/hid']

###
# Return A Dictionary About UUID & Mount Point
####################################################################################
def uuidpth():
	bus = dbus.SystemBus()
	ud_manager_obj = bus.get_object("org.freedesktop.UDisks", "/org/freedesktop/UDisks")
	ud_manager = dbus.Interface(ud_manager_obj, 'org.freedesktop.UDisks')
	uuidlst = []
	mntpthlst = []

	for dev in ud_manager.EnumerateDevices():
		device_obj = bus.get_object("org.freedesktop.UDisks", dev)
		device_props = dbus.Interface(device_obj, dbus.PROPERTIES_IFACE)
		mntpth = str(device_props.Get('org.freedesktop.UDisks.Device', "DeviceMountPaths"))
		iduuid = str(device_props.Get('org.freedesktop.UDisks.Device', "IdUuid"))
		try:
			mntpthlst.append(re.search('.*\'(/.*?)\'.*', mntpth).group(1))
		except AttributeError:
			pass
		else:
			if iduuid != '':
				uuidlst.append(iduuid)

	return dict(zip(uuidlst, mntpthlst))

###
# Insert Key With Column Name And Column Value
####################################################################################
def InsertKey(handle, key, col_val_list):
	col_val_list = iter(col_val_list)
	for col, val in zip(col_val_list, col_val_list):
		handle.insert(key, {col: val})
		print "[INFO] Key '" + key + "' inserted."

###
# MD5 & SHA1
####################################################################################
def hashfile(afile, hasherlst, blocksize=65536):
	print "[INFO] Hashing file '" + afile.name + "'.",
	buf = afile.read(blocksize)
	while len(buf) > 0:
		print "\b.",
		[hasher.update(buf) for hasher in hasherlst]
		buf = afile.read(blocksize)
	print
	return ''.join([hasher.hexdigest() for hasher in hasherlst])

###
# Check If Current Directory Is In Blacklist
####################################################################################
def inblacklist(curdrnm):
	for blk in blklst:
		if curdrnm.startswith(blk):
			return True
	return False

###
# Insert Keys From A Given Point Recursively (UUID)
####################################################################################
def ImportTree(h, devuuid, pth):
	for drnm, sbdrnms, flnms in os.walk(pth):
		print "[INFO] Entering '" + drnm + "'."
		if inblacklist(drnm): continue
		flnmlst = [os.path.abspath(os.path.join(drnm, flnm)) for flnm in set(flnms).difference(blklst)]
		try: rsltlst = [(flnm, hashfile(open(flnm, 'rb'), [hashlib.md5(), hashlib.sha1()]) + str(os.path.getsize(flnm))) for flnm in flnmlst]
		except (OSError, IOError) as err: print "[ERROR] " + str(err) + "."
		else: [InsertKey(h, flnmabs, [devuuid, rslt]) for (flnmabs, rslt) in rsltlst]
	print "[INFO] Job done."

###
# Delete Whole Row Or Multiple Columns
####################################################################################
def DeleteKey(handle, key, col):
	if len(col) != 0:
		handle.remove(key, col);
		print "[INFO] Remove " + str(col) + " from '" + key + "'."
	else:
		handle.remove(key);
		print "[INFO] Remove '" + key + "'."

###
# Return OrderedDick Of The Specified Key (Along With Column Name)
####################################################################################
def GetKey(handle, key, col):
	if len(col) != 0:
		result = handle.get(key, col)
		return result
	else:
		result = handle.get(key)
		return result

###
# Show The Number Of Total Rows / Show Column Names Of The Specified Key
####################################################################################
def ListInfo(handle, key_list):
	if len(key_list) == 0:
		result = dict(handle.get_range(column_count=1, filter_empty=False))
		print "Total", len(result), "row(s)."
		#return result
	else:
		result_col = GetKey(handle, key_list[0], '').keys()
		print "'" + key_list[0] + "' has " + str(result_col) + " column(s)."
		return result_col

###
# Columns Given, Return Keys
####################################################################################
def ComplexGet(handle, key_list, col_list):
	if len(key_list) == 0:
		result = dict(handle.get_range(columns=col_list))
	elif len(col_list) == 0:
		result = dict(handle.multiget(key_list))
	else:
		result = dict(handle.multiget(key_list, col_list))
	print result
	return result

###
# Establish Connection
####################################################################################
def connectserver(address, key_space, column_family):
	pool = ConnectionPool(key_space, address)
	print "[INFO] Connection to '" + key_space + "' established."
	cf = ColumnFamily(pool, column_family)
	print "[INFO] Column family '" + column_family + "' used."
	return pool, cf

###
# Show Help Messages
####################################################################################
def helpmsg():
	print "Usage: ./manager.py -H server -K keyspace -C columnfamily -i key column value"
	print "       ./manager.py -H server -K keyspace -C columnfamily -I path"
	print "       ./manager.py -H server -K keyspace -C columnfamily -d key [column]"
	print "       ./manager.py -H server -K keyspace -C columnfamily -l key [column]"
	print "       ./manager.py -H server -K keyspace -C columnfamily -L [key]"
	print "       ./manager.py -H server -K keyspace -C columnfamily -M [key_string] column_string"
