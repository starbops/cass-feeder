#!/usr/bin/env python
# -*- coding: utf-8 -*-
# file: manager.py

import dbus, getopt, hashlib, os, pycassa, re, subprocess, sys
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

SECMAPSERVLST = ['192.168.100.103', '192.168.100.105', '192.168.100.107', '192.168.100.111']

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
		try: mntpthlst.append(re.search('.*\'(/.*?)\'.*', mntpth).group(1))
		except AttributeError: continue
		if iduuid != '': uuidlst.append(iduuid)

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
def hashfile(file_name, hasherlst, file_size, blocksize=65536):
	print "[INFO] Hashing file '" + file_name + "'."
	if file_size == 0:
		[hasher.update('') for hasher in hasherlst]
	else:
		with open(file_name, 'rb') as afile:
			buf = afile.read(blocksize)
			while len(buf) > 0:
				[hasher.update(buf) for hasher in hasherlst]
				buf = afile.read(blocksize)
	return ''.join([hasher.hexdigest() for hasher in hasherlst]) + str(file_size)

###
# Check If Current Directory Is In Blacklist
####################################################################################
def inblacklist(nm, blklst):
	for blk in blklst:
		if nm.startswith(blk):
			return True
	return False

###
# Insert Keys From A Given Point Recursively (UUID)
####################################################################################
def ImportTree(h, devuuid, pth, blklst=[]):
	for drnm, sbdrnms, flnms in os.walk(pth):
		print "[INFO] Entering '" + drnm + "'."
		flnmabslst = [os.path.abspath(os.path.join(drnm, flnm)) for flnm in flnms]
		for flnmabs in flnmabslst:
			if inblacklist(flnmabs, blklst): continue
			else:
				try:
					hashstr = hashfile(flnmabs, [hashlib.md5(), hashlib.sha1()], os.path.getsize(flnmabs))
					InsertKey(h, flnmabs, [devuuid, hashstr])
				except Exception as err:
					print "[ERROR] " + str(err) + "."
	print "[INFO] Importation done."

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
# Get File Content From Other Cassandra Servers
####################################################################################
def getfilecontent(taskid):
	result = ""
	try:
		(p, c) = connectserver(SECMAPSERVLST, 'SECMAP', 'SUMMARY')
	except Exception as err:
		print "[ERROR] " + str(err) + "\n[ERROR] Connection aborted."
	else:
		try:
			content = GetKey(c, taskid, ['content'], False)
			result = content.values()[0]
		except pycassa.NotFoundException as err:
			print "[ERROR] " + str(err) + "."
		p.dispose()
	return result

###
# Write Content Into File
####################################################################################
def setfile(file_name, content):
	try: os.makedirs(os.getcwd() + os.path.dirname(file_name))
	except OSError as err:
		print "[ERROR] " + str(err) + "."
	full_file_name = os.getcwd() + file_name
	with open(full_file_name, "w") as f:
		f.write(content)
	if re.match(r".*DLL.*", subprocess.check_output(['file', full_file_name])):
		new_file_name = full_file_name + ".dll"
	else:
		new_file_name = full_file_name + ".exe"
	os.system("mv " + full_file_name + " " + new_file_name)

###
# Return OrderedDict Of The Specified Key (Along With Column Name)
####################################################################################
def GetKey(handle, key, col, download=True):
	result = handle.get(key, col)
	if download:
		for content in result.values():
			setfile(key, getfilecontent(content))
	return result

###
# Show The Number Of Total Rows / Show Column Names Of The Specified Key
####################################################################################
def ListInfo(handle, key_list):
	if len(key_list) == 0:
		result = dict(handle.get_range(column_count=0, filter_empty=False))
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
		result = dict(handle.get_range(columns=col_list, buffer_size=1024)).keys()
		for x in result: print x
	else:
		result = dict(handle.multiget(key_list, col_list, buffer_size=1024))
		for x in result.values(): print x.items()[0][1]
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
	print "Usage: ./manager.py -h server -k keyspace -c columnfamily -i key column value"
	print "       ./manager.py -h server -k keyspace -c columnfamily -I [path]"
	print "       ./manager.py -h server -k keyspace -c columnfamily -d key [column]"
	print "       ./manager.py -h server -k keyspace -c columnfamily -l key column"
	print "       ./manager.py -h server -k keyspace -c columnfamily -L [key]"
	print "       ./manager.py -h server -k keyspace -c columnfamily -M [key_string] column_string"
