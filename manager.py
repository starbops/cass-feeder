#!/usr/bin/env python
# -*- coding: utf-8 -*-
# file: manager.py

import getopt, hashlib, os, pycassa, sys
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

blklst = ['nicklistfifo', '/dev']

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
#def HashIt(file_name):
#	mdfive = hashlib.md5()
#	shaone = hashlib.sha1()
#	with open(file_name, 'r') as file_handle:
#		for chunk in iter(lambda: file_handle.read(8 * 1024), b''):
#			mdfive.update(chunk)
#			shaone.update(chunk)
#			#sys.stdout.write('.')
#	hash_result = mdfive.hexdigest() + shaone.hexdigest()
#	return hash_result

###
# Insert Keys From A Given Point Recursively
####################################################################################
#def ImportTree(handle, in_path):
#	for dir_name, subdir_names, file_names in os.walk(in_path):
#		for file_name in file_names:
#			if file_name == "nicklistfifo":
#				print "JUMP" + file_name
#				continue
#			file_path_name = os.path.join(dir_name, file_name)
#			file_path_name_abs = os.path.abspath(file_path_name)
#			try:
#				file_size = os.path.getsize(file_path_name_abs)
#				hash_result = HashIt(file_path_name_abs)
#			except (OSError, IOError) as err:
#				print "[ERROR] " + str(err) + "."
#			result = hash_result + str(file_size)
#			InsertKey(handle, file_path_name_abs, list(('UUID', result)))

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
# Insert Keys From A Given Point Recursively
####################################################################################
def ImportTree(h, pth):
	for drnm, sbdrnms, flnms in os.walk(pth):
		print "[INFO] Entering '" + drnm + "'."
		if inblacklist(drnm): continue
		flnmlst = [os.path.abspath(os.path.join(drnm, flnm)) for flnm in set(flnms).difference(blklst)]
		try: rsltlst = [(flnm, hashfile(open(flnm, 'rb'), [hashlib.md5(), hashlib.sha1()]) + str(os.path.getsize(flnm))) for flnm in flnmlst]
		except (OSError, IOError) as err: print "[ERROR] " + str(err) + "."
		else: [InsertKey(h, flnmabs, ['UUID', rslt]) for (flnmabs, rslt) in rsltlst]
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
#
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

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "H:K:C:i:I:d:l:LM")
		if len(opts) != 4: raise getopt.GetoptError, "Choose one from below:"
	except getopt.GetoptError as err: helpmsg()
	else:
		in_server = opts[0][1]
		in_keyspace = opts[1][1]
		in_columnfamily = opts[2][1]
		try:
			pool, cf = connectserver([in_server], in_keyspace, in_columnfamily)
		except pycassa.InvalidRequestException as err:
			print "[ERROR] " + str(err) + "\n[ERROR] Connection aborted."
		else:
			(o, a) = opts[3]
			if o == '-i':
				if a != '' and len(args) == 2: InsertKey(cf, a, args)
				else: helpmsg()
			elif o == '-I':
				if a != '': ImportTree(cf, a)
				else: helpmsg()
			elif o == '-d':
				if a != '': DeleteKey(cf, a, args)
				else: helpmsg()
			elif o == '-l':
				if a != '':
					try:
						result = GetKey(cf, a, args)
						print "'" + a + "' has " + str(result) + "."
					except pycassa.NotFoundException as err:
						print "[ERROR] " + str(err)
						print "[ERROR] '" + a + "' not found. Action aborted."
				else: helpmsg()
			elif o == '-L':
				try:
					ListInfo(cf, args)
				except pycassa.NotFoundException as err:
					print "[ERROR] " + str(err) + "\n[ERROR] Action aborted."
			elif o == '-M':
				if len(args) == 2:
					kl = args[0].split()
					cl = args[1].split()
					if len(kl) == 0 and len(cl) == 0:
						helpmsg()
					else:
						try: ComplexGet(cf, kl, cl)
						except Exception as err: print "[ERROR] " + str(err) + "."
				elif len(args) == 1:
					cl = args[0].split()
					if len(cl) == 0:
						helpmsg()
					else:
						try: ComplexGet(cf, [], cl)
						except Exception as err: print "[ERROR] " + str(err) + "."
				else: helpmsg()
			else: helpmsg()
			pool.dispose()
			print "[INFO] Connection closed."

if __name__ == '__main__':
	main()
