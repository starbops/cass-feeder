#!/usr/bin/env python
# -*- coding: utf-8 -*-
# file: agent.py

from manager import *

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "h:k:c:i:Id:l:LM")
		if len(opts) != 4: raise getopt.GetoptError, "Choose one from below:"
	except getopt.GetoptError as err: helpmsg()
	else:
		in_server = opts[0][1]
		in_keyspace = opts[1][1]
		in_columnfamily = opts[2][1]
		try:
			pool, cf = connectserver([in_server], in_keyspace, in_columnfamily)
		except Exception as err:
			print "[ERROR] " + str(err) + "\n[ERROR] Connection aborted."
		else:
			(o, a) = opts[3]
			if o == '-i':
				if a != '' and len(args) == 2: InsertKey(cf, a, args)
				else: helpmsg()
			elif o == '-I':
				if len(args) == 1:
					ImportTree(cf, 'UUID', args[0])
				else:
					devinfo = uuidpth()
					blklst = ['nicklistfifo', '/dev', '/run', '/var/lib/dpkg/info', '/sys/kernel/debug/hid']
					for key in devinfo:
						blklsttmp = [x for x in devinfo.values() if x != devinfo[key] if x != '/']
						ImportTree(cf, key, devinfo[key], blklst + blklsttmp)
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
