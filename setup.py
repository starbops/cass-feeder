#!/usr/bin/env python
# file: setup.py

import getopt
import sys

import pycassa
from pycassa.system_manager import *

def get_handle(address):
    handle = SystemManager(address)
    print "[INFO] Connection established."
    return handle

def build_db(handle, key_space, col_fam):
    handle.create_keyspace(key_space, SIMPLE_STRATEGY,
                           {'replication_factor': '1'})
    print "[INFO] Keyspace '%s' was created." % key_space
    handle.create_column_family(key_space, col_fam)
    print "[INFO] Column Family '%s' was created." % col_fam

def destroy_db(handle, key_space):
    handle.drop_keyspace(key_space)
    print "[INFO] Keyspace '%s'was dropped." % key_space

def helpmsg():
    print "Usage: ./setup.py -h server -C keyspace col_fam"
    print "       ./setup.py -h server -D keyspace"

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:C:D:")
        if len(opts) != 2:
            raise getopt.GetoptError, "Need at least one option."
    except getopt.GetoptError:
        helpmsg()
    else:
        in_server = opts[0][1]
        (o, a) = opts[1]
        if o == '-C':
            if a != '' and len(args) == 1:
                try:
                    sm = get_handle(in_server)
                except:
                    print "[ERROR] Cannot connect to '%s'." % str(a)
                    sys.exit(2)
                try:
                    build_db(sm, a, args[0])
                except pycassa.InvalidRequestException as err:
                    print "[ERROR] " + str(err) + "."
                    print "[ERROR] Failed to create keyspace or column family."
                sm.close()
                print "[INFO] Connection closed."
            else:
                helpmsg()
        elif o == '-D':
            if a != '':
                try:
                    sm = get_handle(in_server)
                except:
                    print "[ERROR] Cannot connect to '%s'." % str(a)
                    sys.exit(2)
                try:
                    destroy_db(sm, a)
                except pycassa.InvalidRequestException:
                    print "[ERROR] Keyspace '%s' not found." % a
                sm.close()
                print "[INFO] Connection closed."
            else:
                helpmsg()
        else:
            helpmsg()

if __name__ == '__main__':
    main()
