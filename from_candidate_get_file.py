#!/usr/bin/env python

import os
import re
import subprocess as subp
import sys

import get_file

PATH_CMD_LIST = ['python', 'regparser.py', 'system',
                 'ControlSet001\Control\Session Manager\Environment',
                 'Path'
]
SYSROOT_CMD_LIST = ['python', 'regparser.py', 'software',
                    'Microsoft\Windows NT\CurrentVersion',
                    'SystemRoot'
]

def query_type(query):
    if query.find('\\') != -1:
        t = 0
    else:
        if re.match("^[a-zA-Z]:", query):
            pass
        elif re.match("^\.\.", query):
            pass
    return t

def get_path():

    try:
        path_result = subp.check_output(PATH_CMD_LIST).rstrip('\r\n')
        sysroot_result = subp.check_output(SYSROOT_CMD_LIST).rstrip('\r\n')
    except subp.CalledProcessError as err:
        path_result = ""
        sysroot_result = ""
        print err.returncode
        #exit(-1)
    path_candidate_list = path_result.replace('%SystemRoot%', sysroot_result).split(';')
    return path_candidate_list

def get_candidate_list(filename):
    with open("fcgf.log", "a") as outf:
        outf.write(filename)

    if filename.startswith('\\'):
        print "[ERROR] Invalid file name"
        #candidate_list = [filename]
    elif re.match("^\.\.", filename):
        candidate_list = [os.path.abspath(filename)]
    elif re.match("^[a-zA-Z]:", filename):
        candidate_list = [os.path.abspath(filename)]
    else:
        if filename.islower():
            filename_list = [filename, filename.upper()]
        elif filename.isupper():
            filename_list = [filename, filename.lower()]
        else:
            filename_list = [filename]

        candidate_list = list(filename_list)
        candidate_list.extend([os.path.join(path, fn)
                              for path in get_path()
                              for fn in filename_list])

    return candidate_list

def usage():
    return "Usage:\npython %s <serv> <keyspace> <col_fam> <target>" % sys.argv[0]

def main():
    try:
        serverlist = sys.argv[1]
        keyspace = sys.argv[2]
        columnfamily = sys.argv[3]
        query = sys.argv[4]
    except IndexError as err:
        print usage()
        exit(-1)

    for candidate in get_candidate_list(query):
        location = get_file.get_file(serverlist, keyspace,
                                     columnfamily, candidate)
        if location:
            print location

if __name__ == '__main__':
    main()

