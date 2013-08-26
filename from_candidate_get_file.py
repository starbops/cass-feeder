#!/usr/bin/env python

import os
import re
import subprocess as subp

def get_path():
    path_cmd_list = ['python', 'regparser.py', 'system',
                     'ControlSet001\Control\Session Manager\Environment',
                     'Path'
    ]
    sysroot_cmd_list = ['python', 'regparser.py', 'software',
                        'Microsoft\Windows NT\CurrentVersion',
                        'SystemRoot'
    ]

    try:
        path_result = subp.check_output(path_cmd_list).rstrip('\r\n')
        sysroot_result = subp.check_output(sysroot_cmd_list).rstrip('\r\n')
    except subp.CalledProcessError as err:
        print err.returncode
        exit(-1)
    path_candidate_list = path_result.replace('%SystemRoot%', sysroot_result).split(';')
    return path_candidate_list

def get_candidate_list(filename):
    if re.match("^\.\.", filename):
        candidate_list = [os.path.abspath(filename)]
    elif re.match("^[a-zA-Z]:", filename):
        candidate_list = [os.path.abspath(filename)]
    else:
        candidate_list = [os.path.join(path, filename) for path in get_path()]
    return candidate_list

def main():
    #a = "..\\bonobo.dll"
    #b = "D:\\WIDOW\\bonobo.txt"
    c = "E:\\lala\\haha"
    prefix_list = get_path()
    print prefix_list
    #get_candidate_list(a)
    #get_candidate_list(b)
    for i in get_candidate_list(c):
        try:
            subp.check_output(['python', 'get_file.py', '140.113.121.170', 'ks', 'cf', i])
        except subp.CalledProcessError as err:
            print "[ERROR] " + str(err)
            if err.returncode == -2:
                continue
            else:
                break
        else:
            print "file downloaded"

if __name__ == '__main__':
    main()
