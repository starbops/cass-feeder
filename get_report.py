#!/usr/bin/env python
# file: get_report.py

import pycassa
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

pool = ConnectionPool('SECMAP', ['192.168.100.103'])
cf = ColumnFamily(pool, 'MBA')
result = list(cf.get_range(column_count=0, buffer_size=100, filter_empty=False))
#print len(result)
keys = [i[0] for i in result]
#for k in keys:
#    print k
with open("report.log", 'wb') as f:
    counter = 0
    for k in keys:
        try:
            report = dict(cf.get(k))
            f.write("RowKey (" + str(counter) + ") ===> " + str(k) + '\n')
            f.write(report['OVERALL'].encode("utf-8") + '\n')
        except UnicodeDecodeError as err:
            continue
        except pycassa.NotFoundException as err:
            continue
        except KeyError as err:
            continue
        counter += 1

print "Done."
pool.dispose()
#while(result):
#    try:
#        #unicode('\x80abc', errors='ignore')
#        print unicode(result.next()[1].values()[1])
#    except UnicodeDecodeError as err:
#        print "JUMP!!!"
#        continue
#    except IndexError as err:
#        print "OOOOOOO"
#result.close()
#for i in result:
#    try:
#        a = i[1].values()
#    except UnicodeDecodeError as err:
#        print "JUMP!!!"
