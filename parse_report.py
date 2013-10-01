#!/usr/bin/env python
# file: parse_report.py

import re

pattern = re.compile(r"===== Files tainted =====\n(.*?)===== Is MBR tainted =====", re.DOTALL)

with open("report.log", "r") as inf:
    raw_content = inf.read()
    reports = re.split("\n\n", raw_content)
    for record in reports:
        if not record:
            break
        rowkey = re.findall(r"RowKey \([0-9]+\) ===> (.*)", record)
        files_tainted = re.findall(pattern, record)
        if not files_tainted:
            try:
                print rowkey[0], 0
            except IndexError as err:
                pass
            continue
        print rowkey[0], files_tainted[0].count('\n')
