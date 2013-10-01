#!/usr/bin/env python
# file: parse_report.py

import re

pattern_ft = re.compile(r"===== Files tainted =====\n(.*?)===== Is MBR tainted =====", re.DOTALL)
pattern_rt = re.compile(r"===== Registry tainted =====\n(.*?)===== Process tainted =====", re.DOTALL)

with open("report.log", "r") as inf:
    raw_content = inf.read()
    reports = re.split("\n\n", raw_content)
    for record in reports:
        if not record:
            break
        try:
            rowkey = re.findall(r"RowKey \([0-9]+\) ===> (.*)", record)[0]
        except IndexError as err:
            continue

        file_tainted = re.findall(pattern_ft, record)
        if not file_tainted:
            file_tainted_count = 0
        else:
            file_tainted_count = file_tainted[0].count('\n')

        reg_tainted = re.findall(pattern_rt, record)
        reg_keyword = []
        if reg_tainted:
            if re.search(r"iconv", reg_tainted[0]):
                reg_keyword.append("iconv")
            if re.search(r"_MBA_TMP_tainted_", reg_tainted[0]):
                reg_keyword.append("_MBA_TMP_tainted_")

        print rowkey, file_tainted_count, ' '.join(reg_keyword)
