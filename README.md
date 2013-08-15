# cass-feeder

---

Parse them! Hash them! Eat them! Cassandra is hungry!

## Usage

```
Usage: ./manager.py -H server -K keyspace -C columnfamily -i key column value
       ./manager.py -H server -K keyspace -C columnfamily -I path
       ./manager.py -H server -K keyspace -C columnfamily -d key [column]
       ./manager.py -H server -K keyspace -C columnfamily -l key [column]
       ./manager.py -H server -K keyspace -C columnfamily -L [key]
       ./manager.py -H server -K keyspace -C columnfamily -M [key_string] column_string
```

目前測試環境是 FreeBSD-8.3-Release，目標是移植到 Ubuntu 上

- setup.py
	- 建立 keyspace & column family
	- 刪除 keyspace
- manager.py
	- 插入單筆 key
	- 插入多筆（給定路徑 recursively insert）
	- 刪除單筆 key or key's column
	- 查詢單筆 key or key's column
	- 計算總 key 數
	- 給 column 查詢 key
	- 給 key & column 查詢 value
	- 讀取裝置 UUID 功能
	- 除 UUID 外另增 column of network source
