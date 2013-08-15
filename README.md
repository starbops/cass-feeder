# cass-feeder

Parse them! Hash them! Eat them! Cassandra is hungry!

## Usage

### setup.py
 
```
Usage: ./setup.py -H server -c keyspace columnfamily
       ./setup.py -H server -d keyspace
```

### manage.py

```
Usage: ./manager.py -H server -K keyspace -C columnfamily -i key column value
       ./manager.py -H server -K keyspace -C columnfamily -I path
       ./manager.py -H server -K keyspace -C columnfamily -d key [column]
       ./manager.py -H server -K keyspace -C columnfamily -l key [column]
       ./manager.py -H server -K keyspace -C columnfamily -L [key]
       ./manager.py -H server -K keyspace -C columnfamily -M [key_string] column_string
```

## Progress

目前測試環境是 FreeBSD-8.3-Release，目標是移植到 Ubuntu 上

- setup.py
	- [x] 建立 keyspace & column family
	- [x] 刪除 keyspace
- manager.py
	- [x] 插入單筆 key
	- [x] 插入多筆（給定路徑 recursively insert）
	- [x] 刪除單筆 key or key's column
	- [x] 查詢單筆 key or key's column
	- [x] 計算總 key 數
	- [x] 給 column 查詢 key
	- [x] 給 key & column 查詢 value
	- [ ] 讀取裝置 UUID 功能
	- [ ] 除 UUID 外另增 column of network source
