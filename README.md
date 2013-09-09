# cass-feeder

Parse them! Hash them! Eat them! Cassandra is hungry!

## Environment

- Ubuntu 12.04 LTS
- Windows XP Professional
- Cassandra 1.0.12
- Pycassa 1.9.1
- Regparser

## Usage

### setup.py
 
```
Usage: ./setup.py -h server -C keyspace columnfamily
       ./setup.py -h server -D keyspace
```

### agent.py

```
Usage: ./agent.py -h server -k keyspace -c columnfamily -i key column value
       ./agent.py -h server -k keyspace -c columnfamily -I [path]
       ./agent.py -h server -k keyspace -c columnfamily -d key [column]
       ./agent.py -h server -k keyspace -c columnfamily -l key [column]
       ./agent.py -h server -k keyspace -c columnfamily -L [key]
       ./agent.py -h server -k keyspace -c columnfamily -M [key_string] column_string
```

### get\_file.py

```
Usage: python get_file.py serv keyspace col_fam key
```

### from\_candidate\_get\_file.py

```
Usage: python from_candidate_get_file.py serv keyspace col_fam file
```

### feeder.py

## Features

### setup.py

- 建立 keyspace & column family
- 刪除 keyspace

### manager.py

- 插入單筆 key
- 插入多筆（給定路徑 recursively insert）
- 自動抓取電腦上的裝置並將 mountpoint 之下的 files 匯入 cassandra
- 刪除單筆 key or key's column
- 查詢單筆 key or key's column
- 計算總 key 數
- 給 column 查詢 key
- 給 key & column 查詢 value
- 讀取裝置 UUID 功能
- *除 UUID 外另增 column of network source*
- download file

## Table Structure

|                   | Frontend     | Backend     |
| ----------------- |:------------:|:-----------:|
| **Keyspace**      | KS           | SECMAP      |
| **Column Family** | CF           | SUMMARY     |
| **Key**           | fullPathName | taskID      |
| **Column Name**   | UUID         | content     |
| **Column Value**  | fileHash     | fileContent |
