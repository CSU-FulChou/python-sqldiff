# sqldiff  
 **Python 针对 MySQL 数据库表结构的差异 SQL 工具。**

fork from: https://github.com/camry/python-mysqldiff

重新开始维护 Python 版本的 mysqldiff 工具, 并且对于index的diff判别进行了进一步的分析（针对index diff 不仅仅是 索引名字，更关注索引内在的属性以及顺序


## 使用

```bash
# 查看帮助
./bin/sqldiff --help
# 实例
./bin/sqldiff --source user:password@host:port --db db1:db2
./bin/sqldiff --source user:password@host:port --target user:password@host:port --db db1:db2
```

## 安装

```bash
pip install pyinstaller
pip install click
pip install mysql-connector-python
```

## 打包

```bash
pyinstaller -F sqldiff.py
```
