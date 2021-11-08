# Contents
* sqldict.py

# Functions/Objects:
* ```SqlDict``` -- dict-like object that will use a sql table to perform key lookups.
* ```make_sql_table``` -- function that will take a list of (key, value) pairs and create a sql table in the format used by SqlDict.
* ```AsyncSqlDict``` -- an asynchronous version of SqlDict (using aiosqlite3) to avoid 'database is locked' errors
* ```make_async_sql_table``` -- make_sql_table, but using aiosqlite3 to avoid database is locked errors

# Usage
```python
database_name = "test_database.db"
sql_dict = SqlDict(database_name)
print(sql_dict["key"])
>>> "value1"
sql_dict["new_key"] = "value2"
print(sql_dict["new_key"])
>>> "value2"
```

# Modifications

This has been modified to add the AsyncSqlDict, and make_async_sql_table to avoid 'database is locked' errors :-)
