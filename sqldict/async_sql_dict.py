from aiosqlite import connect
#AioSqlite: https://pypi.org/project/aiosqlite/
#Otherwise, you might get DataBase is locked errors
import pickle
from os.path import expanduser
import sys


async def make_async_sql_table(kv_list=[], db_name="", key_format="String", value_format="BLOB", serializer=pickle, _table_name = "kv_store"):
    if db_name == "":
        raise RuntimeError
    async with connect(db_name) as conn:
        cur = conn.cursor()
        await cur.execute(f'''CREATE TABLE IF NOT EXISTS {_table_name} (key {key_format} PRIMARY KEY, val  {value_format})''')

        for k,v in kv_list:
            if serializer is None:
                await cur.execute(f'INSERT OR IGNORE INTO {_table_name} VALUES (?,?)', (k, v))
            else:
                await cur.execute(f'INSERT OR IGNORE INTO {_table_name} VALUES (?,?)', (k, serializer.dumps(v)))
        conn.commit()


class AsyncSqlDict(object):
    "A version of SqlDict that uses 
    def __init__(self, name,
                 table_name="kv_store", key_col="key", val_col="val",
                 serializer=pickle):
        if not name.endswith('.db'):
            name = name + '.db'
        await make_sql_table(db_name = name, _table_name = "kv_store")
        self.name = expanduser(name)
        self.serializer = serializer
        assert hasattr(serializer, 'loads')
        assert hasattr(serializer, 'dumps')
        self.__tablename = table_name
        self.__key_col = key_col
        self.__val_col = val_col

    async def __getitem__(self, key):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT {} FROM {} WHERE {}=? LIMIT 1"
                        .format(self.__val_col, self.__tablename, self.__key_col), (key,))
            res = await cur.fetchone()
        try:
            if self.serializer is None:
                return res[0]
            else:
                return self.serializer.loads(res[0])
        except:
            raise KeyError(key)

    async def __setitem__(self, key, value):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            if self.serializer is not None:
                value = self.serializer.dumps(value)
            else:
                value = value
            await cur.execute('INSERT OR REPLACE INTO {} VALUES (?,?)'.format(self.__tablename),
                        (key, value))
            conn.commit()  
    async def __delitem__(self, index):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute(f"""DELETE FROM {self.__tablename} 
WHERE {self.__key_col} = ?""", (index))
            await conn.commit()
            

    async def get(self, key, default):
        return self.__getitem__(key, default)

    async def __contains__(self, key):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT COUNT(*) FROM {} WHERE {}=? LIMIT 1"
                        .format(self.__tablename, self.__key_col), (key,))
            res = await cur.fetchone()
            await conn.commit()
        return res[0] != 0

    async def values(self):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT {} FROM {}"
                        .format(self.__val_col, self.__tablename))
            if self.serializer is None:
                res = (x[0] for x in cur)
            else:
                res = (self.serializer.loads(x[0]) for x in cur)
        return res

    async def keys(self):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT {} FROM {}".format(self.__key_col, self.__tablename))
            res = (x[0] for x in cur)
        return res

    async def items(self):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT {},{} FROM {}"
                        .format(self.__key_col, self.__val_col, self.__tablename))
            if self.serializer is None:
                res = ((k, v) async for k,v in cur)
            else:
                res = ((k, self.serializer.loads(v)) async for k,v in cur)
        return res

    async def __len__(self):
        async with connect(self.name) as conn:
            cur = conn.cursor()
            await cur.execute("SELECT Count(*) FROM {}".format(self.__tablename))
            res = await cur.fetchone()[0]
        return res
