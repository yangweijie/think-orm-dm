# think-orm-dm
dm database for ThinkPHP6+

## 配置

~~~ dm_svc.conf 
TIME_ZONE=(480)
LANGUAGE=(cn)
CHAR_CODE=(PG_UTF8)
KEYWORDS=(user,label)
~~~

~~~ dm.ini
COMPATIBLE_MODE=4
~~~


## 兼容性函数

`find_in_set` 参见src/db/dm.sql