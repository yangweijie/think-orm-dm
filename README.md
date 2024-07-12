# think-orm-dm
dm database for ThinkPHP6+

> 达梦数据库对单双引号有着严格的规定，在执行SQL语句的时候，字符串常量应使用单引号括起，关键字、对象名、字段名、别名等则使用双引号括起。而MySQL中则没有严格的规定，在适配过程中MySQL SQL语句中的单双引号严格按照DM的规定修改。

## 配置

~~~ dm_svc.conf  
TIME_ZONE=(480)
LANGUAGE=(cn)
CHAR_CODE=(PG_UTF8)
KEYWORDS=(user,label)
~~~
windows 在 `system32` 里 更改后要重启fpm 只重启服务式无效的。不配置CHAR_CODE php 显示查询数据会gbk乱码，异常也乱码

~~~ dm.ini
COMPATIBLE_MODE=4
~~~


## 兼容性函数

`find_in_set` 参见src/db/dm.sql

## 扩展的安装

### win

从 `dmdbms/drivers/php_pdo` 中复制相应版本的 `phpxx_dm.dll` 和 `pdoxx_dm.dll`

到 php ext 目录里 （如果是nts 就复制 `phpxxnts_dm.dll` 和 `pdoxxnts_dm.dll`）

然后 在 `php.ini` 中添加以下配置:

~~~ ini
[dm]
extension = pdo74nts_dm.dll
extension = php74nts_dm.dll

dm.port=5237

; 是否允许持久性连接

dm.allow_persistent = 1

; 允许建立持久性连接的最大数. -1 为没有限制.

dm.max_persistent = -1

; 允许建立连接的最大数(包括持久性连接). -1 为没有限制.

dm.max_links = -1

; 默认的主机地址

dm.default_host = localhost

; 默认登录的数据库

dm.default_db = SYSTEM

; 默认的连接用户名

dm.default_user = SYSDBA

; 默认的连接口令.

dm.default_pw = SYSDBA

;连接超时，这个参数未实际的用到，等待服务器支持

dm.connect_timeout = 10

;对于各种变长数据类型，每列最大读取的字节数。如果它设置为 0 或是小于 0,那么，读取变长字段时，将显示 NULL 值

dm.defaultlrl = 4096

; 是否读取二进制类型数据，如果它设置为 0，那么二进制将被 NULL 值代替

dm.defaultbinmode = 1

;是否允许检察持久性连接的有效性，如果设置为 ON，那么当重用一个持久性连接时，会检察该连接是否还有效

dm.check_persistent = ON
~~~

然后 将 `dmdbms/bin` 下 dm开头的所有dll 复制到 `system32` 目录里

### linux

## 框架配置

`config/database.php`中配置

~~
'default'='dm',
'dm'=>[
    'type'=>'dm',
    'hostname'=>'localhost',
    'hostport'=>5236,
    'username'=>'SYSDBA',
    'password' => 'SYSDBA',
    'database'=>'blog',
    'charset' => Env::get('database.charset', 'utf8'),
    'prefix' => 'dp_',
]
~~

## 特殊用法
### 随机排序
~~~ php
$ret = ApiLog::order('[rand]')->limit(10)->select();
~~~
### 获取兼容模式

~~~ php
Db::connect('dm')->getCompatibleMode();
~~~
### 私有方法
~~~
\think\db\Dm::procedureName('proc');  // 转换为 `database`.`proc`
~~~

## bug
max(id) bigint 返回了 小数点 dump 函数float 显示了一位小数 var_dump 没问题

## todo

1. - [ ] 测试表分区功能
2. - [ ] 测试xa事务
3. - [ ] 测试mac平台
4. - [ ] 测试出一些mysql函数有dm 没有的 并找出替代方案
5. - [ ] 写单元测试

> PS:
> 
> 经过一轮自测基本使用没问题了，如果你的系统部用到一些mysql 的高级用法 如xa 事务、 分区、锁。
> 可以大胆的用本库进行迁移测试。希望大家多测试，及时反馈问题给我。
> 数据库驱动移植也试我第一次尝试，最终的任务是人力和测试字符串替换了。“锁”粗略看了下，比较复杂，貌似不支持行锁。
> replace into 对应的 是 merge into 文档里也写的比较复杂
