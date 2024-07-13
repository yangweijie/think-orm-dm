<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006~2023 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: liu21st <liu21st@gmail.com>
// +----------------------------------------------------------------------

namespace think\db\connector;

use PDO;
use think\db\PDOConnection;

/**
 * Pgsql数据库驱动.
 */
class Dm extends PDOConnection
{

    protected $config = [
        // 数据库类型
        'type' => '',
        // 服务器地址
        'hostname' => '',
        // 数据库名
        'database' => '',
        // 用户名
        'username' => '',
        // 密码
        'password' => '',
        // 端口
        'hostport' => '',
        // 连接dsn
        'dsn' => '',
        // 数据库连接参数
        'params' => [],
        // 数据库编码默认采用utf8
        'charset' => 'utf8',
        // 数据库表前缀
        'prefix' => '',
        // 数据库部署方式:0 集中式(单一服务器),1 分布式(主从服务器)
        'deploy' => 0,
        // 数据库读写是否分离 主从式有效
        'rw_separate' => false,
        // 读写分离后 主服务器数量
        'master_num' => 1,
        // 指定从服务器序号
        'slave_no' => '',
        // 模型写入后自动读取主服务器
        'read_master' => false,
        // 是否严格检查字段是否存在
        'fields_strict' => true,
        // 开启字段缓存
        'fields_cache' => false,
        // 监听SQL
        'trigger_sql' => true,
        // Builder类
        'builder' => '',
        // Query类
        'query' => 'think\db\Dm',
        // 是否需要断线重连
        'break_reconnect' => false,
        // 断线标识字符串
        'break_match_str' => [],
        // 自动参数绑定
        'auto_param_bind' => true,
    ];

    /**
     * 默认PDO连接参数.
     *
     * @var array
     */
    protected $params = [
        PDO::ATTR_CASE              => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE           => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS      => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => false,
        PDO::ATTR_EMULATE_PREPARES  => false,
    ];
    
    /**
     * 解析pdo连接的dsn信息
     * @access protected
     * @param array $config 连接信息
     * @return string
     */
    protected function parseDsn(array $config): string {
        $dsn = sprintf('dm:host=%s;dbname=%s;charset=%s', $config['hostname']. ($config['hostport']?":{$config['hostport']}":''), $config['database'], $config['charset']);
        return $dsn;
    }

    /**
     * 取得数据表的字段信息.
     *
     * @param string $tableName
     *
     * @return array
     */
    public function getFields(string $tableName): array
    {
        [$tableName] = explode(' ', $tableName);

        $sql = "select * from user_tab_columns where table_name='{$tableName}'";
        $pdo = $this->query($sql, [], true);
        $sql2 = "select a.name COL_NAME from  SYS.SYSCOLUMNS a,all_tables b,sys.sysobjects c where a.INFO2 & 0x01 = 0x01
and a.id=c.id and c.name= b.table_name and b.TABLE_NAME = '{$tableName}'";
        $table_auoinc_fields = array_column($this->query($sql2, [], true), 'COL_NAME');
        $result = $pdo;
        $info   = [];

        if (!empty($result)) {
            foreach ($result as $key => $val) {
                $val = array_change_key_case($val);
                $info[$val['column_name']] = [
                    'name'    => $val['column_name'],
                    'type'    => $val['data_type'],
                    'notnull' => (bool) 'Y' === $val['nullable'],
                    'default' => $val['data_default'],
                    'primary' => $val['column_id'] === 1,
                    'autoinc' => in_array($val['column_name'], $table_auoinc_fields),
                ];
            }
        }

        return $this->fieldCase($info);
    }

    /**
     * 取得数据库的表信息.
     *
     * @param string $dbName
     *
     * @return array
     */
	public function getTables(string $dbName = ''): array {
        static $info;
        if(!$info){
            $sql = "select table_name from USER_TABLES where TABLESPACE_NAME='MAIN'";
            $pdo = $this->getPDOStatement($sql);
            $result = $pdo->fetchAll(PDO::FETCH_ASSOC);
            $info = [];

            foreach ($result as $key => $val) {
                $info[$key] = current($val);
            }
        }
		return $info;
	}

    /**
     * 启动XA事务
     * @access public
     * @param  string $xid XA事务id
     * @return void
     */
    public function startTransXa(string $xid): void
    {
        $this->initConnect(true);
        $this->linkID->exec("XA START '$xid'");
    }


    public function getCompatibleMode() {
		$sql = <<<SQL
SELECT para_name,para_type,para_value FROM V\$DM_INI WHERE PARA_NAME ='COMPATIBLE_MODE'
SQL;
		$pdo = $this->getPDOStatement($sql);
		$result = $pdo->fetchAll(PDO::FETCH_ASSOC);
		return $result[0];
	}

}
