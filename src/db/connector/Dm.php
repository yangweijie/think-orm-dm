<?php
/**
 * 71CMS (c) 南宁小橙科技有限公司
 * 网站地址: http://71cms.net
 * Author: y.Lee <86332603@qq.com>
 * Date: 2022/03/24
 * Time: 10:05
 */

namespace think\db\connector;

use PDO;
use think\db\exception\PDOException;
use think\db\PDOConnection;
use think\db\Query;
use think\db\Raw;
use think\Exception;

/**
 *  达梦数据库驱动
 */
class Dm extends PDOConnection {

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
	 * 默认PDO连接参数
	 * @var array
	 */
	protected $params = [
		PDO::ATTR_CASE => PDO::CASE_NATURAL,
		PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
		PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
		PDO::ATTR_STRINGIFY_FETCHES => false,
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

//    /**
//     * 连接数据库方法
//     * @access public
//     * @param array      $config         连接参数
//     * @param integer    $linkNum        连接序号
//     * @param array|bool $autoConnection 是否自动连接主数据库（用于分布式）
//     * @return PDO
//     * @throws PDOException
//     */
//    public function connect(array $config = [], $linkNum = 0, $autoConnection = false): PDO
//    {
//        if (empty($config)) {
//            $config = $this->config;
//        } else {
//            $config = array_merge($this->config, $config);
//        }
//
//        $PDO = parent::connect($config, $linkNum, $autoConnection);
//
//        $PDO->query(sprintf("set CHAR_CODE %s", mb_strtoupper($config['charset'])));
//        return $PDO;
//    }

	/**
	 * 取得数据表的字段信息
	 * @access public
	 * @param string $tableName
	 * @return array
	 */
	public function getFields(string $tableName): array {
		$config = $this->getConfig();
		$sql = "select * from user_tab_columns where table_name='{$tableName}'";
		try {
			$pdo = $this->query($sql, [], false, true);
			$result = $pdo;
			$info = [];

			if ($result) {
				foreach ($result as $key => $val) {
					$val = array_change_key_case($val);
					$info[$val['column_name']] = [
						'name' => $val['column_name'],
						'type' => $val['data_type'],
						'notnull' => 'Y' === $val['nullable'],
						'default' => $val['data_default'],
						'primary' => $val['column_name'] === 'id',
						'autoinc' => false,
					];
				}
			}
			return $this->fieldCase($info);
		} catch (PDOException $e) {
			throw new Exception(iconv('gbk', 'utf-8', $e->getMessage()));
		}
	}

	/**
	 * 数据分析
	 * @access protected
	 * @param  Query $query     查询对象
	 * @param  array $data      数据
	 * @param  array $fields    字段信息
	 * @param  array $bind      参数绑定
	 * @return array
	 */
	protected function parseData(Query $query, array $data = [], array $fields = [], array $bind = []): array {
		if (empty($data)) {
			return [];
		}

		$options = $query->getOptions();

		// 获取绑定信息
		if (empty($bind)) {
			$bind = $query->getFieldsBindType();
		}

		if (empty($fields)) {
			if (empty($options['field']) || '*' == $options['field']) {
				$fields = array_keys($bind);
			} else {
				$fields = $options['field'];
			}
		}

		$result = [];

		foreach ($data as $key => $val) {
			$item = $this->parseKey($query, $key, true);
			if ($val instanceof Raw) {
				$result[$item] = $this->parseRaw($query, $val);
				continue;
			} elseif (!is_scalar($val) && (in_array($key, (array) $query->getOptions('json')) || 'json' == $query->getFieldType($key))) {
				$val = json_encode($val);
			}

			if (false !== strpos($key, '->')) {
				[$key, $name] = explode('->', $key, 2);
				$item = $this->parseKey($query, $key);

				$result[$item . '->' . $name] = 'json_set(' . $item . ', \'$.' . $name . '\', ' . $this->parseDataBind($query, $key . '->' . $name, $val, $bind) . ')';
			} elseif (false === strpos($key, '.') && !in_array($key, $fields, true)) {
				if ($options['strict']) {
					throw new \think\db\exception\DbException('fields not exists:[' . $key . ']');
				}
			} elseif (is_null($val)) {
				$result[$item] = 'NULL';
			} elseif (is_array($val) && !empty($val) && is_string($val[0])) {
				switch (strtoupper($val[0])) {
				case 'INC':
					$result[$item] = $item . ' + ' . floatval($val[1]);
					break;
				case 'DEC':
					$result[$item] = $item . ' - ' . floatval($val[1]);
					break;
				}
			} elseif (is_scalar($val)) {
				// 过滤非标量数据
				if (!$query->isAutoBind() && PDO::PARAM_STR == $bind[$key]) {
					$val = '\'' . $val . '\'';
				}

				$result[$item] = !$query->isAutoBind() ? $val : $this->parseDataBind($query, $key, $val, $bind);
			}
		}

		return $result;
	}

	/**
	 * 取得数据库的表信息
	 * @access   public
	 * @param string $dbName
	 * @return array
	 */
	public function getTables(string $dbName = ''): array {
		$config = $this->getConfig();
		$sql = "select table_name from USER_TABLES where TABLESPACE_NAME='MAIN'";
		$pdo = $this->getPDOStatement($sql);
		$result = $pdo->fetchAll(PDO::FETCH_ASSOC);
		$info = [];

		foreach ($result as $key => $val) {
			$info[$key] = current($val);
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
