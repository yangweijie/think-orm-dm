<?php

namespace think\db\builder;

use think\db\Builder;
use think\db\exception\DbException as Exception;
use think\db\Expression;
use think\db\Query;
use think\db\Raw;

/**
 * 达梦数据库驱动
 */
class Dm extends Builder
{

    /**
     * 获取当前连接的数据库
     * @return string
     */
    public function getDatabase() :string
    {
        static $connect;
        if(!$connect){
            $connect = $this->getConnection();
        }
        return $connect->getConfig()['database'];
    }

    /**
     * having分析
     * @access protected
     * @param  Query  $query  查询对象
     * @param  string $having
     * @return string
     */
    protected function parseHaving(Query $query, string $having): string
    {
        if($having instanceof Raw){
            return $this->parseRaw($query, $having);
        }
        return !empty($having) ? ' HAVING ' . $this->parseKey($query, $having) : '';
    }


    /**
     * order分析
     * @access protected
     * @param  Query     $query        查询对象
     * @param  mixed     $order
     * @return string
     */
    protected function parseOrder(Query $query, array $order) :string
    {
        foreach ($order as $key => $val) {
            if ($val instanceof Expression) {
                $array[] = $val->getValue();
            } elseif (is_array($val) && preg_match('/^[\w\.]+$/', $key)) {
                $array[] = $this->parseOrderField($query, $key, $val);
            } elseif ('[rand]' == $val) {
                $array[] = $this->parseRand($query);
            } elseif (is_string($val)) {
                if (is_numeric($key)) {
                    list($key, $sort) = explode(' ', strpos($val, ' ') ? $val : $val . ' ');
                } else {
                    $sort = $val;
                }

                $sort    = strtoupper($sort);
                $sort    = in_array($sort, ['ASC', 'DESC'], true) ? ' ' . $sort : '';
                $array[] = $this->parseKey($query, $key, true) . $sort;
            }
        }

        return empty($array) ? '' : ' ORDER BY ' . implode(',', $array);
    }

    /**
     * 字段和表名处理
     * @access public
     * @param  Query      $query        查询对象
     * @param  string     $key
     * @param  bool     $strict
     * @return string
     */
    public function parseKey(Query $query, $key, bool $strict = false) :string
    {
        if (is_numeric($key)) {
            return $key;
        } elseif ($key instanceof Expression) {
            return $key->getValue();
        }

        $key = trim($key);

        if (strpos($key, '.')) {
            list($table, $key) = explode('.', $key, 2);

            $alias = $query->getOptions('alias');

            if ('__TABLE__' == $table) {
                $table = $query->getOptions('table');
                $table = is_array($table) ? array_shift($table) : $table;
            }

            if (isset($alias[$table])) {
                $table = $alias[$table];
            }
        }

        $key = str_replace('`', '', $key);
        if('*' != $key){
            if(!preg_match('/[,\'\"\*\(\).\s]/', $key)){
                $key = "`{$key}`";
            }else{
                $tableName = $query->getTable();
                $tableFields = $query->getTableFields($tableName);
                if(!is_array($tableFields)){
                    $tableFields = implode(' ', $tableFields);
                }
                $key = $this->quoteFields($key, $tableFields);
            }
        }

        if (isset($table)) {
            $key = $table . '.' . $key;
        }

        return $key;
    }

    protected function parseData(Query $query, array $data = [], array $fields = [], array $bind = []): array
    {
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
                [$key, $name]  = explode('->', $key, 2);
                $item          = $this->parseKey($query, $key);

                $result[$item . '->' . $name] = 'json_set(' . $item . ', \'$.' . $name . '\', ' . $this->parseDataBind($query, $key . '->' . $name, $val, $bind) . ')';
            } elseif (false === strpos($key, '.') && !in_array($key, $fields, true)) {
                if ($options['strict']) {
                    throw new Exception('fields not exists:[' . $key . ']');
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
     * 将sql中的数据库字段加``
     * @param string $sql
     * @param array $fields
     * @return string
     */
    public function quoteFields($sql, $fields) :string
    {
        $newString = "";
        foreach ($fields as $field) {
            $replace = preg_quote("`{$field}`", '/');
            // 使用 preg_replace_callback 函数，将字符串中匹配到的单词替换为对应的替换词
            $newString = preg_replace_callback("/\b{$field}\b/", function ($matches) use ($replace) {
                return $replace;
            }, $sql);
            $sql = $newString;
        }
        return $newString;
    }

    /**
     * 分析Raw对象
     *
     * @param \think\db\BaseQuery $query 查询对象
     * @param Raw   $raw   Raw对象
     *
     * @return string
     */
    protected function parseRaw(Query $query, Raw $raw): string
    {
        $sql    = $raw->getValue();
        $bind   = $raw->getBind();

        $tableName = $query->getTable();
        $tableFields = $query->getTableFields($tableName);
        if(!is_array($tableFields)){
            $tableFields = implode(' ', $tableFields);
        }
        $sql = $this->quoteFields($sql, $tableFields);

        // 兼容group_concat
        $sql = str_ireplace(['group_concat'], ['wm_concat'], $sql);
        if ($bind) {
            $query->bindParams($sql, $bind);
        }

        return $sql;
    }

    /**
     * table分析
     * @access protected
     * @param  Query     $query     查询对象
     * @param  mixed     $tables    表名
     * @return string
     */
    protected function parseTable(Query $query, $tables): string
    {
        $item    = [];
        $options = $query->getOptions();
        $database = $this->getDatabase();
//        "`{$database}`.".
        $all_tables = $this->getConnection()->getTables();
        foreach ((array) $tables as $key => $table) {
            $is_alias = !in_array($table, $all_tables);
            $old_table = $table;
            $table = $table instanceof Raw? $table: ($is_alias? $table: "`{$database}`.".$table);
            if ($old_table instanceof Raw) {
                $item[] = $this->parseRaw($query, $table);
            } elseif (!is_numeric($key)) {
                $item[] = $this->parseKey($query, $key) . ' ' . $this->parseKey($query, $table);
            } elseif (isset($options['alias'][$old_table])) {
                $item[] = $this->parseKey($query, $table) . ' ' . $this->parseKey($query, $options['alias'][$old_table]);
            } else {
                $item[] = $this->parseKey($query, $table);
            }
        }

        return implode(',', $item);
    }

    /**
     * 随机排序
     * @access protected
     * @param  Query $query 查询对象
     * @return string
     */
    protected function parseRand(Query $query): string
    {
        return 'RAND()';
    }
}
