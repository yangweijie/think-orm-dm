<?php

namespace think\db\builder;

use think\db\Builder;
use think\facade\Db;
use think\db\exception\DbException as Exception;
use think\db\Expression;
use think\db\Query;
use think\db\Raw;
use think\db\Dm as DmQuery;

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
     * where分析
     * @access protected
     * @param  Query $query   查询对象
     * @param  mixed $where   查询条件
     * @return string
     */
    protected function parseWhere(Query $query, array $where): string
    {
        $options  = $query->getOptions();
        $whereStr = $this->buildWhere($query, $where);
        // 子查询字段
        if(strpos($whereStr, '.') !== false && strpos($whereStr, ')') === false){
            list($tableAlias, $field) = explode('.', $whereStr, 2);
            $whereStr = DmQuery::quoteFields($whereStr, [trim($tableAlias)], true);
        }
        if (!empty($options['soft_delete'])) {
            // 附加软删除条件
            [$field, $condition] = $options['soft_delete'];

            $binds    = $query->getFieldsBindType();
            $whereStr = $whereStr ? '( ' . $whereStr . ' ) AND ' : '';
            $whereStr = $whereStr . $this->parseWhereItem($query, $field, $condition, $binds);
        }

        return empty($whereStr) ? '' : ' WHERE ' . $whereStr;
    }

    /**
     * field分析
     * @access protected
     * @param  Query     $query     查询对象
     * @param  mixed     $fields    字段名
     * @return string
     */
    protected function parseField(Query $query, $fields): string
    {
        if (is_array($fields)) {
            // 支持 'field1'=>'field2' 这样的字段别名定义
            $array = [];
            $tableName = $query->getTable();
            $tableFields = $query->getTableFields($tableName);
            foreach ($fields as $key => $field) {
                if ($field instanceof Raw) {
                    $sql = $field->getValue();
                    $bind = $field->getBind();
                    $sql = str_ireplace('as ', 'AS ', $sql);
                    $sql = $this->parseKey($query, $sql);
                    if(stripos($sql, 'AS') !== false){
                        $as_str = rtrim(strstr($sql, 'AS '));
                        list($as, $alias) = explode('AS ', $as_str);
                        $as = explode('AS ', $sql)[0];
                        // 支持-> json字段查询
                        if(strpos($sql, '->') !== false){
                            $newAs = DmQuery::parseJson($as, $tableFields);
                            $sql = str_ireplace(trim($as), $newAs, $sql);
                        }
                        $field = new Raw(str_ireplace($alias, "`{$alias}`", $sql), $bind);
                    }else{
                        if(strpos($sql, '->') !== false){
                            $sql = DmQuery::parseJson($sql, $tableFields);
                            $field = new Raw($sql, $bind);
                        }
                    }
                    $array[] = $this->parseRaw($query, $field);
                } elseif (!is_numeric($key)) {
                    $array[] = $this->parseKey($query, $key) . ' AS ' . $this->parseKey($query, $field, true);
                } else {
                    $array[] = $this->parseKey($query, $field);
                }
            }

            $fieldsStr = implode(',', $array);
        } else {
            $fieldsStr = '*';
        }

        return $fieldsStr;
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
                $key = DmQuery::quoteFields($key, $tableFields);
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
                $result[$item] = 'json_set(' . $item . ', \'$.' . $name . '\', ' . $this->parseDataBind($query, $key . '->' . $name, $val, $bind) . ')';
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
        if(stripos($sql, '`') === false){
            $sql = DmQuery::quoteFields($sql, $tableFields);
        }

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
        $all_tables = $this->getConnection()->getTables();
        foreach ((array) $tables as $key => $table) {
            $is_alias = !in_array($table, $all_tables) || stripos($table, ')') !== false;
            $old_table = $table;
            if($is_alias && !isset($options['alias'][$key])){
                $alias = DmQuery::parseAliasFromTable($old_table);
                $table = $old_table = Db::raw(str_ireplace(' '.$alias, ' '.$this->parseKey($query, $alias), $old_table));
            }
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
     * union分析
     * @access protected
     * @param  Query $query 查询对象
     * @param  array $union
     * @return string
     */
    protected function parseUnion(Query $query, array $union): string
    {
        if (empty($union)) {
            return '';
        }

        $type = $union['type'];
        unset($union['type']);

        $sql = [];
        foreach ($union as $u) {
            if ($u instanceof Closure) {
                $sql[] = $type . ' ' . $this->parseClosure($query, $u);
            } elseif (is_string($u)) {
                $u = $this->parseRaw($query, new Raw($u));
                $u = str_ireplace('from', 'FROM', $u);
                $table = explode(' ', strstr($u, 'FROM'))[1];
                $u = str_replace($table, $this->parseTable($query, $table), $u);
                $sql[] = $type . ' ( ' . $u . ' )';
            }
        }

        return ' ' . implode(' ', $sql);
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
