<?php

namespace think\db\builder;

use think\db\Builder;
use think\db\BaseQuery as Query;
use think\db\Dm as DmQuery;
use think\db\Raw;
use Exception;

/**
 * Pgsql数据库驱动.
 */
class Dm extends Builder
{
    use \think\db\concern\TableFieldInfo;

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
     * 字段和表名处理.
     *
     * @param Query $query  查询对象
     * @param string|int|Raw $key    字段名
     * @param bool  $strict 严格检测
     *
     * @return string
     */
    public function parseKey(Query $query, string|int|Raw $key, bool $strict = false): string
    {
        if (is_int($key)) {
            return (string) $key;
        } elseif ($key instanceof Raw) {
            return $this->parseRaw($query, $key);
        }

        $key = trim($key);

        if (str_contains($key, '.')) {
            [$table, $key] = explode('.', $key, 2);

            $alias = $query->getOptions('alias');

            if ('__TABLE__' == $table) {
                $table = $query->getOptions('table');
                $table = is_array($table) ? array_shift($table) : $table;
            }

            if (isset($alias[$table])) {
                $table = $alias[$table];
            }

            if ('*' != $key) {
                $key = str_replace('`', '', $key);
                if(!preg_match('/[,\'\"\*\(\).\s]/', $key)){
                    $key = '"' . $key . '"';
                }else{
                    $tableName = $query->getTable();
                    $tableFields = $query->getConnection()->getTableFields($tableName);
                    if(!is_array($tableFields)){
                        $tableFields = implode(' ', $tableFields);
                    }
                    $key = DmQuery::quoteFields($key, $tableFields);

                }
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
        $tableFields = $this->getTableFields($tableName);
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
            if($is_alias && !isset($options['alias'][$old_table])){
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
