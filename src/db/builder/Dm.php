<?php

namespace think\db\builder;

use think\db\Builder;
use think\db\Expression;
use think\db\Query;
use think\db\Raw;

/**
 * 达梦数据库驱动
 */
class Dm extends Builder
{

    public function getDatabase(){
        static $connect;
        if(!$connect){
            $connect = $this->getConnection();
        }
        return $connect->getConfig()['database'];
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
        $database = $this->getDatabase();
        $item    = [];
        $options = $query->getOptions();
        foreach ((array) $tables as $key => $table) {
            if ($table instanceof Raw) {
                $item[] = $this->parseRaw($query, "`$database`.$table");
            } elseif (!is_numeric($key)) {
                $item[] = $this->parseKey($query, $key) . ' ' . $this->parseKey($query, "`$database`.$table");
            } elseif (isset($options['alias'][$table])) {
                $item[] = $this->parseKey($query, "`$database`.$table") . ' ' . $this->parseKey($query, $options['alias'][$table]);
            } else {
                $item[] = $this->parseKey($query, "`$database`.$table");
            }
        }

        return implode(',', $item);
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
        if ('*' != $key && !preg_match('/[,\'\"\*\(\).\s]/', $key)) {
            $key = '"' . $key . '"';
        }

        if (isset($table)) {
            $key = $table . '.' . $key;
        }

        return $key;
    }

    /**
     * 随机排序
     * @access protected
     * @param  Query     $query        查询对象
     * @return string
     */
    protected function parseRand(Query $query) :string
    {
        return 'RAND()';
    }
}
