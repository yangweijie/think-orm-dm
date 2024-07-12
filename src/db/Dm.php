<?php

namespace think\db;

use Exception;
use think\helper\Str;

class Dm extends Query
{

    public static function procedureName(ConnectionInterface $connection, $name) :string
    {
        $database = $connection->getConfig('database.connections.dm.database');
        if(strpos($name, '.') === false){
            return "`{$database}`.`{$name}`";
        }
        return $name;
    }

    public static function parseAliasFromTable($table): string
    {
        $str = strstr( $table, ')');
        $str = str_ireplace(['as', ')'], ['', ''], $str);
        $str = trim($str);
        return $str;
    }


    public static function parseJson($sql, $fields) :string
    {
        $sql = self::quoteFields($sql, $fields);
        if(strpos($sql, '->>') === false){
            list($field, $jsonPath) = explode('->', $sql);
            $express = 'JSON_EXTRACT(%s, %s)';
        }else{
            list($field, $jsonPath) = explode('->>', $sql);
            $express = 'JSON_UNQUOTE(JSON_EXTRACT(%s, %s))';
        }
        $jsonPath = trim($jsonPath);
        if(strpos($jsonPath, "'") === false){
            $jsonPath = "'{$jsonPath}'";
        }
        return sprintf($express, $field, $jsonPath);
    }

    /**
     * 将sql中的数据库字段加``
     * @param string $sql
     * @param array $fields
     * @return string
     */
    public static function quoteFields(string $sql, array $fields) :string
    {
        if(strpos($sql, '`') !== false){
            return $sql;
        }
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
     * 设置是否REPLACE
     * @access public
     * @param bool $replace 是否使用REPLACE写入数据
     * @return $this
     */
    public function replace(bool $replace = true): static
    {
        $this->options['replace'] = false;
        return $this;
    }

    /**
     * 设置DUPLICATE
     * @access public
     * @param array|string|Raw $duplicate DUPLICATE信息
     * @return $this
     * @throws Exception
     */
    public function duplicate($duplicate)
    {
        throw new Exception("不支持 on duplicate key");
    }

    public function getDatabase(){
        static $connect;
        if(!$connect){
            $connect = $this->getConnection();
        }
        return $connect->getConfig()['database'];
    }

    /**
     * 获取Join表名及别名 支持
     * ['prefix_table或者子查询'=>'alias'] 'table alias'
     * @access protected
     * @param array|string|Raw $join  JION表名
     * @param string           $alias 别名
     */
    protected function getJoinTable($join, &$alias = null)
    {
        $database = $this->getDatabase();
        if (is_array($join)) {
            $table = $join;
            $alias = array_shift($join);
            return $table;
        } elseif ($join instanceof Raw) {
            return $join;
        }

        $join = trim($join);

        if (false !== strpos($join, '(')) {
            // 使用子查询
            $table = $join;
        } else {
            // 使用别名
            if (strpos($join, ' ')) {
                // 使用别名
                [$table, $alias] = explode(' ', $join);
            } else {
                $table = $join;
                if (false === strpos($join, '.')) {
                    $alias = $join;
                }
            }

            if ($this->prefix && false === strpos($table, '.') && 0 !== strpos($table, $this->prefix)) {
                $table = $this->getTable($table);
            }
        }
        if(strpos('.', $table) === false){
            $table = "`{$database}`.{$table}";
        }
        if (!empty($alias) && $table != $alias) {
            $table = [$table => $alias];
        }
        return $table;
    }

    /**
     * 查询SQL组装 join
     * @access public
     * @param mixed  $join      关联的表名
     * @param mixed  $condition 条件
     * @param string $type      JOIN类型
     * @param array  $bind      参数绑定
     * @return $this
     */
    public function join($join, string $condition = null, string $type = 'INNER', array $bind = [])
    {
        $table = $this->getJoinTable($join);
        $alias = $this->getOptions('alias')?:[];

        if(is_string($table)){
            $alias[$table] = $table;
        }else{
            $alias = array_merge($alias, $table);
        }
        if (!empty($bind) && $condition) {
            $this->bindParams($condition, $bind);
        }

        $alias_values = array_values($alias);
        $condition = str_ireplace(array_map(function($item){
            return "{$item}.";
        }, $alias_values), array_map(function($item){
            return "`{$item}`.";
        }, $alias_values), $condition);
        $condition = str_ireplace('=`', ' = `', $condition);
        $this->options['join'][] = [$table, strtoupper($type), $condition];

        return $this;
    }

    /**
     * 得到当前或者指定名称的数据表
     * @access public
     * @param string $name 不含前缀的数据表名字
     * @return mixed
     */
    public function getTable(string $name = ''): mixed
    {
        if (empty($name) && isset($this->options['table'])) {
            return $this->options['table'];
        }

        $name = $name ?: $this->name;
        return $this->prefix . (ctype_upper($name)?$name:Str::snake($name));
    }
}